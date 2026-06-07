#pragma once

#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <thread>

#ifdef __linux__
#include <sched.h>
#include <sys/stat.h>
#endif

#include "JournalBase.h"
#include "JournalFrame.h"
#include "JournalPage.h"
#include "JournalPlatform.h"
#include "MmapBuffer.h"
#include "RealtimePriority.h"
#include "TimeUtils.h"

namespace revoq::journal {

struct JournalOptions {
  bool is_writing;
  bool prefault;
  bool background_threads = true;
  size_t retained_page_limit = 0;
  int warmer_timeout_ms = 5000;
  int background_worker_cpu = -1;
};

class Journal {
public:
  Journal(journal::LocationPtr location, std::uint32_t dest_id,
          JournalOptions options)
      : location_(std::move(location)), dest_id_(dest_id),
        is_writing_(options.is_writing), prefault_(options.prefault),
        background_threads_(options.background_threads),
        max_retained_pages_(options.retained_page_limit),
        warmer_timeout_ms_(options.warmer_timeout_ms),
        background_worker_cpu_(options.background_worker_cpu),
        page_size_(findPageSize(location_, dest_id_)),
        frame_(std::shared_ptr<Frame>(new Frame())), page_frame_nb_(0) {
    if (!background_threads_)
      return;

    retire_running_.store(true, std::memory_order_relaxed);
    retire_thread_ = std::thread(&Journal::retireLoop, this);

    if (!prefault_)
      warmer_thread_ = std::thread(&Journal::readerWarmerLoop, this);

#ifdef __linux__
    const bool retire_pinned = detail::pinThreadToBackgroundCpus(
        retire_thread_.native_handle(), background_worker_cpu_,
        "reader retire thread");
    assert(retire_pinned && "reader retire thread affinity pin failed");
    if (warmer_thread_.joinable()) {
      const bool warmer_pinned = detail::pinThreadToBackgroundCpus(
          warmer_thread_.native_handle(), background_worker_cpu_,
          "reader warmer thread");
      assert(warmer_pinned && "reader warmer thread affinity pin failed");
    }
#endif
  }

  ~Journal() {
    {
      std::lock_guard<std::mutex> lock(retire_mutex_);
      if (current_page_)
        retire_queue_.push_back(std::move(current_page_));
      // let's not stall the current thread.
      // let the retire thread own the actual munmap work
      while (!retired_pages_.empty()) {
        retire_queue_.push_back(std::move(retired_pages_.front()));
        retired_pages_.pop_front();
      }
      retire_running_.store(false, std::memory_order_relaxed);
    }
    retire_cv_.notify_all();
    {
      std::lock_guard<std::mutex> wl(warmer_mutex_);
      // retire_running_ already false; hold lock to prevent lost notify.
    }
    warmer_cv_.notify_all();
    if (retire_thread_.joinable())
      retire_thread_.join();
    if (warmer_thread_.joinable())
      warmer_thread_.join();
    releaseWarmedPage();
  }

  Frame *getCurrentFrameRaw() noexcept { return frame_.get(); }

  void next() {
    assert(current_page_);
    next(frame_->getFrameLength(), static_cast<MsgType>(frame_->getMsgType()));
  }

  void next(std::uint32_t frame_length, MsgType msg_type) {
    assert(current_page_);
    if (msg_type == MsgType::PageEnd) {
      loadNextPage();
    } else {
      frame_->moveToNext(frame_length);
      ++page_frame_nb_;
    }
  }

  void seekToTime(std::int64_t timestamp) {
    int page_id = Page::findPageId(location_, dest_id_, timestamp);
    loadPage(page_id);
    SPDLOG_TRACE(
        "seeking timestamp {} in page [{}] [beginTime({}) - endTime({})]",
        revoq::Timestamp(timestamp), page_id,
        revoq::Timestamp(current_page_->beginTime()),
        revoq::Timestamp(current_page_->endTime()));
    while (current_page_->isFull() && current_page_->endTime() < timestamp) {
      loadNextPage();
    }
    while (frame_->hasData() && frame_->getGenTime() < timestamp) {
      next();
    }
  }

private:
  const journal::LocationPtr location_;
  const std::uint32_t dest_id_;
  const bool is_writing_;
  const bool prefault_;
  const bool background_threads_;
  const size_t max_retained_pages_;
  const int warmer_timeout_ms_;
  const int background_worker_cpu_;
  const std::uint32_t page_size_;
  PagePtr current_page_;
  FramePtr frame_;
  int page_frame_nb_;

  std::deque<PagePtr> retired_pages_;

  std::mutex retire_mutex_;
  std::condition_variable retire_cv_;
  std::deque<PagePtr> retire_queue_;
  std::atomic<bool> retire_running_{false};
  std::thread retire_thread_;

  // Reader warmer state (all fields protected by warmer_mutex_ except
  // retire_running_ which is atomic).
  std::mutex warmer_mutex_;
  std::condition_variable warmer_cv_;
  int64_t warmer_next_page_{-1};
  bool warmer_pending_{false};
  std::uintptr_t warmer_addr_{0};
  int64_t warmer_ready_id_{-1};
  std::thread warmer_thread_;

  void retireLoop() {
    while (true) {
      std::unique_lock<std::mutex> lock(retire_mutex_);
      retire_cv_.wait(lock, [this] {
        return !retire_queue_.empty() ||
               !retire_running_.load(std::memory_order_relaxed);
      });
      while (!retire_queue_.empty()) {
        PagePtr page = std::move(retire_queue_.front());
        retire_queue_.pop_front();
        lock.unlock();
        page.reset();
        lock.lock();
      }
      if (!retire_running_.load(std::memory_order_relaxed))
        break;
    }
  }

  void readerWarmerLoop() {
    system::RealtimePriority::setRealtimePriority(
        20, system::SchedulerPolicy::FIFO);

    while (true) {
      int64_t page_to_warm;
      {
        std::unique_lock<std::mutex> lock(warmer_mutex_);
        warmer_cv_.wait(lock, [this] {
          // real work arrived or
          // shutdown was signalled
          return warmer_pending_ ||
                 !retire_running_.load(std::memory_order_relaxed);
        });
        if (!retire_running_.load(std::memory_order_relaxed))
          break;
        warmer_pending_ = false;
        page_to_warm = warmer_next_page_;
      }

      if (page_to_warm < 0)
        continue;

      auto path = Page::getPagePath(location_, dest_id_,
                                    static_cast<int>(page_to_warm));
      bool file_ready = false;
      for (int ms = 0; ms < warmer_timeout_ms_; ++ms) {
        {
          std::lock_guard<std::mutex> lk(warmer_mutex_);
          if (warmer_pending_ ||
              !retire_running_.load(std::memory_order_relaxed)) {
            goto next_signal;
          }
        }
        {
          struct stat st{};
          // stat == 0: the file exists and is reachable
          if (stat(path.c_str(), &st) == 0 &&
              st.st_size == static_cast<off_t>(page_size_)) {
            file_ready = true;
            break;
          }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      // this means the writer is not active enough to fallocate N+1 in timeout
      // seconds so the warmer essentially self-degrades gracefully under low
      // load
      if (!file_ready) {
        goto next_signal;
      }

      {
        // final double check just in case of the tiny window between stat()
        // and here. always good to do so before expensive work
        std::lock_guard<std::mutex> lk(warmer_mutex_);
        if (warmer_pending_ ||
            !retire_running_.load(std::memory_order_relaxed)) {
          goto next_signal;
        }
      }

      {
#ifdef __linux__
        std::uintptr_t addr = 0;
        try {
          addr = os::MmapBuffer::loadExistingReadOnlyMmapBuffer(path.string(),
                                                                page_size_);
        } catch (const os::MmapError &) {
          goto next_signal;
        }

        {
          const auto *hdr = reinterpret_cast<const PageHeader *>(addr);
          bool header_ok = false;
          for (int ms = 0; ms < warmer_timeout_ms_; ++ms) {
            {
              std::lock_guard<std::mutex> lk(warmer_mutex_);
              if (warmer_pending_ ||
                  !retire_running_.load(std::memory_order_relaxed)) {
                os::MmapBuffer::releaseMmapBuffer(addr, page_size_, false,
                                                  os::MsyncMode::NONE);
                goto next_signal;
              }
            }
            std::atomic_thread_fence(std::memory_order_acquire);
            if (hdr->version == JOURNAL_PAGE_VERSION &&
                hdr->page_header_length == sizeof(PageHeader)) {
              header_ok = true;
              break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
          }
          if (!header_ok) {
            os::MmapBuffer::releaseMmapBuffer(addr, page_size_, false,
                                              os::MsyncMode::NONE);
            goto next_signal;
          }
        }
        try {
          os::MmapBuffer::prepareReadOnlyMmapBuffer(
              path.string(), addr, page_size_,
              {.populate_read = true, .advise_hugepage = true});
        } catch (const os::MmapError &) {
          os::MmapBuffer::releaseMmapBuffer(addr, page_size_, false,
                                            os::MsyncMode::NONE);
          goto next_signal;
        }

        {
          std::lock_guard<std::mutex> lk(warmer_mutex_);
          if (warmer_pending_ ||
              !retire_running_.load(std::memory_order_relaxed)) {
            os::MmapBuffer::releaseMmapBuffer(addr, page_size_, false,
                                              os::MsyncMode::NONE);
            goto next_signal;
          }
          if (warmer_addr_ != 0)
            os::MmapBuffer::releaseMmapBuffer(warmer_addr_, page_size_, false,
                                              os::MsyncMode::NONE);
          warmer_addr_ = addr;
          warmer_ready_id_ = page_to_warm;
        }
#endif // __linux__
      }
    next_signal:;
    }
  }

  void releaseWarmedPage() {
    if (warmer_addr_ != 0) {
      os::MmapBuffer::releaseMmapBuffer(warmer_addr_, page_size_, false,
                                        os::MsyncMode::NONE);
      warmer_addr_ = 0;
      warmer_ready_id_ = -1;
    }
  }

  void loadPage(int page_id) {
    if (!current_page_ || current_page_->getPageId() != page_id) {
      PagePtr new_page;
      // Fast path: use the warmer's pre-populated mapping (zero page faults).
      if (!prefault_ && background_threads_) {
        std::lock_guard<std::mutex> lock(warmer_mutex_);
        if (warmer_ready_id_ == page_id && warmer_addr_ != 0) {
          try {
            // prefault=false: ~Page() calls only munmap (no munlock).
            new_page = Page::loadFromAddress(location_, dest_id_, page_id,
                                             warmer_addr_, false);
            warmer_addr_ = 0;
            warmer_ready_id_ = -1;
          } catch (...) {
            // Header not valid yet; release and fall through to lazy load.
            os::MmapBuffer::releaseMmapBuffer(warmer_addr_, page_size_, false,
                                              os::MsyncMode::NONE);
            warmer_addr_ = 0;
            warmer_ready_id_ = -1;
          }
        }
      }
      // Slow path: no-prefault mmap, soft page faults on first access per 4 KB.
      if (!new_page) {
        new_page =
            Page::load(location_, dest_id_, page_id, is_writing_, prefault_);
      }
      PagePtr old_page = std::move(current_page_);
      current_page_ = std::move(new_page);
      frame_->setAddress(current_page_->firstFrameAddress());
      page_frame_nb_ = 0;
      if (old_page) {
        if (background_threads_) {
          retired_pages_.push_back(std::move(old_page));
          while (retired_pages_.size() > max_retained_pages_) {
            PagePtr retired_page = std::move(retired_pages_.front());
            retired_pages_.pop_front();
            {
              std::lock_guard<std::mutex> lock(retire_mutex_);
              retire_queue_.push_back(std::move(retired_page));
            }
            retire_cv_.notify_one();
          }
        }
        // if !background_threads_: old_page destroyed inline here
      }
      // Signal the warmer to start pre-mapping the next page (reader only).
      if (!prefault_ && background_threads_) {
        std::lock_guard<std::mutex> lock(warmer_mutex_);

        warmer_next_page_ = page_id + 1;
        warmer_pending_ = true;
        warmer_cv_.notify_one();
      }
    }
  }

  void loadNextPage() { loadPage(current_page_->getPageId() + 1); }

  friend class JournalReader;
  template <typename> friend class JournalWriter;
};

using JournalPtr = std::shared_ptr<Journal>;

} // namespace revoq::journal
