#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include "JournalBase.h"
#include "JournalFrame.h"
#include "JournalPage.h"
#include "PageWarmer.h"
#include "TimeUtils.h"

#include <spdlog/spdlog.h>

#include "portable_intrinsics.h"

namespace revoq::journal {

struct SingleWriterPolicy {
  static constexpr bool serialize = false;
};
struct MultiWriterPolicy {
  static constexpr bool serialize = true;
};

inline constexpr size_t DEFAULT_JOURNAL_WRITER_RETAINED_PAGES = 10;
inline constexpr size_t kWriterPageWarmerRingSize = 16;

struct JournalWriterOptions {
  bool prefault = true;
  size_t warmup_pages = 0;
  int warmer_cpu_core = -1;
  bool background_threads = true;
  size_t retained_page_limit = DEFAULT_JOURNAL_WRITER_RETAINED_PAGES;
};

template <bool Serialize> struct WriteLockGuard;

template <> struct WriteLockGuard<false> {
  explicit WriteLockGuard(std::atomic<bool> &) noexcept {}
};

template <> struct WriteLockGuard<true> {
  explicit WriteLockGuard(std::atomic<bool> &flag) noexcept : flag_(flag) {
    int backoff = 1;
    while (flag_.exchange(true, std::memory_order_acquire)) {
      for (int i = 0; i < backoff; ++i)
        PORTABLE_PAUSE();
      backoff = std::min(backoff * 2, 64);
    }
  }
  ~WriteLockGuard() { flag_.store(false, std::memory_order_release); }

private:
  std::atomic<bool> &flag_;
};

template <typename WriterPolicy = SingleWriterPolicy> class JournalWriter {
public:
  JournalWriter(LocationPtr location, uint32_t dest_id,
                JournalWriterOptions options = {})
      : location_(std::move(location)), dest_id_(dest_id),
        prefault_(options.prefault),
        background_threads_(options.background_threads),
        max_retained_pages_(options.retained_page_limit),
        page_size_(findPageSize(location_, dest_id_)), writing_(false),
        current_sequence_(0), current_page_id_(0) {
    // 0 means no written page found
    const auto last_page_id = findLastWrittenPage();
    if (last_page_id > 0) {
      current_page_id_.store(last_page_id, std::memory_order_relaxed);
      uint64_t last_seq = findLastSequenceNumber(last_page_id);
      current_sequence_.store(last_seq + 1, std::memory_order_relaxed);
      SPDLOG_INFO("Resuming journal: last page = {}, starting sequence = {}",
                  last_page_id, last_seq + 1);
    } else {
      SPDLOG_INFO("No existing pages found, starting fresh from page 1");
    }
    loadInitialPage();
    if (options.warmup_pages > 0) {
      if (options.warmup_pages > kWriterPageWarmerRingSize) {
        throw std::invalid_argument(
            "warmup_pages exceeds writer page warmer ring size");
      }
      page_warmer_ = std::make_shared<PageWarmer<kWriterPageWarmerRingSize>>(
          location_, dest_id_, page_size_, true, options.warmer_cpu_core,
          options.warmup_pages);
      page_warmer_->start();
      page_warmer_->updateCurrentPage(
          current_page_id_.load(std::memory_order_relaxed));
      SPDLOG_DEBUG("JournalWriter created with PageWarmer ({} pages ahead)",
                   options.warmup_pages);
    }
    // Start background retire thread (§3.2)
    if (background_threads_) {
      retire_running_.store(true, std::memory_order_relaxed);
      retire_thread_ = std::thread(&JournalWriter::retireLoop, this);
    }
  }

  // openData/closeData form a split lock: acquire in open, release in close.
  // RAII can't span function calls, so serialize path uses if constexpr
  // manually.
  template <typename T>
  T &openData(int64_t timestamp, MsgType msg_type,
              uint16_t flags = FRAME_NORMAL) {
    if constexpr (WriterPolicy::serialize) {
      int backoff = 1;
      while (writing_.exchange(true, std::memory_order_acquire)) {
        for (int i = 0; i < backoff; ++i)
          PORTABLE_PAUSE();
        backoff = std::min(backoff * 2, 64);
      }
    }
    if (!current_page_) {
      rotatePage();
    }
    open_frame_header_ = openFrame(timestamp, msg_type, sizeof(T), flags);
    return *reinterpret_cast<T *>(open_frame_header_ + 1);
  }

  template <typename T> void closeData() {
    closeData<T>([](FrameHeader &) noexcept {});
  }

  // Advanced metadata finalization hook. The callback runs immediately before
  // the release-store that publishes length, while the frame is still private
  // to the writer. The default closeData<T>() compiles this hook away.
  template <typename T, typename BeforePublish>
  void closeData(BeforePublish &&before_publish) {
    if (!open_frame_header_) [[unlikely]] {
      if constexpr (WriterPolicy::serialize)
        writing_.store(false, std::memory_order_release);
      throw std::runtime_error("No frame opened");
    }
    try {
      closeFrame(open_frame_header_, sizeof(T),
                 std::forward<BeforePublish>(before_publish));
      open_frame_header_ = nullptr;
    } catch (...) {
      if constexpr (WriterPolicy::serialize)
        writing_.store(false, std::memory_order_release);
      throw;
    }
    if constexpr (WriterPolicy::serialize)
      writing_.store(false, std::memory_order_release);
  }

  ~JournalWriter() {
    if (page_warmer_) {
      page_warmer_->stop();
      page_warmer_.reset();
    }
    current_page_.reset();

    if (background_threads_) {
      std::lock_guard<std::mutex> lock(retire_mutex_);
      while (!retired_pages_.empty()) {
        retire_queue_.push_back(std::move(retired_pages_.front()));
        retired_pages_.pop_front();
      }
      retire_running_.store(false, std::memory_order_relaxed);
    }
    retire_cv_.notify_all();
    if (retire_thread_.joinable())
      retire_thread_.join();
    retired_pages_.clear(); // inline cleanup when no retire thread
  }

  void mark(int64_t timestamp, MsgType msg_type) {
    WriteLockGuard<WriterPolicy::serialize> lock(writing_);
    if (!current_page_) {
      rotatePage();
    }
    auto *frame_hdr = openFrame(timestamp, msg_type, 0, FRAME_NORMAL);
    closeFrame(frame_hdr, 0);
  }

  // Variable-size raw write for language bindings (Python, C API).
  // One memcpy from the caller's buffer into the mmap slot; no hot-path impact.
  void writeBytes(int64_t timestamp, MsgType msg_type, const void *data,
                  std::size_t size) {
    WriteLockGuard<WriterPolicy::serialize> lock(writing_);
    if (!current_page_)
      rotatePage();
    auto *hdr = openFrame(timestamp, msg_type, size, FRAME_NORMAL);
    if (size > 0)
      std::memcpy(hdr + 1, data, size);
    closeFrame(hdr, size);
  }

  [[nodiscard]] uint64_t getCurrentSequence() const {
    return current_sequence_.load(std::memory_order_acquire);
  }
  [[nodiscard]] int64_t getCurrentPageId() const {
    return current_page_id_.load(std::memory_order_acquire);
  }

private:
  const LocationPtr location_;
  const uint32_t dest_id_;
  const bool prefault_;
  const bool background_threads_;
  const size_t max_retained_pages_;
  const uint32_t page_size_;
  std::atomic<bool> writing_;
  std::atomic<uint64_t> current_sequence_;
  std::atomic<int> current_page_id_;

  PagePtr current_page_{nullptr};
  FrameHeader *open_frame_header_{nullptr};
  uintptr_t current_frame_address_{};

  PageWarmerPtr page_warmer_;
  std::deque<PagePtr> retired_pages_;

  std::mutex retire_mutex_;
  std::condition_variable retire_cv_;
  std::deque<PagePtr> retire_queue_;
  std::atomic<bool> retire_running_{false};
  std::thread retire_thread_;

  uint64_t nextSeq() {
    if constexpr (WriterPolicy::serialize) {
      return current_sequence_.fetch_add(1, std::memory_order_relaxed);
    } else {
      uint64_t s = current_sequence_.load(std::memory_order_relaxed);
      current_sequence_.store(s + 1, std::memory_order_relaxed);
      return s;
    }
  }

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
        page.reset(); // ~Page() runs here — msync/munmap off the writer thread
        lock.lock();
      }
      if (!retire_running_.load(std::memory_order_relaxed))
        break;
    }
  }

  int findLastWrittenPage() {
    const auto &page_ids = location_->locator->listPageId(location_, dest_id_);
    if (page_ids.empty()) {
      return 0;
    }
    const auto highest_page = page_ids.back();
    SPDLOG_INFO("Highest existing page: {}", highest_page);
    for (int page_id = highest_page; page_id > 0; --page_id) {
      try {
        PagePtr page =
            Page::load(location_, dest_id_, page_id, false, prefault_);
        uintptr_t last_frame_pos = page->header_->last_frame_position;
        SPDLOG_INFO("Page {} last_frame_position: {}", page_id, last_frame_pos);
        if (last_frame_pos > sizeof(PageHeader)) {
          SPDLOG_INFO("Found last page with data: {}", page_id);
          return page_id;
        }
      } catch (const std::exception &e) {
        SPDLOG_ERROR("Failed to check page {}: {}", page_id, e.what());
      }
    }
    SPDLOG_WARN("No pages with data found, starting from 0");
    return 0;
  }

  void loadInitialPage() {
    int new_page_id =
        current_page_id_.fetch_add(1, std::memory_order_relaxed) + 1;
    current_page_ =
        Page::load(location_, dest_id_, new_page_id, true, prefault_);
    current_frame_address_ = current_page_->firstFrameAddress();
    SPDLOG_DEBUG("Loaded initial page {} at sequence {}", new_page_id,
                 current_sequence_.load());
  }

  uint64_t findLastSequenceNumber(int page_id) {
    if (page_id < 0)
      return 0;
    try {
      PagePtr page = Page::load(location_, dest_id_, page_id, false, prefault_);
      uintptr_t addr = page->firstFrameAddress();
      uintptr_t end_addr = page->address() + page_size_;
      uint64_t last_seq = 0;
      bool found = false;
      while (addr + sizeof(FrameHeader) <= end_addr) {
        auto *hdr = reinterpret_cast<const FrameHeader *>(addr);
        uint32_t len = hdr->length.load(std::memory_order_acquire);
        if (len == 0 || len < sizeof(FrameHeader) || len > page_size_) {
          break;
        }
        uint32_t stride = framePhysicalLength(len);
        if (addr + stride > end_addr) {
          break;
        }
        uint64_t seq = hdr->sequence;
        assert(!found || seq > last_seq);
        last_seq = seq;
        found = true;
        if (static_cast<MsgType>(hdr->msg_type) == MsgType::PageEnd) {
          break;
        }
        addr += stride;
      }
      SPDLOG_DEBUG("Last sequence in page {}: {}", page_id,
                   found ? last_seq : 0);
      return found ? last_seq : 0;
    } catch (const std::exception &e) {
      SPDLOG_ERROR("Failed to read last sequence from page {}: {}", page_id,
                   e.what());
      return 0;
    }
  }

  void rotatePage() {
    int new_page_id =
        current_page_id_.fetch_add(1, std::memory_order_relaxed) + 1;
    PagePtr next_page;
    if (page_warmer_) {
      size_t warmed_size = 0;
      std::uintptr_t warmed_addr =
          page_warmer_->getWarmedPage(new_page_id, warmed_size);
      if (warmed_addr != 0) {
        next_page = Page::loadFromAddress(location_, dest_id_, new_page_id,
                                          warmed_addr, prefault_);
        SPDLOG_TRACE("Using pre-warmed page {}", new_page_id);
      } else {
        next_page =
            Page::load(location_, dest_id_, new_page_id, true, prefault_);
        SPDLOG_TRACE("Created page {} (not pre-warmed)", new_page_id);
      }
      page_warmer_->updateCurrentPage(new_page_id);
    } else {
      SPDLOG_DEBUG("No warmer - load page normally");
      next_page = Page::load(location_, dest_id_, new_page_id, true, prefault_);
    }

    if (current_page_) {
      uintptr_t page_base = current_page_->address();
      uintptr_t border = page_base + page_size_ - FRAME_ALIGNMENT;
      SPDLOG_TRACE("rotatePage: current_frame_address_={:#x}, page_base={:#x}, "
                   "border={:#x}",
                   current_frame_address_, page_base, border);
      assert(current_frame_address_ <= border);
      try {
        auto *end_hdr = std::launder(
            reinterpret_cast<FrameHeader *>(current_frame_address_));
        new (end_hdr) FrameHeader{};
        end_hdr->gen_time = Timestamp::now().nsec();
        end_hdr->event_time = 0;
        end_hdr->msg_type = static_cast<uint16_t>(MsgType::PageEnd);
        end_hdr->flags = FRAME_NORMAL;
        end_hdr->sequence = nextSeq();
        end_hdr->length.store(static_cast<uint32_t>(sizeof(FrameHeader)),
                              std::memory_order_release);
        uintptr_t offset = current_frame_address_ - current_page_->address();
        current_page_->setLastFramePosition(offset);
      } catch (...) {
        SPDLOG_WARN("rotatePage(): failed to write PageEnd sentinel");
      }

      retired_pages_.push_back(std::move(current_page_));

      // Hand overflowing pages to the retire thread (§3.2)
      while (retired_pages_.size() > max_retained_pages_) {
        PagePtr old_page = std::move(retired_pages_.front());
        retired_pages_.pop_front();
        if (background_threads_) {
          {
            std::lock_guard<std::mutex> lock(retire_mutex_);
            retire_queue_.push_back(std::move(old_page));
          }
          retire_cv_.notify_one();
        }
        // if !background_threads_: old_page destroyed inline here
      }
    }

    current_page_ = std::move(next_page);
    current_frame_address_ = current_page_->firstFrameAddress();
    SPDLOG_DEBUG("Rotated to page {} at sequence {}", new_page_id,
                 current_sequence_.load());
  }

  FrameHeader *openFrame(int64_t timestamp, MsgType msg_type,
                         size_t payload_size, uint16_t flags = FRAME_NORMAL) {
    size_t total_size = sizeof(FrameHeader) + payload_size;
    size_t stride = framePhysicalLength(static_cast<uint32_t>(total_size));
    uintptr_t page_addr = current_page_->address();
    uintptr_t border = page_addr + page_size_ - FRAME_ALIGNMENT;
    if (current_frame_address_ + stride > border) [[unlikely]] {
      rotatePage();
    }
    auto *raw = reinterpret_cast<FrameHeader *>(current_frame_address_);
    auto *frame_header = std::launder(new (raw) FrameHeader{});
    frame_header->gen_time = timestamp;
    frame_header->event_time = 0;
    frame_header->msg_type = static_cast<uint16_t>(msg_type);
    frame_header->flags = flags;
    frame_header->sequence = nextSeq();
    frame_header->length.store(0, std::memory_order_relaxed);
    return frame_header;
  }

  void closeFrame(FrameHeader *frame_header, size_t payload_size) {
    closeFrame(frame_header, payload_size, [](FrameHeader &) noexcept {});
  }

  template <typename BeforePublish>
  void closeFrame(FrameHeader *frame_header, size_t payload_size,
                  BeforePublish &&before_publish) {
    size_t total_size = sizeof(FrameHeader) + payload_size;
    std::forward<BeforePublish>(before_publish)(*frame_header);
    frame_header->length.store(static_cast<uint32_t>(total_size),
                               std::memory_order_release);
    auto frame_addr = reinterpret_cast<uintptr_t>(frame_header);
    uintptr_t offset = frame_addr - current_page_->address();
    current_page_->setLastFramePosition(offset);
    current_frame_address_ =
        frame_addr + framePhysicalLength(static_cast<uint32_t>(total_size));
  }

  static uint16_t calculateCRC16(const uint8_t *data, size_t len) {
    uint16_t crc = 0xFFFF;
    for (size_t i = 0; i < len; ++i) {
      crc ^= data[i];
      for (int j = 0; j < 8; ++j) {
        if (crc & 1) {
          crc = (crc >> 1) ^ 0xA001;
        } else {
          crc >>= 1;
        }
      }
    }
    return crc;
  }
};

using JournalWriterPtr = std::shared_ptr<JournalWriter<>>;

} // namespace revoq::journal
