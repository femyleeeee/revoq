#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <pthread.h>
#include <sched.h>
#include <thread>
#include <unistd.h>

#include "CpuAffinity.h"
#include "JournalBase.h"
#include "JournalFrame.h"
#include "JournalPage.h"
#include "MmapBuffer.h"
#include "RealtimePriority.h"
#include "TimeUtils.h"

#include <spdlog/spdlog.h>

namespace revoq::journal {

// Unified file-backed page warmer for JournalWriter
// Pre-warms actual journal file pages to eliminate page faults
template <size_t RingSize = 16> class PageWarmer {
public:
  static_assert((RingSize & (RingSize - 1)) == 0,
                "RingSize must be power of 2");

  struct PageSlot {
    std::atomic<int64_t> page_id{-1};  // -1 = empty, >=0 = valid
    std::uintptr_t address{0};         // File-backed mmap address
    std::size_t size{0};               // Page size
    std::atomic<uint64_t> sequence{0}; // For ordering/versioning
    bool is_owned{false};
    char padding[64 - sizeof(std::atomic<int64_t>) - sizeof(std::uintptr_t) -
                 sizeof(std::size_t) - sizeof(std::atomic<uint64_t>) -
                 sizeof(bool)]{};
  } __attribute__((aligned(64)));

  PageWarmer(LocationPtr location, uint32_t dest_id, size_t page_size,
             bool is_writer, int warmer_cpu_core = -1,
             size_t warmup_distance = 1)
      : location_(std::move(location)), dest_id_(dest_id),
        page_size_(page_size), is_writer_(is_writer),
        warmer_cpu_core_(warmer_cpu_core),
        warmup_distance_(std::min(warmup_distance, RingSize / 2)),
        running_(false), write_index_(0), read_index_(0), current_page_(-1) {
    SPDLOG_INFO("PageWarmer created for {}/{:08x} ({}) - ring size: {}",
                location_->uname, dest_id_, is_writer_ ? "WRITER" : "READER",
                RingSize);
  }

  ~PageWarmer() {
    stop();
    releaseAllPages();
  }

  void start() {
    if (running_.exchange(true, std::memory_order_acquire)) {
      return;
    }
    warmer_thread_ = std::thread(&PageWarmer::warmupLoop, this);
    SPDLOG_INFO("PageWarmer started");
  }

  void stop() {
    if (!running_.exchange(false, std::memory_order_release)) {
      return;
    }
    if (warmer_thread_.joinable()) {
      warmer_thread_.join();
    }
    SPDLOG_INFO("PageWarmer stopped");
  }

  // LOCK-FREE HOT PATH: Get pre-warmed file-backed page
  // Returns file-backed mmap address if available, 0 if not ready
  __attribute__((always_inline)) inline std::uintptr_t
  getWarmedPage(int64_t page_id, std::size_t &size_out) {
    const uint64_t start_read = read_index_.load(std::memory_order_acquire);
    for (size_t i = 0; i < RingSize; ++i) {
      const size_t idx = (start_read + i) & (RingSize - 1);
      PageSlot &slot = ring_[idx];
      int64_t slot_page_id = slot.page_id.load(std::memory_order_acquire);
      if (slot_page_id == page_id) {
        // Found it! Try to claim it atomically
        if (slot.page_id.compare_exchange_strong(slot_page_id, -1,
                                                 std::memory_order_acq_rel)) {
          size_out = slot.size;
          std::uintptr_t addr = slot.address;
          // Mark as not owned by warmer anymore (caller owns it now)
          // the full chain: warmer allocates -> writer claims ->
          // Page owns -> retire thread releases
          slot.is_owned = false;
          // Move read index forward
          read_index_.store((idx + 1) & (RingSize - 1),
                            std::memory_order_release);
          return addr;
        }
      }
    }
    return 0;
  }

  // Update current page (called by writer)
  __attribute__((always_inline)) inline void
  updateCurrentPage(int64_t page_id) {
    current_page_.store(page_id, std::memory_order_release);
  }

private:
  const LocationPtr location_;
  const uint32_t dest_id_;
  const size_t page_size_;
  const bool is_writer_;
  const int warmer_cpu_core_;
  const size_t warmup_distance_;

  std::atomic<bool> running_;

  alignas(64) std::atomic<uint64_t> write_index_{};
  alignas(64) std::atomic<uint64_t> read_index_{};
  alignas(64) std::atomic<int64_t> current_page_{};

  alignas(64) std::array<PageSlot, RingSize> ring_;

  std::thread warmer_thread_;

  void releaseAllPages() {
    for (size_t i = 0; i < RingSize; ++i) {
      PageSlot &slot = ring_[i];
      if (slot.is_owned && slot.address != 0) {
        os::MmapBuffer::releaseMmapBuffer(slot.address, slot.size, false,
                                          os::MsyncMode::NONE);
        slot.address = 0;
        slot.is_owned = false;
      }
    }
  }

  void warmupLoop() {
    if (warmer_cpu_core_ >= 0) {
      if (!system::CpuAffinity::pinToCore(warmer_cpu_core_))
        SPDLOG_ERROR("warmer: failed to pin to core {}", warmer_cpu_core_);
      if (!system::RealtimePriority::setRealtimePriority(50))
        SPDLOG_ERROR("warmer: failed to set RT priority");
      else
        SPDLOG_DEBUG("Warmer thread pinned to CPU {} with RT priority {}",
                     warmer_cpu_core_, 50);
    }
    SPDLOG_DEBUG("Warmer loop started");
    while (running_.load(std::memory_order_acquire)) {
      int64_t current_page = current_page_.load(std::memory_order_acquire);
      if (current_page >= 0) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    uint64_t local_seq = 1;
    while (running_.load(std::memory_order_acquire)) {
      int64_t current_page = current_page_.load(std::memory_order_acquire);
      if (current_page < 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        continue;
      }
      for (size_t i = 1; i <= warmup_distance_; ++i) {
        int64_t next_page_id = current_page + static_cast<int64_t>(i);
        if (isPageInRing(next_page_id)) {
          continue;
        }
        uint64_t write_idx = write_index_.load(std::memory_order_acquire);
        size_t slot_idx = write_idx & (RingSize - 1);
        PageSlot &slot = ring_[slot_idx];
        int64_t expected = -1;
        // sentinel states: -1 = empty, -2 = being warmed, >=0 = ready
        if (!slot.page_id.compare_exchange_strong(expected, -2,
                                                  std::memory_order_acq_rel)) {
          continue;
        }
        try {
          warmPageIntoSlot(next_page_id, slot, local_seq++);
          write_index_.fetch_add(1, std::memory_order_release);
        } catch (const std::exception &e) {
          SPDLOG_ERROR("Failed to warm page {}: {}", next_page_id, e.what());
          slot.page_id.store(-1, std::memory_order_release);
        }
      }
      cleanupOldPages(current_page);
      std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
    SPDLOG_DEBUG("Warmer loop exited");
  }

  [[nodiscard]] bool isPageInRing(int64_t page_id) const {
    for (size_t i = 0; i < RingSize; ++i) {
      if (ring_[i].page_id.load(std::memory_order_acquire) == page_id) {
        return true;
      }
    }
    return false;
  }

  void warmPageIntoSlot(int64_t page_id, PageSlot &slot, uint64_t seq) {
    std::string page_file =
        Page::getPagePath(location_, dest_id_, static_cast<int>(page_id))
            .string();
    assert(!page_file.empty());
    const auto t0 = Timestamp::now();
    const bool file_existed = std::filesystem::exists(page_file);
    // Load ACTUAL FILE-BACKED mmap (not anonymous!)
    // This is the key difference - we're warming the real journal file
    std::uintptr_t addr = os::MmapBuffer::loadMmapBuffer(
        page_file, page_size_,
        is_writer_, // true for writer, false for reader
        true        // prefault=true: pre-fault all pages NOW!
    );

    if (is_writer_ && !file_existed) {
      auto *header = reinterpret_cast<PageHeader *>(addr);
      header->version = JOURNAL_PAGE_VERSION;
      header->page_header_length = sizeof(PageHeader);
      header->page_size = static_cast<uint32_t>(page_size_);
      header->frame_header_length = sizeof(FrameHeader);
      header->last_frame_position = sizeof(PageHeader);
      // zero out rest of page
      std::memset(reinterpret_cast<void *>(addr + sizeof(PageHeader)), 0,
                  page_size_ - sizeof(PageHeader));
      // ensure writes are visible
      std::atomic_thread_fence(std::memory_order_release);
    }
    slot.address = addr;
    slot.size = page_size_;
    slot.is_owned = true; // Warmer owns this mmap
    slot.sequence.store(seq, std::memory_order_release);
    slot.page_id.store(page_id, std::memory_order_release);
    const auto t1 = Timestamp::now();
    SPDLOG_TRACE("Warmed file-backed page {} in {} ns", page_id,
                 (t1 - t0).nsec());
  }

  void cleanupOldPages(int64_t current_page) {
    // this is for the case when writer advances warmer many pages
    const int64_t cleanup_threshold =
        current_page - static_cast<int64_t>(RingSize);
    for (size_t i = 0; i < RingSize; ++i) {
      PageSlot &slot = ring_[i];
      int64_t slot_page_id = slot.page_id.load(std::memory_order_acquire);
      if (slot_page_id >= 0 && slot_page_id < cleanup_threshold) {
        if (slot.page_id.compare_exchange_strong(slot_page_id, -1,
                                                 std::memory_order_acq_rel)) {
          // Release the file-backed mmap
          if (slot.is_owned && slot.address != 0) {
            os::MmapBuffer::releaseMmapBuffer(slot.address, slot.size, false,
                                              os::MsyncMode::NONE);
            slot.address = 0;
            slot.is_owned = false;
          }
          SPDLOG_TRACE("Cleaned up old page {} from ring", slot_page_id);
        }
      }
    }
  }
};

using PageWarmerPtr = std::shared_ptr<PageWarmer<>>;

} // namespace revoq::journal
