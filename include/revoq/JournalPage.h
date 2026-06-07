#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>

#include <fmt/format.h>

#include "JournalBase.h"
#include "JournalError.h"
#include "JournalFrame.h"
#include "MmapBuffer.h"

namespace revoq::journal {

// Page size constants: should be multiple of 2MB
constexpr std::uint32_t PAGE_SIZE_MARKETDATA = 128 * 1024 * 1024; // 128 MB
constexpr std::uint32_t PAGE_SIZE_EXECUTION = 4 * 1024 * 1024;    // 4 MB
constexpr std::uint32_t PAGE_SIZE_DEFAULT = 2 * 1024 * 1024;      // 2 MB
// Optimized page size lookup with inline hint
inline static std::uint32_t findPageSize(const LocationPtr &location,
                                         std::uint32_t dest_id) {
  // Fast path: marketdata and research use large pages
  if (location->category == Category::MARKETDATA ||
      location->category == Category::RESEARCHDATA) {
    return PAGE_SIZE_MARKETDATA;
  }
  // Execution with specific dest_id uses medium pages
  if (location->category == Category::EXECUTION && dest_id != 0) {
    return PAGE_SIZE_EXECUTION;
  }
  // Default case
  return PAGE_SIZE_DEFAULT;
}
#pragma pack(push, 1)
struct alignas(64) PageHeader {
  std::uint32_t version{};
  std::uint32_t page_header_length{};
  std::uint32_t page_size{};
  std::uint32_t frame_header_length{};
  std::uint64_t last_frame_position{};
  char padding[40]{};
};
#pragma pack(pop)
static_assert(sizeof(PageHeader) == 64,
              "PageHeader must be exactly one cache line");
static_assert(alignof(PageHeader) == 64,
              "PageHeader must be cache-line aligned");

class Page;
using PagePtr = std::shared_ptr<Page>;

class Page {
public:
  ~Page() {
    if (owned_by_page_ && header_) {
      const auto msync_mode = (location_->category == Category::MARKETDATA ||
                               location_->category == Category::RESEARCHDATA)
                                  ? revoq::os::MsyncMode::NONE
                                  : revoq::os::MsyncMode::ASYNC;
      if (revoq::os::MmapBuffer::releaseMmapBuffer(address(), size_, prefault_,
                                                   msync_mode)) {
        SPDLOG_TRACE("released page {}/{:08x}.{}.journal", location_->uname,
                     dest_id_, page_id_);
      } else {
        SPDLOG_ERROR("cannot release page {}/{:08x}.{}.journal",
                     location_->uname, dest_id_, page_id_);
      }
    }
  }

  [[nodiscard]] std::uint32_t getPageSize() const { return header_->page_size; }
  [[nodiscard]] journal::LocationPtr getLocation() const { return location_; }
  [[nodiscard]] std::uint32_t getDestId() const { return dest_id_; }
  [[nodiscard]] int getPageId() const { return page_id_; }

  [[nodiscard]] std::int64_t beginTime() const {
    auto *frame = reinterpret_cast<journal::FrameHeader *>(firstFrameAddress());
    // Use acquire to ensure we see the frame properly
    if (frame->length.load(std::memory_order_acquire) == 0) {
      return 0;
    }
    return frame->gen_time;
  }

  [[nodiscard]] std::int64_t endTime() const {
    auto *frame = reinterpret_cast<journal::FrameHeader *>(lastFrameAddress());
    // Use acquire to ensure we see the frame properly
    if (frame->length.load(std::memory_order_acquire) == 0) {
      return 0;
    }
    return frame->gen_time;
  }

  [[nodiscard]] std::uintptr_t address() const {
    return reinterpret_cast<std::uintptr_t>(header_);
  }

  [[nodiscard]] std::uintptr_t addressBorder() const {
    return address() + header_->page_size - journal::FRAME_ALIGNMENT;
  }

  [[nodiscard]] std::uintptr_t firstFrameAddress() const {
    return address() + header_->page_header_length;
  }

  [[nodiscard]] std::uintptr_t lastFrameAddress() const {
    return address() + header_->last_frame_position;
  }

  [[nodiscard]] bool isFull() const {
    auto *last_frame =
        reinterpret_cast<journal::FrameHeader *>(lastFrameAddress());
    // Use acquire to synchronize with the writer's release
    uint32_t frame_len = last_frame->length.load(std::memory_order_acquire);
    return lastFrameAddress() + journal::framePhysicalLength(frame_len) >
           addressBorder();
  }

  static PagePtr load(const journal::LocationPtr &location,
                      std::uint32_t dest_id, int page_id, bool is_writing,
                      bool prefault) {
    std::uint32_t page_size = findPageSize(location, dest_id);
    auto path = getPagePath(location, dest_id, page_id);
    std::uintptr_t address = 0;
    try {
      address = os::MmapBuffer::loadMmapBuffer(path.string(), page_size,
                                               is_writing, prefault);
    } catch (const std::exception &e) {
      throw JournalError(
          fmt::format("Failed to mmap page {}: {}", path.string(), e.what()));
    }

    if (address == 0) {
      throw JournalError(fmt::format(
          "MmapBuffer returned null address for page {}", path.string()));
    }

    auto header = reinterpret_cast<PageHeader *>(address);
    bool is_new_page = (header->last_frame_position == 0);
    if (is_new_page) {
      // Use memset for zeroing (compiler may optimize to efficient instruction)
      std::memset(header, 0, sizeof(PageHeader));
      // Initialize header fields
      header->version = JOURNAL_PAGE_VERSION;
      header->page_header_length = sizeof(PageHeader);
      header->page_size = page_size;
      header->frame_header_length = sizeof(FrameHeader);
      header->last_frame_position =
          sizeof(PageHeader); // Points to first frame position
      // Ensure header is visible before page is used (write barrier)
      std::atomic_thread_fence(std::memory_order_release);
      SPDLOG_DEBUG("Initialized new page {}/{:08x}.{}.journal (size: {} MB)",
                   location->uname, dest_id, page_id,
                   page_size / (1024 * 1024));
    }

    // Validation - check version
    if (header->version != JOURNAL_PAGE_VERSION) {
      throw JournalError(
          fmt::format("Version mismatch for page {}, required {}, found {}",
                      path.string(), JOURNAL_PAGE_VERSION, header->version));
    }

    // Validation - check header length
    if (header->page_header_length != sizeof(PageHeader)) {
      throw JournalError(fmt::format(
          "Header length mismatch for page {}, required {}, found {}",
          path.string(), sizeof(PageHeader), header->page_header_length));
    }

    // Validation - check page size
    if (header->page_size != page_size) {
      throw JournalError(
          fmt::format("Page size mismatch for page {}, required {}, found {}",
                      path.string(), page_size, header->page_size));
    }

    // Validation - check last_frame_position is within bounds
    if (header->last_frame_position >= page_size) {
      SPDLOG_WARN(
          "Invalid last_frame_position {} for page {} (size {}), resetting",
          header->last_frame_position, path.string(), page_size);
      throw JournalError(fmt::format(
          "Invalid last_frame_position {} for page {} (size {}), resetting",
          header->last_frame_position, path.string(), page_size));
      // header->last_frame_position = sizeof(PageHeader);
    }

    SPDLOG_TRACE("Loaded page {}/{:08x}.{}.journal ({})", location->uname,
                 dest_id, page_id, is_new_page ? "new" : "existing");

    // Create and return Page object (owned_by_page=true)
    return std::shared_ptr<Page>(new Page(location, dest_id, page_id, page_size,
                                          prefault, address, true));
  }

  // Load from pre-warmed address (avoids MmapBuffer call)
  static PagePtr loadFromAddress(const journal::LocationPtr &location,
                                 std::uint32_t dest_id, int page_id,
                                 std::uintptr_t address, bool prefault) {
    if (address == 0) {
      throw JournalError("Cannot load page from null address");
    }
    auto header = reinterpret_cast<PageHeader *>(address);
    std::uint32_t page_size = header->page_size;
    // Validation (header should already be initialized by PageWarmer)
    if (header->version != JOURNAL_PAGE_VERSION) {
      throw JournalError(fmt::format(
          "version mismatch for pre-warmed page {} (expected {}, got {})",
          page_id, JOURNAL_PAGE_VERSION, header->version));
    }
    if (header->page_header_length != sizeof(PageHeader)) {
      throw JournalError(fmt::format(
          "header length mismatch for pre-warmed page {} (expected {}, got {})",
          page_id, sizeof(PageHeader), header->page_header_length));
    }
    SPDLOG_TRACE("Loaded pre-warmed page {} from address 0x{:x}", page_id,
                 address);
    // owned_by_page=true means this Page object owns the memory
    return std::shared_ptr<Page>(new Page(location, dest_id, page_id, page_size,
                                          prefault, address, true));
  }

  static std::filesystem::path getPagePath(const LocationPtr &location,
                                           std::uint32_t dest_id, int id) {
    const auto dir =
        location->locator->makeLayoutDir(location, Layout::JOURNAL);
    return dir / fmt::format("{:08x}.{}.journal", dest_id, id);
  }

  static int findPageId(const journal::LocationPtr &location,
                        std::uint32_t dest_id, std::int64_t time) {
    std::vector<int> page_ids =
        location->locator->listPageId(location, dest_id);
    if (page_ids.empty()) {
      return 1;
    }
    if (time == 0) {
      return page_ids.front();
    }

    // Binary search for the right page (assumes pages are time-ordered)
    int left = 0;
    int right = static_cast<int>(page_ids.size()) - 1;
    int best_page = page_ids.front();

    while (left <= right) {
      int mid = left + (right - left) / 2;
      auto page = load(location, dest_id, page_ids[mid], false, false);
      auto page_begin_time = page->beginTime();
      // Skip empty pages (beginTime() returns 0 for empty pages)
      if (page_begin_time == 0) {
        // Empty page - search earlier pages
        right = mid - 1;
        continue;
      }
      if (page_begin_time <= time) {
        best_page = page_ids[mid];
        left = mid + 1; // Look for later pages
      } else {
        right = mid - 1; // Look for earlier pages
      }
    }

    return best_page;
  }

private:
  const LocationPtr location_;
  const std::uint32_t dest_id_;
  const int page_id_;
  const bool prefault_;
  const std::size_t size_;
  const PageHeader *header_;
  const bool owned_by_page_; // Track if we own the memory

  Page(const LocationPtr &location, std::uint32_t dest_id, int page_id,
       std::size_t size, bool prefault, std::uintptr_t address,
       bool owned = true)
      : location_(location), dest_id_(dest_id), page_id_(page_id),
        prefault_(prefault), size_(size),
        header_(reinterpret_cast<PageHeader *>(address)),
        owned_by_page_(owned) {}

  // it is actually an offset from the beginning add
  void setLastFramePosition(std::uint64_t position) {
    const_cast<PageHeader *>(header_)->last_frame_position = position;
  }

  friend class Journal;
  template <typename> friend class JournalWriter;
  friend class JournalReader;
};
} // namespace revoq::journal
