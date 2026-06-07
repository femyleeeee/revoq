#pragma once

#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>
#include <queue>
#include <unordered_set>
#include <utility>
#include <vector>

#include <spdlog/spdlog.h>

#include "Journal.h"

namespace revoq::journal {

struct JournalReaderOptions {
  bool prefault;
  bool background_threads = true;
  size_t retained_page_limit = 0;
  int warmer_timeout_ms = 5000;
  int background_worker_cpu = -1;
};

// Comparator for min-heap: journal with earliest timestamp has highest priority
struct JournalComparator {
  bool operator()(const std::pair<int64_t, size_t> &a,
                  const std::pair<int64_t, size_t> &b) const {
    // Min-heap: smaller timestamp = higher priority
    return a.first > b.first;
  }
};

class JournalReader {
public:
  // The frame wrapper is mutable. This is not a snapshot and must not be kept
  // after the reader advances or its joined journals change.
  struct FrameView {
    Frame *frame{nullptr};
    std::uint32_t frame_length{0};
    std::int32_t msg_type{0};

    [[nodiscard]] explicit operator bool() const noexcept {
      return frame != nullptr;
    }
    [[nodiscard]] Frame *operator->() const noexcept { return frame; }
    [[nodiscard]] Frame &operator*() const noexcept { return *frame; }
  };

  explicit JournalReader(JournalReaderOptions options)
      : prefault_(options.prefault),
        background_threads_(options.background_threads),
        retained_page_limit_(options.retained_page_limit),
        warmer_timeout_ms_(options.warmer_timeout_ms),
        background_worker_cpu_(options.background_worker_cpu),
        current_journal_idx_(static_cast<size_t>(-1)), needs_rebuild_(true),
        use_heap_(true) {}

  ~JournalReader() { journals_.clear(); }

  void join(const journal::LocationPtr &location, std::uint32_t dest_id,
            std::int64_t from_time) {
    // Fast duplicate check using set
    uint64_t key = makeKey(location->uid, dest_id);
    if (joined_keys_.find(key) != joined_keys_.end()) {
      SPDLOG_WARN("reader cannot join journal {}/[{:08x}] more than once",
                  location->uname, dest_id);
      return;
    }
    auto journal = std::make_shared<Journal>(
        location, dest_id,
        JournalOptions{
            .is_writing = false,
            .prefault = prefault_,
            .background_threads = background_threads_,
            .retained_page_limit = retained_page_limit_,
            .warmer_timeout_ms = warmer_timeout_ms_,
            .background_worker_cpu = background_worker_cpu_,
        });
    journal->seekToTime(from_time);
    journals_.push_back(journal);
    joined_keys_.insert(key);
    refreshSingleJournal();
    needs_rebuild_ = true;
  }

  void disjoin(std::uint32_t location_uid) {
    // Remove from journals vector
    auto old_size = journals_.size();
    journals_.erase(std::remove_if(journals_.begin(), journals_.end(),
                                   [location_uid](const JournalPtr &j) {
                                     return j->location_->uid == location_uid;
                                   }),
                    journals_.end());
    if (journals_.size() != old_size) {
      // Rebuild keys set
      joined_keys_.clear();
      for (const auto &j : journals_) {
        joined_keys_.insert(makeKey(j->location_->uid, j->dest_id_));
      }
      current_journal_idx_ = static_cast<size_t>(-1);
      refreshSingleJournal();
      needs_rebuild_ = true;
    }
  }

  int getCurrentJournalPageId() const {
    if (current_journal_idx_ != static_cast<size_t>(-1) &&
        current_journal_idx_ < journals_.size()) {
      return journals_[current_journal_idx_]->current_page_->page_id_;
    }
    return -1;
  }

  std::uintptr_t getCurrentJournalFrameOffset() const {
    if (current_journal_idx_ != static_cast<size_t>(-1) &&
        current_journal_idx_ < journals_.size()) {
      const auto &journal = journals_[current_journal_idx_];
      if (journal->current_page_ && journal->frame_)
        return journal->frame_->address() - journal->current_page_->address();
    }
    return 0;
  }

  std::uint32_t getCurrentJournalPageSize() const {
    if (current_journal_idx_ != static_cast<size_t>(-1) &&
        current_journal_idx_ < journals_.size()) {
      const auto &page = journals_[current_journal_idx_]->current_page_;
      if (page)
        return page->getPageSize();
    }
    return 0;
  }

  [[nodiscard]] FrameView tryCurrentFrame() {
    if (single_frame_)
      return tryCurrentSingleFrame();
    if (!isDataAvailable())
      return {};
    Frame *frame = currentFrameRaw();
    const std::uint32_t frame_length = frame->getFrameLength();
    const std::int32_t msg_type = frame->getMsgType();
    return frame->hasData(frame_length, msg_type)
               ? FrameView{frame, frame_length, msg_type}
               : FrameView{};
  }

  bool isDataAvailable() {
    // Fast path: single journal, already positioned
    if (single_frame_)
      return single_frame_->hasData();
    // Rebuild heap/find minimum if needed
    if (needs_rebuild_) {
      rebuildHeap();
    }
    Frame *frame = currentFrameRaw();
    if (!frame && !journals_.empty()) {
      needs_rebuild_ = true;
    }
    return frame && frame->hasData();
  }

  void seekToTime(std::int64_t timestamp) {
    for (const auto &journal : journals_) {
      journal->seekToTime(timestamp);
    }
    needs_rebuild_ = true;
  }

  void next() {
    if (current_journal_idx_ == static_cast<size_t>(-1) ||
        current_journal_idx_ >= journals_.size()) {
      return;
    }
    // Advance current journal
    if (single_journal_) {
      single_journal_->next();
      return;
    }
    journals_[current_journal_idx_]->next();
    if (use_heap_ && journals_.size() > 3) {
      // Heap-based approach for many journals
      updateHeap();
    } else {
      // Linear scan for few journals (simpler, less overhead)
      needs_rebuild_ = true;
    }
  }

  void advance(FrameView current) {
    if (single_journal_) {
      single_journal_->next(current.frame_length,
                            static_cast<MsgType>(current.msg_type));
      return;
    }
    next();
  }

  // Get number of active journals
  size_t getJournalCount() const { return journals_.size(); }

  // Check if a specific journal is joined
  bool isJoined(std::uint32_t location_uid, std::uint32_t dest_id) const {
    uint64_t key = makeKey(location_uid, dest_id);
    return joined_keys_.find(key) != joined_keys_.end();
  }

  // Enable/disable heap optimization (useful for testing)
  void setUseHeap(bool use_heap) {
    use_heap_ = use_heap;
    needs_rebuild_ = true;
  }

  // The dispatcher is a concrete caller-owned strategy, typically a direct
  // switch over current.msg_type. It is intentionally separate from runtime
  // handler registration so the compiler can inline the complete hot path.
  // It receives every committed frame, including internal PageEnd sentinels.
  template <typename Dispatcher> bool nextStatic(Dispatcher &dispatcher) {
    const FrameView current =
        single_frame_ ? tryCurrentSingleFrame() : tryCurrentFrame();
    if (!current)
      return false;
    dispatcher(current);
    advance(current);
    return true;
  }

private:
  [[nodiscard]] Frame *currentFrameRaw() const noexcept {
    if (current_journal_idx_ != static_cast<size_t>(-1) &&
        current_journal_idx_ < journals_.size()) {
      return journals_[current_journal_idx_]->getCurrentFrameRaw();
    }
    return nullptr;
  }

  [[nodiscard]] FrameView tryCurrentSingleFrame() const noexcept {
    Frame *frame = single_frame_;
    const std::uint32_t frame_length = frame->getFrameLength();
    const std::int32_t msg_type = frame->getMsgType();
    return frame->hasData(frame_length, msg_type)
               ? FrameView{frame, frame_length, msg_type}
               : FrameView{};
  }

  void refreshSingleJournal() noexcept {
    single_journal_ = journals_.size() == 1 ? journals_.front().get() : nullptr;
    single_frame_ =
        single_journal_ ? single_journal_->getCurrentFrameRaw() : nullptr;
    if (single_journal_)
      current_journal_idx_ = 0;
  }

  void rebuildHeap() {
    if (journals_.empty()) {
      current_journal_idx_ = static_cast<size_t>(-1);
      needs_rebuild_ = false;
      return;
    }
    if (use_heap_ && journals_.size() > 3) {
      // Build min-heap of (timestamp, journal_index) pairs
      std::vector<std::pair<int64_t, size_t>> temp;
      temp.reserve(journals_.size());
      for (size_t i = 0; i < journals_.size(); ++i) {
        const auto *frame = journals_[i]->getCurrentFrameRaw();
        if (frame && frame->hasData()) {
          int64_t gen_time = frame->getGenTime();
          temp.emplace_back(gen_time, i);
        }
      }
      if (temp.empty()) {
        current_journal_idx_ = static_cast<size_t>(-1);
      } else {
        // Build heap and extract minimum
        std::make_heap(temp.begin(), temp.end(), JournalComparator{});
        current_journal_idx_ = temp.front().second;
        // Store heap for incremental updates
        heap_ = std::priority_queue<std::pair<int64_t, size_t>,
                                    std::vector<std::pair<int64_t, size_t>>,
                                    JournalComparator>(JournalComparator{},
                                                       std::move(temp));
      }
    } else {
      // Linear scan for few journals
      findMinimumLinear();
    }
    needs_rebuild_ = false;
  }

  void findMinimumLinear() {
    int64_t min_time = std::numeric_limits<int64_t>::max();
    size_t min_index = static_cast<size_t>(-1);
    for (size_t i = 0; i < journals_.size(); ++i) {
      const auto *frame = journals_[i]->getCurrentFrameRaw();
      if (frame && frame->hasData()) {
        int64_t gen_time = frame->getGenTime();
        if (gen_time < min_time) {
          min_time = gen_time;
          min_index = i;
        }
      }
    }
    current_journal_idx_ = min_index;
  }

  void updateHeap() {
    if (heap_.empty()) {
      needs_rebuild_ = true;
      return;
    }
    // Remove old entry for current journal because we just consumed the
    // current journal frame
    heap_.pop();
    // Check if current journal has more data
    const auto *frame = journals_[current_journal_idx_]->getCurrentFrameRaw();
    if (frame && frame->hasData()) {
      int64_t gen_time = frame->getGenTime();
      heap_.emplace(gen_time, current_journal_idx_);
    }
    // Get new minimum
    if (heap_.empty()) {
      current_journal_idx_ = static_cast<size_t>(-1);
      needs_rebuild_ = true;
    } else {
      current_journal_idx_ = heap_.top().second;
    }
  }

  // Create unique key for location+dest pair
  static uint64_t makeKey(std::uint32_t location_uid, std::uint32_t dest_id) {
    return (static_cast<uint64_t>(location_uid) << 32) | dest_id;
  }

  const bool prefault_;
  const bool background_threads_;
  const size_t retained_page_limit_;
  const int warmer_timeout_ms_;
  const int background_worker_cpu_;
  size_t current_journal_idx_; // Index into journals_ vector
  bool needs_rebuild_;         // Flag to rebuild heap/rescan
  bool use_heap_;              // Use heap optimization for many journals

  std::vector<JournalPtr> journals_;
  Journal *single_journal_{nullptr};
  Frame *single_frame_{nullptr};
  std::unordered_set<uint64_t> joined_keys_; // Fast duplicate detection

  // Min-heap for efficient multi-journal merging
  std::priority_queue<std::pair<int64_t, size_t>, // (timestamp, journal_index)
                      std::vector<std::pair<int64_t, size_t>>,
                      JournalComparator>
      heap_;
};

using JournalReaderPtr = std::shared_ptr<JournalReader>;

} // namespace revoq::journal
