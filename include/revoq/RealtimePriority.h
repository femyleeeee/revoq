#pragma once

#include <pthread.h>
#include <sched.h>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/resource.h>
#include <thread>

#ifdef __linux__
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace revoq::system {

enum class SchedulerPolicy {
  NORMAL = SCHED_OTHER, // Default Linux scheduler
  FIFO = SCHED_FIFO,    // Real-time FIFO (First-In-First-Out)
  RR = SCHED_RR,        // Real-time Round-Robin
#ifdef __linux__
  BATCH = SCHED_BATCH, // For batch processing (non-interactive)
  IDLE = SCHED_IDLE    // Very low priority
#endif
};

class RealtimePriority {
public:
  // Set real-time priority for current thread
  // priority: 1-99 (higher = more important, 99 = highest)
  // Note: Requires CAP_SYS_NICE capability or root
  static bool
  setRealtimePriority(int priority,
                      SchedulerPolicy policy = SchedulerPolicy::FIFO) {
#ifdef __linux__
    if (priority < 1 || priority > 99) {
      SPDLOG_ERROR("Priority must be 1-99, got {}", priority);
      return false;
    }

    struct sched_param param;
    param.sched_priority = priority;

    pthread_t thread = pthread_self();
    int result =
        pthread_setschedparam(thread, static_cast<int>(policy), &param);

    if (result != 0) {
      SPDLOG_ERROR("Failed to set real-time priority: {} (are you root?)",
                   strerror(result));
      return false;
    }

    SPDLOG_INFO("Set thread to RT priority {} with policy {}", priority,
                policy == SchedulerPolicy::FIFO ? "FIFO" : "RR");
    return true;
#else
    SPDLOG_WARN("Real-time scheduling not supported on this platform");
    return false;
#endif
  }

  // Set to normal (non-real-time) priority
  static bool setNormalPriority() {
#ifdef __linux__
    struct sched_param param;
    param.sched_priority = 0;

    pthread_t thread = pthread_self();
    int result = pthread_setschedparam(thread, SCHED_OTHER, &param);

    if (result != 0) {
      SPDLOG_ERROR("Failed to set normal priority: {}", strerror(result));
      return false;
    }

    return true;
#else
    return true;
#endif
  }

  // Get current scheduling policy and priority
  static std::pair<SchedulerPolicy, int> getCurrentPriority() {
#ifdef __linux__
    struct sched_param param;
    int policy;

    pthread_t thread = pthread_self();
    if (pthread_getschedparam(thread, &policy, &param) == 0) {
      return {static_cast<SchedulerPolicy>(policy), param.sched_priority};
    }
#endif
    return {SchedulerPolicy::NORMAL, 0};
  }

  // Lock all current and future memory to prevent page faults.
  // MCL_FUTURE causes every future mmap to be bulk-faulted on creation —
  // do NOT use this before lazy journal page mmaps.
  static bool lockMemory() {
#ifdef __linux__
    if (mlockall(MCL_CURRENT | MCL_FUTURE) != 0) {
      SPDLOG_ERROR("Failed to lock memory: {} (are you root?)",
                   strerror(errno));
      return false;
    }
    SPDLOG_INFO("Memory locked (MCL_CURRENT | MCL_FUTURE)");
    return true;
#else
    SPDLOG_WARN("Memory locking not supported on this platform");
    return false;
#endif
  }

  // Lock only currently mapped pages. Use this for lazy-mmap consumers
  // (e.g. journal readers) where MCL_FUTURE would bulk-fault future mmaps
  // and defeat lazy page loading.
  static bool lockCurrentMemory() {
#ifdef __linux__
    if (mlockall(MCL_CURRENT) != 0) {
      SPDLOG_ERROR("Failed to lock current memory: {} (are you root?)",
                   strerror(errno));
      return false;
    }
    SPDLOG_INFO("Memory locked (MCL_CURRENT only)");
    return true;
#else
    SPDLOG_WARN("Memory locking not supported on this platform");
    return false;
#endif
  }

  // Unlock memory
  static bool unlockMemory() {
#ifdef __linux__
    if (munlockall() != 0) {
      SPDLOG_ERROR("Failed to unlock memory: {}", strerror(errno));
      return false;
    }
    return true;
#else
    return true;
#endif
  }

  // Pre-fault stack to avoid page faults during execution
  static void prefaultStack(size_t stack_size = 8 * 1024 * 1024) {
#ifdef __linux__
    // Allocate on heap instead of VLA
    std::vector<char> stack_buffer(stack_size);
    volatile char *ptr = stack_buffer.data();

    // Touch each page
    for (size_t i = 0; i < stack_size; i += 4096) {
      ptr[i] = 0;
    }
#else
    (void)stack_size; // Unused on non-Linux
                      // macOS handles stack differently, no need to prefault
#endif
  }

  // Check if running with real-time priority
  static bool isRealtime() {
    auto [policy, priority] = getCurrentPriority();
    return policy == SchedulerPolicy::FIFO || policy == SchedulerPolicy::RR;
  }

  // Get min/max priority for a policy
  static std::pair<int, int> getPriorityRange(SchedulerPolicy policy) {
#ifdef __linux__
    int min = sched_get_priority_min(static_cast<int>(policy));
    int max = sched_get_priority_max(static_cast<int>(policy));
    return {min, max};
#else
    return {0, 0};
#endif
  }
};

// RAII wrapper for real-time priority
class ScopedRealtimePriority {
public:
  explicit ScopedRealtimePriority(
      int priority, SchedulerPolicy policy = SchedulerPolicy::FIFO)
      : enabled_(false) {

    // Save original priority
    auto [orig_policy, orig_priority] = RealtimePriority::getCurrentPriority();
    original_policy_ = orig_policy;
    original_priority_ = orig_priority;

    // Set real-time priority
    enabled_ = RealtimePriority::setRealtimePriority(priority, policy);
  }

  ~ScopedRealtimePriority() {
    if (enabled_) {
      // Restore original priority
      if (original_policy_ == SchedulerPolicy::NORMAL) {
        RealtimePriority::setNormalPriority();
      } else {
        RealtimePriority::setRealtimePriority(original_priority_,
                                              original_policy_);
      }
    }
  }

  bool isEnabled() const { return enabled_; }

private:
  bool enabled_;
  SchedulerPolicy original_policy_;
  int original_priority_;
};

} // namespace revoq::system
