#pragma once

// Portable CPU intrinsics for x86 and ARM

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) ||             \
    defined(_M_IX86)
// ============ x86/x64 Platform ============
#include <immintrin.h>

// Spin-wait hint
#define PORTABLE_PAUSE() _mm_pause()

// Memory fences
#define PORTABLE_LFENCE() _mm_lfence() // Load fence
#define PORTABLE_SFENCE() _mm_sfence() // Store fence
#define PORTABLE_MFENCE() _mm_mfence() // Full fence

// Prefetch
#define PORTABLE_PREFETCH(addr) _mm_prefetch((const char *)(addr), _MM_HINT_T0)

#elif defined(__aarch64__) || defined(__arm64__) || defined(_M_ARM64)
// ============ ARM64 Platform ============

// Spin-wait hint
static inline void PORTABLE_PAUSE() {
  __asm__ __volatile__("yield" ::: "memory");
}

// Memory fences (using DMB - Data Memory Barrier)
static inline void PORTABLE_LFENCE() {
  __asm__ __volatile__("dmb ishld" ::: "memory"); // Load barrier
}

static inline void PORTABLE_SFENCE() {
  __asm__ __volatile__("dmb ishst" ::: "memory"); // Store barrier
}

static inline void PORTABLE_MFENCE() {
  __asm__ __volatile__("dmb ish" ::: "memory"); // Full barrier
}

// Prefetch
#define PORTABLE_PREFETCH(addr) __builtin_prefetch((addr), 0, 3)

#else
// ============ Fallback for other platforms ============
#warning "Unknown architecture - using compiler barriers"

#define PORTABLE_PAUSE()                                                       \
  do {                                                                         \
  } while (0)
#define PORTABLE_LFENCE() __asm__ __volatile__("" ::: "memory")
#define PORTABLE_SFENCE() __asm__ __volatile__("" ::: "memory")
#define PORTABLE_MFENCE() __asm__ __volatile__("" ::: "memory")
#define PORTABLE_PREFETCH(addr) __builtin_prefetch((addr), 0, 3)

#endif