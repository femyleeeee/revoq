// HashUtils.h
#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>

#include "MurMurHash3.h"

namespace revoq {
class HashUtils {
public:
  // Optimized for both compile-time AND runtime
  static constexpr uint32_t fnv1a_32(std::string_view str) noexcept {
    uint32_t hash = 2166136261u;
    for (char c : str) {
      hash ^= static_cast<uint8_t>(c);
      hash *= 16777619u;
    }
    return hash;
  }

  // Raw byte array hashing
  static inline uint32_t hash32(const unsigned char *key, int32_t length,
                                uint32_t seed = 0) {
    uint32_t h;
    MurmurHash3_x86_32(key, length, seed, &h);
    return h;
  }

  // Optimized string_view version (zero-copy, inline-friendly)
  static inline uint32_t hashStr32(std::string_view key, uint32_t seed = 0) {
    return hash32(reinterpret_cast<const unsigned char *>(key.data()),
                  static_cast<int32_t>(key.length()), seed);
  }

  // Optimized for C-strings with known length
  static inline uint32_t hashCStr32(const char *key, size_t length,
                                    uint32_t seed = 0) {
    return hash32(reinterpret_cast<const unsigned char *>(key),
                  static_cast<int32_t>(length), seed);
  }

  // Hash POD structs (useful for order IDs, prices, etc.)
  template <typename T>
  static inline uint32_t hashStruct32(const T &data, uint32_t seed = 0) {
    static_assert(std::is_trivially_copyable_v<T>,
                  "Type must be trivially copyable");
    return hash32(reinterpret_cast<const unsigned char *>(&data),
                  static_cast<int32_t>(sizeof(T)), seed);
  }
};
} // namespace revoq
