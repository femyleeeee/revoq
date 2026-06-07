#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace revoq {
namespace os {
class MmapError : public std::runtime_error {
public:
  explicit MmapError(const std::string &message)
      : std::runtime_error(message) {}
};

enum class MsyncMode { NONE, ASYNC, SYNC };

struct ReadOnlyMmapOptions {
  bool populate_read{false};
  bool advise_hugepage{false};
};

class MmapBuffer {
public:
  static std::uintptr_t loadMmapBuffer(const std::string &path,
                                       std::size_t size, bool is_writing,
                                       bool prefault);
  static std::uintptr_t
  loadExistingReadOnlyMmapBuffer(const std::string &path, std::size_t size,
                                 ReadOnlyMmapOptions options = {});
  static void prepareReadOnlyMmapBuffer(const std::string &path,
                                        std::uintptr_t address,
                                        std::size_t size,
                                        ReadOnlyMmapOptions options);
  static bool releaseMmapBuffer(std::uintptr_t address, std::size_t size,
                                bool prefault,
                                MsyncMode msync_mode = MsyncMode::SYNC);
};
} // namespace os
} // namespace revoq
