#pragma once

#include <cassert>
#include <filesystem>
#include <fstream>

#include <fmt/format.h>

namespace revoq {
class FileUtils {
public:
  static inline bool rmDir(const std::filesystem::path &dir) {
    return std::filesystem::remove_all(dir) != 0;
  }

  static inline bool mkDir(const std::filesystem::path &directory) {
    return std::filesystem::create_directory(directory);
  }

  // both filename and filedir are acceptable.
  static inline bool exists(const std::filesystem::path &path) {
    return std::filesystem::exists(path);
  }

  static inline bool touch(const std::filesystem::path &path) {
    std::ofstream f(path);
    return f.is_open();
  }

  static inline std::vector<std::filesystem::path>
  listDirs(const std::filesystem::path &path) {
    std::vector<std::filesystem::path> dirs;
    for (const auto &entry : std::filesystem::directory_iterator(path)) {
      if (entry.is_directory()) {
        dirs.emplace_back(entry.path());
      }
    }
    return dirs;
  }

  [[nodiscard]] static inline bool hasEnv(const std::string &name) {
    return std::getenv(name.c_str()) != nullptr;
  }

  [[nodiscard]] static inline std::string getEnv(const std::string &name) {
    const char *v = std::getenv(name.c_str());
    return !v ? "" : v;
  }
};
} // namespace revoq
