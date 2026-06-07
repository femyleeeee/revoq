#pragma once

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <thread>
#include <type_traits>

#include "JournalBase.h"
#include "JournalFrame.h"
#include "JournalReader.h"
#include "JournalWriter.h"

using namespace revoq::journal;
namespace fs = std::filesystem;

namespace revoq::journal::test {
// Helper to create a temporary test directory
class TempTestDir {
public:
  TempTestDir() {
    base_path_ =
        fs::temp_directory_path() / "journal_test" /
        std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count());
    fs::create_directories(base_path_);
  }

  ~TempTestDir() {
    try {
      fs::remove_all(base_path_);
    } catch (...) {
      // Ignore cleanup errors in destructor
    }
  }

  fs::path path() const { return base_path_; }

  std::string string() const { return base_path_.string(); }

private:
  fs::path base_path_;
};

// Helper to create a test location
inline LocationPtr createTestLocation(const std::string &path) {
  const auto locator = std::make_shared<Locator>(path);
  auto loc = Location::make(Mode::LIVE, Category::EXECUTION, "test",
                            "writer_test_resume", locator);
  return loc;
}

template <typename Writer, typename T>
void publish(Writer &writer, int64_t timestamp, MsgType msg_type, const T &data,
             uint16_t flags = FRAME_NORMAL) {
  static_assert(std::is_trivially_copyable_v<T>,
                "journal test messages must be trivially copyable");
  auto &slot = writer.template openData<T>(timestamp, msg_type, flags);
  std::memcpy(&slot, &data, sizeof(T));
  writer.template closeData<T>();
}
} // namespace revoq::journal::test
