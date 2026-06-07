#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <stdexcept>

#include <spdlog/spdlog.h>

#include "JournalBase.h"
#include "JournalReader.h"
#include "JournalWriter.h"

namespace fs = std::filesystem;
using namespace revoq::journal;

namespace {

struct Trade {
  std::int64_t timestamp;
  std::uint32_t symbol_id;
  double price;
};

struct Bbo {
  std::int64_t timestamp;
  std::uint32_t bid_size;
  std::uint32_t ask_size;
};

class TempDir {
public:
  explicit TempDir(const char *name) {
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = fs::temp_directory_path() / (std::string("revoq-example-") + name +
                                         "-" + std::to_string(stamp));
    fs::create_directories(path_);
  }

  ~TempDir() {
    std::error_code ignored;
    fs::remove_all(path_, ignored);
  }

  [[nodiscard]] std::string string() const { return path_.string(); }

private:
  fs::path path_;
};

template <typename T>
void publish(JournalWriter<> &writer, std::int64_t timestamp, MsgType msg_type,
             const T &message) {
  auto &slot = writer.openData<T>(timestamp, msg_type);
  slot = message;
  writer.closeData<T>();
}

} // namespace

int main() {
  try {
    spdlog::set_level(spdlog::level::warn);

    TempDir dir("static-dispatch");
    auto locator = std::make_shared<Locator>(dir.string());
    auto location = Location::make(Mode::LIVE, Category::EXECUTION, "examples",
                                   "static-dispatch", locator);
    constexpr std::uint32_t dest_id = 0;
    constexpr int messages = 1000;

    {
      JournalWriter<> writer(
          location, dest_id,
          JournalWriterOptions{.prefault = false, .background_threads = false});
      for (int i = 0; i < messages; ++i) {
        const auto ts = static_cast<std::int64_t>(i + 1);
        if (i % 3 == 0) {
          publish(writer, ts, MsgType::Trade,
                  Trade{ts, 101, 100.0 + static_cast<double>(i)});
        } else if (i % 3 == 1) {
          publish(writer, ts, MsgType::BBO,
                  Bbo{ts, static_cast<std::uint32_t>(i),
                      static_cast<std::uint32_t>(i + 1)});
        } else {
          writer.mark(ts, MsgType::BookReset);
        }
      }
    }

    JournalReader reader(
        JournalReaderOptions{.prefault = false, .background_threads = false});
    reader.join(location, dest_id, 0);

    int handled = 0;
    std::uint64_t checksum = 0;
    auto dispatcher = [&](JournalReader::FrameView current) {
      switch (static_cast<MsgType>(current.msg_type)) {
      case MsgType::Trade: {
        const auto &trade = current->data<Trade>();
        checksum ^= static_cast<std::uint64_t>(trade.symbol_id) +
                    static_cast<std::uint64_t>(trade.price);
        ++handled;
        break;
      }
      case MsgType::BBO: {
        const auto &bbo = current->data<Bbo>();
        checksum ^= bbo.bid_size + bbo.ask_size;
        ++handled;
        break;
      }
      case MsgType::BookReset:
        ++handled;
        break;
      case MsgType::PageEnd:
        break;
      default:
        throw std::runtime_error("unexpected message type");
      }
    };

    while (reader.nextStatic(dispatcher)) {
    }

    if (handled != messages)
      throw std::runtime_error("static dispatch verification failed");

    std::cout << "ok static_dispatch handled=" << handled
              << " checksum=" << checksum << '\n';
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "error: " << e.what() << '\n';
    return 1;
  }
}
