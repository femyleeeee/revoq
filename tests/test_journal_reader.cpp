#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <filesystem>
#include <thread>

#include "JournalReader.h"
#include "JournalWriter.h"
#include "TimeUtils.h"
#include "test_helpers.h"

using namespace revoq::journal;
using namespace revoq::journal::test;
namespace fs = std::filesystem;

// Test data
struct TestMessage {
  int64_t timestamp;
  uint32_t id;
  double value;
};

// Test fixture
class JournalReaderFixture {
public:
  JournalReaderFixture() {
    test_dir = fs::temp_directory_path() / "journal_reader_test";

    if (fs::exists(test_dir)) {
      fs::remove_all(test_dir);
    }

    fs::create_directories(test_dir);

    locator = std::make_shared<Locator>(test_dir.string());
    location = Location::make(Mode::LIVE, Category::EXECUTION, "test", "reader",
                              locator);
  }

  ~JournalReaderFixture() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    if (fs::exists(test_dir)) {
      try {
        fs::remove_all(test_dir);
      } catch (...) {
      }
    }
  }

  // Helper: Write test data
  void writeTestData(uint32_t dest_id, int num_messages,
                     int64_t start_time = 0) {
    auto writer = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = true});

    if (start_time == 0) {
      start_time = revoq::TimeUtils::now().nsec();
    }

    for (int i = 0; i < num_messages; ++i) {
      TestMessage msg{start_time + i * 1000, static_cast<uint32_t>(i),
                      100.0 + i};
      publish(*writer, msg.timestamp, MsgType::Trade, msg);
    }
  }

  fs::path test_dir;
  ILocatorPtr locator;
  LocationPtr location;
};

// ============================================================================
// Test 1: Reader Construction
// ============================================================================

TEST_CASE_METHOD(JournalReaderFixture, "JournalReader construction",
                 "[reader][construct]") {
  SECTION("Can create reader with prefault=true") {
    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
    REQUIRE(reader != nullptr);
  }

  SECTION("Can create reader with prefault=false") {
    auto reader = std::make_shared<JournalReader>(
        JournalReaderOptions{.prefault = false});
    REQUIRE(reader != nullptr);
  }

  SECTION("New reader has no journals") {
    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
    REQUIRE(reader->getJournalCount() == 0);
    REQUIRE_FALSE(reader->isDataAvailable());
  }
}

// ============================================================================
// Test 2: Join/Disjoin
// ============================================================================

TEST_CASE_METHOD(JournalReaderFixture, "JournalReader join/disjoin",
                 "[reader][join]") {
  writeTestData(0, 10);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  auto reader =
      std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});

  SECTION("Can join journal") {
    reader->join(location, 0, 0);
    REQUIRE(reader->getJournalCount() == 1);
    REQUIRE(reader->isJoined(location->uid, 0));
  }

  SECTION("Cannot join same journal twice") {
    reader->join(location, 0, 0);
    reader->join(location, 0, 0); // Should warn but not crash
    REQUIRE(reader->getJournalCount() == 1);
  }

  SECTION("Can join multiple journals") {
    writeTestData(1, 10);
    writeTestData(2, 10);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    reader->join(location, 0, 0);
    reader->join(location, 1, 0);
    reader->join(location, 2, 0);

    REQUIRE(reader->getJournalCount() == 3);
  }

  SECTION("Can disjoin journal") {
    reader->join(location, 0, 0);
    REQUIRE(reader->getJournalCount() == 1);

    reader->disjoin(location->uid);
    REQUIRE(reader->getJournalCount() == 0);
    REQUIRE_FALSE(reader->isJoined(location->uid, 0));
  }

  SECTION("Disjoin non-existent journal is safe") {
    reader->join(location, 0, 0);
    reader->disjoin(999);                    // Non-existent uid
    REQUIRE(reader->getJournalCount() == 1); // Should still have journal
  }
}

// ============================================================================
// Test 3: Basic Reading
// ============================================================================

TEST_CASE_METHOD(JournalReaderFixture, "JournalReader basic reading",
                 "[reader][read]") {
  const int NUM_MESSAGES = 100;
  writeTestData(0, NUM_MESSAGES);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  auto reader =
      std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
  reader->join(location, 0, 0);

  SECTION("Can read all messages") {
    int count = 0;
    while (reader->isDataAvailable()) {
      auto frame = reader->tryCurrentFrame();
      REQUIRE(frame);
      REQUIRE(frame->hasData());

      const auto &msg = frame->data<TestMessage>();
      REQUIRE(msg.id == static_cast<uint32_t>(count));

      count++;
      reader->next();
    }

    REQUIRE(count == NUM_MESSAGES);
  }

  SECTION("tryCurrentFrame returns empty view when no data") {
    // Read all data
    while (reader->isDataAvailable()) {
      reader->next();
    }

    auto frame = reader->tryCurrentFrame();
    if (frame) {
      REQUIRE_FALSE(frame->hasData());
    }
  }

  SECTION("Can read messages in sequence") {
    std::vector<uint32_t> ids;

    while (reader->isDataAvailable()) {
      auto frame = reader->tryCurrentFrame();
      const auto &msg = frame->data<TestMessage>();
      ids.push_back(msg.id);
      reader->next();
    }

    // Verify sequential
    for (size_t i = 1; i < ids.size(); ++i) {
      REQUIRE(ids[i] == ids[i - 1] + 1);
    }
  }
}

// ============================================================================
// Test 4: Multiple Journal Reading (Merge)
// ============================================================================

TEST_CASE_METHOD(JournalReaderFixture, "JournalReader merges journals",
                 "[reader][merge]") {
  // Write to 3 different journals with interleaved timestamps
  int64_t base_time = revoq::TimeUtils::now().nsec();

  // Journal 0: times 0, 3, 6, 9...
  {
    auto writer = std::make_shared<JournalWriter<>>(
        location, 0, JournalWriterOptions{.prefault = true});
    for (int i = 0; i < 10; ++i) {
      TestMessage msg{base_time + i * 3000, static_cast<uint32_t>(i), 0.0};
      publish(*writer, msg.timestamp, MsgType::Trade, msg);
    }
  }

  // Journal 1: times 1, 4, 7, 10...
  {
    auto writer = std::make_shared<JournalWriter<>>(
        location, 1, JournalWriterOptions{.prefault = true});
    for (int i = 0; i < 10; ++i) {
      TestMessage msg{base_time + 1000 + i * 3000, static_cast<uint32_t>(i),
                      1.0};
      publish(*writer, msg.timestamp, MsgType::Trade, msg);
    }
  }

  // Journal 2: times 2, 5, 8, 11...
  {
    auto writer = std::make_shared<JournalWriter<>>(
        location, 2, JournalWriterOptions{.prefault = true});
    for (int i = 0; i < 10; ++i) {
      TestMessage msg{base_time + 2000 + i * 3000, static_cast<uint32_t>(i),
                      2.0};
      publish(*writer, msg.timestamp, MsgType::Trade, msg);
    }
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  SECTION("Merges journals in timestamp order") {
    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
    reader->join(location, 0, 0);
    reader->join(location, 1, 0);
    reader->join(location, 2, 0);

    std::vector<int64_t> timestamps;

    while (reader->isDataAvailable()) {
      auto frame = reader->tryCurrentFrame();
      timestamps.push_back(frame->getGenTime());
      reader->next();
    }

    // Verify sorted by timestamp
    REQUIRE(timestamps.size() == 30); // 10 from each journal

    for (size_t i = 1; i < timestamps.size(); ++i) {
      REQUIRE(timestamps[i] >= timestamps[i - 1]);
    }
  }

  SECTION("Can identify source journal") {
    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
    reader->join(location, 0, 0);
    reader->join(location, 1, 0);
    reader->join(location, 2, 0);

    int count_j0 = 0, count_j1 = 0, count_j2 = 0;

    while (reader->isDataAvailable()) {
      auto frame = reader->tryCurrentFrame();
      const auto &msg = frame->data<TestMessage>();

      if (msg.value == 0.0)
        count_j0++;
      else if (msg.value == 1.0)
        count_j1++;
      else if (msg.value == 2.0)
        count_j2++;

      reader->next();
    }

    REQUIRE(count_j0 == 10);
    REQUIRE(count_j1 == 10);
    REQUIRE(count_j2 == 10);
  }
}

// ============================================================================
// Test 5: Seek Operations
// ============================================================================

TEST_CASE_METHOD(JournalReaderFixture, "JournalReader seek", "[reader][seek]") {
  int64_t base_time = revoq::TimeUtils::now().nsec();

  // Write 100 messages with known timestamps
  {
    auto writer = std::make_shared<JournalWriter<>>(
        location, 0, JournalWriterOptions{.prefault = true});
    for (int i = 0; i < 100; ++i) {
      TestMessage msg{base_time + i * 1000, static_cast<uint32_t>(i),
                      100.0 + i};
      publish(*writer, msg.timestamp, MsgType::Trade, msg);
    }
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  SECTION("Can seek to specific time") {
    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
    reader->join(location, 0, base_time + 50000); // Seek to message 50

    if (reader->isDataAvailable()) {
      auto frame = reader->tryCurrentFrame();
      const auto &msg = frame->data<TestMessage>();
      REQUIRE(msg.id >= 50); // Should start at or after message 50
    }
  }

  SECTION("Can seek during reading") {
    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
    reader->join(location, 0, 0);

    // Read first 20 messages
    for (int i = 0; i < 20 && reader->isDataAvailable(); ++i) {
      reader->next();
    }

    // Seek to message 50
    reader->seekToTime(base_time + 50000);

    if (reader->isDataAvailable()) {
      auto frame = reader->tryCurrentFrame();
      const auto &msg = frame->data<TestMessage>();
      REQUIRE(msg.id >= 50);
    }
  }
}

// ============================================================================
// Test 6: Performance - Sort Optimization
// ============================================================================

TEST_CASE_METHOD(JournalReaderFixture, "JournalReader performance",
                 "[reader][perf][!benchmark]") {
  const int NUM_MESSAGES = 10000;
  writeTestData(0, NUM_MESSAGES);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  auto reader =
      std::make_shared<JournalReader>(JournalReaderOptions{.prefault = false});
  reader->join(location, 0, 0);

  SECTION("Reading throughput") {
    auto start = std::chrono::high_resolution_clock::now();

    int count = 0;
    while (reader->isDataAvailable()) {
      auto frame = reader->tryCurrentFrame();
      [[maybe_unused]] const auto &msg = frame->data<TestMessage>();
      count++;
      reader->next();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    double avg_latency = static_cast<double>(duration.count()) / count;
    double throughput = (count * 1000000.0) / duration.count();

    INFO("Read " << count << " messages");
    INFO("Average latency: " << avg_latency << " µs");
    INFO("Throughput: " << throughput << " msgs/sec");

    REQUIRE(count == NUM_MESSAGES);
    REQUIRE(avg_latency < 2.0); // Should be fast
  }
}

// ============================================================================
// Test 7: Edge Cases
// ============================================================================

TEST_CASE_METHOD(JournalReaderFixture, "JournalReader edge cases",
                 "[reader][edge]") {
  SECTION("Reading empty journal") {
    writeTestData(0, 0); // Empty
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
    reader->join(location, 0, 0);

    REQUIRE_FALSE(reader->isDataAvailable());
  }

  SECTION("Reading after disjoin all") {
    writeTestData(0, 10);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
    reader->join(location, 0, 0);

    REQUIRE(reader->isDataAvailable());

    reader->disjoin(location->uid);

    REQUIRE_FALSE(reader->isDataAvailable());
  }

  SECTION("next() on empty reader is safe") {
    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});

    reader->next(); // Should not crash
    REQUIRE_FALSE(reader->isDataAvailable());
  }

  SECTION("tryCurrentFrame on empty reader") {
    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});

    auto frame = reader->tryCurrentFrame();
    REQUIRE_FALSE(frame);
  }
}

// ============================================================================
// Test 8: Concurrent Reading
// ============================================================================

TEST_CASE_METHOD(JournalReaderFixture, "JournalReader thread safety",
                 "[reader][thread]") {
  const int NUM_MESSAGES = 1000;
  writeTestData(0, NUM_MESSAGES);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  SECTION("Multiple readers can read same journal") {
    std::vector<std::thread> threads;
    std::atomic<int> total_read{0};

    for (int i = 0; i < 3; ++i) {
      threads.emplace_back([&]() {
        auto reader = std::make_shared<JournalReader>(
            JournalReaderOptions{.prefault = true});
        reader->join(location, 0, 0);

        int count = 0;
        while (reader->isDataAvailable()) {
          reader->next();
          count++;
        }

        total_read += count;
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    // Each reader should read all messages
    REQUIRE(total_read == NUM_MESSAGES * 3);
  }
}
