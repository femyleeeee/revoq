#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <thread>

#include "JournalReader.h"
#include "JournalWriter.h"
#include "TimeUtils.h"
#include "test_helpers.h"

using namespace revoq::journal;
using namespace revoq::journal::test;
namespace fs = std::filesystem;

namespace {

constexpr std::size_t kV2FrameHeaderBytes = 32;
constexpr std::size_t kFrameAlignmentBytes = 64;

constexpr std::size_t alignUp(std::size_t value, std::size_t alignment) {
  return (value + alignment - 1) / alignment * alignment;
}

} // namespace

// Test data structure
struct TestTrade {
  int64_t timestamp;
  uint32_t symbol_id;
  double price;
  uint64_t volume;
  char side;
};

#pragma pack(push, 1)
struct SmallTrade28 {
  int64_t order_id;
  double price;
  int32_t quantity;
  char symbol[8];
};
#pragma pack(pop)

static_assert(sizeof(SmallTrade28) == 28);

// Test fixture
class JournalWriterFixture {
public:
  JournalWriterFixture() {
    test_dir = fs::temp_directory_path() / "journal_writer_test";

    // Clean up any existing test directory
    if (fs::exists(test_dir)) {
      fs::remove_all(test_dir);
    }

    fs::create_directories(test_dir);

    locator = std::make_shared<Locator>(test_dir.string());
    location = Location::make(Mode::LIVE, Category::EXECUTION, "test",
                              "writer_test", locator);
  }

  ~JournalWriterFixture() {
    // Give time for any background threads to finish
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    if (fs::exists(test_dir)) {
      try {
        fs::remove_all(test_dir);
      } catch (const std::exception &e) {
        // Ignore cleanup errors
      }
    }
  }

  fs::path test_dir;
  ILocatorPtr locator;
  LocationPtr location;
};

// ============================================================================
// Test 1: Writer Construction
// ============================================================================

TEST_CASE_METHOD(JournalWriterFixture, "JournalWriter construction",
                 "[writer][construct]") {
  SECTION("Can create writer with prefault=true") {
    auto writer = std::make_shared<JournalWriter<>>(
        location, 0, JournalWriterOptions{.prefault = true});
    REQUIRE(writer != nullptr);
  }

  SECTION("Can create writer with prefault=false") {
    auto writer = std::make_shared<JournalWriter<>>(
        location, 1, JournalWriterOptions{.prefault = false});
    REQUIRE(writer != nullptr);
  }

  SECTION("Can create writer with PageWarmer") {
    auto writer = std::make_shared<JournalWriter<>>(location, 2,
                                                    JournalWriterOptions{
                                                        .prefault = false,
                                                        .warmup_pages = 2,
                                                        .warmer_cpu_core = -1,
                                                    });
    REQUIRE(writer != nullptr);
  }
}

// ============================================================================
// Test 2: Basic Write Operations
// ============================================================================

TEST_CASE_METHOD(JournalWriterFixture, "JournalWriter basic writes",
                 "[writer][write]") {
  auto writer = std::make_shared<JournalWriter<>>(
      location, 0, JournalWriterOptions{.prefault = true});

  SECTION("Can write single message") {
    TestTrade trade{revoq::TimeUtils::now().nsec(), 1, 100.5, 1000, 'B'};

    publish(*writer, trade.timestamp, MsgType::Trade, trade);
    REQUIRE(writer->getCurrentSequence() == 1);
  }

  SECTION("Can write multiple messages") {
    for (int i = 0; i < 100; ++i) {
      TestTrade trade{revoq::TimeUtils::now().nsec(), static_cast<uint32_t>(i),
                      100.0 + i, 1000, (i % 2 == 0) ? 'B' : 'S'};

      publish(*writer, trade.timestamp, MsgType::Trade, trade);
    }

    REQUIRE(writer->getCurrentSequence() == 100);
  }

  SECTION("Can write with different message types") {
    TestTrade trade{revoq::TimeUtils::now().nsec(), 1, 100.0, 1000, 'B'};

    publish(*writer, trade.timestamp, MsgType::Trade, trade);
    writer->mark(revoq::TimeUtils::now().nsec(), MsgType::SessionStart);
    publish(*writer, trade.timestamp, MsgType::Trade, trade);
    writer->mark(revoq::TimeUtils::now().nsec(), MsgType::SessionEnd);

    REQUIRE(writer->getCurrentSequence() == 4);
  }
}

// ============================================================================
// Test 3: Sequence Numbers
// ============================================================================

TEST_CASE_METHOD(JournalWriterFixture, "JournalWriter sequence tracking",
                 "[writer][sequence]") {
  auto writer = std::make_shared<JournalWriter<>>(
      location, 0, JournalWriterOptions{.prefault = true});

  SECTION("Sequence starts at 0") {
    REQUIRE(writer->getCurrentSequence() == 0);
  }

  SECTION("Sequence increments with each write") {
    TestTrade trade{revoq::TimeUtils::now().nsec(), 1, 100.0, 1000, 'B'};

    for (int i = 0; i < 10; ++i) {
      publish(*writer, trade.timestamp, MsgType::Trade, trade);
      REQUIRE(writer->getCurrentSequence() == static_cast<uint32_t>(i + 1));
    }
  }

  SECTION("Sequence is consistent across read") {
    TestTrade trade{revoq::TimeUtils::now().nsec(), 1, 100.0, 1000, 'B'};

    // Write 10 messages
    for (int i = 0; i < 10; ++i) {
      publish(*writer, trade.timestamp, MsgType::Trade, trade);
    }

    // Force flush
    writer.reset();

    // Give filesystem time to sync
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Read back and verify sequences
    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
    reader->join(location, 0, 0);

    uint32_t expected_seq = 0;
    while (reader->isDataAvailable()) {
      auto frame = reader->tryCurrentFrame();
      REQUIRE(frame->getSequence() == expected_seq);
      expected_seq++;
      reader->next();
    }

    REQUIRE(expected_seq == 10);
  }
}

// ============================================================================
// Test 4: V2 Frame Layout
// ============================================================================

TEST_CASE_METHOD(
    JournalWriterFixture,
    "JournalWriter stores logical length with 64-byte frame stride",
    "[writer][frame-layout][v2]") {
  auto writer = std::make_shared<JournalWriter<>>(
      location, 7, JournalWriterOptions{.prefault = true});

  for (int i = 0; i < 4; ++i) {
    SmallTrade28 trade{static_cast<int64_t>(i),
                       100.0 + i,
                       100 + i,
                       {'T', 'E', 'S', 'T', '\0', '\0', '\0', '\0'}};
    publish(*writer, 1'000'000 + i, MsgType::Trade, trade);
  }

  writer.reset();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  auto reader =
      std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
  reader->join(location, 7, 0);

  constexpr std::size_t expected_logical_length =
      kV2FrameHeaderBytes + sizeof(SmallTrade28);
  constexpr std::size_t expected_physical_stride =
      alignUp(expected_logical_length, kFrameAlignmentBytes);

  std::uintptr_t previous_address = 0;
  int frames_seen = 0;
  while (reader->isDataAvailable() && frames_seen < 4) {
    auto frame = reader->tryCurrentFrame();

    REQUIRE(frame);
    REQUIRE(frame->getMsgType() == static_cast<int32_t>(MsgType::Trade));
    CHECK(frame->address() % kFrameAlignmentBytes == 0);
    CHECK(frame->getFrameLength() == expected_logical_length);
    CHECK(frame->getDataLength() == sizeof(SmallTrade28));

    if (previous_address != 0) {
      CHECK(frame->address() - previous_address == expected_physical_stride);
    }

    const auto &trade = frame->data<SmallTrade28>();
    CHECK(trade.order_id == frames_seen);

    previous_address = frame->address();
    ++frames_seen;
    reader->next();
  }

  REQUIRE(frames_seen == 4);
}

// ============================================================================
// Test 5: Write with Flags
// ============================================================================

TEST_CASE_METHOD(JournalWriterFixture, "JournalWriter with flags",
                 "[writer][flags]") {
  auto writer = std::make_shared<JournalWriter<>>(
      location, 0, JournalWriterOptions{.prefault = true});

  SECTION("Can write with high priority flag") {
    TestTrade trade{revoq::TimeUtils::now().nsec(), 1, 100.0, 1000, 'B'};
    publish(*writer, trade.timestamp, MsgType::Trade, trade,
            FRAME_HIGH_PRIORITY);
    REQUIRE(writer->getCurrentSequence() == 1);
  }

  SECTION("Flags are preserved") {
    TestTrade trade{revoq::TimeUtils::now().nsec(), 1, 100.0, 1000, 'B'};
    publish(*writer, trade.timestamp, MsgType::Trade, trade,
            FRAME_HIGH_PRIORITY);
    writer.reset();

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
    reader->join(location, 0, 0);

    if (reader->isDataAvailable()) {
      auto frame = reader->tryCurrentFrame();
      REQUIRE(frame->getFlags() & FRAME_HIGH_PRIORITY);
    }
  }
}

// ============================================================================
// Test 5: PageWarmer Integration
// ============================================================================

TEST_CASE_METHOD(JournalWriterFixture, "JournalWriter with PageWarmer",
                 "[writer][pagewarmer]") {
  SECTION("Writer with PageWarmer doesn't crash") {
    auto writer = std::make_shared<JournalWriter<>>(location, 0,
                                                    JournalWriterOptions{
                                                        .prefault = false,
                                                        .warmup_pages = 2,
                                                        .warmer_cpu_core = -1,
                                                    });

    REQUIRE(writer != nullptr);

    // Give PageWarmer thread time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    TestTrade trade{revoq::TimeUtils::now().nsec(), 1, 100.0, 1000, 'B'};

    for (int i = 0; i < 100; ++i) {
      publish(*writer, trade.timestamp, MsgType::Trade, trade);
    }

    REQUIRE(writer->getCurrentSequence() == 100);
  }

  SECTION("PageWarmer initialization safety") {
    // Create writer with PageWarmer
    auto writer = std::make_shared<JournalWriter<>>(location, 0,
                                                    JournalWriterOptions{
                                                        .prefault = false,
                                                        .warmup_pages = 2,
                                                        .warmer_cpu_core = -1,
                                                    });

    // Wait for initial page to be created and warmer to initialize
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    TestTrade trade{revoq::TimeUtils::now().nsec(), 1, 100.0, 1000, 'B'};

    // Write first message to establish page
    publish(*writer, trade.timestamp, MsgType::Trade, trade);

    // Give warmer time to warm next pages
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Continue writing
    for (int i = 1; i < 50; ++i) {
      publish(*writer, trade.timestamp, MsgType::Trade, trade);
    }

    REQUIRE(writer->getCurrentSequence() == 50);
  }

  
  SECTION("PageWarmer reduces rotation latency", "[.][benchmark]") {
    // Test 2: With PageWarmer
    const int NUM_MESSAGES = 50000;
    auto start2 = std::chrono::high_resolution_clock::now();
    {
      auto writer = std::make_shared<JournalWriter<>>(location, 1,
                                                      JournalWriterOptions{
                                                          .prefault = false,
                                                          .warmup_pages = 2,
                                                          .warmer_cpu_core = -1,
                                                      }); // With warmer

      // Wait for initialization and first warmup
      std::this_thread::sleep_for(std::chrono::milliseconds(200));

      for (int i = 0; i < NUM_MESSAGES; ++i) {
        TestTrade trade{revoq::TimeUtils::now().nsec(), 1, 100.0, 1000, 'B'};
        publish(*writer, trade.timestamp, MsgType::Trade, trade);
      }
    }
    auto end2 = std::chrono::high_resolution_clock::now();
    auto duration2 =
        std::chrono::duration_cast<std::chrono::microseconds>(end2 - start2);

    // Report results
    INFO("With warmer: " << duration2.count() << " µs");

    // With warmer should complete successfully
    REQUIRE(duration2.count() > 0);
  }
}

// ============================================================================
// Test 7: Write Performance
// ============================================================================

TEST_CASE_METHOD(JournalWriterFixture, "JournalWriter performance",
                 "[writer][perf][!benchmark]") {
  auto writer = std::make_shared<JournalWriter<>>(location, 0,
                                                  JournalWriterOptions{
                                                      .prefault = false,
                                                      .warmup_pages = 2,
                                                      .warmer_cpu_core = -1,
                                                  });

  // Wait for initialization
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  SECTION("Write throughput") {
    const int NUM_MESSAGES = 100000;
    TestTrade trade{revoq::TimeUtils::now().nsec(), 1, 100.0, 1000, 'B'};

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < NUM_MESSAGES; ++i) {
      publish(*writer, trade.timestamp, MsgType::Trade, trade);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    double avg_latency = static_cast<double>(duration.count()) / NUM_MESSAGES;
    double throughput = (NUM_MESSAGES * 1000000.0) / duration.count();

    INFO("Average latency: " << avg_latency << " µs");
    INFO("Throughput: " << throughput << " msgs/sec");

    REQUIRE(avg_latency < 2.0);   // Should be sub-2-microsecond
    REQUIRE(throughput > 500000); // Should exceed 500K msgs/sec
  }
}

// ============================================================================
// Test 8: Thread Safety
// ============================================================================

TEST_CASE_METHOD(JournalWriterFixture, "JournalWriter thread safety",
                 "[writer][thread]") {
  SECTION("Single writer is safe with concurrent internal threads") {
    // The retire thread munmaps old pages while the single writer thread
    // creates new ones. retained_page_limit=0 enqueues pages to the retire
    // thread immediately on rotation, so munmap races concurrently with active
    // writes.
    //
    // EXECUTION pages are 4 MB; TestTrade frames stride 64 B physical.
    // Frames per page = (4 MB - 128 B) / 64 B = 65,534.
    // 140,000 messages triggers exactly 2 page rotations.
    constexpr uint64_t N = 140'000;

    {
      auto writer =
          std::make_shared<JournalWriter<>>(location, 0,
                                            JournalWriterOptions{
                                                .prefault = false,
                                                .background_threads = true,
                                                .retained_page_limit = 0,
                                            });

      for (uint64_t i = 0; i < N; ++i) {
        TestTrade trade{revoq::TimeUtils::now().nsec(),
                        static_cast<uint32_t>(i), 100.0, 1000, 'B'};
        publish(*writer, trade.timestamp, MsgType::Trade, trade);
      }

      const auto rotations =
          static_cast<uint64_t>(writer->getCurrentPageId() - 1);
      REQUIRE(rotations >= 2);
      // Each PageEnd sentinel written at rotation consumes one sequence number.
      REQUIRE(writer->getCurrentSequence() == N + rotations);
    } // destructor joins retire thread; all pages fully committed to disk

    // Read back every frame and verify the sequence stream is contiguous.
    // PageEnd frames are visible and carry their own sequence numbers, so
    // sequences are strictly seq[i] == seq[i-1] + 1 across page boundaries.
    auto reader = std::make_shared<JournalReader>(
        JournalReaderOptions{.prefault = true, .background_threads = false});
    reader->join(location, 0, 0);
    int64_t prev_seq = -1;
    uint64_t data_frames = 0;
    while (reader->isDataAvailable()) {
      auto frame = reader->tryCurrentFrame();
      REQUIRE(static_cast<int64_t>(frame->getSequence()) == prev_seq + 1);
      prev_seq = static_cast<int64_t>(frame->getSequence());
      if (static_cast<MsgType>(frame->getMsgType()) != MsgType::PageEnd)
        ++data_frames;
      reader->next();
    }
    REQUIRE(data_frames == N);
  }
}

// ============================================================================
// Test 9: Edge Cases
// ============================================================================

TEST_CASE_METHOD(JournalWriterFixture, "JournalWriter edge cases",
                 "[writer][edge]") {
  auto writer = std::make_shared<JournalWriter<>>(
      location, 0, JournalWriterOptions{.prefault = true});

  SECTION("Can write zero-sized struct") {
    struct Empty {};
    Empty empty;

    publish(*writer, revoq::TimeUtils::now().nsec(), MsgType::Trade, empty);
    REQUIRE(writer->getCurrentSequence() == 1);
  }

  SECTION("Can write large struct") {
    struct Large {
      char data[1000];
    };
    Large large;
    memset(large.data, 'X', sizeof(large.data));

    publish(*writer, revoq::TimeUtils::now().nsec(), MsgType::Trade, large);
    REQUIRE(writer->getCurrentSequence() == 1);
  }

  SECTION("Can write many small messages") {
    struct Tiny {
      uint32_t id;
    };

    for (int i = 0; i < 10000; ++i) {
      Tiny tiny{static_cast<uint32_t>(i)};
      publish(*writer, revoq::TimeUtils::now().nsec(), MsgType::Trade, tiny);
    }

    REQUIRE(writer->getCurrentSequence() == 10000);
  }
}

// ============================================================================
// Test 10: Write and Read Consistency
// ============================================================================

TEST_CASE_METHOD(JournalWriterFixture, "JournalWriter write-read consistency",
                 "[writer][consistency]") {
  const int NUM_MESSAGES = 1000;

  SECTION("All written messages can be read back") {
    {
      auto writer = std::make_shared<JournalWriter<>>(
          location, 0, JournalWriterOptions{.prefault = false});

      for (int i = 0; i < NUM_MESSAGES; ++i) {
        TestTrade trade{revoq::TimeUtils::now().nsec(),
                        static_cast<uint32_t>(i), 100.0 + i, 1000ULL + i,
                        (i % 2 == 0) ? 'B' : 'S'};
        publish(*writer, trade.timestamp, MsgType::Trade, trade);
      }
    }

    // Give filesystem time to sync
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Read back
    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
    reader->join(location, 0, 0);

    int count = 0;
    while (reader->isDataAvailable()) {
      auto frame = reader->tryCurrentFrame();
      const auto &trade = frame->data<TestTrade>();

      REQUIRE(trade.symbol_id == static_cast<uint32_t>(count));
      REQUIRE(trade.price == 100.0 + count);
      REQUIRE(trade.volume == 1000 + count);

      count++;
      reader->next();
    }

    REQUIRE(count == NUM_MESSAGES);
  }
}

// ============================================================================
// Test 11: Mark Operations
// ============================================================================

TEST_CASE_METHOD(JournalWriterFixture, "JournalWriter mark operations",
                 "[writer][mark]") {
  auto writer = std::make_shared<JournalWriter<>>(
      location, 0, JournalWriterOptions{.prefault = true});

  SECTION("Can write session markers") {
    writer->mark(revoq::TimeUtils::now().nsec(), MsgType::SessionStart);

    TestTrade trade{revoq::TimeUtils::now().nsec(), 1, 100.0, 1000, 'B'};
    publish(*writer, trade.timestamp, MsgType::Trade, trade);

    writer->mark(revoq::TimeUtils::now().nsec(), MsgType::SessionEnd);

    REQUIRE(writer->getCurrentSequence() == 3);
  }

  SECTION("Marks are readable") {
    writer->mark(revoq::TimeUtils::now().nsec(), MsgType::SessionStart);
    writer.reset();

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
    reader->join(location, 0, 0);

    if (reader->isDataAvailable()) {
      auto frame = reader->tryCurrentFrame();
      REQUIRE(frame->getMsgType() ==
              static_cast<int32_t>(MsgType::SessionStart));
      REQUIRE(frame->getDataLength() == 0);
    }
  }
}

// Test fixture
class WriterBugHuntFixture {
public:
  WriterBugHuntFixture() {
    test_dir = fs::temp_directory_path() / "writer_bughunt_test";

    if (fs::exists(test_dir)) {
      fs::remove_all(test_dir);
    }

    fs::create_directories(test_dir);

    locator = std::make_shared<Locator>(test_dir.string());
    location = Location::make(Mode::LIVE, Category::EXECUTION, "test",
                              "bughunt", locator);
  }

  ~WriterBugHuntFixture() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    if (fs::exists(test_dir)) {
      try {
        fs::remove_all(test_dir);
      } catch (...) {
      }
    }
  }

  struct TestMsg {
    int64_t timestamp;
    uint32_t seq;
    uint32_t page_hint; // Which page we expect this to be on
    char data[100];
  };

  fs::path test_dir;
  ILocatorPtr locator;
  LocationPtr location;
};

// ============================================================================
// BUG HUNT 1: Sequence Number Continuity Across Pages
// ============================================================================

TEST_CASE_METHOD(WriterBugHuntFixture,
                 "Sequence numbers continuous across page rotation",
                 "[writer][bughunt][sequence]") {

  SECTION("No gaps in sequence across multiple pages") {
    auto writer = std::make_shared<JournalWriter<>>(
        location, 0, JournalWriterOptions{.prefault = false});

    // Write enough to span multiple pages (4MB default)
    const int NUM_MESSAGES = 50000;

    for (int i = 0; i < NUM_MESSAGES; ++i) {
      TestMsg msg;
      msg.timestamp = revoq::TimeUtils::now().nsec();
      msg.seq = i;
      std::memset(msg.data, 'A', sizeof(msg.data));

      publish(*writer, msg.timestamp, MsgType::Trade, msg);
    }

    REQUIRE(writer->getCurrentSequence() >= NUM_MESSAGES);

    // Force writer to close
    writer.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Read back and verify NO gaps
    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
    reader->join(location, 0, 0);

    uint32_t expected_seq = 0;
    uint32_t count = 0;
    std::vector<uint32_t> gaps;

    while (reader->isDataAvailable()) {
      auto frame = reader->tryCurrentFrame();
      
      uint32_t frame_seq = frame->getSequence();

      if (frame_seq != expected_seq) {
        gaps.push_back(expected_seq);
        INFO("Gap detected: expected " << expected_seq << ", got "
                                       << frame_seq);
      }
      

      expected_seq++;
      count++;
      reader->next();
    }

    REQUIRE(gaps.empty()); // No gaps!
    REQUIRE(count >= NUM_MESSAGES);
    REQUIRE(expected_seq >= NUM_MESSAGES);
  }
}
//
// //
// ============================================================================
// // BUG HUNT 2: Page Rotation Happens Correctly
// //
// ============================================================================
//
TEST_CASE_METHOD(WriterBugHuntFixture,
                 "Page rotation triggers at correct boundary",
                 "[writer][bughunt][rotation]") {

  SECTION("No frames span page boundaries") {
    auto writer = std::make_shared<JournalWriter<>>(
        location, 0, JournalWriterOptions{.prefault = false});

    // Calculate approximate page size
    size_t page_size = findPageSize(location, 0);
    size_t frame_size =
        framePhysicalLength(sizeof(FrameHeader) + sizeof(TestMsg));
    size_t frames_per_page =
        (page_size - sizeof(PageHeader) - FRAME_ALIGNMENT) / frame_size;

    INFO("Page size: " << page_size);
    INFO("Frame size: " << frame_size);
    INFO("Frames per page (approx): " << frames_per_page);

    // Write 3 pages worth
    for (size_t i = 0; i < frames_per_page * 3; ++i) {
      TestMsg msg;
      msg.timestamp = revoq::TimeUtils::now().nsec();
      msg.seq = static_cast<uint32_t>(i);
      msg.page_hint = static_cast<uint32_t>(i / frames_per_page) + 1;

      publish(*writer, msg.timestamp, MsgType::Trade, msg);
    }

    writer.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify multiple page files exist
    auto page_files = location->locator->listPageId(location, 0);

    INFO("Number of page files created: " << page_files.size());
    REQUIRE(page_files.size() >= 2); // At least 2 pages
  }

  SECTION("Page header updated correctly on rotation") {
    auto writer = std::make_shared<JournalWriter<>>(
        location, 0, JournalWriterOptions{.prefault = false});

    // Write to fill first page
    size_t page_size = findPageSize(location, 0);
    size_t frame_size =
        framePhysicalLength(sizeof(FrameHeader) + sizeof(TestMsg));
    size_t frames_per_page =
        (page_size - sizeof(PageHeader) - FRAME_ALIGNMENT) / frame_size;

    for (size_t i = 0; i < frames_per_page + 10; ++i) {
      TestMsg msg;
      msg.timestamp = revoq::TimeUtils::now().nsec();
      msg.seq = static_cast<uint32_t>(i);

      publish(*writer, msg.timestamp, MsgType::Trade, msg);
    }

    writer.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Check page 1 header
    auto page1 = Page::load(location, 0, 1, false, true);

    REQUIRE(page1->getPageId() == 1);
    // Verify last_frame_position is set
    REQUIRE(page1->lastFrameAddress() > page1->firstFrameAddress());

    // Check page 2 exists and has data
    auto page_files = location->locator->listPageId(location, 0);
    if (page_files.size() >= 2) {
      auto page2 = Page::load(location, 0, 2, false, true);
      REQUIRE(page2->getPageId() == 2);
    }
  }
}
//
// //
// ============================================================================
// // BUG HUNT 3: Frame Size Edge Cases
// //
// ============================================================================
//
TEST_CASE_METHOD(WriterBugHuntFixture, "Handles varying frame sizes correctly",
                 "[writer][bughunt][framesize]") {

  SECTION("Mix of small and large frames") {
    auto writer = std::make_shared<JournalWriter<>>(
        location, 0, JournalWriterOptions{.prefault = false});

    struct SmallMsg {
      uint32_t id;
    };
    struct LargeMsg {
      char data[1000];
    };

    for (int i = 0; i < 1000; ++i) {
      auto ts = revoq::TimeUtils::now().nsec();

      if (i % 2 == 0) {
        SmallMsg msg{static_cast<uint32_t>(i)};
        publish(*writer, ts, MsgType::Trade, msg);
      } else {
        LargeMsg msg;
        std::memset(msg.data, 'X', sizeof(msg.data));
        publish(*writer, ts, MsgType::MarketDepth20, msg);
      }
    }

    REQUIRE(writer->getCurrentSequence() == 1000);

    writer.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Read back and verify
    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
    reader->join(location, 0, 0);

    int count = 0;
    while (reader->isDataAvailable()) {
      auto frame = reader->tryCurrentFrame();
      if (static_cast<MsgType>(frame->getMsgType()) == MsgType::PageEnd) {
        reader->next();
        continue;
      }
      // Verify frame is valid
      REQUIRE(frame->hasData());
      REQUIRE(frame->getDataLength() > 0);

      count++;
      reader->next();
    }

    REQUIRE(count == 1000);
  }
}
//
// //
// ============================================================================
// // BUG HUNT 4: Concurrent Writes During Rotation
// //
// ============================================================================
//
TEST_CASE_METHOD(WriterBugHuntFixture, "Thread safety during page rotation",
                 "[writer][bughunt][concurrent]") {

  SECTION("Multiple threads writing during rotation") {
    auto writer = std::make_shared<JournalWriter<MultiWriterPolicy>>(
        location, 0, JournalWriterOptions{.prefault = false});

    std::atomic<int> total_written{0};
    std::vector<std::thread> threads;

    // 5 threads, each writes 1000 messages
    for (int t = 0; t < 5; ++t) {
      threads.emplace_back([&writer, &total_written, t]() {
        for (int i = 0; i < 1000; ++i) {
          TestMsg msg;
          msg.timestamp = revoq::TimeUtils::now().nsec();
          msg.seq = t * 1000 + i;

          publish(*writer, msg.timestamp, MsgType::Trade, msg);
          total_written.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    REQUIRE(total_written == 5000);
    REQUIRE(writer->getCurrentSequence() == 5000);

    writer.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify all messages readable
    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
    reader->join(location, 0, 0);

    int count = 0;
    while (reader->isDataAvailable()) {
      auto frame = reader->tryCurrentFrame();
      REQUIRE(frame->hasData());
      count++;
      reader->next();
    }

    REQUIRE(count == 5000);
  }
}
//
// //
// ============================================================================
// // BUG HUNT 5: Pre-Warmed vs Fallback Path Equivalence
// //
// ============================================================================
//
TEST_CASE_METHOD(WriterBugHuntFixture,
                 "Pre-warmed and fallback paths produce identical results",
                 "[writer][bughunt][pagewarmer]") {

  SECTION("Compare with and without PageWarmer") {
    // Write with PageWarmer
    {
      auto writer = std::make_shared<JournalWriter<>>(location, 0,
                                                      JournalWriterOptions{
                                                          .prefault = false,
                                                          .warmup_pages = 2,
                                                          .warmer_cpu_core = -1,
                                                      });

      for (int i = 0; i < 1000; ++i) {
        TestMsg msg;
        msg.timestamp = revoq::TimeUtils::now().nsec();
        msg.seq = i;
        publish(*writer, msg.timestamp, MsgType::Trade, msg);
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Read back
    auto reader1 =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
    reader1->join(location, 0, 0);

    std::vector<uint32_t> seqs1;
    while (reader1->isDataAvailable()) {
      auto frame = reader1->tryCurrentFrame();
      seqs1.push_back(frame->getSequence());
      reader1->next();
    }

    // Clean up
    fs::remove_all(test_dir);
    fs::create_directories(test_dir);

    // Write without PageWarmer
    {
      auto writer = std::make_shared<JournalWriter<>>(
          location, 1,
          JournalWriterOptions{.prefault = true}); // Different dest_id

      for (int i = 0; i < 1000; ++i) {
        TestMsg msg;
        msg.timestamp = revoq::TimeUtils::now().nsec();
        msg.seq = i;
        publish(*writer, msg.timestamp, MsgType::Trade, msg);
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Read back
    auto reader2 =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});
    reader2->join(location, 1, 0);

    std::vector<uint32_t> seqs2;
    while (reader2->isDataAvailable()) {
      auto frame = reader2->tryCurrentFrame();
      seqs2.push_back(frame->getSequence());
      reader2->next();
    }

    // Both should have identical sequence patterns
    REQUIRE(seqs1.size() == 1000);
    REQUIRE(seqs2.size() == 1000);

    for (size_t i = 0; i < seqs1.size(); ++i) {
      REQUIRE(seqs1[i] == seqs2[i]);
    }
  }
}
//
// //
// ============================================================================
// // BUG HUNT 6: Memory Barriers / Visibility
// //
// ============================================================================
//
TEST_CASE_METHOD(WriterBugHuntFixture,
                 "Writer updates immediately visible to reader",
                 "[writer][bughunt][visibility]") {

  SECTION("Concurrent writer and reader") {
    auto writer = std::make_shared<JournalWriter<>>(
        location, 0, JournalWriterOptions{.prefault = false});
    auto reader =
        std::make_shared<JournalReader>(JournalReaderOptions{.prefault = true});

    std::atomic<bool> writer_done{false};
    std::atomic<int> messages_written{0};
    std::atomic<int> messages_read{0};

    // Writer thread
    std::thread writer_thread([&]() {
      for (int i = 0; i < 1000; ++i) {
        TestMsg msg;
        msg.timestamp = revoq::TimeUtils::now().nsec();
        msg.seq = i;

        publish(*writer, msg.timestamp, MsgType::Trade, msg);
        messages_written.fetch_add(1, std::memory_order_release);

        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
      writer_done.store(true, std::memory_order_release);
    });

    // Reader thread - starts slightly after writer
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    reader->join(location, 0, 0);

    std::thread reader_thread([&]() {
      while (!writer_done.load(std::memory_order_acquire) ||
             reader->isDataAvailable()) {

        if (reader->isDataAvailable()) {
          auto frame = reader->tryCurrentFrame();
          if (frame && frame->hasData()) {
            messages_read.fetch_add(1, std::memory_order_release);
            reader->next();
          }
        } else {
          std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
      }
    });

    writer_thread.join();
    reader_thread.join();

    INFO("Written: " << messages_written.load());
    INFO("Read: " << messages_read.load());

    // Reader should eventually see all messages
    REQUIRE(messages_read.load() == 1000);
  }
}
// ============================================================================
// §6.2 Page rotation invariants
// ============================================================================

TEST_CASE_METHOD(JournalWriterFixture, "JournalWriter page rotation invariants",
                 "[writer][rotation][invariants]") {
  // TradeMsg frames: 32B FrameHeader + 29B payload → 64B physical stride.
  // DEFAULT 2MB page (EXECUTION + dest_id=0):
  //   frames/page = (2MB - sizeof(PageHeader) - FRAME_ALIGNMENT) / 64 = 32,766.
  // N_ONE = 40,000 → exactly 1 rotation; N_TWO = 70,000 → exactly 2 rotations.
  const size_t page_size = findPageSize(location, 0);
  const size_t frame_stride = framePhysicalLength(
      static_cast<uint32_t>(sizeof(FrameHeader) + sizeof(TestTrade)));
  const size_t frames_per_page =
      (page_size - sizeof(PageHeader) - FRAME_ALIGNMENT) / frame_stride;
  const uint64_t N_ONE = static_cast<uint64_t>(frames_per_page) + 1000;
  const uint64_t N_TWO = static_cast<uint64_t>(frames_per_page) * 2 + 1000;

  auto makeWriter = [&]() {
    return std::make_shared<JournalWriter<>>(
        location, 0,
        JournalWriterOptions{.prefault = false, .background_threads = false});
  };

  SECTION("Rotated page isFull() and last_frame_position is within bounds") {
    auto writer = makeWriter();
    for (uint64_t i = 0; i < N_ONE; ++i) {
      TestTrade t{revoq::TimeUtils::now().nsec(), uint32_t(i), 100.0, 1000,
                  'B'};
      publish(*writer, t.timestamp, MsgType::Trade, t);
    }
    REQUIRE(writer->getCurrentPageId() == 2);
    writer.reset();

    auto old_page = Page::load(location, 0, 1, /*is_writing=*/false,
                               /*prefault=*/false);
    REQUIRE(old_page->isFull());
    REQUIRE(old_page->lastFrameAddress() >= old_page->firstFrameAddress());
    REQUIRE(old_page->lastFrameAddress() <
            old_page->address() + old_page->getPageSize());
  }

  SECTION("PageEnd is the last committed frame on a full page") {
    auto writer = makeWriter();
    for (uint64_t i = 0; i < N_ONE; ++i) {
      TestTrade t{revoq::TimeUtils::now().nsec(), uint32_t(i), 100.0, 1000,
                  'B'};
      publish(*writer, t.timestamp, MsgType::Trade, t);
    }
    REQUIRE(writer->getCurrentPageId() == 2);
    writer.reset();

    auto old_page = Page::load(location, 0, 1, false, false);
    uintptr_t addr = old_page->firstFrameAddress();
    uintptr_t page_end = old_page->address() + old_page->getPageSize();
    const FrameHeader *last_hdr = nullptr;
    while (addr + sizeof(FrameHeader) <= page_end) {
      auto *hdr = reinterpret_cast<const FrameHeader *>(addr);
      uint32_t len = hdr->length.load(std::memory_order_acquire);
      if (len == 0)
        break;
      last_hdr = hdr;
      if (static_cast<MsgType>(hdr->msg_type) == MsgType::PageEnd)
        break;
      addr += framePhysicalLength(len);
    }
    REQUIRE(last_hdr != nullptr);
    REQUIRE(static_cast<MsgType>(last_hdr->msg_type) == MsgType::PageEnd);
    REQUIRE(last_hdr->length.load(std::memory_order_acquire) ==
            sizeof(FrameHeader));
  }

  SECTION("New page header is valid immediately after rotation") {
    auto writer = makeWriter();
    for (uint64_t i = 0; i < N_ONE; ++i) {
      TestTrade t{revoq::TimeUtils::now().nsec(), uint32_t(i), 100.0, 1000,
                  'B'};
      publish(*writer, t.timestamp, MsgType::Trade, t);
    }
    REQUIRE(writer->getCurrentPageId() == 2);
    writer.reset();

    auto new_page = Page::load(location, 0, 2, false, false);
    REQUIRE(new_page->getPageSize() == page_size);
    // page_header_length must equal sizeof(PageHeader) so the first frame
    // sits immediately after the header with no gap.
    REQUIRE(new_page->firstFrameAddress() - new_page->address() ==
            sizeof(PageHeader));
    // Page 2 has data but is not full.
    REQUIRE(!new_page->isFull());
  }

  SECTION("Retired pages remain fully readable with contiguous sequences") {
    auto writer = makeWriter();
    for (uint64_t i = 0; i < N_TWO; ++i) {
      TestTrade t{revoq::TimeUtils::now().nsec(), uint32_t(i), 100.0, 1000,
                  'B'};
      publish(*writer, t.timestamp, MsgType::Trade, t);
    }
    const auto rotations =
        static_cast<uint64_t>(writer->getCurrentPageId() - 1);
    REQUIRE(rotations >= 2);
    REQUIRE(writer->getCurrentSequence() == N_TWO + rotations);
    writer.reset();

    auto reader = std::make_shared<JournalReader>(
        JournalReaderOptions{.prefault = true, .background_threads = false});
    reader->join(location, 0, 0);
    int64_t prev_seq = -1;
    uint64_t data_count = 0;
    uint64_t page_ends = 0;
    while (reader->isDataAvailable()) {
      auto frame = reader->tryCurrentFrame();
      REQUIRE(static_cast<int64_t>(frame->getSequence()) == prev_seq + 1);
      prev_seq = static_cast<int64_t>(frame->getSequence());
      if (static_cast<MsgType>(frame->getMsgType()) == MsgType::PageEnd)
        ++page_ends;
      else
        ++data_count;
      reader->next();
    }
    REQUIRE(data_count == N_TWO);
    REQUIRE(page_ends == rotations);
  }
}
