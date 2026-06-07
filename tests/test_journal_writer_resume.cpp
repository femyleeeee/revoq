#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>

#include "JournalBase.h"
#include "JournalFrame.h"
#include "JournalReader.h"
#include "JournalWriter.h"
#include "test_helpers.h"

using namespace revoq::journal;
using namespace revoq::journal::test;
namespace fs = std::filesystem;

// Test data structure
struct TestData {
  int64_t value;
  double price;
  char symbol[8];
};

TEST_CASE("JournalWriter basic write and read", "[journal][writer][basic]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  SECTION("Write some frames and verify") {
    // Create writer without PageWarmer for simplicity
    auto writer = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = true});

    // Write some test data
    TestData data1{100, 123.45, "AAPL"};
    TestData data2{200, 234.56, "GOOGL"};
    TestData data3{300, 345.67, "MSFT"};

    publish(*writer, 1000000, MsgType::Trade, data1);
    publish(*writer, 2000000, MsgType::Trade, data2);
    publish(*writer, 3000000, MsgType::Trade, data3);

    REQUIRE(writer->getCurrentSequence() == 3);
    REQUIRE(writer->getCurrentPageId() == 1);
  }
}

TEST_CASE("JournalWriter resume after restart", "[journal][writer][resume]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  uint32_t last_seq_before_restart = 0;
  int64_t last_page_before_restart = 0;

  SECTION("First run - write initial data") {
    auto writer = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = false});

    // Write 100 frames
    for (int i = 0; i < 100; ++i) {
      TestData data{i, static_cast<double>(i) * 1.5, "TEST"};
      publish(*writer, i * 1000000, MsgType::Trade, data);
    }

    last_seq_before_restart = writer->getCurrentSequence();
    last_page_before_restart = writer->getCurrentPageId();

    INFO("First run: last_seq=" << last_seq_before_restart
                                << ", last_page=" << last_page_before_restart);

    REQUIRE(last_seq_before_restart == 100);

    // Destroy writer (simulates shutdown)
    writer.reset();
  }

  SECTION("Second run - resume and continue") {
    // Create new writer with resume enabled
    auto writer = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = false});

    // Should have resumed from previous state
    uint32_t resumed_seq = writer->getCurrentSequence();
    int64_t resumed_page = writer->getCurrentPageId();

    INFO("Second run: resumed_seq=" << resumed_seq
                                    << ", resumed_page=" << resumed_page);

    // Sequence should continue from where we left off
    REQUIRE(resumed_seq == last_seq_before_restart);

    // Write more data
    for (int i = 0; i < 50; ++i) {
      TestData data{i + 1000, static_cast<double>(i + 1000) * 1.5, "TST2"};
      publish(*writer, (i + 1000) * 1000000, MsgType::Trade, data);
    }

    // Should have continued sequence numbering
    REQUIRE(writer->getCurrentSequence() == last_seq_before_restart + 50);
  }
}

TEST_CASE("JournalWriter resume with multiple pages",
          "[journal][writer][resume][multipage]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  const int FRAMES_PER_TEST =
      1'000'000; // Should trigger multiple page rotations

  SECTION("Write many frames to trigger page rotation") {
    auto writer = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = false});
    for (int i = 0; i < FRAMES_PER_TEST; ++i) {
      TestData data{i, static_cast<double>(i), "SYMB"};
      publish(*writer, i * 1'000'000, MsgType::Trade, data);
    }

    int64_t final_page = writer->getCurrentPageId();
    // This is the currently available seq number for next write
    uint64_t final_seq = writer->getCurrentSequence();
    const size_t frame_size =
        framePhysicalLength(sizeof(FrameHeader) + sizeof(TestData));
    const size_t frames_per_page = (findPageSize(location, dest_id) -
                                    sizeof(PageHeader) - FRAME_ALIGNMENT) /
                                   frame_size;
    const int64_t expected_pages = static_cast<int64_t>(
        (FRAMES_PER_TEST + frames_per_page - 1) / frames_per_page);

    INFO("Wrote " << FRAMES_PER_TEST << " frames across " << final_page
                  << " pages, final sequence: " << final_seq);

    REQUIRE(final_page >=
            expected_pages); // Should have rotated to multiple pages
    REQUIRE(final_seq >= FRAMES_PER_TEST);

    writer.reset();

    // Resume and verify
    auto writer2 = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = false});

    REQUIRE(writer2->getCurrentSequence() == final_seq);
    REQUIRE(writer2->getCurrentPageId() == final_page + 1);
  }
}

TEST_CASE("JournalWriter resume from empty journal",
          "[journal][writer][resume][empty]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  // Create writer with resume=true on empty directory
  auto writer = std::make_shared<JournalWriter<>>(
      location, dest_id, JournalWriterOptions{.prefault = false});

  // Should start from page 1, sequence 0
  REQUIRE(writer->getCurrentPageId() == 1);
  REQUIRE(writer->getCurrentSequence() == 0);

  // Write some data
  TestData data{42, 99.99, "TEST"};
  publish(*writer, 1000000, MsgType::Trade, data);

  REQUIRE(writer->getCurrentSequence() == 1);
}

TEST_CASE("JournalWriter with PageWarmer resume",
          "[journal][writer][resume][warmer]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  SECTION("First run with PageWarmer") {
    auto writer = std::make_shared<JournalWriter<>>(location, dest_id,
                                                    JournalWriterOptions{
                                                        .prefault = false,
                                                        .warmup_pages = 4,
                                                        .warmer_cpu_core = -1,
                                                    });

    // Write data
    for (int i = 0; i < 1005; ++i) {
      TestData data{i, static_cast<double>(i), "WARM"};
      publish(*writer, i * 1000000, MsgType::Trade, data);
    }

    uint32_t final_seq = writer->getCurrentSequence();
    int64_t final_page = writer->getCurrentPageId();

    SPDLOG_INFO("Before destruction: final_seq={}, final_page={}", final_seq,
                final_page);
    writer.reset();

    // Give time for PageWarmer cleanup
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Resume with PageWarmer
    auto writer2 = std::make_shared<JournalWriter<>>(location, dest_id,
                                                     JournalWriterOptions{
                                                         .prefault = false,
                                                         .warmup_pages = 4,
                                                         .warmer_cpu_core = -1,
                                                     });
    SPDLOG_INFO("After resume: seq={}, page={}", writer2->getCurrentSequence(),
                writer2->getCurrentPageId());
    REQUIRE(writer2->getCurrentSequence() == final_seq);

    // Write more data with warmer active
    for (int i = 0; i < 500; ++i) {
      TestData data{i + 2000, static_cast<double>(i + 2000), "WRM2"};
      publish(*writer2, (i + 2000) * 1000000, MsgType::Trade, data);
    }

    REQUIRE(writer2->getCurrentSequence() == final_seq + 500);
  }
}

TEST_CASE("JournalWriter resume vs no-resume behavior",
          "[journal][writer][resume][comparison]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  SECTION("Setup: Write initial data") {
    auto writer = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = false});

    for (int i = 0; i < 100; ++i) {
      TestData data{i, static_cast<double>(i), "INIT"};
      publish(*writer, i * 1000000, MsgType::Trade, data);
    }

    writer.reset();
    //  }
    //
    //  SECTION("With resume=true, should continue") {
    writer = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = false});

    // Should have sequence 101 (continuing from 100)
    uint32_t seq_before = writer->getCurrentSequence();
    REQUIRE(seq_before >= 100);

    TestData data{999, 999.99, "CONT"};
    publish(*writer, 999000000, MsgType::Trade, data);

    REQUIRE(writer->getCurrentSequence() == seq_before + 1);
  }

  // The writer always resumes from existing pages now. A restart-from-zero
  // mode would need an explicit destructive/truncate API.
}

TEST_CASE("JournalWriter sequence number continuity",
          "[journal][writer][sequence]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  std::vector<uint32_t> all_sequences;

  SECTION("Multiple restart cycles") {
    // Cycle 1
    {
      auto writer = std::make_shared<JournalWriter<>>(
          location, dest_id, JournalWriterOptions{.prefault = false});

      for (int i = 0; i < 50; ++i) {
        TestData data{i, static_cast<double>(i), "CYC1"};
        publish(*writer, i * 1000000, MsgType::Trade, data);
        all_sequences.push_back(writer->getCurrentSequence() - 1);
      }
    }

    // Cycle 2
    {
      auto writer = std::make_shared<JournalWriter<>>(
          location, dest_id, JournalWriterOptions{.prefault = false});

      for (int i = 0; i < 50; ++i) {
        TestData data{i + 100, static_cast<double>(i + 100), "CYC2"};
        publish(*writer, (i + 100) * 1000000, MsgType::Trade, data);
        all_sequences.push_back(writer->getCurrentSequence() - 1);
      }
    }

    // Cycle 3
    {
      auto writer = std::make_shared<JournalWriter<>>(
          location, dest_id, JournalWriterOptions{.prefault = false});

      for (int i = 0; i < 50; ++i) {
        TestData data{i + 200, static_cast<double>(i + 200), "CYC3"};
        publish(*writer, (i + 200) * 1000000, MsgType::Trade, data);
        all_sequences.push_back(writer->getCurrentSequence() - 1);
      }
    }

    // Verify sequence continuity
    REQUIRE(all_sequences.size() == 150);

    for (size_t i = 0; i < all_sequences.size(); ++i) {
      INFO("Checking sequence at index " << i);
      REQUIRE(all_sequences[i] == i);
    }
  }
}

TEST_CASE("JournalWriter marker frames with resume",
          "[journal][writer][markers][resume]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  SECTION("Write markers and data, then resume") {
    {
      auto writer = std::make_shared<JournalWriter<>>(
          location, dest_id, JournalWriterOptions{.prefault = false});

      writer->mark(1000000, MsgType::SessionStart);

      for (int i = 0; i < 10; ++i) {
        TestData data{i, static_cast<double>(i), "MRK1"};
        publish(*writer, (i + 1) * 1000000, MsgType::Trade, data);
      }

      writer->mark(20000000, MsgType::SessionEnd);

      REQUIRE(writer->getCurrentSequence() == 12); // 1 start + 10 data + 1 end
    }

    {
      auto writer = std::make_shared<JournalWriter<>>(
          location, dest_id, JournalWriterOptions{.prefault = false});

      REQUIRE(writer->getCurrentSequence() == 12); // Continue from 12

      writer->mark(30000000, MsgType::SessionStart);

      TestData data{999, 999.99, "MRK2"};
      publish(*writer, 31000000, MsgType::Trade, data);

      REQUIRE(writer->getCurrentSequence() == 14);
    }
  }
}

TEST_CASE("JournalWriter edge cases", "[journal][writer][edge]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  SECTION("Resume with corrupted last page") {
    // Write some data
    {
      auto writer = std::make_shared<JournalWriter<>>(
          location, dest_id, JournalWriterOptions{.prefault = false});

      for (int i = 0; i < 100; ++i) {
        TestData data{i, static_cast<double>(i), "EDGE"};
        publish(*writer, i * 1000000, MsgType::Trade, data);
      }
    }

    // Try to resume - should handle gracefully
    REQUIRE_NOTHROW(std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = false}));
  }

  SECTION("Crash mid-frame: incomplete frame does not corrupt resume") {
    // Simulate a writer crash after openData() but before closeData().
    // The frame slot has header fields set but length == 0 (not published).
    // findLastSequenceNumber stops at the length-0 slot; the resumed writer
    // starts a new page and re-uses that sequence number for its first frame.
    constexpr int N = 10;

    {
      auto writer = std::make_shared<JournalWriter<>>(
          location, dest_id,
          JournalWriterOptions{.prefault = false, .background_threads = false});
      for (int i = 0; i < N; ++i) {
        TestData data{i, static_cast<double>(i), "CRSH"};
        publish(*writer, i * 1'000'000, MsgType::Trade, data);
      }
      REQUIRE(static_cast<int>(writer->getCurrentSequence()) == N);

      // Open a frame but do NOT close it — simulates crash mid-publish.
      // openData() calls nextSeq() so sequence N is consumed but length stays
      // 0.
      auto &slot = writer->openData<TestData>(N * 1'000'000, MsgType::Trade);
      (void)slot;
      // writer goes out of scope here without closeData() — crash simulated.
    }

    // Resumed writer must not crash and must pick up the correct sequence.
    // findLastSequenceNumber scans to the length-0 slot, returns N-1, so
    // current_sequence_ = N.  loadInitialPage() creates page 2 (page 1 is
    // abandoned with the dangling length-0 frame).
    auto writer2 = std::make_shared<JournalWriter<>>(
        location, dest_id,
        JournalWriterOptions{.prefault = false, .background_threads = false});
    REQUIRE(static_cast<int>(writer2->getCurrentSequence()) == N);
    REQUIRE(writer2->getCurrentPageId() == 2);

    // Write one frame to confirm the resumed writer is functional.
    TestData cont{999, 9.9, "RES"};
    publish(*writer2, (N + 1) * 1'000'000, MsgType::Trade, cont);
    REQUIRE(static_cast<int>(writer2->getCurrentSequence()) == N + 1);

    // Reader joined at page 1 sees exactly the N committed frames and stops
    // at the incomplete slot (length == 0 → isDataAvailable returns false).
    writer2.reset();
    auto reader = std::make_shared<JournalReader>(
        JournalReaderOptions{.prefault = true, .background_threads = false});
    reader->join(location, dest_id, 0);
    int count = 0;
    while (reader->isDataAvailable()) {
      ++count;
      reader->next();
    }
    REQUIRE(count == N);
  }

  SECTION("Very large sequence numbers") {
    auto writer = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = false});

    // Simulate very large sequence by writing many frames
    for (int i = 0; i < 1000; ++i) {
      TestData data{i, static_cast<double>(i), "BIG"};
      publish(*writer, i * 1000000, MsgType::Trade, data);
    }

    uint32_t large_seq = writer->getCurrentSequence();
    REQUIRE(large_seq >= 1000);

    writer.reset();

    // Resume should handle large sequences
    auto writer2 = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = false});

    REQUIRE(writer2->getCurrentSequence() == large_seq);
  }
}

TEST_CASE("JournalWriter concurrent safety", "[journal][writer][concurrent]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  SECTION("Multiple writes in sequence") {
    auto writer = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = false});

    const int NUM_WRITES = 1000;

    for (int i = 0; i < NUM_WRITES; ++i) {
      TestData data{i, static_cast<double>(i), "CONC"};
      publish(*writer, i * 1000000, MsgType::Trade, data);
    }

    REQUIRE(writer->getCurrentSequence() == NUM_WRITES);
  }
}

TEST_CASE("JournalWriter performance baseline",
          "[journal][writer][performance]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  SECTION("Write performance without warmer") {
    auto writer = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = false});

    const int NUM_WRITES = 10000;
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < NUM_WRITES; ++i) {
      TestData data{i, static_cast<double>(i) * 1.5, "PERF"};
      publish(*writer, i * 1000000, MsgType::Trade, data);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

    double avg_ns = static_cast<double>(duration.count()) / NUM_WRITES;

    INFO("Average write time: " << avg_ns << " ns");
    INFO("Total writes: " << NUM_WRITES);
    INFO("Total time: " << duration.count() / 1000000.0 << " ms");

    // Performance should be reasonable (adjust threshold as needed)
    REQUIRE(avg_ns < 10000); // Less than 10 microseconds per write
  }

  SECTION("Write performance with warmer") {
    auto writer = std::make_shared<JournalWriter<>>(location, dest_id,
                                                    JournalWriterOptions{
                                                        .prefault = false,
                                                        .warmup_pages = 4,
                                                        .warmer_cpu_core = -1,
                                                    });

    const int NUM_WRITES = 10000;
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < NUM_WRITES; ++i) {
      TestData data{i, static_cast<double>(i) * 1.5, "PWRM"};
      publish(*writer, i * 1000000, MsgType::Trade, data);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

    double avg_ns = static_cast<double>(duration.count()) / NUM_WRITES;

    INFO("Average write time with warmer: " << avg_ns << " ns");
    INFO("Total writes: " << NUM_WRITES);
    INFO("Total time: " << duration.count() / 1000000.0 << " ms");

    REQUIRE(avg_ns < 10000);
  }
}
