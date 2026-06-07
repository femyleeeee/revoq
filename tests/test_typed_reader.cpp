#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <thread>
#include <vector>

#include "JournalReader.h"
#include "JournalWriter.h"
#include "TypedJournalDispatcher.h"
#include "test_helpers.h"

using namespace revoq::journal;
using namespace revoq::journal::test;

namespace {

struct TypedTrade {
  static constexpr MsgType kMsgType = MsgType::Trade;

  int64_t timestamp;
  uint32_t id;
  double price;
};

struct TypedBBO {
  static constexpr MsgType kMsgType = MsgType::BBO;

  int64_t timestamp;
  uint32_t bid_size;
  uint32_t ask_size;
};

struct TypedBookReset {
  static constexpr MsgType kMsgType = MsgType::BookReset;
};

struct TinyTrade {
  int64_t timestamp;
  uint32_t id;
};

} // namespace

TEST_CASE("JournalReader typed dispatch invokes registered handlers",
          "[reader][typed]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  constexpr uint32_t dest_id = 0;

  {
    auto writer = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = true});
    TypedTrade trade1{1'000'000, 10, 101.25};
    TypedBBO bbo{2'000'000, 100, 200};
    TypedTrade trade2{3'000'000, 11, 102.50};

    publish(*writer, trade1.timestamp, TypedTrade::kMsgType, trade1);
    publish(*writer, bbo.timestamp, TypedBBO::kMsgType, bbo);
    writer->mark(2'500'000, TypedBookReset::kMsgType);
    publish(*writer, trade2.timestamp, TypedTrade::kMsgType, trade2);
  }

  JournalReader reader(JournalReaderOptions{.prefault = true});
  reader.join(location, dest_id, 0);

  std::vector<uint32_t> trade_ids;
  std::vector<uint64_t> trade_sequences;
  uint32_t total_bbo_size = 0;
  int reset_count = 0;

  TypedJournalDispatcher dispatcher(reader);
  dispatcher
      .on<TypedTrade>([&](const TypedTrade &trade, const Frame &frame) {
        trade_ids.push_back(trade.id);
        trade_sequences.push_back(frame.getSequence());
      })
      .on<TypedBBO>([&](const TypedBBO &bbo) {
        total_bbo_size = bbo.bid_size + bbo.ask_size;
      })
      .on<TypedBookReset>([&](const TypedBookReset &, const Frame &frame) {
        ++reset_count;
        CHECK(frame.getDataLength() == 0);
      });

  CHECK(dispatcher.run() == 4);
  CHECK(trade_ids == std::vector<uint32_t>{10, 11});
  CHECK(trade_sequences == std::vector<uint64_t>{0, 3});
  CHECK(total_bbo_size == 300);
  CHECK(reset_count == 1);
}

TEST_CASE("JournalReader typed dispatch skips unknown message types",
          "[reader][typed]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  constexpr uint32_t dest_id = 1;

  {
    auto writer = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = true});
    TypedTrade trade{1'000'000, 42, 99.0};
    TypedBBO bbo{2'000'000, 5, 7};

    publish(*writer, trade.timestamp, TypedTrade::kMsgType, trade);
    publish(*writer, bbo.timestamp, TypedBBO::kMsgType, bbo);
  }

  JournalReader reader(JournalReaderOptions{.prefault = true});
  reader.join(location, dest_id, 0);

  int trade_count = 0;
  std::vector<int32_t> unknown_types;

  TypedJournalDispatcher dispatcher(reader);
  dispatcher
      .on<TypedTrade>([&](const TypedTrade &trade) {
        ++trade_count;
        CHECK(trade.id == 42);
      })
      .onUnknown([&](const Frame &frame) {
        unknown_types.push_back(frame.getMsgType());
      });

  CHECK(dispatcher.run() == 2);
  CHECK(trade_count == 1);
  REQUIRE(unknown_types.size() == 1);
  CHECK(unknown_types.front() == static_cast<int32_t>(TypedBBO::kMsgType));
}

TEST_CASE("JournalReader typed dispatch rejects mismatched payload sizes",
          "[reader][typed]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  constexpr uint32_t dest_id = 2;

  {
    auto writer = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = true});
    TinyTrade tiny{1'000'000, 7};
    publish(*writer, tiny.timestamp, MsgType::Trade, tiny);
  }

  JournalReader reader(JournalReaderOptions{.prefault = true});
  reader.join(location, dest_id, 0);
  TypedJournalDispatcher dispatcher(reader);
  dispatcher.on<TypedTrade>([](const TypedTrade &) {});

  CHECK_THROWS_AS(dispatcher.next(), std::runtime_error);
}

TEST_CASE("TypedJournalDispatcher dispatches and advances one journal",
          "[reader][typed]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  constexpr uint32_t dest_id = 3;

  {
    auto writer = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = true});
    TypedTrade trade{1'000'000, 17, 103.25};
    TypedBBO bbo{2'000'000, 11, 13};
    publish(*writer, trade.timestamp, TypedTrade::kMsgType, trade);
    publish(*writer, bbo.timestamp, TypedBBO::kMsgType, bbo);
  }

  JournalReader reader(JournalReaderOptions{.prefault = true});
  reader.join(location, dest_id, 0);

  int trade_count = 0;
  int unknown_count = 0;
  TypedJournalDispatcher dispatcher(reader);
  dispatcher
      .on<TypedTrade>([&](const TypedTrade &trade) {
        ++trade_count;
        CHECK(trade.id == 17);
      })
      .onUnknown([&](const Frame &frame) {
        ++unknown_count;
        CHECK(frame.getMsgType() == static_cast<int32_t>(TypedBBO::kMsgType));
      });

  const auto first = reader.tryCurrentFrame();
  REQUIRE(first);
  CHECK(first->getMsgType() == static_cast<int32_t>(TypedTrade::kMsgType));
  CHECK(first.frame_length == sizeof(FrameHeader) + sizeof(TypedTrade));
  CHECK(first.msg_type == static_cast<int32_t>(TypedTrade::kMsgType));

  CHECK(dispatcher.next());
  CHECK(dispatcher.next());
  CHECK_FALSE(dispatcher.next());
  CHECK_FALSE(reader.tryCurrentFrame());
  CHECK(trade_count == 1);
  CHECK(unknown_count == 1);
}

TEST_CASE("JournalReader static fast path invokes concrete dispatcher",
          "[reader][typed][static]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  constexpr uint32_t dest_id = 4;

  {
    auto writer = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = true});
    TypedTrade trade{1'000'000, 23, 104.75};
    TypedBBO bbo{2'000'000, 19, 29};
    publish(*writer, trade.timestamp, TypedTrade::kMsgType, trade);
    publish(*writer, bbo.timestamp, TypedBBO::kMsgType, bbo);
  }

  JournalReader reader(JournalReaderOptions{.prefault = true});
  reader.join(location, dest_id, 0);

  int trade_count = 0;
  int bbo_count = 0;
  auto dispatcher = [&](JournalReader::FrameView current) {
    switch (static_cast<MsgType>(current.msg_type)) {
    case MsgType::Trade:
      ++trade_count;
      CHECK(current->data<TypedTrade>().id == 23);
      break;
    case MsgType::BBO:
      ++bbo_count;
      CHECK(current->data<TypedBBO>().ask_size == 29);
      break;
    default:
      FAIL("unexpected static-dispatch message type");
    }
  };

  CHECK(reader.nextStatic(dispatcher));
  CHECK(reader.nextStatic(dispatcher));
  CHECK_FALSE(reader.nextStatic(dispatcher));
  CHECK(trade_count == 1);
  CHECK(bbo_count == 1);
}

TEST_CASE("JournalReader static fast path clears its single-journal cache",
          "[reader][typed][static]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  constexpr uint32_t first_dest_id = 6;
  constexpr uint32_t second_dest_id = 7;

  {
    auto writer = std::make_shared<JournalWriter<>>(
        location, first_dest_id, JournalWriterOptions{.prefault = true});
    TypedTrade trade{2'000'000, 61, 106.25};
    publish(*writer, trade.timestamp, TypedTrade::kMsgType, trade);
  }
  {
    auto writer = std::make_shared<JournalWriter<>>(
        location, second_dest_id, JournalWriterOptions{.prefault = true});
    TypedTrade trade{1'000'000, 71, 107.25};
    publish(*writer, trade.timestamp, TypedTrade::kMsgType, trade);
  }

  JournalReader reader(JournalReaderOptions{.prefault = true});
  reader.join(location, first_dest_id, 0);
  reader.join(location, second_dest_id, 0);

  uint32_t trade_id = 0;
  auto dispatcher = [&](JournalReader::FrameView current) {
    REQUIRE(current.msg_type == static_cast<int32_t>(TypedTrade::kMsgType));
    trade_id = current->data<TypedTrade>().id;
  };

  CHECK(reader.nextStatic(dispatcher));
  CHECK(trade_id == 71);

  reader.disjoin(location->uid);
  CHECK_FALSE(reader.nextStatic(dispatcher));
}

TEST_CASE("JournalWriter closeData finalizes metadata before publication",
          "[writer][publish][metadata]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  constexpr uint32_t dest_id = 5;
  constexpr int64_t event_time = 9'876'543;

  {
    auto writer = std::make_shared<JournalWriter<>>(
        location, dest_id, JournalWriterOptions{.prefault = true});
    auto &slot = writer->openData<TypedTrade>(1'234'567, TypedTrade::kMsgType);
    slot = TypedTrade{1'234'567, 31, 105.50};
    writer->closeData<TypedTrade>(
        [](FrameHeader &header) { header.event_time = event_time; });
  }

  JournalReader reader(JournalReaderOptions{.prefault = true});
  reader.join(location, dest_id, 0);
  const auto current = reader.tryCurrentFrame();
  REQUIRE(current);
  CHECK(current->getGenTime() == 1'234'567);
  CHECK(current->getEventTime() == event_time);
  CHECK(current->data<TypedTrade>().id == 31);
}
