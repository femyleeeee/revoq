// Minimal application-path latency benchmark for revoq journal dispatch.
//
// This public benchmark uses portable defaults rather than machine-specific
// tuning. It measures the path from writer timestamp to reader handler endpoint
// using a deterministic mix of small fixed-size messages.

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <numeric>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <vector>

#ifdef __linux__
#include <csignal>
#include <linux/magic.h>
#include <sys/mman.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include "CpuAffinity.h"
#include "JournalBase.h"
#include "JournalFrame.h"
#include "JournalReader.h"
#include "JournalWriter.h"
#include "RealtimePriority.h"
#include "TimeUtils.h"
#include "portable_intrinsics.h"

#ifndef HUGETLBFS_MAGIC
#define HUGETLBFS_MAGIC 0x958458f6
#endif

namespace {

using revoq::Timestamp;
using namespace revoq::journal;

constexpr std::uint32_t kDestId = 0;
constexpr std::uint64_t kDefaultMessages = 1'000'000;
constexpr std::uint64_t kDefaultWarmupMessages = 50'000;
constexpr std::uint64_t kDefaultIntervalNs = 1'000;
constexpr std::size_t kTunedWriterWarmupPages = 8;
constexpr std::size_t kTunedWriterRetainedPages = 10;

template <std::uint64_t Tag> struct BenchMessage {
  std::uint64_t sequence;
  std::uint64_t value0;
  std::uint64_t value1;
  std::uint64_t value2;
};

using TradeMessage = BenchMessage<0>;
using MarketDepth20Message = BenchMessage<1>;
using BboMessage = BenchMessage<2>;
using BookResetMessage = BenchMessage<3>;

static_assert(sizeof(TradeMessage) == 32);
static_assert(std::is_trivially_copyable_v<TradeMessage>);
static_assert(framePhysicalLength(sizeof(FrameHeader) + sizeof(TradeMessage)) ==
              FRAME_ALIGNMENT);

struct BenchmarkConfig {
  std::uint64_t messages{kDefaultMessages};
  std::uint64_t warmup_messages{kDefaultWarmupMessages};
  std::uint64_t interval_ns{kDefaultIntervalNs};
  std::filesystem::path path;
  bool keep_files{false};
  bool path_set{false};
  bool tuned{false};
  bool fork_reader{false};
  int writer_cpu{-1};
  int reader_cpu{-1};
  int background_cpu{-1};
  int rt_priority{80};
};

struct LatencyStats {
  std::uint64_t samples{0};
  std::int64_t min_ns{0};
  std::int64_t p50_ns{0};
  std::int64_t p90_ns{0};
  std::int64_t p99_ns{0};
  std::int64_t p999_ns{0};
  std::int64_t max_ns{0};
  double mean_ns{0.0};
};

struct BenchmarkResult {
  LatencyStats latency;
  std::uint64_t handled_messages{0};
  std::uint64_t checksum{0};
};
static_assert(std::is_trivially_copyable_v<BenchmarkResult>);

std::filesystem::path defaultPath() {
  const char *tmpdir = std::getenv("TMPDIR");
  const auto base =
      tmpdir ? std::filesystem::path(tmpdir) : std::filesystem::path("/tmp");
  const auto stamp =
      std::chrono::steady_clock::now().time_since_epoch().count();
  return base / fmt::format("revoq-journal-handler-p99-{}", stamp);
}

std::uint64_t parseU64(std::string_view text, std::string_view name) {
  std::uint64_t value = 0;
  const char *begin = text.data();
  const char *end = begin + text.size();
  const auto parsed = std::from_chars(begin, end, value);
  if (text.empty() || parsed.ec != std::errc{} || parsed.ptr != end) {
    throw std::invalid_argument(
        fmt::format("{} expects an unsigned integer", name));
  }
  return value;
}

int parseInt(std::string_view text, std::string_view name, int min_value,
             int max_value) {
  int value = 0;
  const char *begin = text.data();
  const char *end = begin + text.size();
  const auto parsed = std::from_chars(begin, end, value);
  if (text.empty() || parsed.ec != std::errc{} || parsed.ptr != end ||
      value < min_value || value > max_value) {
    throw std::invalid_argument(fmt::format("{} expects an integer in [{}, {}]",
                                            name, min_value, max_value));
  }
  return value;
}

std::string_view readOptionValue(std::string_view arg, std::string_view name,
                                 int argc, char **argv, int &index) {
  if (arg == name) {
    if (index + 1 >= argc)
      throw std::invalid_argument(fmt::format("{} requires a value", name));
    return argv[++index];
  }

  const std::string prefix = std::string(name) + "=";
  if (arg.starts_with(prefix))
    return arg.substr(prefix.size());

  return {};
}

bool optionMatches(std::string_view arg, std::string_view name) {
  const std::string prefix = std::string(name) + "=";
  return arg == name || arg.starts_with(prefix);
}

void printUsage(const char *exe) {
  fmt::print(stderr,
             "Usage: {} [--messages N] [--warmup N] [--interval-ns N] "
             "[--path DIR] [--keep-files]\n",
             exe);
  fmt::print(stderr,
             "       {} --tuned --path DIR --writer-cpu N --reader-cpu N "
             "--background-cpu N [--rt-priority N] [--fork]\n",
             exe);
  fmt::print(stderr, "Defaults: --messages {} --warmup {} --interval-ns {}\n",
             kDefaultMessages, kDefaultWarmupMessages, kDefaultIntervalNs);
  fmt::print(stderr,
             "Use --interval-ns 0 for an unpaced writer. The default path is "
             "a temporary directory under /tmp.\n");
  fmt::print(stderr,
             "Tuned mode requires Linux, a hugetlbfs --path, explicit CPU "
             "assignments, and usually root or CAP_SYS_NICE.\n");
}

BenchmarkConfig parseArgs(int argc, char **argv) {
  BenchmarkConfig cfg;
  cfg.path = defaultPath();

  for (int i = 1; i < argc; ++i) {
    const std::string_view arg(argv[i]);

    if (arg == "--help" || arg == "-h") {
      printUsage(argv[0]);
      std::exit(0);
    }
    if (arg == "--keep-files") {
      cfg.keep_files = true;
      continue;
    }
    if (arg == "--tuned") {
      cfg.tuned = true;
      cfg.fork_reader = true;
      continue;
    }
    if (arg == "--fork") {
      cfg.fork_reader = true;
      continue;
    }
    if (optionMatches(arg, "--messages")) {
      cfg.messages = parseU64(readOptionValue(arg, "--messages", argc, argv, i),
                              "--messages");
      if (cfg.messages == 0)
        throw std::invalid_argument("--messages must be greater than zero");
      continue;
    }
    if (optionMatches(arg, "--warmup")) {
      cfg.warmup_messages =
          parseU64(readOptionValue(arg, "--warmup", argc, argv, i), "--warmup");
      continue;
    }
    if (optionMatches(arg, "--interval-ns")) {
      cfg.interval_ns =
          parseU64(readOptionValue(arg, "--interval-ns", argc, argv, i),
                   "--interval-ns");
      continue;
    }
    if (optionMatches(arg, "--path")) {
      cfg.path = std::filesystem::path(
          std::string(readOptionValue(arg, "--path", argc, argv, i)));
      cfg.path_set = true;
      continue;
    }
    if (optionMatches(arg, "--writer-cpu")) {
      cfg.writer_cpu =
          parseInt(readOptionValue(arg, "--writer-cpu", argc, argv, i),
                   "--writer-cpu", 0, 4096);
      continue;
    }
    if (optionMatches(arg, "--reader-cpu")) {
      cfg.reader_cpu =
          parseInt(readOptionValue(arg, "--reader-cpu", argc, argv, i),
                   "--reader-cpu", 0, 4096);
      continue;
    }
    if (optionMatches(arg, "--background-cpu")) {
      cfg.background_cpu =
          parseInt(readOptionValue(arg, "--background-cpu", argc, argv, i),
                   "--background-cpu", 0, 4096);
      continue;
    }
    if (optionMatches(arg, "--rt-priority")) {
      cfg.rt_priority =
          parseInt(readOptionValue(arg, "--rt-priority", argc, argv, i),
                   "--rt-priority", 1, 99);
      continue;
    }

    throw std::invalid_argument(fmt::format("unknown argument: {}", arg));
  }

  if (!cfg.tuned) {
    if (cfg.fork_reader || cfg.writer_cpu >= 0 || cfg.reader_cpu >= 0 ||
        cfg.background_cpu >= 0) {
      throw std::invalid_argument(
          "--fork and CPU options are tuned-mode options; add --tuned");
    }
    return cfg;
  }

#ifndef __linux__
  throw std::invalid_argument("--tuned is currently supported only on Linux");
#endif

  cfg.fork_reader = true;
  if (!cfg.path_set) {
    throw std::invalid_argument(
        "--tuned requires an explicit --path on hugetlbfs, for example "
        "--path /dev/hugepages/revoq-bench");
  }
  if (cfg.writer_cpu < 0 || cfg.reader_cpu < 0 || cfg.background_cpu < 0) {
    throw std::invalid_argument(
        "--tuned requires --writer-cpu, --reader-cpu, and --background-cpu");
  }
  if (cfg.writer_cpu == cfg.reader_cpu ||
      cfg.writer_cpu == cfg.background_cpu ||
      cfg.reader_cpu == cfg.background_cpu) {
    throw std::invalid_argument(
        "tuned CPUs must be distinct: writer, reader, and background");
  }
  return cfg;
}

void validateTunedPath(const std::filesystem::path &path) {
#ifdef __linux__
  struct statfs fs{};
  if (statfs(path.c_str(), &fs) != 0) {
    throw std::runtime_error(
        fmt::format("statfs({}) failed: {}", path.string(), strerror(errno)));
  }
  if (static_cast<unsigned long>(fs.f_type) !=
      static_cast<unsigned long>(HUGETLBFS_MAGIC)) {
    throw std::runtime_error(fmt::format(
        "{} is not on hugetlbfs. Mount huge pages first, for example "
        "at /dev/hugepages, then pass a subdirectory with --path.",
        path.string()));
  }
#else
  (void)path;
  throw std::runtime_error("tuned benchmark path validation requires Linux");
#endif
}

struct BenchmarkFixture {
  explicit BenchmarkFixture(const BenchmarkConfig &cfg)
      : path(cfg.path), keep_files(cfg.keep_files) {
    if (std::filesystem::exists(path) && !std::filesystem::is_directory(path)) {
      throw std::runtime_error(
          fmt::format("benchmark path is not a directory: {}", path.string()));
    }
    if (std::filesystem::exists(path) && !std::filesystem::is_empty(path)) {
      throw std::runtime_error(fmt::format(
          "benchmark path is not empty: {}. Pick a new --path or remove it.",
          path.string()));
    }

    std::filesystem::create_directories(path);
    try {
      if (cfg.tuned)
        validateTunedPath(path);
      locator = std::make_shared<Locator>(path.string());
      location = Location::make(Mode::LIVE, Category::MARKETDATA, "bench",
                                "journal-handler-p99", locator);
    } catch (...) {
      if (!keep_files) {
        std::error_code ignored;
        std::filesystem::remove_all(path, ignored);
      }
      throw;
    }
  }

  ~BenchmarkFixture() {
    if (!keep_files) {
      std::error_code ignored;
      std::filesystem::remove_all(path, ignored);
    }
  }

  std::filesystem::path path;
  bool keep_files{false};
  ILocatorPtr locator;
  LocationPtr location;
};

std::uint64_t pseudoRandom(std::uint64_t value) {
  value ^= value >> 30U;
  value *= 0xbf58476d1ce4e5b9ULL;
  value ^= value >> 27U;
  value *= 0x94d049bb133111ebULL;
  return value ^ (value >> 31U);
}

MsgType messageTypeFor(std::uint64_t sequence) {
  static constexpr std::array<MsgType, 4> types{
      MsgType::Trade,
      MsgType::MarketDepth20,
      MsgType::BBO,
      MsgType::BookReset,
  };
  return types[pseudoRandom(sequence) & 3U];
}

template <typename T>
void writeTypedFrame(JournalWriter<> &writer, std::int64_t gen_time,
                     MsgType msg_type, std::uint64_t sequence) {
  auto &message = writer.openData<T>(gen_time, msg_type);
  message = T{
      .sequence = sequence,
      .value0 = sequence ^ 0x9e3779b97f4a7c15ULL,
      .value1 = sequence + 0xbf58476d1ce4e5b9ULL,
      .value2 = sequence * 0x94d049bb133111ebULL,
  };
  writer.closeData<T>();
}

void writeFrame(JournalWriter<> &writer, std::uint64_t sequence) {
  const auto msg_type = messageTypeFor(sequence);
  const auto gen_time = Timestamp::now().nsec();

  switch (msg_type) {
  case MsgType::Trade:
    return writeTypedFrame<TradeMessage>(writer, gen_time, msg_type, sequence);
  case MsgType::MarketDepth20:
    return writeTypedFrame<MarketDepth20Message>(writer, gen_time, msg_type,
                                                 sequence);
  case MsgType::BBO:
    return writeTypedFrame<BboMessage>(writer, gen_time, msg_type, sequence);
  case MsgType::BookReset:
    return writeTypedFrame<BookResetMessage>(writer, gen_time, msg_type,
                                             sequence);
  default:
    throw std::runtime_error("unsupported benchmark message type");
  }
}

void prepareLatencyBuffer(std::vector<std::int64_t> &latencies,
                          std::uint64_t samples, bool prefault);

class HandlerCollector {
public:
  HandlerCollector(std::uint64_t warmup_messages, std::uint64_t messages,
                   bool prefault_latencies)
      : warmup_messages_(warmup_messages),
        prefault_latencies_(prefault_latencies) {
    prepareLatencyBuffer(latencies_, messages, prefault_latencies_);
  }

  template <typename T> void handle(const T &message, const Frame &frame) {
    checksum_ = (checksum_ << 7U) ^ (checksum_ >> 3U) ^ message.sequence ^
                message.value0 ^ message.value1 ^ message.value2;
    std::atomic_signal_fence(std::memory_order_seq_cst);

    if (handled_messages_ >= warmup_messages_) {
      const auto latency = Timestamp::now().nsec() - frame.getGenTime();
      if (latency >= 0 && latency < 1'000'000'000LL) {
        if (prefault_latencies_) {
          if (latency_count_ < latencies_.size())
            latencies_[latency_count_++] = latency;
        } else {
          latencies_.push_back(latency);
        }
      }
    }
    ++handled_messages_;
  }

  [[nodiscard]] std::uint64_t handledMessages() const {
    return handled_messages_;
  }

  [[nodiscard]] std::uint64_t checksum() const { return checksum_; }

  std::vector<std::int64_t> takeLatencies() {
    if (prefault_latencies_)
      latencies_.resize(latency_count_);
    return std::move(latencies_);
  }

private:
  const std::uint64_t warmup_messages_;
  const bool prefault_latencies_;
  std::vector<std::int64_t> latencies_;
  std::uint64_t handled_messages_{0};
  std::size_t latency_count_{0};
  std::uint64_t checksum_{0};
};

class StaticDispatcher {
public:
  explicit StaticDispatcher(HandlerCollector &collector)
      : collector_(collector) {}

  void operator()(JournalReader::FrameView current) {
    switch (static_cast<MsgType>(current.msg_type)) {
    case MsgType::Trade:
      return handle<TradeMessage>(current);
    case MsgType::MarketDepth20:
      return handle<MarketDepth20Message>(current);
    case MsgType::BBO:
      return handle<BboMessage>(current);
    case MsgType::BookReset:
      return handle<BookResetMessage>(current);
    case MsgType::PageEnd:
      return;
    default:
      throw std::runtime_error("unexpected frame type in benchmark");
    }
  }

private:
  template <typename T> void handle(JournalReader::FrameView current) {
    if (current->getDataLength(current.frame_length) != sizeof(T)) {
      throw std::runtime_error("unexpected payload size in benchmark");
    }
    collector_.handle(current->data<T>(), *current);
  }

  HandlerCollector &collector_;
};

std::int64_t percentile(const std::vector<std::int64_t> &values,
                        std::uint64_t numerator, std::uint64_t denominator) {
  if (values.empty())
    return 0;
  std::uint64_t index = (static_cast<std::uint64_t>(values.size()) * numerator +
                         denominator - 1) /
                            denominator -
                        1;
  index = std::min<std::uint64_t>(index, values.size() - 1);
  return values[static_cast<std::size_t>(index)];
}

LatencyStats computeStats(std::vector<std::int64_t> values) {
  LatencyStats stats{};
  if (values.empty())
    return stats;

  std::sort(values.begin(), values.end());
  stats.samples = values.size();
  stats.min_ns = values.front();
  stats.p50_ns = percentile(values, 50, 100);
  stats.p90_ns = percentile(values, 90, 100);
  stats.p99_ns = percentile(values, 99, 100);
  stats.p999_ns = percentile(values, 999, 1000);
  stats.max_ns = values.back();
  const auto sum = std::accumulate(values.begin(), values.end(), 0.0L);
  stats.mean_ns = static_cast<double>(sum / values.size());
  return stats;
}

void prepareLatencyBuffer(std::vector<std::int64_t> &latencies,
                          std::uint64_t samples, bool prefault) {
  const auto count = static_cast<std::size_t>(samples);
  if (!prefault) {
    latencies.reserve(count);
    return;
  }

  latencies.resize(count);
  if (latencies.empty())
    return;

#ifdef __linux__
  const std::size_t bytes = latencies.size() * sizeof(latencies[0]);
  const long sys_page_size = sysconf(_SC_PAGESIZE);
  const auto page_size =
      static_cast<std::uintptr_t>(sys_page_size > 0 ? sys_page_size : 4096);
  const auto begin = reinterpret_cast<std::uintptr_t>(latencies.data());
  const auto end = begin + bytes;
  const auto aligned_begin = begin - (begin % page_size);
  const auto aligned_end = ((end + page_size - 1) / page_size) * page_size;
  const auto advise_size =
      static_cast<std::size_t>(aligned_end - aligned_begin);
  if (madvise(reinterpret_cast<void *>(aligned_begin), advise_size,
              MADV_NOHUGEPAGE) != 0) {
    fmt::print(stderr, "reader: MADV_NOHUGEPAGE latency buffer failed: {}\n",
               strerror(errno));
  }
#endif

  constexpr std::size_t kStride = 4096 / sizeof(std::int64_t);
  for (std::size_t i = 0; i < latencies.size(); i += kStride)
    latencies[i] = 0;
  latencies.back() = 0;

#ifdef __linux__
  if (mlock(latencies.data(), bytes) != 0) {
    fmt::print(stderr, "reader: mlock latency buffer failed: {}\n",
               strerror(errno));
  }
#endif
}

enum class BenchmarkRole { Writer, Reader };

const char *roleName(BenchmarkRole role) {
  return role == BenchmarkRole::Writer ? "writer" : "reader";
}

int roleCpu(const BenchmarkConfig &cfg, BenchmarkRole role) {
  return role == BenchmarkRole::Writer ? cfg.writer_cpu : cfg.reader_cpu;
}

void applyTunedSetup(const BenchmarkConfig &cfg, BenchmarkRole role) {
  if (!cfg.tuned)
    return;

  const int cpu = roleCpu(cfg, role);
  if (!revoq::system::CpuAffinity::pinToCore(cpu)) {
    throw std::runtime_error(
        fmt::format("{} failed to pin to CPU {}", roleName(role), cpu));
  }

  revoq::system::RealtimePriority::prefaultStack();
  if (!revoq::system::RealtimePriority::lockCurrentMemory()) {
    throw std::runtime_error(fmt::format(
        "{} failed to lock current memory; check ulimit -l", roleName(role)));
  }
  if (!revoq::system::RealtimePriority::setRealtimePriority(
          cfg.rt_priority, revoq::system::SchedulerPolicy::FIFO)) {
    throw std::runtime_error(fmt::format(
        "{} failed to set real-time priority {}; run as root or grant "
        "CAP_SYS_NICE",
        roleName(role), cfg.rt_priority));
  }
}

JournalWriterOptions writerOptions(const BenchmarkConfig &cfg) {
  if (!cfg.tuned) {
    return JournalWriterOptions{
        .prefault = false,
        .warmup_pages = 0,
        .warmer_cpu_core = -1,
        .background_threads = false,
        .retained_page_limit = 2,
    };
  }

  return JournalWriterOptions{
      .prefault = true,
      .warmup_pages = kTunedWriterWarmupPages,
      .warmer_cpu_core = cfg.background_cpu,
      .background_threads = true,
      .retained_page_limit = kTunedWriterRetainedPages,
  };
}

JournalReaderOptions readerOptions(const BenchmarkConfig &cfg) {
  if (!cfg.tuned) {
    return JournalReaderOptions{
        .prefault = false,
        .background_threads = false,
        .retained_page_limit = 0,
        .warmer_timeout_ms = 0,
        .background_worker_cpu = -1,
    };
  }

  return JournalReaderOptions{
      .prefault = false,
      .background_threads = true,
      .retained_page_limit = 0,
      .warmer_timeout_ms = 5000,
      .background_worker_cpu = cfg.background_cpu,
  };
}

void runWriter(JournalWriter<> &writer, const BenchmarkConfig &cfg,
               const std::atomic<bool> &reader_ready,
               const std::atomic<bool> &stop_requested) {
  while (!reader_ready.load(std::memory_order_acquire) &&
         !stop_requested.load(std::memory_order_acquire)) {
    PORTABLE_PAUSE();
  }
  if (stop_requested.load(std::memory_order_acquire))
    return;

  const auto total_messages = cfg.warmup_messages + cfg.messages;
  auto next_write_time = Timestamp::now().nsec();
  for (std::uint64_t i = 0; i < total_messages; ++i) {
    if (stop_requested.load(std::memory_order_acquire))
      return;
    if (cfg.interval_ns > 0) {
      while (Timestamp::now().nsec() < next_write_time &&
             !stop_requested.load(std::memory_order_acquire)) {
        PORTABLE_PAUSE();
      }
      next_write_time += static_cast<std::int64_t>(cfg.interval_ns);
    }
    writeFrame(writer, i);
  }
}

BenchmarkResult runReader(const LocationPtr &location,
                          const BenchmarkConfig &cfg,
                          std::atomic<bool> &reader_ready,
                          const std::atomic<bool> &writer_done,
                          const std::atomic<bool> &stop_requested) {
  applyTunedSetup(cfg, BenchmarkRole::Reader);
  JournalReader reader(readerOptions(cfg));
  reader.join(location, kDestId, 0);

  HandlerCollector collector(cfg.warmup_messages, cfg.messages, cfg.tuned);
  StaticDispatcher dispatcher(collector);
  const auto total_messages = cfg.warmup_messages + cfg.messages;
  reader_ready.store(true, std::memory_order_release);

  while (collector.handledMessages() < total_messages) {
    if (reader.nextStatic(dispatcher))
      continue;
    if (writer_done.load(std::memory_order_acquire) ||
        stop_requested.load(std::memory_order_acquire)) {
      throw std::runtime_error("reader stopped before consuming all messages");
    }
    PORTABLE_PAUSE();
  }

  auto latencies = collector.takeLatencies();
  BenchmarkResult result{
      .latency = computeStats(std::move(latencies)),
      .handled_messages = collector.handledMessages(),
      .checksum = collector.checksum(),
  };
  return result;
}

#ifdef __linux__
void closeFd(int &fd) noexcept {
  if (fd >= 0) {
    close(fd);
    fd = -1;
  }
}

bool writeAll(int fd, const void *data, std::size_t size) {
  const auto *bytes = static_cast<const char *>(data);
  std::size_t written = 0;
  while (written < size) {
    const ssize_t n = write(fd, bytes + written, size - written);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      return false;
    written += static_cast<std::size_t>(n);
  }
  return true;
}

bool readAll(int fd, void *data, std::size_t size) {
  auto *bytes = static_cast<char *>(data);
  std::size_t received = 0;
  while (received < size) {
    const ssize_t n = read(fd, bytes + received, size - received);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      return false;
    received += static_cast<std::size_t>(n);
  }
  return true;
}

void writeSignal(int fd) {
  const char ready = 'R';
  if (!writeAll(fd, &ready, sizeof(ready)))
    throw std::runtime_error("failed to write pipe signal");
}

bool readSignal(int fd) {
  char ready = 0;
  return readAll(fd, &ready, sizeof(ready)) && ready == 'R';
}

template <typename T> void writePipeValue(int fd, const T &value) {
  static_assert(std::is_trivially_copyable_v<T>);
  if (!writeAll(fd, &value, sizeof(value)))
    throw std::runtime_error("failed to write pipe value");
}

template <typename T> T readPipeValue(int fd) {
  static_assert(std::is_trivially_copyable_v<T>);
  T value{};
  if (!readAll(fd, &value, sizeof(value)))
    throw std::runtime_error("failed to read pipe value");
  return value;
}

BenchmarkResult runReaderProcess(const LocationPtr &location,
                                 const BenchmarkConfig &cfg, int ready_fd) {
  applyTunedSetup(cfg, BenchmarkRole::Reader);
  JournalReader reader(readerOptions(cfg));
  reader.join(location, kDestId, 0);

  HandlerCollector collector(cfg.warmup_messages, cfg.messages, cfg.tuned);
  StaticDispatcher dispatcher(collector);
  const auto total_messages = cfg.warmup_messages + cfg.messages;
  writeSignal(ready_fd);
  closeFd(ready_fd);

  std::uint64_t no_data_spins = 0;
  constexpr std::uint64_t kMaxNoDataSpins = 500'000'000;
  while (collector.handledMessages() < total_messages) {
    if (reader.nextStatic(dispatcher)) {
      no_data_spins = 0;
      continue;
    }
    if (++no_data_spins >= kMaxNoDataSpins) {
      throw std::runtime_error("reader timed out waiting for messages");
    }
    PORTABLE_PAUSE();
  }

  auto latencies = collector.takeLatencies();
  return BenchmarkResult{
      .latency = computeStats(std::move(latencies)),
      .handled_messages = collector.handledMessages(),
      .checksum = collector.checksum(),
  };
}

BenchmarkResult runForkedBenchmark(const BenchmarkConfig &cfg) {
  BenchmarkFixture fixture(cfg);

  int start_pipe[2] = {-1, -1};
  int ready_pipe[2] = {-1, -1};
  int result_pipe[2] = {-1, -1};
  if (pipe(start_pipe) != 0 || pipe(ready_pipe) != 0 ||
      pipe(result_pipe) != 0) {
    closeFd(start_pipe[0]);
    closeFd(start_pipe[1]);
    closeFd(ready_pipe[0]);
    closeFd(ready_pipe[1]);
    closeFd(result_pipe[0]);
    closeFd(result_pipe[1]);
    throw std::runtime_error(fmt::format("pipe failed: {}", strerror(errno)));
  }

  std::fflush(stdout);
  const pid_t pid = fork();
  if (pid < 0) {
    closeFd(start_pipe[0]);
    closeFd(start_pipe[1]);
    closeFd(ready_pipe[0]);
    closeFd(ready_pipe[1]);
    closeFd(result_pipe[0]);
    closeFd(result_pipe[1]);
    throw std::runtime_error(fmt::format("fork failed: {}", strerror(errno)));
  }

  if (pid == 0) {
    closeFd(start_pipe[1]);
    closeFd(ready_pipe[0]);
    closeFd(result_pipe[0]);
    try {
      if (!readSignal(start_pipe[0]))
        throw std::runtime_error("reader did not receive writer-ready signal");
      closeFd(start_pipe[0]);
      const auto result =
          runReaderProcess(fixture.location, cfg, ready_pipe[1]);
      writePipeValue(result_pipe[1], result);
      closeFd(result_pipe[1]);
      std::fflush(stdout);
      std::fflush(stderr);
      _exit(0);
    } catch (const std::exception &e) {
      fmt::print(stderr, "reader error: {}\n", e.what());
      std::fflush(stderr);
      _exit(1);
    }
  }

  closeFd(start_pipe[0]);
  closeFd(ready_pipe[1]);
  closeFd(result_pipe[1]);

  bool child_reaped = false;
  try {
    applyTunedSetup(cfg, BenchmarkRole::Writer);
    JournalWriter<> writer(fixture.location, kDestId, writerOptions(cfg));
    writeSignal(start_pipe[1]);
    closeFd(start_pipe[1]);
    if (!readSignal(ready_pipe[0]))
      throw std::runtime_error("reader did not signal ready");
    closeFd(ready_pipe[0]);

    std::atomic<bool> reader_ready{true};
    std::atomic<bool> stop_requested{false};
    runWriter(writer, cfg, reader_ready, stop_requested);

    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
      throw std::runtime_error(
          fmt::format("waitpid failed: {}", strerror(errno)));
    }
    child_reaped = true;
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
      throw std::runtime_error("reader process failed");
    }

    auto result = readPipeValue<BenchmarkResult>(result_pipe[0]);
    closeFd(result_pipe[0]);
    if (cfg.keep_files)
      fmt::print("journal_path={}\n", fixture.path.string());
    return result;
  } catch (...) {
    closeFd(start_pipe[1]);
    closeFd(ready_pipe[0]);
    closeFd(result_pipe[0]);
    if (!child_reaped) {
      kill(pid, SIGTERM);
      waitpid(pid, nullptr, 0);
    }
    throw;
  }
}
#endif

BenchmarkResult runBenchmark(const BenchmarkConfig &cfg) {
#ifdef __linux__
  if (cfg.tuned && cfg.fork_reader)
    return runForkedBenchmark(cfg);
#endif

  BenchmarkFixture fixture(cfg);

  if (cfg.tuned)
    applyTunedSetup(cfg, BenchmarkRole::Writer);
  JournalWriter<> writer(fixture.location, kDestId, writerOptions(cfg));

  std::atomic<bool> reader_ready{false};
  std::atomic<bool> writer_done{false};
  std::atomic<bool> stop_requested{false};
  std::exception_ptr writer_error;
  std::exception_ptr reader_error;
  BenchmarkResult result;

  std::thread reader_thread([&] {
    try {
      result = runReader(fixture.location, cfg, reader_ready, writer_done,
                         stop_requested);
    } catch (...) {
      reader_error = std::current_exception();
      stop_requested.store(true, std::memory_order_release);
      reader_ready.store(true, std::memory_order_release);
    }
  });

  std::thread writer_thread([&] {
    try {
      runWriter(writer, cfg, reader_ready, stop_requested);
    } catch (...) {
      writer_error = std::current_exception();
      stop_requested.store(true, std::memory_order_release);
    }
    writer_done.store(true, std::memory_order_release);
  });

  writer_thread.join();
  reader_thread.join();

  if (writer_error)
    std::rethrow_exception(writer_error);
  if (reader_error)
    std::rethrow_exception(reader_error);

  if (cfg.keep_files)
    fmt::print("journal_path={}\n", fixture.path.string());

  return result;
}

void printSummary(const BenchmarkConfig &cfg, const BenchmarkResult &result) {
  const auto &s = result.latency;
  const double rate_mps = cfg.interval_ns == 0
                              ? 0.0
                              : 1000.0 / static_cast<double>(cfg.interval_ns);

  fmt::print("benchmark=revoq_journal_handler_p99\n");
  fmt::print("mode={}\n", cfg.tuned ? "tuned" : "portable");
  if (cfg.tuned) {
    fmt::print("path={} writer_cpu={} reader_cpu={} background_cpu={} "
               "rt_priority={} fork={}\n",
               cfg.path.string(), cfg.writer_cpu, cfg.reader_cpu,
               cfg.background_cpu, cfg.rt_priority,
               cfg.fork_reader ? "yes" : "no");
  }
  fmt::print("messages={} warmup={} interval_ns={}", cfg.messages,
             cfg.warmup_messages, cfg.interval_ns);
  if (cfg.interval_ns > 0)
    fmt::print(" rate_mps={:.3f}", rate_mps);
  fmt::print("\n");
  fmt::print("payload_bytes=32 message_types=4 dispatch=static_switch\n");
  fmt::print("samples={} min_ns={} p50_ns={} p90_ns={} p99_ns={} p999_ns={} "
             "max_ns={} mean_ns={:.1f}\n",
             s.samples, s.min_ns, s.p50_ns, s.p90_ns, s.p99_ns, s.p999_ns,
             s.max_ns, s.mean_ns);
  fmt::print("handled_messages={} checksum={}\n", result.handled_messages,
             result.checksum);
}

} // namespace

int main(int argc, char **argv) {
  try {
    spdlog::set_level(spdlog::level::warn);

    const auto cfg = parseArgs(argc, argv);
    const auto result = runBenchmark(cfg);
    printSummary(cfg, result);
    return 0;
  } catch (const std::exception &e) {
    fmt::print(stderr, "error: {}\n", e.what());
    printUsage(argv[0]);
    return 1;
  }
}
