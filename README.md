# revoq

[![CI](https://github.com/femyleeeee/revoq/actions/workflows/ci.yml/badge.svg)](https://github.com/femyleeeee/revoq/actions/workflows/ci.yml)
[![Release](https://img.shields.io/badge/release-0.1.0-blue.svg)](CHANGELOG.md)
[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg)](#quick-start)
[![Platform](https://img.shields.io/badge/platform-Linux-lightgrey.svg)](#quick-start)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A file-backed, memory-mapped **IPC journal** for low-latency same-host messaging.

## At a glance

- Open-source transport primitive behind [RevoRacer](https://revoracer.com)
  infrastructure.
- Same-host IPC for market data, execution events, strategy signals, and
  telemetry.
- One hot-path writer per journal, many independent readers; slow readers do not
  stall the writer.
- Release/acquire publication: payload first, `length` last.
- C++23 core, optional Python bindings, Linux tuned path, Apache-2.0 license.

## Design limits

revoq targets one latency-sensitive producer publishing committed frames to
independent same-host consumers. The limits are intentional:

- **Not a network transport.** revoq is same-host IPC over mapped files.
- **Not lock-free MPSC.** The multi-writer policy serializes writers.
- **Not a latency guarantee on an untuned box.** Portable mode is for functional
  checks; the tail-latency story requires Linux, isolated cores, and hugepages.
- **Not yet API-stable.** This is `0.1.0`; signatures may still change.

## Architecture

![revoq architecture: a single writer publishes frames into memory-mapped journal pages; independent readers acquire committed frame lengths and read payloads](docs/assets/architecture.svg)

`JournalWriter` appends aligned frames to memory-mapped page files;
`JournalReader` joins journals and reads committed frames. Each frame carries
`length` (published last), message metadata, timestamps, and payload bytes.

See [docs/architecture.md](docs/architecture.md),
[docs/memory_model.md](docs/memory_model.md), and
[docs/file_format.md](docs/file_format.md).

## Correctness

The core publication rule is tested directly: reserve a slot, fill metadata and
payload, release-store `length`; acquire-load `length`, then read the committed
frame.

Tests cover publication ordering, multiprocess handoff, resume/recovery, frame
and reader behavior, hugepages, typed dispatch, and Python round trips.

```bash
CXX=/usr/bin/clang++-20 ./build.sh test
```

## Quick start

Requirements: Linux (recommended for latency work), CMake ≥ 3.23, a C++23
compiler such as Clang 20, and Git submodules for the bundled deps (`fmt`,
`spdlog`, `Catch2`, `pybind11`).

```bash
CXX=/usr/bin/clang++-20 ./build.sh            # build
CXX=/usr/bin/clang++-20 ./build.sh test       # run C++ tests
CXX=/usr/bin/clang++-20 ./build.sh examples   # build C++ examples
CXX=/usr/bin/clang++-20 ./build.sh python     # build + test Python bindings
```

### Write and read (C++)

```cpp
using namespace racer::journal;

struct Trade { std::int64_t ts; std::uint32_t symbol_id; double price; std::uint64_t qty; };

auto locator  = std::make_shared<Locator>(dir);
auto location = Location::make(Mode::LIVE, Category::EXECUTION, "examples", "demo", locator);

// writer: fill the slot, then publish
JournalWriter<> writer(location, /*dest_id=*/0,
    JournalWriterOptions{.prefault = false, .background_threads = false});
auto &slot = writer.openData<Trade>(/*gen_time=*/1, MsgType::Trade);
slot = Trade{1, 101, 100.25, 10};
writer.closeData<Trade>();

// reader: walk committed frames
JournalReader reader(JournalReaderOptions{.prefault = false, .background_threads = false});
reader.join(location, /*dest_id=*/0, 0);
while (const auto frame = reader.tryCurrentFrame()) {
  if (static_cast<MsgType>(frame.msg_type) == MsgType::Trade)
    use(frame->data<Trade>());
  reader.advance(frame);
}
```

### Write and read (Python)

```python
import revoq, struct
location = revoq.make_location(path, mode="live", category="executiondata",
                               group="examples", name="demo")

writer = revoq.JournalWriter(location, 0, prefault=False, background_threads=False)
writer.write(msg_type=100, gen_time=1, data=struct.pack("<Qd", 1, 100.0))
del writer

reader = revoq.JournalReader(prefault=False, background_threads=False)
reader.join(location, 0, 0)
frames = [f for f in reader if f.msg_type == 100]
```

Runnable versions live in [`examples/`](examples). Each C++ example exits nonzero
on failure and prints an `ok ...` line on success.

## Benchmarking

The latency benchmark has two modes, and is deliberate about what each one is
allowed to claim.

The reference tuned run below reports 177 ns p99 in the recommended separate-core
topology, with host details and reproduction scripts included. Low-latency claims
without the machine are not useful.

**Portable mode** — no root, no pinning, no hugepages. Good for functional checks
and rough local comparisons. *Not* a source of host-to-host p99 claims.

```bash
CXX=/usr/bin/clang++-20 ./build.sh benchmarks
./build/revoq_benchmark_journal_handler_benchmark_p99 \
  --messages 1000000 --warmup 50000 --interval-ns 1000
```

**Tuned mode** — Linux only: explicit writer/reader/background CPUs, real-time
priority, a `hugetlbfs` path, and a forked reader process. Before your first
tuned run, prepare the host with `scripts/setup_benchmark_session.sh` — see
[docs/tuning_overview.md](docs/tuning_overview.md).

```bash
sudo ./build/revoq_benchmark_journal_handler_benchmark_p99 \
  --tuned --fork \
  --path /dev/hugepages/revoq-bench \
  --writer-cpu 2 --reader-cpu 4 --background-cpu 6 \
  --rt-priority 80 \
  --messages 100000000 --warmup 50000 --interval-ns 100
```

**What is measured:** writer timestamp to reader handler endpoint, over a
deterministic mix of four 32-byte payloads. The benchmark reports the
distribution — `p50`, `p90`, `p99`, `p999`, `max`, and `mean` in nanoseconds —
plus sample count and a checksum, because in this domain the tail is the number
that matters, not the average.

### Reference results

> [!NOTE]
> These are reference numbers from one host. They are **not** a guarantee for
> yours — the whole point of the tuned harness is that you reproduce them on
> your own hardware. The host configuration is published alongside the numbers
> so the run is auditable.

Host: AWS `r7iz.metal-16xl` · Intel Xeon Gold 6455B · 3.90 GHz · Linux 6.17.0-1017-aws · isolated CPUs 2,4,6,36 · 8GB hugepages · 100M messages · 32-byte payloads · 4 message types · static dispatch · `interval-ns=100`

| configuration | p50 | p90 | p99 | p999 | max | mean |
| --- | --- | --- | --- | --- | --- | --- |
| separate physical cores — writer cpu2, reader cpu4 | 132 ns | 154 ns | 177 ns | 403 ns | 11007 ns | 133 ns |
| SMT siblings — writer cpu4, reader cpu36 | 51 ns | 60 ns | 65 ns | 133 ns | 9961 ns | 50 ns |

The **separate cores** row is the recommended production topology. The SMT row
shows the cache-topology effect, not a real deployment shape. The `max` values
(~10–11 us) are single-spike OS interrupt outliers; the tail story is in `p999`.

Reproduce the run with `./scripts/check_system.sh` and
`./scripts/collect_benchmark_env.sh`, and see
[docs/benchmarking.md](docs/benchmarking.md) and
[docs/tuning_overview.md](docs/tuning_overview.md) for the full methodology.

## Status

Early public release (`0.1.0`): C++23 memory-mapped journal core, C++ tests,
optional Python bindings, and the portable/tuned benchmark. The API may still
change — see [CHANGELOG.md](CHANGELOG.md).

## Commercial

revoq is open source so teams can inspect the memory model, file format,
benchmarks, and failure semantics. RevoRacer builds low-latency trading
infrastructure for hedge funds and trading teams.

We can help with revoq integration, benchmark reproduction on your hardware,
Linux host tuning, feed-handler and execution-gateway paths, and custom same-host
transport designs.

Contact us through [revoracer.com](https://revoracer.com). Details:
[docs/commercial_support.md](docs/commercial_support.md).

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
