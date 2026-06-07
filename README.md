# revoq

[![CI](https://github.com/femyleeeee/revoq/actions/workflows/ci.yml/badge.svg)](https://github.com/femyleeeee/revoq/actions/workflows/ci.yml)
[![Release](https://img.shields.io/badge/release-0.1.0-blue.svg)](CHANGELOG.md)
[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg)](#quick-start)
[![Platform](https://img.shields.io/badge/platform-Linux-lightgrey.svg)](#quick-start)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A file-backed, memory-mapped **journal** for handing off messages from a single
writer to multiple independent reader processes on the same host, with bounded
and predictable tail latency.

revoq is the transport primitive [RevoRacer](https://revoracer.com) uses to move
market-data and execution-style messages between processes. It is compact, C++23,
and built around one idea: publish a frame with release semantics, read it with
acquire semantics, and keep the hot path free of locks and allocation.

## The problem it solves

Passing messages between processes is easy. Passing them with a tail latency you
can put in front of a trading desk is not. revoq is built around the constraints
that actually shape that tail:

- **Single producer per journal.** One writer owns a journal and appends frames.
  A serialized multi-writer policy exists, but the design target — and the
  numbers — are for the single-writer hot path.
- **Independent reader processes.** Readers are separate OS processes that join a
  journal and track their own position. A slow, restarting, or crashed reader
  does not stall the writer.
- **Memory-mapped pages.** Journals are page files mapped into each process.
  Handoff is a store and a load against shared memory, not a syscall per message.
- **Explicit publication.** The writer fills a frame, then publishes its length
  last with `memory_order_release`. The reader acquires the length before it
  touches metadata or payload. Visibility is a stated rule, not an accident.
- **No silent host reconfiguration.** The tuned path exposes CPU pinning,
  real-time priority, and hugepages as explicit knobs. revoq never quietly
  changes your machine to flatter a benchmark.

## What it is *not*

> [!IMPORTANT]
> The limits below matter more than the feature list. If one of them is a
> dealbreaker for you, revoq is the wrong tool — and we would rather you know now.

- **Not a network transport.** revoq is same-host IPC over mapped files. It does
  not cross machines.
- **Not multi-producer-first.** If you need a lock-free MPSC queue, this is the
  wrong tool. The multi-writer policy serializes writers; it is not the design
  center.
- **Not magic on an untuned box.** Portable mode runs anywhere and is fine for
  functional checks, but the tail-latency story requires Linux, isolated cores,
  and hugepages. revoq makes that requirement explicit rather than hiding it.
- **Not yet API-stable.** This is `0.1.0`. The core is exercised by tests, but
  signatures may still change.

## Architecture

```
  writer process                                reader processes
  (pinned core)                                 (independent, same host)

  ┌───────────────┐                           ┌───────────────┐
  │ JournalWriter │                           │ JournalReader │  ← reader 0
  └───────┬───────┘                           └───────▲───────┘
          │                                           │
          │  ┌──────────────────────────────────────┐│         ┌───────────────┐
          └─▶│  memory-mapped journal pages          ├┴────────▶│ JournalReader │  ← reader N
             │  [ PageHeader │ frame │ frame │ … ]    │          └───────────────┘
             └──────────────────────────────────────┘
            ① release-store the frame length         ② acquire-load the frame length
               (publish: payload first, length last)    (read metadata + payload only once committed)
```

- `JournalWriter` appends frames to a destination journal.
- `JournalReader` joins one or more journals and reads committed frames.
- A frame carries `length` (published last), `msg_type`, `flags`, `sequence`,
  `gen_time`, an optional `event_time`, and payload bytes.
- Pages have a fixed header and `FRAME_ALIGNMENT`-aligned frames; the writer
  drops a `PageEnd` sentinel when it rotates to the next page.

See [docs/architecture.md](docs/architecture.md),
[docs/memory_model.md](docs/memory_model.md), and
[docs/file_format.md](docs/file_format.md).

## Correctness

The scariest part of code like this is concurrent visibility. revoq pins that
down with a single publication rule (writer reserves → fills metadata and
payload → release-stores `length`; reader acquire-loads `length` → reads the rest
only once committed) and a test suite that targets it directly:

- `test_publish_ordering` — the release/acquire publication contract.
- `test_multiprocess` — a real cross-process writer/reader handoff.
- `test_journal_writer_resume` — recovery and resume against existing pages.
- `test_frame_header`, `test_journal_reader`, `test_typed_reader`,
  `test_hugepage`, plus Python round-trip and reader/writer tests.

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

A direct CMake build is also supported:

```bash
git submodule update --init --recursive
cmake -S . -B build -DCMAKE_CXX_COMPILER=/usr/bin/clang++-20
cmake --build build --parallel
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

revoq ships **one** benchmark binary with two modes, and is deliberate about what
each one is allowed to claim.

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

The **separate cores** configuration is the recommended production topology —
the writer core is fully dedicated with no shared execution resources. The SMT
siblings result (cpu4 and cpu36 share the same physical core and L1/L2 cache)
is included to show the cache topology effect but is not representative of a
real deployment.

The `max` values (~10–11 µs) are single-spike outliers from OS interrupts
landing on a benchmark CPU. The tail story is in `p999`.

We intentionally do **not** print headline latency figures here: they are only
meaningful with the host that produced them. Reproduce them on your own hardware
with `./scripts/check_system.sh` and `./scripts/collect_benchmark_env.sh`, and
see [docs/benchmarking.md](docs/benchmarking.md) and
[docs/tuning_overview.md](docs/tuning_overview.md) for the full methodology and
host requirements.

## Status

Early public release (`0.1.0`): C++23 memory-mapped journal core, C++ tests,
optional Python bindings, and the portable/tuned benchmark. The API may still
change — see [CHANGELOG.md](CHANGELOG.md).

## Commercial

revoq is open source so the core engineering is visible and verifiable. RevoRacer
builds low-latency trading infrastructure for trading teams and early-stage
funds; if you want help integrating it, reproducing the benchmarks on your
hardware, or tuning a host, talk to us at
[revoracer.com](https://revoracer.com). Details:
[docs/commercial_support.md](docs/commercial_support.md).

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
