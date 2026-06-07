# Benchmarking

Revoq ships a small benchmark suite. The public goal is reproducibility: every benchmark prints machine-readable output, verifies its own message count, and avoids hidden host tuning.

## Benchmarks

- `revoq_benchmark_journal_handler_benchmark_p99`: end-to-end writer timestamp to reader handler latency. This is the main tail-latency benchmark and has portable and tuned modes.
- `revoq_benchmark_journal_write_throughput`: single-writer append throughput for payload sizes `0`, `32`, `128`, and `512` bytes.
- `revoq_benchmark_journal_read_throughput`: single-reader replay throughput for the same payload sizes.
- `revoq_benchmark_typed_dispatch_overhead`: replay cost comparison between a raw frame loop, a static `switch`, and `TypedJournalDispatcher`.

The latency benchmark has two modes:

- Portable mode: no root requirement, no CPU pinning, no hugepage requirement.
- Tuned mode: Linux-only, explicit CPU pinning, real-time priority, hugetlbfs path, and a forked reader process.

## Build

```bash
CXX=/usr/bin/clang++-20 ./build.sh benchmarks
```

## Portable Run

```bash
./build/revoq_benchmark_journal_handler_benchmark_p99
./build/revoq_benchmark_journal_write_throughput
./build/revoq_benchmark_journal_read_throughput
./build/revoq_benchmark_typed_dispatch_overhead
```

Portable results are useful for functional checks and rough comparisons. They should not be used as host-to-host p99 claims.

Use optional CLI arguments to change run length:

```bash
./build/revoq_benchmark_journal_handler_benchmark_p99 --messages 1000000 --warmup 50000 --interval-ns 1000
./build/revoq_benchmark_journal_write_throughput --messages 200000
./build/revoq_benchmark_journal_read_throughput --messages 200000
./build/revoq_benchmark_typed_dispatch_overhead --messages 1000000
```

## Tuned Run

```bash
sudo ./build/revoq_benchmark_journal_handler_benchmark_p99 \
  --tuned --fork \
  --path /dev/hugepages/revoq-bench \
  --writer-cpu 2 \
  --reader-cpu 4 \
  --background-cpu 6 \
  --rt-priority 80 \
  --messages 100000000 \
  --warmup 50000 \
  --interval-ns 100
```

Tuned mode expects:

- `/dev/hugepages` mounted as `hugetlbfs`.
- A writable empty benchmark directory.
- Distinct writer, reader, and background CPUs.
- Sufficient `memlock` limit.
- Root or `CAP_SYS_NICE` for real-time scheduling.

## Output

The latency benchmark reports latency in nanoseconds:

- `p50_ns`, `p90_ns`, `p99_ns`, `p999_ns`
- `max_ns`
- `mean_ns`
- sample count and checksum

The measurement is writer timestamp to reader handler endpoint for a deterministic mix of four 32-byte message payloads.

The throughput benchmarks print aligned tables with `Mmsg/s`, `ns/msg`, and a checksum so automated runs can detect broken loops. The dispatch-overhead benchmark prints a `guard` kind and value: `frame` for the raw frame-scan baseline, and `payload` for static and typed dispatch. Only rows with the same guard kind are expected to have matching values. These values are benchmark verification guards, not cryptographic or storage-integrity checksums.

## Environment Capture

Use the scripts before publishing benchmark numbers:

```bash
./scripts/check_system.sh
./scripts/collect_benchmark_env.sh > benchmark-env.txt
```
