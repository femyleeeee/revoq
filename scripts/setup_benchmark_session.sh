#!/usr/bin/env bash
set -euo pipefail

# Session-scoped tuning for revoq tuned benchmark runs.
# Changes made here are NOT persistent across reboots.
# For boot-time tuning (isolcpus, nohz_full), see docs/tuning_overview.md.

WRITER_CPU=""
READER_CPU=""
BACKGROUND_CPU=""
HUGEPAGES=4096
HUGEPAGE_PATH="/dev/hugepages"

usage() {
  cat <<EOF
Usage: sudo ./scripts/setup_benchmark_session.sh [options]

Applies session-scoped tuning for the revoq tuned benchmark.
Changes are NOT persistent across reboots.

Options:
  --writer-cpu N        Writer CPU number (sets governor to performance)
  --reader-cpu N        Reader CPU number (sets governor to performance)
  --background-cpu N    Background CPU number (sets governor to performance)
  --hugepages N         2MB hugepages to allocate (default: 4096 = 8GB)
  --hugepage-path PATH  hugetlbfs mount point (default: /dev/hugepages)
  -h, --help            Show this help

Example:
  sudo ./scripts/setup_benchmark_session.sh \\
    --writer-cpu 2 --reader-cpu 4 --background-cpu 6

Then verify with:
  ./scripts/check_system.sh
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --writer-cpu)     WRITER_CPU="$2";     shift 2 ;;
    --reader-cpu)     READER_CPU="$2";     shift 2 ;;
    --background-cpu) BACKGROUND_CPU="$2"; shift 2 ;;
    --hugepages)      HUGEPAGES="$2";      shift 2 ;;
    --hugepage-path)  HUGEPAGE_PATH="$2";  shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ "${EUID}" -ne 0 ]]; then
  echo "error: this script requires root. Run with sudo." >&2
  exit 1
fi

ok()   { printf '  ok   %s\n' "$1"; }
skip() { printf '  skip %s\n' "$1"; }
warn() { printf '  warn %s\n' "$1"; }

section() { printf '\n== %s ==\n' "$1"; }

# ---------------------------------------------------------------------------
section "CPU Governor"

set_governor() {
  local cpu="$1"
  local gov_path="/sys/devices/system/cpu/cpu${cpu}/cpufreq/scaling_governor"
  if [[ ! -f "${gov_path}" ]]; then
    warn "cpu${cpu}: no cpufreq governor (fixed-frequency or virtualized host)"
    return
  fi
  local current
  current=$(cat "${gov_path}")
  if [[ "${current}" == "performance" ]]; then
    skip "cpu${cpu}: already performance"
  else
    echo "performance" > "${gov_path}"
    ok "cpu${cpu}: ${current} -> performance"
  fi
}

if [[ -n "${WRITER_CPU}${READER_CPU}${BACKGROUND_CPU}" ]]; then
  for cpu in ${WRITER_CPU} ${READER_CPU} ${BACKGROUND_CPU}; do
    set_governor "${cpu}"
  done
else
  echo "  No CPUs specified — setting all CPUs to performance."
  if compgen -G "/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor" >/dev/null 2>&1; then
    for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
      cpu="${gov%/cpufreq/scaling_governor}"
      cpu="${cpu##*/cpu}"
      set_governor "${cpu}"
    done
  else
    warn "no cpufreq governors found (virtualized host?)"
  fi
fi

# ---------------------------------------------------------------------------
section "Hugepages (2MB)"

HP_SYS="/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages"
current=$(cat "${HP_SYS}" 2>/dev/null || echo 0)

if [[ "${current}" -ge "${HUGEPAGES}" ]]; then
  skip "${current} x 2MB hugepages already allocated (>= ${HUGEPAGES} requested)"
else
  echo "${HUGEPAGES}" > "${HP_SYS}"
  actual=$(cat "${HP_SYS}")
  if [[ "${actual}" -ge "${HUGEPAGES}" ]]; then
    ok "allocated ${actual} x 2MB hugepages = $(( actual * 2 ))MB"
  else
    warn "requested ${HUGEPAGES} but only ${actual} allocated"
    warn "system may lack contiguous memory — try allocating earlier at boot"
    warn "or reduce --hugepages"
  fi
fi

# ---------------------------------------------------------------------------
section "hugetlbfs Mount"

if mount | grep -q " on ${HUGEPAGE_PATH} type hugetlbfs "; then
  skip "${HUGEPAGE_PATH} already mounted as hugetlbfs"
else
  mkdir -p "${HUGEPAGE_PATH}"
  mount -t hugetlbfs -o pagesize=2M none "${HUGEPAGE_PATH}"
  ok "mounted hugetlbfs at ${HUGEPAGE_PATH} (pagesize=2M)"
fi

# ---------------------------------------------------------------------------
section "Boot-time Tuning (not applied — requires reboot)"

cpu_list=""
for cpu in ${WRITER_CPU:-} ${READER_CPU:-} ${BACKGROUND_CPU:-}; do
  [[ -n "${cpu}" ]] && cpu_list="${cpu_list:+${cpu_list},}${cpu}"
done
[[ -z "${cpu_list}" ]] && cpu_list="<writer-cpu>,<reader-cpu>,<background-cpu>"

cat <<EOF
  For full scheduler isolation add these kernel parameters to your bootloader:

    isolcpus=${cpu_list} nohz_full=${cpu_list} rcu_nocbs=${cpu_list}

  On Ubuntu/Debian:
    1. Edit GRUB_CMDLINE_LINUX_DEFAULT in /etc/default/grub
    2. Run: sudo update-grub
    3. Reboot

  See docs/tuning_overview.md for details.
EOF

# ---------------------------------------------------------------------------
section "Next Steps"

cat <<EOF
  Verify your setup:
    ./scripts/check_system.sh

  Run the tuned benchmark:
    sudo ./build/revoq_benchmark_journal_handler_benchmark_p99 \\
      --tuned --fork \\
      --path ${HUGEPAGE_PATH}/revoq-bench \\
      --writer-cpu ${WRITER_CPU:-<N>} \\
      --reader-cpu ${READER_CPU:-<N>} \\
      --background-cpu ${BACKGROUND_CPU:-<N>} \\
      --rt-priority 80 \\
      --messages 100000000 --warmup 50000 --interval-ns 100

  Capture the environment alongside your results:
    ./scripts/collect_benchmark_env.sh > env.txt
EOF
