#!/usr/bin/env bash
set -euo pipefail

read_file() {
  local path="$1"
  if [[ -r "${path}" ]]; then
    cat "${path}"
  else
    echo "unavailable"
  fi
}

section() {
  printf '\n== %s ==\n' "$1"
}

section "Tools"
echo "cmake: $(cmake --version 2>/dev/null | head -n 1 || echo unavailable)"
echo "git: $(git --version 2>/dev/null || echo unavailable)"
echo "cxx: ${CXX:-c++}"
if command -v "${CXX:-c++}" >/dev/null 2>&1; then
  "${CXX:-c++}" --version | head -n 1
else
  echo "C++ compiler not found. Set CXX=/path/to/clang++."
fi

section "Host"
echo "kernel: $(uname -srmo)"
echo "cpu_count: $(getconf _NPROCESSORS_ONLN 2>/dev/null || echo unavailable)"
echo "isolated_cpus: $(read_file /sys/devices/system/cpu/isolated)"

section "CPU Governors"
if compgen -G "/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor" >/dev/null; then
  for governor in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    cpu="${governor%/cpufreq/scaling_governor}"
    cpu="${cpu##*/cpu}"
    echo "cpu${cpu}: $(cat "${governor}")"
  done
else
  echo "unavailable"
fi

section "Huge Pages"
if mount | grep -q ' type hugetlbfs '; then
  mount | grep ' type hugetlbfs ' | sed 's/^/hugetlbfs mount: /'
else
  echo "hugetlbfs mount: not found"
fi
echo "2MB free_hugepages: $(read_file /sys/kernel/mm/hugepages/hugepages-2048kB/free_hugepages)"
echo "2MB resv_hugepages: $(read_file /sys/kernel/mm/hugepages/hugepages-2048kB/resv_hugepages)"

section "Limits"
echo "memlock: $(ulimit -l)"
if command -v chrt >/dev/null 2>&1; then
  echo "chrt: available"
else
  echo "chrt: unavailable"
fi
