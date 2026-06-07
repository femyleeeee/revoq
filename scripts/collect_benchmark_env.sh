#!/usr/bin/env bash
set -euo pipefail

read_file() {
  local path="$1"
  if [[ -r "${path}" ]]; then
    tr '\n' ' ' < "${path}" | sed 's/[[:space:]]*$//'
  else
    echo "unavailable"
  fi
}

first_match() {
  local pattern="$1"
  local file="$2"
  if [[ -r "${file}" ]]; then
    grep -m 1 "${pattern}" "${file}" | cut -d ':' -f 2- | sed 's/^ *//'
  else
    echo "unavailable"
  fi
}

echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "kernel=$(uname -srmo)"
echo "cpu_count=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo unavailable)"
echo "cpu_model=$(first_match 'model name' /proc/cpuinfo)"
echo "isolated_cpus=$(read_file /sys/devices/system/cpu/isolated)"
echo "memlock_kb=$(ulimit -l)"
echo "hugetlbfs_mounts=$(mount | awk '$5 == "hugetlbfs" {print $3}' | paste -sd, -)"
echo "hugepages_2mb_free=$(read_file /sys/kernel/mm/hugepages/hugepages-2048kB/free_hugepages)"
echo "hugepages_2mb_reserved=$(read_file /sys/kernel/mm/hugepages/hugepages-2048kB/resv_hugepages)"
echo "cmake_version=$(cmake --version 2>/dev/null | head -n 1 || echo unavailable)"
echo "git_version=$(git --version 2>/dev/null || echo unavailable)"
if command -v "${CXX:-c++}" >/dev/null 2>&1; then
  echo "cxx=$("${CXX:-c++}" --version | head -n 1)"
else
  echo "cxx=unavailable"
fi

if compgen -G "/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor" >/dev/null; then
  for governor in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    cpu="${governor%/cpufreq/scaling_governor}"
    cpu="${cpu##*/cpu}"
    echo "cpu${cpu}_governor=$(cat "${governor}")"
  done
fi
