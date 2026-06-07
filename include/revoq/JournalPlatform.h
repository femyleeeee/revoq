#pragma once

#include <cstring>
#include <fstream>
#include <sstream>
#include <string>

#include <spdlog/spdlog.h>

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#endif

namespace revoq::journal::detail {

#ifdef __linux__

inline std::string readCpuListFile(const char *path) {
  std::ifstream f(path);
  std::string line;
  if (f)
    std::getline(f, line);
  return line;
}

inline bool cpuListContains(const std::string &list, int cpu) {
  std::stringstream ss(list);
  std::string token;
  while (std::getline(ss, token, ',')) {
    if (token.empty() || token.front() < '0' || token.front() > '9')
      continue;
    const auto dash = token.find('-');
    const int lo = std::stoi(token.substr(0, dash));
    const int hi =
        dash == std::string::npos ? lo : std::stoi(token.substr(dash + 1));
    if (cpu >= lo && cpu <= hi)
      return true;
  }
  return false;
}

inline bool makeHousekeepingCpuSet(cpu_set_t &cpuset) {
  CPU_ZERO(&cpuset);
  const std::string isolated =
      readCpuListFile("/sys/devices/system/cpu/isolated");
  const std::string nohz_full =
      readCpuListFile("/sys/devices/system/cpu/nohz_full");
  long cpu_count = sysconf(_SC_NPROCESSORS_CONF);
  if (cpu_count <= 0 || cpu_count > CPU_SETSIZE)
    cpu_count = CPU_SETSIZE;
  int selected = 0;
  for (int cpu = 0; cpu < cpu_count; ++cpu) {
    if (cpuListContains(isolated, cpu) || cpuListContains(nohz_full, cpu))
      continue;
    CPU_SET(cpu, &cpuset);
    ++selected;
  }
  if (selected > 0)
    return true;

  for (int cpu = 0; cpu < cpu_count; ++cpu)
    CPU_SET(cpu, &cpuset);
  return cpu_count > 0;
}

inline bool pinThreadToBackgroundCpus(pthread_t thread,
                                      int background_worker_cpu,
                                      const char *thread_name) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  if (background_worker_cpu >= 0) {
    CPU_SET(background_worker_cpu, &cpuset);
    const int rc = pthread_setaffinity_np(thread, sizeof(cpuset), &cpuset);
    if (rc == 0) {
      SPDLOG_DEBUG("{} pinned to CPU {}", thread_name, background_worker_cpu);
      return true;
    }
    SPDLOG_WARN("{} failed to pin to CPU {}: {}", thread_name,
                background_worker_cpu, std::strerror(rc));
  }

  if (!makeHousekeepingCpuSet(cpuset))
    return false;
  const int rc = pthread_setaffinity_np(thread, sizeof(cpuset), &cpuset);
  if (rc != 0) {
    SPDLOG_WARN("{} failed to pin to housekeeping CPUs: {}", thread_name,
                std::strerror(rc));
    return false;
  }
  SPDLOG_DEBUG("{} pinned to housekeeping CPUs", thread_name);
  return true;
}

#endif

} // namespace revoq::journal::detail
