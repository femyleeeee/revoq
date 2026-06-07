#include "MmapBuffer.h"

#include <cerrno>
#include <cstring>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <spdlog/spdlog.h>

// Platform detection
#ifdef __APPLE__
#include <TargetConditionals.h>
#define IS_MACOS 1
#else
#define IS_MACOS 0
#endif

#ifdef __linux__
#define IS_LINUX 1
#else
#define IS_LINUX 0
#endif

#if IS_LINUX && !defined(MADV_POPULATE_READ)
#define MADV_POPULATE_READ 22
#endif

namespace revoq {
namespace os {

std::uintptr_t MmapBuffer::loadMmapBuffer(const std::string &path,
                                          std::size_t size, bool is_writing,
                                          bool prefault) {
  bool master = is_writing || prefault;

  // Check if file exists
  struct stat st;
  bool file_exists = (stat(path.c_str(), &st) == 0);

  // Open file with close-on-exec flag
  // create if not exist
  int fd =
      open(path.c_str(), (master ? O_RDWR : O_RDONLY) | O_CREAT | O_CLOEXEC,
           (mode_t)0600);

  if (fd < 0) {
    throw MmapError("failed to open file: " + path +
                    " errno: " + std::to_string(errno));
  }

  // Allocate file size if master and file doesn't exist or is wrong size
  if (master && (!file_exists || st.st_size != static_cast<off_t>(size))) {
#if IS_LINUX
    // Try fallocate first (faster, allocates contiguous blocks)
    if (fallocate(fd, 0, 0, size) != 0) {
      // Fallback to ftruncate
      if (ftruncate(fd, size) != 0) {
        int err = errno;
        close(fd);
        throw MmapError("failed to allocate file: " + path +
                        " errno: " + std::to_string(err));
      }
    }
#else
    // macOS doesn't have fallocate, use ftruncate directly
    if (ftruncate(fd, size) != 0) {
      int err = errno;
      close(fd);
      throw MmapError("failed to allocate file: " + path +
                      " errno: " + std::to_string(err));
    }
#endif
  }

  // Setup mmap flags
  int prot = master ? (PROT_READ | PROT_WRITE) : PROT_READ;
  int flags = MAP_SHARED;

  if (prefault) {
#if IS_LINUX
    // Prefault all pages immediately (critical for HFT)
    flags |= MAP_POPULATE;
#endif
    // hugetlbfs-backed files get explicit huge pages from the filesystem.
    // Ordinary files stay on the regular-page path and may be promoted by THP.
  }

  void *buffer = mmap(nullptr, size, prot, flags, fd, 0);
  int err = errno;
  close(fd); // Close fd immediately after mmap

  if (buffer == MAP_FAILED) {
    throw MmapError("mmap failed: " + path + " errno: " + std::to_string(err) +
                    " (" + std::string(strerror(err)) + ")");
  }

  if (prefault) {
#if IS_LINUX
    // Advise kernel about access pattern
    // MADV_SEQUENTIAL: For journal/log files (sequential access)
    // MADV_RANDOM: For hash tables/indexes (random access)
    if (madvise(buffer, size, MADV_SEQUENTIAL) != 0) {
      SPDLOG_WARN("madvise MADV_SEQUENTIAL failed for {}: {}", path,
                  strerror(errno));
    }

    // Request transparent huge pages if explicit huge pages weren't available
    if (madvise(buffer, size, MADV_HUGEPAGE) != 0) {
      SPDLOG_DEBUG("MADV_HUGEPAGE failed for {}: {}", path, strerror(errno));
    }
#elif IS_MACOS
    // macOS equivalent: advise sequential access
    if (madvise(buffer, size, MADV_SEQUENTIAL) != 0) {
      SPDLOG_WARN("madvise MADV_SEQUENTIAL failed for {}: {}", path,
                  strerror(errno));
    }

    // macOS doesn't have MADV_HUGEPAGE, but we can use MADV_WILLNEED
    // to prefault pages
    if (madvise(buffer, size, MADV_WILLNEED) != 0) {
      SPDLOG_DEBUG("madvise MADV_WILLNEED failed for {}: {}", path,
                   strerror(errno));
    }
#endif

    // Lock pages in RAM to prevent swapping (critical for HFT)
    if (mlock(buffer, size) != 0) {
      int lock_err = errno;
#if IS_LINUX
      munmap(buffer, size);
      throw MmapError(
          "mlock failed: " + path + " errno: " + std::to_string(lock_err) +
          " (" + std::string(strerror(lock_err)) + ")" + " - check ulimit -l");
#else
      SPDLOG_WARN(
          "mlock failed: {} - errno: {} ({}). Continuing without locking.",
          path, lock_err, strerror(lock_err));
#endif
    }

    // Explicitly fault in all pages by touching them
    // Only zero-fill if this is a NEW file being created by master
    if (master && !file_exists) {
      // Zero-fill the entire region for new files
      std::memset(buffer, 0, size);
    } else {
      // For existing files or read-only, just touch each page to fault it in
      volatile char *ptr = static_cast<volatile char *>(buffer);
#if IS_MACOS
      // macOS page size is typically 16KB on M1 (ARM64)
      std::size_t page_size = 16384;
#else
      // Linux x86_64 page size is typically 4KB
      std::size_t page_size = 4096;
#endif

      for (std::size_t i = 0; i < size; i += page_size) {
        // Just read to fault in the page, don't modify
        (void)ptr[i];
      }
    }
  }

  SPDLOG_TRACE("mapped {} - {} - {} - {} bytes", path, is_writing ? "rw" : "r",
               prefault ? "locked" : "lazy", size);

  return reinterpret_cast<std::uintptr_t>(buffer);
}

std::uintptr_t MmapBuffer::loadExistingReadOnlyMmapBuffer(
    const std::string &path, std::size_t size, ReadOnlyMmapOptions options) {
  int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    throw MmapError("failed to open existing file: " + path +
                    " errno: " + std::to_string(errno));
  }

  struct stat st{};
  if (fstat(fd, &st) != 0) {
    int err = errno;
    close(fd);
    throw MmapError("failed to stat existing file: " + path +
                    " errno: " + std::to_string(err));
  }
  if (st.st_size != static_cast<off_t>(size)) {
    close(fd);
    throw MmapError("unexpected file size: " + path +
                    " required: " + std::to_string(size) +
                    " found: " + std::to_string(st.st_size));
  }

  void *buffer = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
  int err = errno;
  close(fd);
  if (buffer == MAP_FAILED) {
    throw MmapError("read-only mmap failed: " + path +
                    " errno: " + std::to_string(err) + " (" +
                    std::string(strerror(err)) + ")");
  }

  try {
    prepareReadOnlyMmapBuffer(path, reinterpret_cast<std::uintptr_t>(buffer),
                              size, options);
  } catch (...) {
    munmap(buffer, size);
    throw;
  }

  SPDLOG_TRACE("mapped existing {} - r - {} bytes", path, size);
  return reinterpret_cast<std::uintptr_t>(buffer);
}

void MmapBuffer::prepareReadOnlyMmapBuffer(const std::string &path,
                                           std::uintptr_t address,
                                           std::size_t size,
                                           ReadOnlyMmapOptions options) {
  void *buffer = reinterpret_cast<void *>(address);
  if (buffer == nullptr) {
    throw MmapError("cannot prepare null read-only mmap: " + path);
  }

#if IS_LINUX
  if (options.advise_hugepage && madvise(buffer, size, MADV_HUGEPAGE) != 0) {
    SPDLOG_DEBUG("MADV_HUGEPAGE failed for {}: {}", path, strerror(errno));
  }

  if (options.populate_read && madvise(buffer, size, MADV_POPULATE_READ) != 0) {
    int err = errno;
    throw MmapError("MADV_POPULATE_READ failed: " + path +
                    " errno: " + std::to_string(err) + " (" +
                    std::string(strerror(err)) + ")");
  }
#else
  if (options.populate_read) {
    throw MmapError("MADV_POPULATE_READ is unsupported for: " + path);
  }
#endif
}

bool MmapBuffer::releaseMmapBuffer(std::uintptr_t address, std::size_t size,
                                   bool prefault, MsyncMode msync_mode) {
  void *buffer = reinterpret_cast<void *>(address);
  if (buffer == nullptr) {
    SPDLOG_ERROR("Attempted to release null buffer");
    return false;
  }
  if (prefault) {
    // If the buffer was prefaulted, that means we have pin the pages in RAM
    // preventing the OS from swapping them out. Now we'd release that pin,
    // allowing the kernel to reclaim the physical pages under memory pressure.
    if (munlock(buffer, size) != 0) {
      SPDLOG_ERROR("munlock failed: {}", strerror(errno));
    }
  }
  if (msync_mode == MsyncMode::SYNC) {
    // block until the dirty pages in the page cache are written to disk
    if (msync(buffer, size, MS_SYNC) != 0) {
      SPDLOG_ERROR("msync failed: {}", strerror(errno));
      return false;
    }
  } else if (msync_mode == MsyncMode::ASYNC) {
    msync(buffer, size, MS_ASYNC); // best-effort; ignore error
  }
  // MsyncMode::NONE: rely on kernel writeback

  // remove the virtual address mapping. After this the address range is invalid
  // and any access to it segfaults
  if (munmap(buffer, size) != 0) {
    SPDLOG_ERROR("munmap failed: {}", strerror(errno));
    return false;
  }
  SPDLOG_TRACE("released buffer at {} - {} bytes",
               reinterpret_cast<void *>(address), size);
  return true;
}

} // namespace os
} // namespace revoq
