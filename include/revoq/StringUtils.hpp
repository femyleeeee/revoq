// StringUtils.hpp
#pragma once

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace revoq {

class StringUtils {
public:
  // ========================================================================
  // REPLACE OPERATIONS
  // ========================================================================

  // Original version - modified in-place
  static inline void replaceAll(std::string &source, std::string_view from,
                                std::string_view to) {
    if (from.empty())
      return;

    std::string new_str;
    // The formula overestimates because it's a conservative heuristic.
    new_str.reserve(source.length() + (to.length() > from.length()
                                           ? (source.length() / from.length()) *
                                                 (to.length() - from.length())
                                           : 0));

    size_t last_pos = 0;
    size_t find_pos;
    while (std::string::npos != (find_pos = source.find(from, last_pos))) {
      new_str.append(source, last_pos, find_pos - last_pos);
      new_str.append(to);
      last_pos = find_pos + from.length();
    }
    new_str.append(source, last_pos, std::string::npos);
    source.swap(new_str);
  }

  // Hot-path version: replace single character (common in trading: space,
  // comma, etc.)
  static inline void replaceChar(std::string &source, char from,
                                 char to) noexcept {
    for (char &c : source) {
      if (c == from)
        c = to;
    }
  }

  // Fast double parsing (optimized for price/quantity format)
  // Assumes format: "12345.67" - no scientific notation
  static inline double fast_parse_double(const char *str, size_t len) {
    const char *p = str;
    const char *end = str + len;

    // Parse as integer mantissa, track decimal position
    std::uint64_t mantissa = 0;
    int sign = 1;
    int decimals = -1; // -1 means before decimal point

    if (*p == '-') [[unlikely]] {
      sign = -1;
      ++p;
    }

    // Hot loop: parse all digits
    while (p < end) [[likely]] {
      char c = *p++;
      if (c >= '0' && c <= '9') [[likely]] {
        mantissa = mantissa * 10 + (c - '0');
        if (decimals >= 0)
          ++decimals;
      } else if (c == '.' && decimals < 0) [[likely]] {
        decimals = 0;
      } else [[unlikely]] {
        break; // Stop on invalid char
      }
    }

    // Convert mantissa to double with appropriate scaling
    if (decimals <= 0) {
      return sign * static_cast<double>(mantissa);
    }

    // Use lookup table for powers of 10
    static constexpr double pow10_negative[16] = {
        1e-0, 1e-1, 1e-2,  1e-3,  1e-4,  1e-5,  1e-6,  1e-7,
        1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15};

    return sign * mantissa * pow10_negative[decimals];
  }
  // ========================================================================
  // CASE OPERATIONS
  // ========================================================================

  // Check if string is uppercase (optimized for trading symbols)
  static inline bool isUpper(std::string_view s) noexcept {
    return std::all_of(s.begin(), s.end(), [](unsigned char c) {
      return (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' ||
             c == '-';
    });
  }

  // Convert to lowercase - returns new string
  static inline std::string toLower(std::string s) {
    for (char &c : s) {
      if (c >= 'A' && c <= 'Z') {
        c = c + ('a' - 'A'); // Branchless case conversion
      }
    }
    return s;
  }

  // Convert to lowercase in-place (zero allocation)
  static inline void toLowerInPlace(std::string &s) noexcept {
    for (char &c : s) {
      if (c >= 'A' && c <= 'Z') {
        c = c + ('a' - 'A');
      }
    }
  }

  // Convert to uppercase
  static inline std::string toUpper(std::string s) {
    for (char &c : s) {
      if (c >= 'a' && c <= 'z') {
        c = c - ('a' - 'A');
      }
    }
    return s;
  }

  // Convert to uppercase in-place
  static inline void toUpperInPlace(std::string &s) noexcept {
    for (char &c : s) {
      if (c >= 'a' && c <= 'z') {
        c = c - ('a' - 'A');
      }
    }
  }

  // ========================================================================
  // SPLIT OPERATIONS
  // ========================================================================

  // Original split - allocates strings
  static inline std::vector<std::string> split(std::string_view src,
                                               char delimiter) {
    std::vector<std::string> result;
    result.reserve(8); // Pre-allocate for common case

    size_t start = 0;
    size_t end = src.find(delimiter);

    while (end != std::string_view::npos) {
      result.emplace_back(src.substr(start, end - start));
      start = end + 1;
      end = src.find(delimiter, start);
    }

    result.emplace_back(src.substr(start));
    return result;
  }

  // Zero-copy split - returns views (HOT PATH for parsing)
  static inline std::vector<std::string_view> splitView(std::string_view src,
                                                        char delimiter) {
    std::vector<std::string_view> result;
    result.reserve(8);

    size_t start = 0;
    size_t end = src.find(delimiter);

    while (end != std::string_view::npos) {
      result.push_back(src.substr(start, end - start));
      start = end + 1;
      end = src.find(delimiter, start);
    }

    result.push_back(src.substr(start));
    return result;
  }

  // Fixed-size split for known number of fields (zero allocations)
  // Returns number of fields parsed
  template <size_t N>
  static inline size_t
  splitFixed(std::string_view src, char delimiter,
             std::array<std::string_view, N> &out) noexcept {
    size_t count = 0;
    size_t start = 0;
    size_t end = src.find(delimiter);

    while (end != std::string_view::npos && count < N) {
      out[count++] = src.substr(start, end - start);
      start = end + 1;
      end = src.find(delimiter, start);
    }

    if (count < N && start <= src.length()) {
      out[count++] = src.substr(start);
    }

    return count;
  }

  // Split on multiple delimiters (e.g., space, tab, comma)
  static inline std::vector<std::string_view>
  splitAny(std::string_view src, std::string_view delimiters) {
    std::vector<std::string_view> result;
    result.reserve(8);

    size_t start = 0;
    while (start < src.length()) {
      size_t end = src.find_first_of(delimiters, start);
      if (end == std::string_view::npos) {
        result.push_back(src.substr(start));
        break;
      }
      if (end > start) { // Skip empty tokens
        result.push_back(src.substr(start, end - start));
      }
      start = end + 1;
    }

    return result;
  }

  // ========================================================================
  // JOIN OPERATIONS
  // ========================================================================

  // Original join with iterators
  template <typename Iterator>
  static inline std::string join(Iterator begin, Iterator end,
                                 std::string_view separator) {
    if (begin == end)
      return "";

    std::string result;

    // Pre-calculate size for single allocation (optimization)
    size_t total_size = 0;
    size_t count = 0;
    for (auto it = begin; it != end; ++it) {
      total_size += it->length();
      ++count;
    }
    if (count > 1) {
      total_size += separator.length() * (count - 1);
    }
    result.reserve(total_size);

    // Build string
    auto it = begin;
    result.append(*it);
    ++it;
    for (; it != end; ++it) {
      result.append(separator);
      result.append(*it);
    }

    return result;
  }

  // Join with container (more convenient)
  template <typename Container>
  static inline std::string joinContainer(const Container &items,
                                          std::string_view separator) {
    return join(items.begin(), items.end(), separator);
  }

  // ========================================================================
  // TRIM OPERATIONS (common in message parsing)
  // ========================================================================

  // Trim whitespace from both ends
  static inline std::string_view trim(std::string_view s) noexcept {
    const char *ws = " \t\n\r\f\v";
    size_t start = s.find_first_not_of(ws);
    if (start == std::string_view::npos)
      return "";
    size_t end = s.find_last_not_of(ws);
    return s.substr(start, end - start + 1);
  }

  static inline std::string_view trimLeft(std::string_view s) noexcept {
    size_t start = s.find_first_not_of(" \t\n\r\f\v");
    return (start == std::string_view::npos) ? "" : s.substr(start);
  }

  static inline std::string_view trimRight(std::string_view s) noexcept {
    size_t end = s.find_last_not_of(" \t\n\r\f\v");
    return (end == std::string_view::npos) ? "" : s.substr(0, end + 1);
  }

  // ========================================================================
  // COMPARISON OPERATIONS
  // ========================================================================

  // Case-insensitive comparison (optimized)
  static inline bool equalsIgnoreCase(std::string_view a,
                                      std::string_view b) noexcept {
    if (a.length() != b.length())
      return false;

    for (size_t i = 0; i < a.length(); ++i) {
      char ca = a[i];
      char cb = b[i];

      // Convert to lowercase for comparison
      if (ca >= 'A' && ca <= 'Z')
        ca += ('a' - 'A');
      if (cb >= 'A' && cb <= 'Z')
        cb += ('a' - 'A');

      if (ca != cb)
        return false;
    }
    return true;
  }

  // Check if string starts with prefix
  static inline bool startsWith(std::string_view str,
                                std::string_view prefix) noexcept {
    return str.size() >= prefix.size() &&
           str.compare(0, prefix.size(), prefix) == 0;
  }

  // Check if string ends with suffix
  static inline bool endsWith(std::string_view str,
                              std::string_view suffix) noexcept {
    return str.size() >= suffix.size() &&
           str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
  }

  // ========================================================================
  // PARSING OPERATIONS (critical for trading messages)
  // ========================================================================

  // Fast integer parsing (no exceptions, returns success)
  static inline bool parseInt(std::string_view s, int64_t &out) noexcept {
    if (s.empty())
      return false;

    const char *ptr = s.data();
    const char *end = ptr + s.length();
    int64_t result = 0;
    bool negative = false;

    // Handle sign
    if (*ptr == '-') {
      negative = true;
      ++ptr;
    } else if (*ptr == '+') {
      ++ptr;
    }

    if (ptr == end)
      return false;

    // Parse digits
    while (ptr < end) {
      char c = *ptr++;
      if (c < '0' || c > '9')
        return false;
      result = result * 10 + (c - '0');
    }

    out = negative ? -result : result;
    return true;
  }

  // Fast double parsing (simple version, no scientific notation)
  static inline bool parseDouble(std::string_view s, double &out) noexcept {
    if (s.empty())
      return false;

    const char *ptr = s.data();
    const char *end = ptr + s.length();
    double result = 0.0;
    bool negative = false;

    // Handle sign
    if (*ptr == '-') {
      negative = true;
      ++ptr;
    } else if (*ptr == '+') {
      ++ptr;
    }

    if (ptr == end)
      return false;

    // Parse integer part
    while (ptr < end && *ptr >= '0' && *ptr <= '9') {
      result = result * 10.0 + (*ptr++ - '0');
    }

    // Parse fractional part
    if (ptr < end && *ptr == '.') {
      ++ptr;
      double fraction = 0.1;
      while (ptr < end && *ptr >= '0' && *ptr <= '9') {
        result += (*ptr++ - '0') * fraction;
        fraction *= 0.1;
      }
    }

    if (ptr != end)
      return false; // Unexpected characters

    out = negative ? -result : result;
    return true;
  }

  // ========================================================================
  // UTILITY OPERATIONS
  // ========================================================================

  // Check if string contains substring
  static inline bool contains(std::string_view str,
                              std::string_view substr) noexcept {
    return str.find(substr) != std::string_view::npos;
  }

  // Count occurrences of character
  static inline size_t countChar(std::string_view str, char c) noexcept {
    return std::count(str.begin(), str.end(), c);
  }

  // Pad string to fixed width (useful for fixed-width protocols)
  static inline std::string padRight(std::string_view str, size_t width,
                                     char fill = ' ') {
    if (str.length() >= width)
      return std::string(str);
    std::string result;
    result.reserve(width);
    result.append(str);
    result.append(width - str.length(), fill);
    return result;
  }

  static inline std::string padLeft(std::string_view str, size_t width,
                                    char fill = ' ') {
    if (str.length() >= width)
      return std::string(str);
    std::string result;
    result.reserve(width);
    result.append(width - str.length(), fill);
    result.append(str);
    return result;
  }
};

} // namespace revoq
