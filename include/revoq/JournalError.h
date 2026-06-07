#pragma once

#include <stdexcept>

namespace revoq {
namespace journal {
class JournalError : public std::runtime_error {
public:
  explicit JournalError(const std::string &message)
      : std::runtime_error(message) {}
};
} // namespace journal
} // namespace revoq
