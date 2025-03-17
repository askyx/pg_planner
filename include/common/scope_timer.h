
#include <chrono>

#include "common/macros.h"

namespace pgp {

template <class resolution>
class ScopedTimer {
 public:
  /**
   * Constucts a ScopedTimer. This marks the beginning of the timer's elapsed time.
   * @param elapsed pointer to write the elapsed time (milliseconds) on destruction
   */
  explicit ScopedTimer(uint64_t *const elapsed)
      : start_(std::chrono::high_resolution_clock::now()), elapsed_(elapsed) {}

  /**
   * This marks the end of the timer's elapsed time. The ScopedTimer will update the original constructor argument's
   * value on destruction.
   */
  ~ScopedTimer() {
    auto end = std::chrono::high_resolution_clock::now();
    *elapsed_ = static_cast<uint64_t>(std::chrono::duration_cast<resolution>(end - start_).count());
  }
  DISALLOW_COPY_AND_MOVE(ScopedTimer)
 private:
  const std::chrono::high_resolution_clock::time_point start_;
  uint64_t *const elapsed_;
};
}  // namespace pgp