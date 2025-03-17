#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>
#include <x86intrin.h>

#include "common/macros.h"

using hash_t = uint64_t;

namespace pgp {

class HashUtil {
 public:
  /** This class cannot be instantiated. */
  DISALLOW_INSTANTIATION(HashUtil);
  /** This class cannot be copied or moved. */
  DISALLOW_COPY_AND_MOVE(HashUtil);

  static hash_t HashBytes(const std::byte *bytes, const uint64_t length) {
    hash_t hash = length;
    for (uint64_t i = 0; i < length; ++i) {
      hash = ((hash << 5) ^ (hash >> 27)) ^ static_cast<uint8_t>(bytes[i]);
    }
    return hash;
  }

  static hash_t HashNode(const void *expr);

  template <typename T>
  static auto Hash(const T &obj) -> hash_t
    requires(!std::is_arithmetic_v<T> && !std::is_same_v<T, std::string> && !std::is_same_v<T, char>)
  {
    return HashBytes(reinterpret_cast<const std::byte *>(&obj), sizeof(T));
  }

  static hash_t SumHashes(const hash_t l, const hash_t r) {
    static const hash_t prime_factor = 10000019;
    return (l % prime_factor + r % prime_factor) % prime_factor;
  }

  template <typename T>
  static auto Hash(T val) -> hash_t
    requires(std::is_arithmetic_v<T>)
  {
    return HashMurmur(val);
  }

  static hash_t Hash(const char *str) { return HashUtil::Hash(std::string_view(str)); }

  static auto Hash(const std::string_view s) -> hash_t {
    return HashBytes(reinterpret_cast<const std::byte *>(s.data()), s.length());
  }

  static hash_t CombineHashes(const hash_t first_hash, const hash_t second_hash) {
    // Based on Hash128to64() from cityhash.xxh3
    static constexpr auto k_mul = uint64_t(0x9ddfea08eb382d69);
    hash_t a = (first_hash ^ second_hash) * k_mul;
    a ^= (a >> 47u);
    hash_t b = (second_hash ^ a) * k_mul;
    b ^= (b >> 47u);
    b *= k_mul;
    return b;
  }

  template <typename T>
  static auto HashMurmur(T val, hash_t seed) -> hash_t
    requires(std::is_arithmetic_v<T>)
  {
    auto k = static_cast<uint64_t>(val);
    k ^= seed;
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdLLU;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53LLU;
    k ^= k >> 33;
    return k;
  }

  /**
   * Integer Murmur3 hashing with a seed of 0.
   */
  template <typename T>
  static auto HashMurmur(T val) -> hash_t
    requires(std::is_fundamental_v<T>)
  {
    return HashMurmur(val, 0);
  }
};

}  // namespace pgp
