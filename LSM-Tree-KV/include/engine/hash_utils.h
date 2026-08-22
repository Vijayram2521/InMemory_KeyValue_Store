#pragma once
#include <cstdint>
#include <string>

namespace kv_engine {

// FNV-1a: a small, fast, well-distributed non-cryptographic hash. `seed`
// perturbs the offset basis so the same key can produce genuinely
// different-looking hashes for different callers (e.g. BloomFilter's two
// probe hashes, or HashRing's node/key hashes) without needing a second,
// unrelated hash function.
//
// Shared here (rather than duplicated) because both BloomFilter and
// HashRing need a hash with the same properties -- extracted verbatim from
// its original home in bloom_filter.cpp, same constants, same algorithm,
// zero behavior change for existing callers.
inline uint64_t fnv1a(const std::string& s, uint64_t seed) {
    constexpr uint64_t kFnvOffsetBasis = 1469598103934665603ULL;
    constexpr uint64_t kFnvPrime = 1099511628211ULL;
    uint64_t hash = kFnvOffsetBasis ^ seed;
    for (unsigned char c : s) {
        hash ^= c;
        hash *= kFnvPrime;
    }
    return hash;
}

// MurmurHash3's 64-bit finalizer ("fmix64"). FNV-1a's raw output is NOT
// uniformly spread across the full 64-bit range for structurally similar
// short inputs -- e.g. "compute-1"/"compute-2"/"compute-3" all land within
// a few billion of each other, while "sample_key_0".."sample_key_N" land
// in a completely different, similarly narrow band, both far from
// uniformly covering 2^64. That's harmless for BloomFilter (its `% m`
// re-bounds the result into a small array regardless), but fatal for
// anything that uses the raw hash as a position on the full 64-bit space
// -- e.g. HashRing, where it caused every key to route to a single node
// (verified: every sampled key's raw hash exceeded every node's raw hash,
// so ring lookups always wrapped to the same smallest-hash node). This
// avalanche step scrambles the bits thoroughly so the *output* is well
// distributed even when the *input* isn't.
inline uint64_t avalanche(uint64_t h) {
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return h;
}

} // namespace kv_engine
