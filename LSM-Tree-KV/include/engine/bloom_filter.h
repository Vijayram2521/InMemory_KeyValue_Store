#pragma once
#include <cstddef>
#include <string>
#include <vector>

namespace kv_engine {

// A simple Bloom filter: a bit array plus kNumHashes probe indices per key,
// used to answer "definitely not present" without touching disk. This is a
// membership *pre-check* only -- a `true` result means "maybe present, go
// check the real index," never "definitely present."
//
// Probe indices come from the Kirsch-Mitzenmacher double-hashing technique
// (h1(key) + i * h2(key), mod bit-array size, for i in [0, kNumHashes)):
// two real hash computations stand in for kNumHashes independent ones. This
// is provably as effective in practice as using kNumHashes genuinely
// separate hash functions, and is what most production Bloom filters
// (Guava, Cassandra, Redis) actually do -- simpler and cheaper than hashing
// the key kNumHashes separate times, and safer than hand-picking kNumHashes
// small primes for a naive modulo scheme, which can produce correlated bit
// patterns if the primes aren't chosen carefully.
class BloomFilter {
public:
    static constexpr unsigned kNumHashes = 5;
    // ~10 bits/key is the classic rule of thumb for ~1% false-positive rate;
    // at a fixed kNumHashes=5 (rather than the theoretical optimum of ~7 for
    // 10 bits/key) this lands at ~0.94% FPR -- still squarely in the
    // standard ballpark.
    static constexpr size_t kBitsPerKey = 10;

    // Sized up front for `expected_keys` entries. Bloom filters can't grow
    // after construction without invalidating already-set bits, so the
    // caller must know (or reasonably estimate) the key count first --
    // for this project, that's always known exactly, since a filter is
    // built either from a freshly-flushed SSTable's key set or from an
    // already-loaded index's key list.
    explicit BloomFilter(size_t expected_keys);

    void add(const std::string& key);

    // false = key is DEFINITELY not in the set this filter was built from
    //         (safe to skip the SSTable this filter belongs to entirely).
    // true  = key MAYBE present -- still must check the real index/data;
    //         this is the only kind of false positive a Bloom filter can
    //         have (it never produces false negatives).
    bool maybe_contains(const std::string& key) const;

    size_t bit_count() const { return bits_.size(); }

private:
    void probe_indices(const std::string& key, size_t (&out)[kNumHashes]) const;

    std::vector<bool> bits_;
};

} // namespace kv_engine
