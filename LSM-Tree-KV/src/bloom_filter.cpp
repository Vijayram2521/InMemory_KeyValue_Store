#include "engine/bloom_filter.h"
#include "engine/hash_utils.h"

#include <algorithm>
#include <cstdint>

namespace kv_engine {

BloomFilter::BloomFilter(size_t expected_keys)
    : bits_(std::max<size_t>(expected_keys * kBitsPerKey, 64), false) {}

void BloomFilter::probe_indices(const std::string& key, size_t (&out)[kNumHashes]) const {
    uint64_t h1 = fnv1a(key, 0);
    uint64_t h2 = fnv1a(key, 0x9E3779B97F4A7C15ULL); // golden-ratio constant as a seed
    size_t m = bits_.size();
    for (unsigned i = 0; i < kNumHashes; ++i) {
        out[i] = static_cast<size_t>((h1 + static_cast<uint64_t>(i) * h2) % m);
    }
}

void BloomFilter::add(const std::string& key) {
    size_t idx[kNumHashes];
    probe_indices(key, idx);
    for (unsigned i = 0; i < kNumHashes; ++i) {
        bits_[idx[i]] = true;
    }
}

bool BloomFilter::maybe_contains(const std::string& key) const {
    size_t idx[kNumHashes];
    probe_indices(key, idx);
    for (unsigned i = 0; i < kNumHashes; ++i) {
        if (!bits_[idx[i]]) return false; // a single unset bit proves absence
    }
    return true;
}

} // namespace kv_engine
