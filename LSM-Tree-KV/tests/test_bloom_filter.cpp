#include "engine/bloom_filter.h"
#include "tests.h"
#include "test_framework.h"
#include <string>

void kv_tests::run_test_bloom_filter() {
    std::cout << "--- [Test] Bloom Filter ---" << std::endl;

    // A Bloom filter can never produce a false NEGATIVE: every key that was
    // added must always report maybe_contains() == true. This is a hard
    // guarantee (not probabilistic), so it's checked exactly, not sampled.
    const size_t kNumKeys = 10000;
    kv_engine::BloomFilter bloom(kNumKeys);
    for (size_t i = 0; i < kNumKeys; ++i) {
        bloom.add("present_" + std::to_string(i));
    }

    bool all_present_found = true;
    for (size_t i = 0; i < kNumKeys; ++i) {
        if (!bloom.maybe_contains("present_" + std::to_string(i))) {
            all_present_found = false;
            break;
        }
    }
    KV_CHECK(all_present_found, "every added key reports maybe_contains() == true (no false negatives)");

    // False POSITIVES are expected -- that's the whole tradeoff -- but the
    // rate should land in the standard ballpark for kNumHashes=5 at
    // kBitsPerKey=10 (~0.94% analytically). Sample a large, disjoint set of
    // never-added keys and check the observed rate stays well below a loose
    // upper bound; wide enough margin (5x the expected rate) to not be
    // flaky from ordinary statistical variance, tight enough to catch a
    // genuinely broken implementation (e.g. hashes that collide constantly).
    size_t false_positives = 0;
    for (size_t i = 0; i < kNumKeys; ++i) {
        if (bloom.maybe_contains("absent_" + std::to_string(i))) {
            ++false_positives;
        }
    }
    double observed_fpr = static_cast<double>(false_positives) / static_cast<double>(kNumKeys);
    KV_CHECK(observed_fpr < 0.05,
             "observed false-positive rate on 10,000 never-added keys stays under 5% "
             "(analytically expected ~0.94% at kNumHashes=5, kBitsPerKey=10)");

    // A fresh, empty-of-these-keys filter should not claim to contain
    // anything from a disjoint keyspace (same false-negative-never guarantee
    // as above, just phrased the other way: nothing added means nothing
    // should come back "definitely maybe" except by the same bounded false-
    // positive chance already checked above).
    kv_engine::BloomFilter empty_bloom(1000);
    KV_CHECK_FALSE(empty_bloom.maybe_contains("never_added_to_empty_filter"),
                   "a filter with nothing added reports a specific key as not present");

    // Sizing sanity: bit_count() should scale with expected_keys per
    // kBitsPerKey, with a floor for tiny/zero-sized filters (a zero-sized
    // bit array would make the modulo in probe_indices undefined behavior).
    kv_engine::BloomFilter sized(1000);
    KV_CHECK_EQ(size_t(1000 * kv_engine::BloomFilter::kBitsPerKey), sized.bit_count(),
                "bit_count() matches expected_keys * kBitsPerKey for a non-trivial size");
    kv_engine::BloomFilter tiny(0);
    KV_CHECK(tiny.bit_count() > 0, "a zero-expected-keys filter still allocates a non-empty bit array");
}
