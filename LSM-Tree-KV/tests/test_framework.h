#pragma once

#include <iostream>
#include <sstream>
#include <string>

// Minimal header-only assertion framework for kv_tests.
// Unlike plain std::cout checks, a failed KV_CHECK* actually flips the
// process exit code (see kv_tests::stats() consumed in main.cpp), so CI/CTest
// can tell a real failure apart from a wall of text.

namespace kv_tests {

struct TestStats {
    int passed = 0;
    int failed = 0;
};

inline TestStats& stats() {
    static TestStats s;
    return s;
}

inline void report_pass(const std::string& what) {
    stats().passed++;
    std::cout << "  [PASS] " << what << std::endl;
}

inline void report_fail(const std::string& what, const std::string& detail) {
    stats().failed++;
    std::cout << "  [FAIL] " << what;
    if (!detail.empty()) std::cout << " -- " << detail;
    std::cout << std::endl;
}

} // namespace kv_tests

#define KV_CHECK(cond, msg) \
    do { \
        if (cond) { kv_tests::report_pass(msg); } \
        else { kv_tests::report_fail(msg, "condition false: " #cond); } \
    } while (0)

#define KV_CHECK_FALSE(cond, msg) KV_CHECK(!(cond), msg)

#define KV_CHECK_EQ(expected, actual, msg) \
    do { \
        auto kv_check_eq_expected = (expected); \
        auto kv_check_eq_actual = (actual); \
        if (kv_check_eq_expected == kv_check_eq_actual) { \
            kv_tests::report_pass(msg); \
        } else { \
            std::ostringstream kv_check_eq_oss; \
            kv_check_eq_oss << "expected=[" << kv_check_eq_expected \
                             << "] actual=[" << kv_check_eq_actual << "]"; \
            kv_tests::report_fail(msg, kv_check_eq_oss.str()); \
        } \
    } while (0)
