// Entry point for kv_cluster_tests -- separate from kv_tests/src/main.cpp
// so the core engine test binary stays exactly as fast and platform-neutral
// as it already is (this binary and everything it links is Linux-only, see
// CMakeLists.txt's if(UNIX) guard).
#include <iostream>
#include "tests.h"
#include "test_framework.h"

int main() {
    try {
        kv_tests::run_test_hash_ring();
        std::cout << std::endl;

        kv_tests::run_test_wire_protocol();
        std::cout << std::endl;

        const auto& s = kv_tests::stats();
        std::cout << "===================================" << std::endl;
        std::cout << "Cluster test results: " << s.passed << " passed, " << s.failed << " failed" << std::endl;
        std::cout << "===================================" << std::endl;

        return s.failed == 0 ? 0 : 1;
    } catch (const std::exception& e) {
        std::cerr << "Cluster test suite crashed with error: " << e.what() << std::endl;
        return 1;
    }
}
