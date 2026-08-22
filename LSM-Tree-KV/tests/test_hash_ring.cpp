#include "cluster/hash_ring.h"
#include "tests.h"
#include "test_framework.h"
#include <set>
#include <string>

void kv_tests::run_test_hash_ring() {
    std::cout << "--- [Test] HashRing ---" << std::endl;

    kv_cluster::HashRing empty_ring;
    KV_CHECK_FALSE(empty_ring.get_node("any_key").has_value(),
                   "an empty ring (no nodes set) returns nullopt for any key");

    kv_cluster::HashRing single;
    single.set_nodes({"only-node"});
    auto r = single.get_node("whatever_key");
    KV_CHECK(r.has_value() && *r == "only-node", "a single-node ring routes every key to that node");

    kv_cluster::HashRing ring;
    const std::vector<std::string> node_ids = {"compute-1", "compute-2", "compute-3"};
    ring.set_nodes(node_ids);

    // Determinism: the same key against the same (unchanged) ring must
    // always route to the same node -- this is the property everything
    // else (correct request routing) depends on.
    bool deterministic = true;
    for (int i = 0; i < 1000; ++i) {
        std::string key = "det_check_" + std::to_string(i);
        auto first = ring.get_node(key);
        auto second = ring.get_node(key);
        if (first != second) { deterministic = false; break; }
    }
    KV_CHECK(deterministic, "get_node(key) returns the same node on repeated calls for the same key");

    // Every configured node must be reachable -- across a large enough
    // sample, no node should end up with zero keys (that would mean the
    // ring math is broken, e.g. a node's hash point never actually gets
    // hit by lower_bound/wraparound).
    std::set<std::string> nodes_hit;
    const int kSampleSize = 50000;
    for (int i = 0; i < kSampleSize; ++i) {
        auto node = ring.get_node("sample_key_" + std::to_string(i));
        if (node.has_value()) nodes_hit.insert(*node);
    }
    KV_CHECK_EQ(size_t(3), nodes_hit.size(),
                "all 3 configured nodes receive at least one key across a 50,000-key sample");

    // set_nodes() replaces the whole node set (used at startup, not a live
    // resize -- that's a future phase). After calling it again with a
    // different set, the ring should reflect only the new set.
    kv_cluster::HashRing replaced;
    replaced.set_nodes({"old-a", "old-b"});
    replaced.set_nodes({"new-x"});
    auto replaced_nodes = replaced.nodes();
    KV_CHECK_EQ(size_t(1), replaced_nodes.size(), "set_nodes() replaces the previous node set, not merges with it");
    if (replaced_nodes.size() == 1) {
        KV_CHECK_EQ(std::string("new-x"), replaced_nodes[0], "the replaced ring only knows about the new node");
    }
}
