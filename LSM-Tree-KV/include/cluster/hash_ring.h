#pragma once
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace kv_cluster {

// Routes keys to a fixed set of node IDs by consistent hashing.
//
// Deliberately minimal for this phase: one ring point per node, membership
// set once via set_nodes() and never changed afterward. No virtual nodes,
// no add_node/remove_node -- those (plus failover and replication) are a
// later phase, built on top of this same ring structure rather than a
// rewrite of it. With a fixed node set this phase doesn't need consistent
// hashing's "minimal remapping on membership change" property yet, but
// using the same ring shape now means that property is available for free
// once virtual nodes and dynamic membership are added later.
class HashRing {
public:
    // Replaces the entire node set. Call once at startup; the ring is not
    // safe to mutate concurrently with get_node() calls from other threads
    // (callers should finish set_nodes() before serving any requests).
    void set_nodes(const std::vector<std::string>& node_ids);

    // Returns the node ID responsible for `key`, or std::nullopt if the
    // ring has no nodes.
    std::optional<std::string> get_node(const std::string& key) const;

    // Returns the current node set (for tests/diagnostics).
    std::vector<std::string> nodes() const;

private:
    std::map<uint64_t, std::string> ring_; // hash(node_id) -> node_id
};

} // namespace kv_cluster
