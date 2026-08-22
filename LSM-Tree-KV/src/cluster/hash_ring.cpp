#include "cluster/hash_ring.h"
#include "engine/hash_utils.h"

namespace kv_cluster {

void HashRing::set_nodes(const std::vector<std::string>& node_ids) {
    ring_.clear();
    for (const auto& id : node_ids) {
        ring_[kv_engine::avalanche(kv_engine::fnv1a(id, 0))] = id;
    }
}

std::optional<std::string> HashRing::get_node(const std::string& key) const {
    if (ring_.empty()) return std::nullopt;

    uint64_t h = kv_engine::avalanche(kv_engine::fnv1a(key, 0));
    auto it = ring_.lower_bound(h);
    if (it == ring_.end()) {
        it = ring_.begin(); // wrap around the ring
    }
    return it->second;
}

std::vector<std::string> HashRing::nodes() const {
    std::vector<std::string> result;
    result.reserve(ring_.size());
    for (const auto& [hash, id] : ring_) {
        result.push_back(id);
    }
    return result;
}

} // namespace kv_cluster
