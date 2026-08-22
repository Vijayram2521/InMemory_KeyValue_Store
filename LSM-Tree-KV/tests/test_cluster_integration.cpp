#include "cluster/compute_node_server.h"
#include "cluster/hash_ring.h"
#include "cluster/leader_server.h"
#include "cluster/tcp_socket.h"
#include "cluster/wire_protocol.h"
#include "engine/storage_engine.h"
#include "tests.h"
#include "test_framework.h"
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace {

// Small helpers so the two sections below (compute-node-alone, full
// leader+3-node) don't repeat the same "send a request, decode the
// expected response type" boilerplate.
bool wire_put(kv_cluster::TcpSocket& sock, const std::string& key, const std::string& value) {
    if (!sock.send_all(kv_cluster::encode_put_request(key, value))) return false;
    auto msg = kv_cluster::receive_message(sock);
    auto* r = msg ? std::get_if<kv_cluster::PutResponse>(&*msg) : nullptr;
    return r && r->ok;
}

// nullopt = not found or the request/response itself failed (caller
// distinguishes via a separate has_value check if needed).
std::optional<std::string> wire_get(kv_cluster::TcpSocket& sock, const std::string& key) {
    if (!sock.send_all(kv_cluster::encode_get_request(key))) return std::nullopt;
    auto msg = kv_cluster::receive_message(sock);
    auto* r = msg ? std::get_if<kv_cluster::GetResponse>(&*msg) : nullptr;
    if (!r || !r->found) return std::nullopt;
    return r->value;
}

bool wire_delete(kv_cluster::TcpSocket& sock, const std::string& key) {
    if (!sock.send_all(kv_cluster::encode_delete_request(key))) return false;
    auto msg = kv_cluster::receive_message(sock);
    auto* r = msg ? std::get_if<kv_cluster::DeleteResponse>(&*msg) : nullptr;
    return r && r->ok;
}

} // namespace

void kv_tests::run_test_cluster_integration() {
    // --- Section 1: a single compute node over real TCP -------------------
    std::cout << "--- [Test] Cluster Integration (compute node over real TCP) ---" << std::endl;
    {
        std::string data_dir = "./TestStorage/test_cluster_compute_node";
        cleanup_test_dir(data_dir);

        kv_engine::StorageEngine engine(data_dir);
        kv_cluster::ComputeNodeServer server(engine);
        KV_CHECK(server.start(0), "ComputeNodeServer starts on an OS-assigned ephemeral port");
        uint16_t port = server.port();

        kv_cluster::TcpSocket client;
        KV_CHECK(client.connect("127.0.0.1", port), "client connects to the compute node over real TCP");

        KV_CHECK(wire_put(client, "hello", "world"), "PUT over the wire succeeds");
        auto v = wire_get(client, "hello");
        KV_CHECK(v.has_value() && *v == "world", "GET over the wire returns the value just PUT through the wire");
        KV_CHECK_FALSE(wire_get(client, "never_written").has_value(),
                       "GET for a never-written key reports not found over the wire");
        KV_CHECK(wire_delete(client, "hello"), "DELETE over the wire succeeds");
        KV_CHECK_FALSE(wire_get(client, "hello").has_value(),
                       "GET after DELETE reports not found over the wire, matching direct-API tombstone behavior");

        {
            kv_cluster::TcpSocket second_client;
            KV_CHECK(second_client.connect("127.0.0.1", port), "a second client connects while the first is still open");
            KV_CHECK(wire_put(second_client, "from_second_client", "value2"),
                     "second client's PUT succeeds independently of the first");
            auto verify = wire_get(client, "from_second_client");
            KV_CHECK(verify.has_value() && *verify == "value2",
                     "data written by the second client is visible to the first (same underlying StorageEngine)");
            second_client.close(); // before server.stop() below -- see lifetime note at the bottom of this file
        }

        client.close(); // close client sockets before stopping the server they're talking to (see note below)
        server.stop();
        cleanup_test_dir(data_dir);
    }

    // --- Section 2: full leader + 3 compute nodes, real consistent-hash routing ---
    std::cout << "--- [Test] Cluster Integration (leader + 3 compute nodes) ---" << std::endl;
    {
        const std::vector<std::string> data_dirs = {
            "./TestStorage/test_cluster_leader_node1",
            "./TestStorage/test_cluster_leader_node2",
            "./TestStorage/test_cluster_leader_node3",
        };
        for (const auto& d : data_dirs) cleanup_test_dir(d);

        std::vector<std::unique_ptr<kv_engine::StorageEngine>> engines;
        std::vector<std::unique_ptr<kv_cluster::ComputeNodeServer>> compute_servers;
        std::vector<kv_cluster::ComputeNodeEndpoint> endpoints;

        bool all_compute_started = true;
        for (size_t i = 0; i < data_dirs.size(); ++i) {
            auto engine = std::make_unique<kv_engine::StorageEngine>(data_dirs[i]);
            auto server = std::make_unique<kv_cluster::ComputeNodeServer>(*engine);
            if (!server->start(0)) all_compute_started = false;
            std::string node_id = "compute-" + std::to_string(i + 1);
            endpoints.push_back({node_id, "127.0.0.1", server->port()});
            engines.push_back(std::move(engine));
            compute_servers.push_back(std::move(server));
        }
        KV_CHECK(all_compute_started, "all 3 compute nodes start successfully on ephemeral ports");

        // Independent HashRing (same node IDs, same algorithm) used only to
        // PREDICT which node a key should land on, so the test can verify
        // routing actually happened -- not just that *some* node answered.
        kv_cluster::HashRing verification_ring;
        {
            std::vector<std::string> node_ids;
            for (const auto& ep : endpoints) node_ids.push_back(ep.node_id);
            verification_ring.set_nodes(node_ids);
        }

        kv_cluster::LeaderServer leader(endpoints);
        KV_CHECK(leader.start(0), "LeaderServer starts on an OS-assigned ephemeral port");

        kv_cluster::TcpSocket client;
        KV_CHECK(client.connect("127.0.0.1", leader.port()), "client connects to the leader over real TCP");

        // PUT a batch of distinct keys through the leader, then verify each
        // one landed in exactly the compute node the hash ring predicts --
        // proof of real routing, not just "a correct answer came back from
        // somewhere."
        const int kKeyCount = 60;
        std::map<std::string, std::string> expected;
        bool all_puts_ok = true;
        for (int i = 0; i < kKeyCount; ++i) {
            std::string key = "leader_test_key_" + std::to_string(i);
            std::string value = "value_" + std::to_string(i);
            if (!wire_put(client, key, value)) all_puts_ok = false;
            expected[key] = value;
        }
        KV_CHECK(all_puts_ok, "all " + std::to_string(kKeyCount) + " PUTs through the leader succeed");

        bool all_routed_correctly = true;
        std::map<std::string, int> hits_per_node;
        for (const auto& [key, value] : expected) {
            auto predicted_node = verification_ring.get_node(key);
            if (!predicted_node) { all_routed_correctly = false; continue; }
            // Find which physical engine that node_id corresponds to.
            size_t idx = static_cast<size_t>(predicted_node->back() - '1'); // "compute-N" -> N-1
            if (idx >= engines.size() || !engines[idx]->Get(key).has_value() ||
                *engines[idx]->Get(key) != value) {
                all_routed_correctly = false;
            } else {
                hits_per_node[*predicted_node]++;
            }
        }
        KV_CHECK(all_routed_correctly,
                 "every key PUT through the leader landed in exactly the compute node HashRing predicts");
        KV_CHECK_EQ(size_t(3), hits_per_node.size(),
                    "all 3 compute nodes received at least one key across the batch (routing isn't degenerate)");

        // GET each key back through the leader and confirm the value.
        bool all_gets_ok = true;
        for (const auto& [key, value] : expected) {
            auto got = wire_get(client, key);
            if (!got.has_value() || *got != value) all_gets_ok = false;
        }
        KV_CHECK(all_gets_ok, "GET through the leader returns the correct value for every key, regardless of which node holds it");

        // A guaranteed-miss key routes somewhere and correctly reports not found.
        KV_CHECK_FALSE(wire_get(client, "leader_test_key_never_written").has_value(),
                       "GET through the leader for a never-written key reports not found");

        // DELETE through the leader, then confirm via GET.
        auto some_key = expected.begin()->first;
        KV_CHECK(wire_delete(client, some_key), "DELETE through the leader succeeds");
        KV_CHECK_FALSE(wire_get(client, some_key).has_value(),
                       "GET after DELETE through the leader reports not found");

        client.close(); // before stopping the servers -- see lifetime note below
        leader.stop();
        for (auto& server : compute_servers) server->stop();
        for (const auto& d : data_dirs) cleanup_test_dir(d);
    }

    // Lifetime note: ComputeNodeServer/LeaderServer detach their per-
    // connection handler threads rather than tracking/joining them (fire-
    // and-forget, matching this codebase's simple-first style -- see
    // compute_node_server.cpp). In the real compute_node/leader_node
    // binaries this is safe because the server object lives for the whole
    // process. In tests, where server objects have a much shorter
    // lifetime, explicitly closing every client socket BEFORE calling
    // stop() (rather than relying on scope-exit destruction order) gives
    // any still-blocked handler thread a clean signal to exit before the
    // StorageEngine/HashRing/etc. it references goes away.
}
