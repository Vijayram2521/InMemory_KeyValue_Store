#include "cluster/compute_node_server.h"
#include "cluster/tcp_socket.h"
#include "cluster/wire_protocol.h"
#include "engine/storage_engine.h"
#include "tests.h"
#include "test_framework.h"
#include <string>

void kv_tests::run_test_cluster_integration() {
    std::cout << "--- [Test] Cluster Integration (compute node over real TCP) ---" << std::endl;

    std::string data_dir = "./TestStorage/test_cluster_compute_node";
    cleanup_test_dir(data_dir);

    kv_engine::StorageEngine engine(data_dir);
    kv_cluster::ComputeNodeServer server(engine);
    KV_CHECK(server.start(0), "ComputeNodeServer starts on an OS-assigned ephemeral port");
    uint16_t port = server.port();

    kv_cluster::TcpSocket client;
    KV_CHECK(client.connect("127.0.0.1", port), "client connects to the compute node over real TCP");

    // PUT, over the wire, decoded from a real socket response -- not a
    // direct StorageEngine call. This is the point of this test: proving
    // the full encode -> send -> compute-node decode -> StorageEngine ->
    // encode -> send -> client decode round trip actually works.
    {
        KV_CHECK(client.send_all(kv_cluster::encode_put_request("hello", "world")),
                 "PUT_REQUEST sends successfully over the socket");
        auto msg = kv_cluster::receive_message(client);
        auto* resp = msg ? std::get_if<kv_cluster::PutResponse>(&*msg) : nullptr;
        KV_CHECK(resp != nullptr && resp->ok, "PUT_RESPONSE comes back ok over the wire");
    }

    // GET a key that was just PUT, over the wire.
    {
        KV_CHECK(client.send_all(kv_cluster::encode_get_request("hello")),
                 "GET_REQUEST sends successfully");
        auto msg = kv_cluster::receive_message(client);
        auto* resp = msg ? std::get_if<kv_cluster::GetResponse>(&*msg) : nullptr;
        KV_CHECK(resp != nullptr && resp->found && resp->value == "world",
                 "GET_RESPONSE over the wire returns the value just PUT through the wire");
    }

    // GET a key that was never written.
    {
        client.send_all(kv_cluster::encode_get_request("never_written"));
        auto msg = kv_cluster::receive_message(client);
        auto* resp = msg ? std::get_if<kv_cluster::GetResponse>(&*msg) : nullptr;
        KV_CHECK(resp != nullptr && !resp->found, "GET_RESPONSE for a never-written key reports not found");
    }

    // DELETE, then GET should report not found.
    {
        client.send_all(kv_cluster::encode_delete_request("hello"));
        auto del_msg = kv_cluster::receive_message(client);
        auto* del_resp = del_msg ? std::get_if<kv_cluster::DeleteResponse>(&*del_msg) : nullptr;
        KV_CHECK(del_resp != nullptr && del_resp->ok, "DELETE_RESPONSE comes back ok over the wire");

        client.send_all(kv_cluster::encode_get_request("hello"));
        auto get_msg = kv_cluster::receive_message(client);
        auto* get_resp = get_msg ? std::get_if<kv_cluster::GetResponse>(&*get_msg) : nullptr;
        KV_CHECK(get_resp != nullptr && !get_resp->found,
                 "GET after DELETE reports not found, over the wire, matching direct-API tombstone behavior");
    }

    // A second, independent client connection works concurrently with the
    // first -- proves the server's thread-per-connection model plus
    // StorageEngine's own locking actually handle concurrent connections,
    // not just a single one.
    {
        kv_cluster::TcpSocket second_client;
        KV_CHECK(second_client.connect("127.0.0.1", port), "a second client connects while the first is still open");
        second_client.send_all(kv_cluster::encode_put_request("from_second_client", "value2"));
        auto msg = kv_cluster::receive_message(second_client);
        auto* resp = msg ? std::get_if<kv_cluster::PutResponse>(&*msg) : nullptr;
        KV_CHECK(resp != nullptr && resp->ok, "second client's PUT succeeds independently of the first");

        // Confirm via the FIRST client that the data is visible (same
        // engine instance, so no propagation delay to worry about).
        client.send_all(kv_cluster::encode_get_request("from_second_client"));
        auto verify_msg = kv_cluster::receive_message(client);
        auto* verify_resp = verify_msg ? std::get_if<kv_cluster::GetResponse>(&*verify_msg) : nullptr;
        KV_CHECK(verify_resp != nullptr && verify_resp->found && verify_resp->value == "value2",
                 "data written by the second client is visible to the first (same underlying StorageEngine)");
    }

    server.stop();
    cleanup_test_dir(data_dir);
}
