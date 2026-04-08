#include <chrono>
#include <cstdint>
#include <format>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include <gtest/gtest.h>

#include "common/logger.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_config.hpp"
#include "toolings/test_ctrl.hpp"
#include "kv/kv_client.hpp"

using namespace toolings;

static constexpr uint64_t KV_PORT_OFFSET = 1000;
static const std::string KV_NODE_APP_PATH = "../app/kv_node";

ABSL_FLAG(int, tester_verb2, 1, "Tester verbosity level");
ABSL_FLAG(int, raft_node_verb2, 0, "Raft node verbosity level");

// ---------------------------------------------------------------------------
// Same fixture pattern as kv_test.cpp — 3-replica cluster
// ---------------------------------------------------------------------------
class KvExtraFixture : public ::testing::Test {
protected:
  static constexpr uint64_t NUM_NODES = 3;
  static constexpr uint64_t RAFT_BASE_PORT = 50150; // different port from kv_test

  void SetUp() override {
    rafty::utils::init_logger();

    auto test_name = ::testing::UnitTest::GetInstance()
                         ->current_test_info()->name();
    auto logger_name = std::format("kv_extra_{}", test_name);
    logger = spdlog::get(logger_name);
    if (!logger) {
      logger = spdlog::basic_logger_mt(
          logger_name, std::format("logs/{}.log", logger_name), true);
    }

    auto insts = ConfigGen::gen_local_instances(NUM_NODES, RAFT_BASE_PORT);

    std::unordered_map<uint64_t, uint64_t> node_tester_ports;
    uint64_t tester_port = 55101;

    for (const auto &inst : insts) {
      std::map<uint64_t, std::string> peer_addrs;
      for (const auto &peer : insts) {
        if (peer.id == inst.id) continue;
        peer_addrs[peer.id] = peer.external_addr;
      }
      rafty::Config config = {
          .id = inst.id,
          .addr = inst.listening_addr,
          .peer_addrs = peer_addrs
      };
      configs.push_back(config);
      node_tester_ports[inst.id] = tester_port++;
      kv_addrs.push_back(
          "localhost:" + std::to_string(inst.port + KV_PORT_OFFSET));
    }

    const std::string ctrl_addr = "0.0.0.0:55100";
    DDBConfig ddb_conf{};
    ctrl = std::make_unique<RaftTestCtrl>(
        configs, node_tester_ports, KV_NODE_APP_PATH,
        ctrl_addr, 0, absl::GetFlag(FLAGS_raft_node_verb2),
        logger, ddb_conf);

    ctrl->register_applier_handler([](testerpb::ApplyResult m) { (void)m; });
    ctrl->run();
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  void TearDown() override {
    if (ctrl) ctrl->kill();
  }

  std::unique_ptr<kv::KvClient> make_client() {
    return std::make_unique<kv::KvClient>(kv_addrs);
  }

  void disconnect(uint64_t id) { ctrl->disconnect({id}); }
  void reconnect(uint64_t id)  { ctrl->reconnect({id});  }

  std::shared_ptr<spdlog::logger> logger;
  std::unique_ptr<RaftTestCtrl> ctrl;
  std::vector<rafty::Config> configs;
  std::vector<std::string> kv_addrs;
};

// ---------------------------------------------------------------------------
// Test: Get on empty store returns empty string, not an error
// ---------------------------------------------------------------------------
TEST_F(KvExtraFixture, GetEmptyKeyReturnsEmpty) {
  auto client = make_client();
  auto [status, val] = client->get("does_not_exist");
  ASSERT_EQ(status, kvpb::KV_SUCCESS);
  ASSERT_EQ(val, "");
}

// ---------------------------------------------------------------------------
// Test: Append to non-existent key acts like Put
// ---------------------------------------------------------------------------
TEST_F(KvExtraFixture, AppendToNewKeyActsLikePut) {
  auto client = make_client();
  ASSERT_EQ(client->append("newkey", "hello"), kvpb::KV_SUCCESS);
  auto [status, val] = client->get("newkey");
  ASSERT_EQ(status, kvpb::KV_SUCCESS);
  ASSERT_EQ(val, "hello");
}

// ---------------------------------------------------------------------------
// Test: Put with empty string value is valid
// ---------------------------------------------------------------------------
TEST_F(KvExtraFixture, PutEmptyValue) {
  auto client = make_client();
  ASSERT_EQ(client->put("emptyval", ""), kvpb::KV_SUCCESS);
  auto [status, val] = client->get("emptyval");
  ASSERT_EQ(status, kvpb::KV_SUCCESS);
  ASSERT_EQ(val, "");
}

// ---------------------------------------------------------------------------
// Test: Put overwrites value set by Append
// ---------------------------------------------------------------------------
TEST_F(KvExtraFixture, PutAfterAppendOverwrites) {
  auto client = make_client();
  ASSERT_EQ(client->append("k", "aaa"), kvpb::KV_SUCCESS);
  ASSERT_EQ(client->append("k", "bbb"), kvpb::KV_SUCCESS);
  ASSERT_EQ(client->put("k", "fresh"), kvpb::KV_SUCCESS);
  auto [status, val] = client->get("k");
  ASSERT_EQ(status, kvpb::KV_SUCCESS);
  ASSERT_EQ(val, "fresh");
}

// ---------------------------------------------------------------------------
// Test: Large number of sequential ops on same key
// ---------------------------------------------------------------------------
TEST_F(KvExtraFixture, ManySequentialOps) {
  auto client = make_client();
  ASSERT_EQ(client->put("counter", ""), kvpb::KV_SUCCESS);

  constexpr int N = 50;
  for (int i = 0; i < N; i++) {
    ASSERT_EQ(client->append("counter", "x"), kvpb::KV_SUCCESS);
  }

  auto [status, val] = client->get("counter");
  ASSERT_EQ(status, kvpb::KV_SUCCESS);
  ASSERT_EQ(val, std::string(N, 'x'));
}

// ---------------------------------------------------------------------------
// Test: Data survives a follower disconnect and reconnect
// // A follower going down should not affect the cluster
// ---------------------------------------------------------------------------
TEST_F(KvExtraFixture, FollowerFailureDoesNotAffectCluster) {
  auto client = make_client();

  ASSERT_EQ(client->put("key", "before"), kvpb::KV_SUCCESS);

  // Find and disconnect a follower
  auto states = ctrl->get_all_states();
  uint64_t follower_id = UINT64_MAX;
  for (const auto &s : states) {
    if (!s.is_leader()) {
      follower_id = s.id();
      break;
    }
  }

  ASSERT_NE(follower_id, UINT64_MAX) << "No follower found";
  disconnect(follower_id);

  // Cluster still has majority (2/3) — ops should work
  ASSERT_EQ(client->put("key", "after"), kvpb::KV_SUCCESS);
  auto [status, val] = client->get("key");
  ASSERT_EQ(status, kvpb::KV_SUCCESS);
  ASSERT_EQ(val, "after");

  // Reconnect follower
  reconnect(follower_id);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Verify follower caught up
  auto [s2, v2] = client->get("key");
  ASSERT_EQ(s2, kvpb::KV_SUCCESS);
  ASSERT_EQ(v2, "after");
}

// ---------------------------------------------------------------------------
// Test: Multiple concurrent clients writing to the SAME key are linearizable
// Final value must be one of the valid Put values, not a mix
// ---------------------------------------------------------------------------
TEST_F(KvExtraFixture, ConcurrentPutsSameKey) {
  constexpr int NUM_CLIENTS = 5;
  auto client = make_client();
  ASSERT_EQ(client->put("shared_key", "init"), kvpb::KV_SUCCESS);

  std::vector<std::future<kvpb::KvStatus>> futs;
  for (int c = 0; c < NUM_CLIENTS; c++) {
    futs.push_back(std::async(std::launch::async, [this, c]() {
      auto cl = make_client();
      return cl->put("shared_key", std::format("client{}", c));
    }));
  }

  for (auto &f : futs) {
    ASSERT_EQ(f.get(), kvpb::KV_SUCCESS);
  }

  // Final value must be exactly one of the client values
  auto [status, val] = client->get("shared_key");
  ASSERT_EQ(status, kvpb::KV_SUCCESS);

  bool valid = false;
  for (int c = 0; c < NUM_CLIENTS; c++) {
    if (val == std::format("client{}", c)) {
      valid = true;
      break;
    }
  }
  ASSERT_TRUE(valid) << "Final value '" << val << "' is not a valid Put value";
}

// ---------------------------------------------------------------------------
// Test: Reconnected node catches up and serves correct reads
// ---------------------------------------------------------------------------
TEST_F(KvExtraFixture, ReconnectedNodeCatchesUp) {
  auto client = make_client();

  // Find a follower and disconnect it
  auto states = ctrl->get_all_states();
  uint64_t follower_id = UINT64_MAX;
  for (const auto &s : states) {
    if (!s.is_leader()) {
      follower_id = s.id();
      break;
    }
  }
  ASSERT_NE(follower_id, UINT64_MAX);
  disconnect(follower_id);

  // Write data while follower is disconnected
  for (int i = 0; i < 20; i++) {
    ASSERT_EQ(client->put(std::format("k{}", i),
                          std::format("v{}", i)), kvpb::KV_SUCCESS);
  }

  // Reconnect and wait for catch-up
  reconnect(follower_id);
  std::this_thread::sleep_for(std::chrono::seconds(3));

  // Reads should reflect all committed writes
  for (int i = 0; i < 20; i++) {
    auto [status, val] = client->get(std::format("k{}", i));
    ASSERT_EQ(status, kvpb::KV_SUCCESS);
    ASSERT_EQ(val, std::format("v{}", i));
  }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);
  return RUN_ALL_TESTS();
}