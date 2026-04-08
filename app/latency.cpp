#include <algorithm>
#include <chrono>
#include <cstdint>
#include <format>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <csignal>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "common/logger.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_ctrl.hpp"
#include "kv/kv_client.hpp"

static constexpr uint64_t KV_PORT_OFFSET = 1000;
static constexpr uint64_t RAFT_BASE_PORT = 50250;
static constexpr uint64_t TESTER_BASE_PORT = 55201;
static const std::string CTRL_ADDR = "0.0.0.0:55200";
static const std::string KV_NODE_PATH = "./kv_node";

ABSL_FLAG(uint64_t, num_nodes, 3, "Number of KV replica nodes");
ABSL_FLAG(uint64_t, num_ops, 1000, "Number of sequential Put operations");

// Compute percentile from a sorted vector
double percentile(const std::vector<double> &sorted, double p) {
  if (sorted.empty()) return 0.0;
  size_t idx = static_cast<size_t>(p / 100.0 * sorted.size());
  if (idx >= sorted.size()) idx = sorted.size() - 1;
  return sorted[idx];
}

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);
  rafty::utils::init_logger();

  uint64_t num_nodes = absl::GetFlag(FLAGS_num_nodes);
  uint64_t num_ops   = absl::GetFlag(FLAGS_num_ops);

  // Logger
  auto logger = spdlog::get("latency");
  if (!logger) {
    logger = spdlog::basic_logger_mt("latency", "logs/latency.log", true);
  }

  // Build cluster configs
  std::vector<rafty::Config> configs;
  std::unordered_map<uint64_t, uint64_t> node_tester_ports;
  std::vector<std::string> kv_addrs;
  uint64_t tester_port = TESTER_BASE_PORT;

  auto insts = toolings::ConfigGen::gen_local_instances(num_nodes, RAFT_BASE_PORT);
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
    kv_addrs.push_back("localhost:" +
                       std::to_string(inst.port + KV_PORT_OFFSET));
  }

  // Spawn cluster
  toolings::DDBConfig ddb_conf{};
  auto ctrl = std::make_unique<toolings::RaftTestCtrl>(
      configs, node_tester_ports, KV_NODE_PATH,
      CTRL_ADDR, 0, 0, logger, ddb_conf);

  ctrl->register_applier_handler([](testerpb::ApplyResult m) { (void)m; });
  ctrl->run();

  // Wait for cluster to stabilize and elect a leader
  std::cout << "Waiting for cluster to stabilize..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Create client
  kv::KvClient client(kv_addrs);

  // Warm up — 10 ops not measured
  for (int i = 0; i < 10; i++) {
    client.put("warmup", "warmup");
  }

  // Measure latency of num_ops sequential Puts
  std::cout << "Running " << num_ops << " sequential Put operations..."
            << std::endl;

  std::vector<double> latencies_ms;
  latencies_ms.reserve(num_ops);

  for (uint64_t i = 0; i < num_ops; i++) {
    auto key   = std::format("key_{}", i);
    auto value = std::format("value_{}", i);

    auto start = std::chrono::steady_clock::now();
    client.put(key, value);
    auto end = std::chrono::steady_clock::now();

    double ms = std::chrono::duration<double, std::milli>(end - start).count();
    latencies_ms.push_back(ms);
  }

  // Compute stats
  std::sort(latencies_ms.begin(), latencies_ms.end());

  double sum = 0.0;
  for (double v : latencies_ms) sum += v;
  double avg = sum / latencies_ms.size();
  double p50 = percentile(latencies_ms, 50.0);
  double p99 = percentile(latencies_ms, 99.0);

  // Print results
  std::cout << "\n######################################\n";
  std::cout << std::format("#    {:>10}   {:>10}   {:>10}\n",
                           "latAvg", "latP50", "latP99");
  std::cout << std::format("#    {:>10}   {:>10}   {:>10}\n",
                           "(ms)", "(ms)", "(ms)");
  std::cout << "--------------------------------------\n";
  std::cout << std::format("     {:>10.2f}   {:>10.2f}   {:>10.2f}\n",
                           avg, p50, p99);

  // Cleanup
  ctrl->kill();
  return 0;
}