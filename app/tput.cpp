#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <format>
#include <future>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "common/logger.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_ctrl.hpp"
#include "kv/kv_client.hpp"

static constexpr uint64_t KV_PORT_OFFSET   = 1000;
static constexpr uint64_t RAFT_BASE_PORT   = 50350;
static constexpr uint64_t TESTER_BASE_PORT = 55301;
static const std::string  KV_NODE_PATH     = "./kv_node";
static constexpr uint64_t NUM_NODES        = 3;
static constexpr uint64_t KEYSPACE         = 1000;
static constexpr uint64_t OPS_PER_CLIENT   = 1000;

ABSL_FLAG(std::string, output, "result.txt", "Output file for results");

double percentile(const std::vector<double> &sorted, double p) {
  if (sorted.empty()) return 0.0;
  size_t idx = static_cast<size_t>(p / 100.0 * sorted.size());
  if (idx >= sorted.size()) idx = sorted.size() - 1;
  return sorted[idx];
}

std::unique_ptr<toolings::RaftTestCtrl> make_cluster(
    uint64_t round,
    std::vector<std::string> &kv_addrs_out,
    std::vector<rafty::Config> &configs_out,
    std::shared_ptr<spdlog::logger> logger) {

  kv_addrs_out.clear();
  configs_out.clear();

  uint64_t raft_base    = RAFT_BASE_PORT   + round * 100;
  uint64_t tester_base  = TESTER_BASE_PORT + round * 100;
  std::string ctrl_addr = "0.0.0.0:" + std::to_string(55300 + round * 100);

  auto insts = toolings::ConfigGen::gen_local_instances(NUM_NODES, raft_base);
  std::unordered_map<uint64_t, uint64_t> node_tester_ports;
  uint64_t tester_port = tester_base;

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
    configs_out.push_back(config);
    node_tester_ports[inst.id] = tester_port++;
    kv_addrs_out.push_back("localhost:" +
                           std::to_string(inst.port + KV_PORT_OFFSET));
  }

  toolings::DDBConfig ddb_conf{};
  auto ctrl = std::make_unique<toolings::RaftTestCtrl>(
      configs_out, node_tester_ports, KV_NODE_PATH,
      ctrl_addr, 0, 0, logger, ddb_conf);

  ctrl->register_applier_handler([](testerpb::ApplyResult m) { (void)m; });
  ctrl->run();
  std::this_thread::sleep_for(std::chrono::seconds(2));
  return ctrl;
}

void populate_keys(kv::KvClient &client) {
  for (uint64_t i = 1; i <= KEYSPACE; i++) {
    client.put(std::format("key_{}", i), std::format("init_{}", i));
  }
}

int main(int argc, char **argv) {
  // Parse positional args first
  if (argc < 3) {
    std::cerr << "Usage: ./tput <MaxClientCount> <PutRatio>\n";
    return 1;
  }
  uint64_t max_clients = std::stoull(argv[1]);
  uint64_t put_ratio   = std::stoull(argv[2]);

  // Let absl parse its own flags (--output etc)
  absl::ParseCommandLine(argc, argv);
  rafty::utils::init_logger();

  std::string output_file = absl::GetFlag(FLAGS_output);

  auto logger = spdlog::get("tput");
  if (!logger) {
    logger = spdlog::basic_logger_mt("tput", "logs/tput.log", true);
  }

  // Header
  static const std::string HEADER_LINE(82, '#');
  static const std::string DIVIDER_LINE(82, '-');
  std::string col_header = std::format(
      "# {:>11}  {:>10}  {:>10}  {:>10}  {:>10}  {:>12}",
      "clientCount", "latAvg", "latP50", "latP90", "latP99", "throughput");
  std::string unit_header = std::format(
      "# {:>11}  {:>10}  {:>10}  {:>10}  {:>10}  {:>12}",
      "", "(ms)", "(ms)", "(ms)", "(ms)", "(ops/sec)");

  std::ofstream out(output_file);
  out  << HEADER_LINE  << "\n"
       << col_header   << "\n"
       << unit_header  << "\n"
       << DIVIDER_LINE << "\n";

  std::cout << HEADER_LINE  << "\n"
            << col_header   << "\n"
            << unit_header  << "\n"
            << DIVIDER_LINE << "\n";

  uint64_t round = 0;

  for (uint64_t num_clients = 1; num_clients <= max_clients;
       num_clients *= 2, round++) {

    // Fresh cluster
    std::vector<std::string> kv_addrs;
    std::vector<rafty::Config> configs;
    auto ctrl = make_cluster(round, kv_addrs, configs, logger);

    // Pre-populate keyspace
    kv::KvClient setup_client(kv_addrs);
    populate_keys(setup_client);

    // Shared state
    std::mutex lat_mu;
    std::vector<double> all_latencies;
    all_latencies.reserve(num_clients * OPS_PER_CLIENT);
    std::atomic<uint64_t> total_ops{0};

    auto wall_start = std::chrono::steady_clock::now();

    // Launch client threads
    std::vector<std::future<void>> futs;
    for (uint64_t c = 0; c < num_clients; c++) {
      futs.push_back(std::async(std::launch::async,
          [&kv_addrs, &lat_mu, &all_latencies,
           &total_ops, put_ratio]() {

        thread_local std::mt19937_64 rng{std::random_device{}()};
        std::uniform_int_distribution<uint64_t> key_dist(1, KEYSPACE);
        std::uniform_int_distribution<uint64_t> op_dist(1, 100);

        kv::KvClient client(kv_addrs);
        std::vector<double> local_lats;
        local_lats.reserve(OPS_PER_CLIENT);

        for (uint64_t i = 0; i < OPS_PER_CLIENT; i++) {
          auto key    = std::format("key_{}", key_dist(rng));
          bool is_put = (op_dist(rng) <= put_ratio);

          auto start = std::chrono::steady_clock::now();
          if (is_put) {
            client.put(key, "val");
          } else {
            client.get(key);
          }
          auto end = std::chrono::steady_clock::now();

          double ms = std::chrono::duration<double, std::milli>(
                          end - start).count();
          local_lats.push_back(ms);
        }

        {
          std::lock_guard<std::mutex> lock(lat_mu);
          all_latencies.insert(all_latencies.end(),
                               local_lats.begin(), local_lats.end());
          total_ops += OPS_PER_CLIENT;
        }
      }));
    }

    for (auto &f : futs) f.get();

    auto wall_end = std::chrono::steady_clock::now();
    double wall_ms = std::chrono::duration<double, std::milli>(
                         wall_end - wall_start).count();

    // Stats
    std::sort(all_latencies.begin(), all_latencies.end());
    double avg  = std::accumulate(all_latencies.begin(),
                                  all_latencies.end(), 0.0)
                  / all_latencies.size();
    double p50  = percentile(all_latencies, 50.0);
    double p90  = percentile(all_latencies, 90.0);
    double p99  = percentile(all_latencies, 99.0);
    double tput = (total_ops.load() / wall_ms) * 1000.0;

    std::string row = std::format(
        "  {:>11}  {:>10.2f}  {:>10.2f}  {:>10.2f}  {:>10.2f}  {:>12.0f}\n",
        num_clients, avg, p50, p90, p99, tput);

    out   << row;
    std::cout << row << std::flush;

    ctrl->kill();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  out.close();
  std::cout << "Results written to " << output_file << std::endl;
  return 0;
}