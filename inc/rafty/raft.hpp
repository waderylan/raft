#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <chrono>
#include <condition_variable>
#include <optional>
#include <thread>
#include <map>
#include <random>

#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "common/config.hpp"
#include "common/logger.hpp"
#include "toolings/msg_queue.hpp"
#include "rafty/rpc_service.hpp"

// it will pick up correct header
// when you generate the grpc proto files
#include "raft.grpc.pb.h"

using namespace toolings;

enum class Role { Follower, Candidate, Leader };

namespace rafty {
using RaftServiceStub = std::unique_ptr<raftpb::RaftService::Stub>;
using grpc::Server;

class Raft {
public:
  Raft(const Config &config, MessageQueue<ApplyResult> &ready);
  ~Raft();

  // WARN: do not modify the signature
  // TODO: implement `run`, `propose` and `get_state`
  void run();                                      /* lab 1 */
  ProposalResult propose(const std::string &data); /* lab 1 */
  State get_state() const;                         /* lab 1? */

  // lab3: sync propose
  ProposalResult propose_sync(const std::string &data);

  // WARN: do not modify the signature
  void start_server();
  void stop_server();
  void connect_peers();
  bool is_dead() const;
  void kill();

private:
  // WARN: do not modify `create_context` and `apply`.

  // invoke `create_context` when creating context for rpc call.
  // args: the id of which raft instance the RPC will go to.
  std::unique_ptr<grpc::ClientContext> create_context(uint64_t to) const;
  void apply(const ApplyResult &result);

  void timer_loop_();
  std::chrono::milliseconds rand_election_timeout_() const;
  void send_heartbeats_();
  void start_election_();
  void become_leader_locked_();

protected:
  // WARN: do not modify `mtx` and `logger`.
  mutable std::mutex mtx;
  std::unique_ptr<rafty::utils::logger> logger;

private:
  friend class RaftServiceImpl;

  // WARN: do not modify the declaration of
  // `id`, `listening_addr`, `peer_addrs`,
  // `dead`, `ready_queue`, `peers_`, and `server_`.

  // node identity
  uint64_t id;
  std::string listening_addr;
  std::map<uint64_t, std::string> peer_addrs;

  // infastructure
  std::atomic<bool> dead;
  MessageQueue<ApplyResult> &ready_queue;
  std::unordered_map<uint64_t, RaftServiceStub> peers_;
  std::unique_ptr<Server> server_;
  std::unique_ptr<raftpb::RaftService::Service> service_;

  // persistent state
  uint64_t current_term_ = 0;
  std::optional<uint64_t> voted_for_; // peer id this node voted for
  std::vector<raftpb::Entry> log_;

  // election state
  uint64_t votes_received_ = 0;
  uint64_t required_majority_ = 0; // number of votes needed to win election
  Role role_ = Role::Follower;

  // all-server volatile state
  uint64_t commit_index_ = 0; // highest entry known to be committed
  uint64_t last_applied_ = 0; // highest entry applied to state machine

  // leader-only volatile state (reinitialized after election)
  std::unordered_map<uint64_t, uint64_t> next_index_; // per peer, what index to send next
  std::unordered_map<uint64_t, uint64_t> match_index_; // per peer, what index is confirmed replicated

  // timing
  std::chrono::milliseconds heartbeat_interval_{100};
  std::chrono::milliseconds election_timeout_min_{150};
  std::chrono::milliseconds election_timeout_max_{300};
  std::chrono::steady_clock::time_point last_heartbeat_received_;
  std::chrono::steady_clock::time_point last_heartbeat_sent_;
  std::chrono::steady_clock::time_point next_heartbeat_deadline_;

  // threading
  std::atomic<bool> running_{false};
  std::atomic<bool> stop_{false};
  std::condition_variable timer_cv_;
  std::thread background_;
};
} // namespace rafty

#include "rafty/impl/raft.ipp" // IWYU pragma: keep
