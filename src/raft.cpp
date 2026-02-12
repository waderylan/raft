#include "common/utils/rand_gen.hpp"
#include "rafty/raft.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif

namespace rafty {
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::experimental::ClientInterceptorFactoryInterface;
using grpc::experimental::CreateCustomChannelWithInterceptors;

Raft::Raft(const Config &config, MessageQueue<ApplyResult> &ready)
    : logger(utils::logger::get_logger(config.id)), id(config.id), listening_addr(config.addr),
      peer_addrs(config.peer_addrs), dead(false), ready_queue(ready)
// TODO: add more field if desired
{
  service_ = std::make_unique<RaftServiceImpl>(this);
}

Raft::~Raft() {
  this->stop_server();
}

void Raft::run() {
  // TODO: kick off the raft instance
  // Note: this function should be non-blocking

  // lab 1

  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    // Someone already started it, do nothing.
    return;
  }

  stop_.store(false);

  {
    std::lock_guard<std::mutex> lock(mtx);
    last_heartbeat_received_ = std::chrono::steady_clock::now();
  }

  background_ = std::thread([this] { this->timer_loop_(); });
}

State Raft::get_state() const {
  // TODO: lab 1
}

ProposalResult Raft::propose(const std::string &data) {
  // TODO: lab 2
}

ProposalResult Raft::propose_sync(const std::string &data) {
  // TODO: lab 3
}

void Raft::timer_loop_() {
  std::unique_lock<std::mutex> lock(mtx);

  while (!dead.load() && !stop_.load()) {

    auto election_deadline = last_heartbeat_received_ + election_timeout_min_;

    timer_cv_.wait_until(lock, election_deadline);

    if (dead.load() || stop_.load()) {
      break;
    }

    auto now = std::chrono::steady_clock::now();

    if (now > election_deadline) {
      logger->info("Election timeout reached (term={})", current_term_);

      last_heartbeat_received_ = now;
    }
  }
}

} // namespace rafty
