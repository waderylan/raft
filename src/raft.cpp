#include "common/utils/rand_gen.hpp"
#include "raft.pb.h"
#include <chrono>
#include <mutex>
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
  required_majority_ = (static_cast<uint64_t>(peer_addrs.size()) + 1) / 2 + 1;
}

Raft::~Raft() {
  stop_.store(true);
  timer_cv_.notify_all();

  if (background_.joinable()) {
    background_.join();
  }

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
    std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
    last_heartbeat_received_ = now;
    next_heartbeat_deadline_ = now + heartbeat_interval_;
    last_heartbeat_sent_ = now;
  }
  background_ = std::thread([this] { this->timer_loop_(); });
}

State Raft::get_state() const {
  // lab 1
  std::lock_guard<std::mutex> lock(mtx);

  State s{};
  s.term = current_term_;
  s.is_leader = (role_ == Role::Leader);
  return s;
}

ProposalResult Raft::propose(const std::string &data) {
  // TODO: lab 2
}

ProposalResult Raft::propose_sync(const std::string &data) {
  // TODO: lab 3
}

void Raft::timer_loop_() {
  std::unique_lock<std::mutex> lock(mtx);

  std::chrono::milliseconds timeout = rand_election_timeout_();
  std::chrono::steady_clock::time_point election_deadline = last_heartbeat_received_ + timeout;

  while (!dead.load() && !stop_.load()) {

    logger->info("TIMER: role={} term={}",
                 (role_ == Role::Leader ? "L" : (role_ == Role::Candidate ? "C" : "F")),
                 current_term_);

    if (role_ == Role::Leader) {
      timer_cv_.wait_until(lock, next_heartbeat_deadline_);

      if (dead.load() || stop_.load()) {
        break;
      }

      // Check if lost leader status while sleeping
      if (role_ != Role::Leader) {
        timeout = rand_election_timeout_();
        election_deadline = last_heartbeat_received_ + timeout;
        continue;
      }

      std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
      if (now >= next_heartbeat_deadline_) {
        next_heartbeat_deadline_ = now + heartbeat_interval_;

        lock.unlock();
        send_heartbeats_();
        lock.lock();
      }

    } else {

      timer_cv_.wait_until(lock, election_deadline);

      if (dead.load() || stop_.load()) {
        break;
      }

      std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();

      // If we became leader while sleeping, send heartbeat ASAP
      if (role_ == Role::Leader) {
        next_heartbeat_deadline_ = now;
        continue;
      }

      if (now >= election_deadline) {
        logger->info("Election timeout reached (term={})", current_term_);

        lock.unlock();
        start_election_();
        lock.lock();

        timeout = rand_election_timeout_();
        election_deadline = last_heartbeat_received_ + timeout;
      } else {
        election_deadline = last_heartbeat_received_ + timeout;
      }
    }
  }
}

std::chrono::milliseconds Raft::rand_election_timeout_() const {
  thread_local std::mt19937_64 rng{std::random_device{}()};

  std::uniform_int_distribution<int64_t> dist(election_timeout_min_.count(),
                                              election_timeout_max_.count());

  return std::chrono::milliseconds(dist(rng));
}

void Raft::send_heartbeats_() {
  uint64_t term = 0;
  {
    std::lock_guard<std::mutex> lock(mtx);
    if (role_ != Role::Leader) {
      return;
    }
    term = current_term_;
  }

  logger->info("Heartbeat send: term={} to {} peers", term, peers_.size());

  for (const std::pair<const uint64_t, RaftServiceStub> &peer : peers_) {
    const uint64_t peer_id = peer.first;

    raftpb::AppendEntriesRequest req;
    req.set_term(term);
    req.set_leaderid(id);
    req.set_prevlogindex(0);
    req.set_prevlogterm(0);
    req.set_leadercommit(0);

    raftpb::AppendEntriesReply resp;

    std::unique_ptr<grpc::ClientContext> ctx = this->create_context(peer_id);
    grpc::Status status = peers_[peer_id]->AppendEntries(ctx.get(), req, &resp);

    if (!status.ok()) {
      continue;
    }

    if (resp.term() > term) {
      // Response has higher term, stand down
      std::lock_guard<std::mutex> lock(mtx);
      if (resp.term() > current_term_) {
        current_term_ = resp.term();
        role_ = Role::Follower;
        voted_for_.reset();
        last_heartbeat_received_ = std::chrono::steady_clock::now();
        timer_cv_.notify_all();
      }
      return;
    }
  }

  {
    std::lock_guard<std::mutex> lock(mtx);
    last_heartbeat_sent_ = std::chrono::steady_clock::now();
  }
}

void Raft::become_follower_(uint64_t new_term) {
  std::lock_guard<std::mutex> lock(mtx);

  if (new_term > current_term_) {
    current_term_ = new_term;
    voted_for_.reset();
  }

  role_ = Role::Follower;
  votes_received_ = 0;
  last_heartbeat_received_ = std::chrono::steady_clock::now();
  timer_cv_.notify_all();

  logger->info("ID {} became follower: term={}", id, current_term_);
}

void Raft::become_leader_() {
  std::lock_guard<std::mutex> lock(mtx);

  role_ = Role::Leader;
  votes_received_ = 0;

  next_heartbeat_deadline_ = std::chrono::steady_clock::now(); // send ASAP
  timer_cv_.notify_all();

  logger->info("ID {} became leader: term={}", id, current_term_);
}

void Raft::start_election_() {
  uint64_t election_term = 0;
  {
    std::lock_guard<std::mutex> lock(mtx);

    role_ = Role::Candidate;
    current_term_ += 1;

    // self vote
    voted_for_ = id;
    votes_received_ = 1;

    last_heartbeat_received_ = std::chrono::steady_clock::now();
    timer_cv_.notify_all();

    election_term = current_term_;

    logger->info("ID {} started election: term={} voted_for={} votes={}", id, current_term_, id,
                 votes_received_);
  }

  for (const std::pair<const uint64_t, RaftServiceStub> &peer : peers_) {
    const uint64_t peer_id = peer.first;

    raftpb::RequestVoteRequest req;
    req.set_term(election_term);
    req.set_candidateid(id);
    req.set_lastlogindex(0);
    req.set_lastlogterm(0);

    raftpb::RequestVoteReply rep;

    std::unique_ptr<grpc::ClientContext> ctx = this->create_context(peer_id);
    grpc::Status status = peers_[peer_id]->RequestVote(ctx.get(), req, &rep);

    if (!status.ok()) {
      continue;
    }

    {
      std::lock_guard<std::mutex> lock(mtx);

      if (role_ != Role::Candidate) {
        continue;
      }
      if (current_term_ != election_term) {
        continue;
      }

      // Check if peer has higher term and step down
      if (rep.term() > current_term_) {
        current_term_ = rep.term();
        role_ = Role::Follower;
        voted_for_.reset();
        votes_received_ = 0;

        last_heartbeat_received_ = std::chrono::steady_clock::now();
        timer_cv_.notify_all();
        continue;
      }

      if (rep.votegranted()) {
        votes_received_ += 1;

        logger->info("Vote granted by {}: votes={}, required majority={}, term={}", peer_id,
                     votes_received_, required_majority_, current_term_);

        if (votes_received_ >= required_majority_) {
          role_ = Role::Leader;
          votes_received_ = 0;

          next_heartbeat_deadline_ = std::chrono::steady_clock::now();
          timer_cv_.notify_all();

          logger->info("ID {} became leader: term={}", id, current_term_);
          break;
        }
      }
    }
  }
}

} // namespace rafty
