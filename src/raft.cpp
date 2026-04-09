#include "common/common.hpp"
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

  // Add an empty entry to the log so it can start at index 1
  raftpb::Entry dummy;
  dummy.set_term(0);
  dummy.set_command("");
  log_.push_back(dummy);
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
  // lab 2
  std::lock_guard<std::mutex> lock(mtx);

  ProposalResult result;
  result.is_leader = (role_ == Role::Leader);

  if (!result.is_leader) {
    // in real-world implementation this would forward to a leader
    logger->info("Propose rejected: id={} not leader (term={})", id, current_term_);
    result.term = current_term_;
    result.index = 0;
    return result;
  }

  raftpb::Entry entry;
  entry.set_term(current_term_);
  entry.set_command(data);
  log_.push_back(entry);

  result.term = current_term_;
  result.index = log_.size() - 1;

  logger->info("Proposed: id={} index={} term={} data={}", id, result.index, result.term, data);

  // Trigger immediate replication instead of waiting for heartbeat
  next_heartbeat_deadline_ = std::chrono::steady_clock::now();
  timer_cv_.notify_one();

  return result;
}

ProposalResult Raft::propose_sync(const std::string &data) {
  // TODO: lab 3
}

bool Raft::has_valid_lease() const {
  std::lock_guard<std::mutex> lock(mtx);
  return role_ == Role::Leader &&
         std::chrono::steady_clock::now() < lease_expiry_ &&
         commit_index_ == log_.size() - 1;  // could also use last_applied here
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
      election_deadline = last_heartbeat_received_ + timeout;
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

  std::atomic<uint64_t> ack_count{1}; // count self

  for (const std::pair<const uint64_t, RaftServiceStub> &peer : peers_) {
    const uint64_t peer_id = peer.first;

    uint64_t next_idx = 0;
    uint64_t prev_log_index = 0;
    uint64_t prev_log_term = 0;
    uint64_t leader_commit = 0;

    {
      std::lock_guard<std::mutex> lock(mtx);
      if (role_ != Role::Leader) {
        return;
      }
      next_idx = next_index_[peer_id];
      prev_log_index = next_idx - 1;
      prev_log_term = log_[prev_log_index].term();
      leader_commit = commit_index_;
    }

    raftpb::AppendEntriesRequest req;
    req.set_term(term);
    req.set_leaderid(id);
    req.set_prevlogindex(prev_log_index);
    req.set_prevlogterm(prev_log_term);
    req.set_leadercommit(leader_commit);

    {
      std::lock_guard<std::mutex> lock(mtx);
      for (uint64_t i = next_idx; i < log_.size(); i++) {
        raftpb::Entry *entry = req.add_entries();
        *entry = log_[i];
      }
    }

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
        lease_expiry_ = std::chrono::steady_clock::time_point::min();
        last_heartbeat_received_ = std::chrono::steady_clock::now();
        timer_cv_.notify_all();
      }
      return;
    }

    {
      std::unique_lock<std::mutex> lock(mtx);

      // stale reply check
      if (role_ != Role::Leader || current_term_ != term) {
        return;
      }

      if (resp.success()) {
        if (req.entries_size() > 0) {
          // update next and match index for this peer only if we actually sent data
          next_index_[peer_id] = next_idx + req.entries_size();
          match_index_[peer_id] = next_index_[peer_id] - 1;
        }
        ack_count++;

        // find highest index replicated on at least the majority of nodes
        uint64_t new_commit_index = commit_index_;
        for (uint64_t n = log_.size() - 1; n > commit_index_; n--) {
          uint64_t replication_count = 1; // count self

          // loop through peers and see who is caught up to n
          for (const std::pair<const uint64_t, uint64_t> &entry : match_index_) {
            if (entry.second >= n) {
              replication_count++;
            }
          }
          // done if we have the majority
          if (replication_count >= required_majority_) {
            new_commit_index = n;
            break;
          }
        }

        // only commit if new_commit_index is newer and belongs to current term
        if (new_commit_index > commit_index_ && log_[new_commit_index].term() == current_term_) {
          commit_index_ = new_commit_index;

          // collect all newly committed entries
          std::vector<ApplyResult> apply_batch; // so that apply is not called when the lock is held
          while (last_applied_ < commit_index_) {
            last_applied_++;

            ApplyResult result;
            result.valid = true;
            result.index = last_applied_;
            result.data = log_[last_applied_].command();

            apply_batch.push_back(result);
          }
          // unlock and apply all results in the batch
          lock.unlock();
          for (const ApplyResult &result : apply_batch) {
            apply(result);
          }
          lock.lock();
        }
      } else {
        // log inconsistency, back up one step and retry next heartbeat
        if (next_index_[peer_id] > 1) {
          next_index_[peer_id]--;
        }
      }
    }
  }
  // Update lease if majority acknowledged
  {
    std::lock_guard<std::mutex> lock(mtx);
    if (ack_count >= required_majority_) {
      lease_expiry_ = std::chrono::steady_clock::now() + election_timeout_min_;
    } else {
      lease_expiry_ = std::chrono::steady_clock::time_point::min();
    }
  }

  {
    std::lock_guard<std::mutex> lock(mtx);
    last_heartbeat_sent_ = std::chrono::steady_clock::now();
  }
}

void Raft::become_leader_locked_() {
  role_ = Role::Leader;
  votes_received_ = 0;
  lease_expiry_ = std::chrono::steady_clock::time_point::min();

  // initialize leader-only volatile state
  for (const std::pair<const uint64_t, RaftServiceStub> &peer : peers_) {
    next_index_[peer.first] = log_.size();
    match_index_[peer.first] = 0;
  }

  next_heartbeat_deadline_ = std::chrono::steady_clock::now(); // send ASAP
  timer_cv_.notify_all();

  logger->info("ID {} became leader: term={}", id, current_term_);
}

void Raft::start_election_() {
  uint64_t election_term = 0;
  uint64_t last_log_index = 0;
  uint64_t last_log_term = 0;
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
    last_log_index = log_.size() - 1;
    last_log_term = log_.back().term();

    logger->info("ID {} started election: term={} voted_for={} votes={}", id, current_term_, id,
                 votes_received_);
  }

  for (const std::pair<const uint64_t, RaftServiceStub> &peer : peers_) {
    const uint64_t peer_id = peer.first;

    raftpb::RequestVoteRequest req;
    req.set_term(election_term);
    req.set_candidateid(id);
    req.set_lastlogindex(last_log_index);
    req.set_lastlogterm(last_log_term);

    raftpb::RequestVoteReply rep;

    std::unique_ptr<grpc::ClientContext> ctx = this->create_context(peer_id);
    grpc::Status status = peers_[peer_id]->RequestVote(ctx.get(), req, &rep);

    if (!status.ok()) {
      continue;
    }

    {
      std::unique_lock<std::mutex> lock(mtx);
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
        lease_expiry_ = std::chrono::steady_clock::time_point::min();
        last_heartbeat_received_ = std::chrono::steady_clock::now();
        timer_cv_.notify_all();
        continue;
      }

      if (rep.votegranted()) {
        votes_received_ += 1;

        logger->info("Vote granted by {}: votes={}, required majority={}, term={}", peer_id,
                     votes_received_, required_majority_, current_term_);

        if (votes_received_ >= required_majority_) {
          become_leader_locked_();
          break;
        }
      }
    }
  }
}

} // namespace rafty
