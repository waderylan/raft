#include "rafty/rpc_service.hpp"
#include "common/common.hpp"
#include "rafty/raft.hpp"

#include <algorithm>
#include <grpcpp/grpcpp.h>

#include <mutex>
#include <chrono>

namespace rafty {

grpc::Status RaftServiceImpl::AppendEntries(grpc::ServerContext*,
                                            const raftpb::AppendEntriesRequest *req,
                                            raftpb::AppendEntriesReply *rep) {
  std::lock_guard<std::mutex> lock(raft_->mtx);
  
  // reject if request term is lower than our term
  if (req->term() < raft_->current_term_) {
    rep->set_term(raft_->current_term_);
    rep->set_success(false);
    return grpc::Status::OK;
  }

  // step down if we observe a higher term
  if (req->term() > raft_->current_term_) {
    raft_->current_term_ = req->term();
    raft_->voted_for_.reset();
  }
  raft_->role_ = Role::Follower;
  raft_->last_heartbeat_received_ = std::chrono::steady_clock::now();
  raft_->timer_cv_.notify_all();

  raft_->logger->info("Heartbeat received: from leader={} req_term={} my_term={}",
                    req->leaderid(), req->term(), raft_->current_term_);

  rep->set_term(raft_->current_term_);
  
  uint64_t prev_log_index = req->prevlogindex();
  uint64_t prev_log_term  = req->prevlogterm();
  
  if (prev_log_index >= raft_->log_.size() || prev_log_term != raft_->log_[prev_log_index].term()) {
    rep->set_success(false);
    return grpc::Status::OK;
  }

  for (uint64_t i = 0; i < (uint64_t)req->entries().size(); i++) {
    if (prev_log_index + 1 + i < raft_->log_.size()) {
      if (req->entries()[i].term() != raft_->log_[prev_log_index + 1 + i].term()) {
        // term inconsistency, truncate log to everything before this index
        raft_->log_.resize(prev_log_index + 1 + i);
        break;
      }
    } else {
      // past the end of our log, no conflicts
      break;
    }
  }

  // POTENTIAL ISSUE: this is another loop which may be ineffecient
  // append all incoming log messages that are past the end of our log
  for (uint64_t i = 0; i < (uint64_t)req->entries().size(); i++) {
    if (prev_log_index + 1 + i >= raft_->log_.size()) {
      raft_->log_.push_back(req->entries()[i]);
    }
  }

  if (req->leadercommit() > raft_->commit_index_) {
    raft_->commit_index_ = std::min(req->leadercommit(), prev_log_index + (uint64_t)req->entries().size());  
  }

  while (raft_->last_applied_ < raft_->commit_index_) {
    raft_->last_applied_++;
    
    ApplyResult result;
    result.valid = true;
    result.index = raft_->last_applied_;
    result.data = raft_->log_[raft_->last_applied_].command();

    raft_->apply(result);
  }
  rep->set_success(true);
  return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::RequestVote(grpc::ServerContext *,
                                          const raftpb::RequestVoteRequest *req,
                                          raftpb::RequestVoteReply *rep) {

  std::lock_guard<std::mutex> lock(raft_->mtx);

  const uint64_t req_term = req->term();
  const uint64_t candidate_id = req->candidateid();

  if (req_term < raft_->current_term_) {
    rep->set_term(raft_->current_term_);
    rep->set_votegranted(false);
    return grpc::Status::OK;
  }

  if (req_term > raft_->current_term_) {
    raft_->current_term_ = req_term;
    raft_->role_ = Role::Follower;
    raft_->voted_for_.reset();
  }

  const bool can_vote = (!raft_->voted_for_.has_value() || raft_->voted_for_.value() == candidate_id);
  if (can_vote) {
    raft_->voted_for_ = candidate_id;

    raft_->last_heartbeat_received_ = std::chrono::steady_clock::now();
    raft_->timer_cv_.notify_all();

    rep->set_term(raft_->current_term_);
    rep->set_votegranted(true);

    raft_->logger->info("Node {} Voted: candidate={} term={}", raft_->id, candidate_id, raft_->current_term_);
    return grpc::Status::OK;
  }
  else {
    rep->set_term(raft_->current_term_);
    rep->set_votegranted(false);
    return grpc::Status::OK;
  }
}

} // namespace rafty
