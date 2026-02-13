#include "rafty/rpc_service.hpp"
#include "rafty/raft.hpp"

#include <grpcpp/grpcpp.h>

#include <mutex>
#include <chrono>

namespace rafty {

grpc::Status RaftServiceImpl::AppendEntries(grpc::ServerContext*,
                                            const raftpb::AppendEntriesRequest *req,
                                            raftpb::AppendEntriesReply *rep) {
  std::lock_guard<std::mutex> lock(raft_->mtx);

  if (req->term() < raft_->current_term_) {
    rep->set_term(raft_->current_term_);
    rep->set_success(false);
    return grpc::Status::OK;
  }

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
