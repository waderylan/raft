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
    raft_->logger->info("Heartbeat recieved: from leader={} req_term={} my_term={}",
                      req->leaderid(), req->term(), raft_->current_term_);

    raft_->current_term_ = req->term();
    raft_->voted_for_.reset();
  }

  raft_->role_ = Role::Follower;
  raft_->last_heartbeat_received_ = std::chrono::steady_clock::now();
  raft_->timer_cv_.notify_all();

  rep->set_term(raft_->current_term_);
  rep->set_success(true);
  return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::RequestVote(grpc::ServerContext *,
                                          const raftpb::RequestVoteRequest *req,
                                          raftpb::RequestVoteReply *rep) {

  // Temporary
  rep->set_term(req->term());
  rep->set_votegranted(false);
  return grpc::Status::OK;
}

} // namespace rafty
