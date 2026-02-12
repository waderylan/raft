#include "rafty/rpc_service.hpp"

namespace rafty {

grpc::Status RaftServiceImpl::AppendEntries(grpc::ServerContext *context,
                                            const raftpb::AppendEntriesRequest *req,
                                            raftpb::AppendEntriesReply *rep) {

  // Temporary
  rep->set_term(req->term());
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
