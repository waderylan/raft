#include <grpcpp/grpcpp.h>
#include "raft.grpc.pb.h"

namespace rafty {

class Raft;

class RaftServiceImpl final : public raftpb::RaftService::Service {
    public: 
        explicit RaftServiceImpl(Raft* raft) : raft_(raft) {}


        grpc::Status AppendEntries(
            grpc::ServerContext* context, 
            const raftpb::AppendEntriesRequest* request,
            raftpb::AppendEntriesReply* reply) override;
        
        grpc::Status RequestVote(
            grpc::ServerContext* context,
            const raftpb::RequestVoteRequest* request,
            raftpb::RequestVoteReply* reply) override;

    private:
        Raft* raft_;
};

} // namespace rafty