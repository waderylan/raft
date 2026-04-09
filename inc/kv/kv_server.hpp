#pragma once

#include <cstdint>
#include <future>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>

#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "kv.grpc.pb.h"
#include "rafty/raft.hpp"

namespace kv {

class KvServer : public kvpb::KvService::Service {
public:
  explicit KvServer(rafty::Raft &raft) : raft_(raft) {
    // TODO (lab 3): initialize your data structures here.
    // You may want to start background threads, set up condition variables, etc.
  }

  ~KvServer() {
    // TODO (lab 3): clean up any background threads.
  }

  // -----------------------------------------------------------------
  // on_apply is called by the node wrapper each time Raft commits a
  // log entry. The ApplyResult contains the index and data of the
  // committed entry (the same string you passed to raft_.propose()).
  //
  // You should:
  //   1. Deserialize the operation from result.data
  //   2. Apply it to your in-memory key/value map
  //   3. Handle RIFL duplicate detection
  //   4. Notify the waiting RPC handler that its operation has committed
  // -----------------------------------------------------------------
  void on_apply(const rafty::ApplyResult &result) {
    std::string op, key, value;
    uint64_t client_id = 0, seq_num = 0;

    // No-op entries are internal to Raft and have no waiting RPC handler;
    // erase defensively in case a prior leader left a stale promise at this index.
    if (result.data.starts_with("NOOP")) {
        std::lock_guard<std::mutex> lock(mu_);
        pending_.erase(result.index);
        return;
    }

    // Deserialize "OP|key|value|client_id|seq_num"
    std::istringstream ss(result.data);
    std::getline(ss, op, '|');
    std::getline(ss, key, '|');
    std::getline(ss, value, '|');
    std::string tmp;
    std::getline(ss, tmp, '|'); client_id = std::stoull(tmp);
    std::getline(ss, tmp, '|'); seq_num = std::stoull(tmp);

    std::string result_value;
    kvpb::KvStatus status = kvpb::KV_SUCCESS;

    std::lock_guard<std::mutex> lock(mu_);

    // RIFL check for duplicate
    auto it = rifl_.find(client_id);
    if (it != rifl_.end() && seq_num <= it->second.seq_num) {
      // Already applied — use cached result
      result_value = it->second.cached_value;
      status = it->second.cached_status;
    } else {
      // Apply to store
      if (op == "PUT") {
        store_[key] = value;
      } else if (op == "APPEND") {
        store_[key] += value;
      } else if (op == "GET") {
        auto sit = store_.find(key);
        result_value = (sit != store_.end()) ? sit->second : "";
      }
      // Update RIFL cache
      rifl_[client_id] = {seq_num, result_value, kvpb::KV_SUCCESS};
    }

    // Wake up waiting RPC handler
    auto pit = pending_.find(result.index);
    if (pit != pending_.end()) {
      ApplyNotification notif;
      notif.data = result.data;
      notif.value = result_value;
      notif.status = status;
      pit->second.set_value(std::move(notif));
      pending_.erase(pit);
    }

  }

  // -----------------------------------------------------------------
  // gRPC handlers for client operations.
  //
  // Each handler should:
  //   1. Check if this node is the Raft leader (if not, return KV_NOTLEADER)
  //   2. Serialize the operation into a string
  //   3. Call raft_.propose(serialized_op) to submit to Raft
  //   4. Wait for on_apply() to process the committed entry at the
  //      returned index
  //   5. Return the result to the client
  //
  // Use RIFL (client_id + seq_num) to detect and handle duplicate
  // requests, just like Lab 0b.
  // -----------------------------------------------------------------

  grpc::Status Put(grpc::ServerContext *context,
                   const kvpb::PutRequest *request,
                   kvpb::KvResponse *response) override {
    // lab 3
    (void)context;
    
    // RIFL cache check
    kvpb::KvStatus cached;
    if (check_rifl(request->client_id(), request->seq_num(), cached)) {
      response->set_status(cached);
      return grpc::Status::OK;
    }

    std::string data = serialize("PUT", request->key(), request->value(),
                              request->client_id(), request->seq_num());

    kvpb::KvStatus status;
    grpc::Status s = propose_and_wait(data, request->client_id(),
                                      request->seq_num(), status);
    response->set_status(status);
    return s;
  }

  grpc::Status Get(grpc::ServerContext *context,
                   const kvpb::GetRequest *request,
                   kvpb::GetResponse *response) override {
    // lab 3
    (void)context;

    kvpb::KvStatus cached;
    std::string cached_value;
    if (check_rifl(request->client_id(), request->seq_num(), cached, &cached_value)) {
      response->set_status(cached);
      response->set_value(cached_value);
      return grpc::Status::OK;
    }

    if (raft_.has_valid_lease()) {
      std::lock_guard<std::mutex> lock(mu_);

      // Look up the current value from local store
      auto it = store_.find(request->key());
      std::string val = (it != store_.end()) ? it->second : "";

      // Cache the result so duplicate requests return immediately without hitting Raft
      RiflEntry entry;
      entry.seq_num = request->seq_num();
      entry.cached_value = val;
      entry.cached_status = kvpb::KV_SUCCESS;
      rifl_[request->client_id()] = entry;

      response->set_status(kvpb::KV_SUCCESS);
      response->set_value(val);
      return grpc::Status::OK;
    }

    std::string data = serialize("GET", request->key(), "",
                                  request->client_id(), request->seq_num());

    kvpb::KvStatus status;
    std::string value;
    grpc::Status s = propose_and_wait(data, request->client_id(),
                                      request->seq_num(), status, &value);
    response->set_status(status);
    response->set_value(value);
    return s;
  }

  grpc::Status Append(grpc::ServerContext *context,
                      const kvpb::AppendRequest *request,
                      kvpb::KvResponse *response) override {
    // lab 3
    (void)context;

    // RIFL cache check
    kvpb::KvStatus cached;
    if (check_rifl(request->client_id(), request->seq_num(), cached)) {
      response->set_status(cached);
      return grpc::Status::OK;
    }

    std::string data = serialize("APPEND", request->key(), request->value(),
                                  request->client_id(), request->seq_num());

    kvpb::KvStatus status;
    grpc::Status s = propose_and_wait(data, request->client_id(),
                                      request->seq_num(), status);
    response->set_status(status);
    return s;
    }

private:

  // shared helper for put and append handlers
  grpc::Status propose_and_wait(const std::string &data,
                                uint64_t client_id,
                                uint64_t seq_num,
                                kvpb::KvStatus &out_status,
                                std::string *out_value = nullptr) {
    // Propose to Raft
    rafty::ProposalResult proposal = raft_.propose(data);
    if (!proposal.is_leader) {
      out_status = kvpb::KV_NOTLEADER;
      return grpc::Status::OK;
    }

    // Register promise
    std::future<ApplyNotification> fut;
    {
      std::lock_guard<std::mutex> lock(mu_);
      std::promise<ApplyNotification> prom;
      fut = prom.get_future();
      pending_[proposal.index] = std::move(prom);
    }

    // Wait for commit
    if (fut.wait_for(std::chrono::seconds(2)) == std::future_status::timeout) {
      std::lock_guard<std::mutex> lock(mu_);
      pending_.erase(proposal.index);
      out_status = kvpb::KV_TIMEOUT;
      return grpc::Status::OK;
    }

    // Verify it's ours
    ApplyNotification notif = fut.get();
    std::string our_tag = std::to_string(client_id) + "|"
                          + std::to_string(seq_num);
    if (notif.data.find(our_tag) == std::string::npos) {
      out_status = kvpb::KV_TIMEOUT;
      return grpc::Status::OK;
    }

    out_status = kvpb::KV_SUCCESS;
    if (out_value) *out_value = notif.value;
    return grpc::Status::OK;
  }

  std::string serialize(const std::string &op,
                        const std::string &key,
                        const std::string &value,
                        uint64_t client_id,
                        uint64_t seq_num) {
    return op + "|" + key + "|" + value + "|"
          + std::to_string(client_id) + "|"
          + std::to_string(seq_num);
  }

  // Check if this operation is a duplicate. Returns true if already applied
  // fill out_status and out_value with the cached result
  bool check_rifl(uint64_t client_id, uint64_t seq_num,
                  kvpb::KvStatus &out_status, std::string *out_value = nullptr) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = rifl_.find(client_id);
    if (it != rifl_.end() && seq_num <= it->second.seq_num) {
      out_status = it->second.cached_status;
      if (out_value) *out_value = it->second.cached_value;
      return true;
    }
    return false;
  }

  struct ApplyNotification {
    std::string data;
    std::string value;
    kvpb::KvStatus status;
  };

  struct RiflEntry {
    uint64_t seq_num;
    std::string cached_value;
    kvpb::KvStatus cached_status;
  };

  rafty::Raft &raft_;
  std::mutex mu_;
  std::unordered_map<std::string, std::string> store_;
  std::unordered_map<uint64_t, RiflEntry> rifl_;
  std::unordered_map<uint64_t, std::promise<ApplyNotification>> pending_;

};

} // namespace kv
