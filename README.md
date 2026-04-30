# Raft Consensus Protocol

Implementation of the [Raft consensus protocol](https://raft.github.io/raft.pdf) in C++ with gRPC, built for USC CSCI 546 (Distributed Systems, Spring 2026).

Covers leader election, log replication, and a fault-tolerant key/value service layered on top.

---

## Labs

### Lab 1: Leader Election
Implements the leader election portion of Raft (Figure 2 of the extended paper). Nodes communicate exclusively via gRPC. Tolerates network disconnections and partitions.

**Branch:** `lab-1-solution-ddl1` / `lab-1-solution-ddl2`

### Lab 2: Log Replication
Extends Lab 1 with full log replication. The leader accepts proposals, replicates entries to followers, and commits once a majority acknowledges. Handles network partitions, delayed RPCs, and follower catch-up.

**Branch:** `lab-2-solution-ddl1` / `lab-2-solution-ddl2`

### Lab 3: Fault-Tolerant Key/Value Service
Builds a linearizable key/value store (Put/Get/Append) on top of the Raft implementation. Includes RIFL duplicate detection, stale-read prevention, and performance optimization.

**Branch:** `lab-3-solution-ddl1` / `lab-3-solution-ddl2`

---

## Project Structure

```
.
├── app/                  # Node runner applications (node, multinode, kv_node)
├── build/                # CMake build output
├── generated/            # Symlinks to generated protobuf/gRPC files
├── inc/
│   ├── common/           # Shared utilities and types
│   ├── rafty/            # Raft header files and inline implementations
│   └── kv/               # KV server and client headers (Lab 3)
├── integration_tests/    # Automated test suites
├── libs/                 # Third-party libraries (googletest, spdlog)
├── proto/                # Protobuf definitions (raft.proto, kv.proto)
├── src/                  # Raft source implementation
├── bench/                # Benchmark scripts (Lab 3)
└── unittests/            # Unit tests
```

---

## Building

**Requirements:** `cmake >= 3.22.1`, `g++ >= 13.1.0`, gRPC (installed via Lab 0)

```bash
./setup.sh          # Clone submodules (googletest, spdlog)

mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

For grader-equivalent release build:
```bash
cmake .. -DTRACING=OFF -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

---

## Running

**Single node:**
```bash
./node --id 0 --port 50050 --peers 1+localhost:50051,2+localhost:50052
```

**Three-node cluster (shortcut):**
```bash
./multinode --num 3
```

Once running, interactive commands:
| Command | Action |
|---|---|
| `r` | Start the Raft instance |
| `dis <id>` | Disconnect a node |
| `conn <id>` | Reconnect a node |
| `prop <data>` | Propose a log entry (Lab 2+) |
| `k` | Kill the instance |

---

## Tests

```bash
cd build/integration_tests

# Run all tests
./raft_test

# Lab 1 tests only
./raft_test --gtest_filter="*A"

# Lab 2 tests only
./raft_test --gtest_filter="*B"

# Lab 3 KV tests
./kv_test

# With verbose logging
./raft_test --raft_node_verb=2 --tester_verb=2
```

Logs are written to `build/integration_tests/logs/`.

---

## Key Implementation Notes

- All inter-node communication goes through gRPC — no shared variables between instances
- Every RPC call must use `create_context(target_id)` to generate the client context
- The shared `mutable std::mutex mtx` is provided for safe concurrent access
- Network failures are simulated by the test harness; always check `grpc::Status::ok()` on RPC returns
- Heartbeat interval and election timeout are tuned to elect a leader within ~0.5–1s under stable conditions
