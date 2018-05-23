//
// Created by alogfans on 5/5/18.
//

#include <iostream>
#include <thread>
#include "Worker.h"
#include "Logger.h"

using namespace universe;

bool Worker::Connect(const std::string &address, int rank, uint64_t size, uint64_t align) {
    if (this->rank >= 0) {
        return false;
    }

    channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    stub = Controller::NewStub(channel);
    device = rdma::Device::Open();
    if (!device) {
        return false;
    }

    endpoint = device->CreateEndPoint(IBV_QPT_RC);
    if (!endpoint) {
        return false;
    }

    memory = device->Malloc(size, true, align);
    if (!endpoint) {
        return false;
    }

    grpc::ClientContext context;
    grpc::Status status;
    JoinGroupRequest request;
    JoinGroupReply reply;

    this->rank = rank;
    request.set_rank(rank);
    request.set_qpn(endpoint->QPN());
    request.set_lid(device->LID());
    request.set_addr(memory->VirtualAddress());
    request.set_key(memory->Key());
    request.set_size(size);
    status = stub->JoinGroup(&context, request, &reply);
    if (!status.ok()) {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return false;
    }

    num_procs = reply.num_of_procs();
    return true;
}

bool Worker::Disconnect() {
    if (rank < 0) {
        return false;
    }

    grpc::ClientContext context;
    grpc::Status status;
    LeaveGroupRequest request;
    LeaveGroupReply reply;

    request.set_rank(rank);
    status = stub->LeaveGroup(&context, request, &reply);
    if (!status.ok()) {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return false;
    }

    rank = -1;
    return true;
}

bool Worker::UpdateWorkerMap() {
    if (num_procs <= 0) {
        return false;
    }

    grpc::ClientContext context;
    grpc::Status status;
    CacheWorkerMapRequest request;
    CacheWorkerMapReply reply;

    for (int i = 0; i < num_procs; i++) {
        request.add_want_rank(i);
    }

    status = stub->CacheWorkerMap(&context, request, &reply);
    if (!status.ok()) {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return false;
    }

    worker_map.clear();
    worker_map.resize(static_cast<ulong>(num_procs));
    for (auto &v : worker_map) {
        v.Valid = false;
    }

    for (auto &v : reply.worker()) {
        int rank = v.rank();
        if (rank < 0 || rank >= num_procs) {
            continue;
        }
        worker_map[rank].Valid = true;
        worker_map[rank].LocalID = static_cast<uint16_t>(v.lid() & 0xffff);
        worker_map[rank].QueuePairNum = v.qpn();
        worker_map[rank].LocalVirtAddr = v.addr();
        worker_map[rank].MemRegionKey = v.key();
        worker_map[rank].MemSize = v.size();
    }

    return true;
}

bool Worker::WaitUntilReady(int interval_ms, int max_attempt, bool dump) {
    if (rank < 0) {
        return false;
    }

    int attempt = 0;
    do {
        if (!UpdateWorkerMap()) {
            return false;
        }
        if (dump) {
            map_dump();
        }
        if (map_ready()) {
            return true;
        }
        attempt++;
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    } while (attempt < max_attempt);

    return false;
}

bool Worker::map_ready() {
    if (num_procs <= 0 || worker_map.size() != num_procs) {
        return false;
    }

    for (auto &v : worker_map) {
        if (!v.Valid)
            return false;
    }
    return true;
}

void Worker::map_dump() {
    if (num_procs <= 0 || worker_map.size() != num_procs) {
        std::cout << "map is broken" << std::endl << std::endl;
        return;
    }

    for (int i = 0; i < num_procs; i++) {
        std::cout << "Rank " << i << " ";
        if (worker_map[i].Valid) {
            std::cout << " QPN " << worker_map[i].QueuePairNum << " LID " << worker_map[i].LocalID;
            std::cout << " LVirtAddr " << worker_map[i].LocalVirtAddr << " LKey " << worker_map[i].MemRegionKey;
            std::cout << " MemSize " << worker_map[i].MemSize << std::endl;
        } else {
            std::cout << " Not ready" << std::endl;
        }
    }

    std::cout << std::endl;
}

uint64_t Worker::GlobalAddress(uint64_t local_addr, int target_rank) const {
    uint64_t offset = 0;

    if (target_rank < 0) {
        target_rank = rank;
    }

    for (int i = 0; i < target_rank; i++) {
        offset += worker_map[i].MemSize;
    }

    return offset + local_addr;
}

void Worker::Release() {
    std::atomic_thread_fence(std::memory_order_release);
    // TODO flush write buffer
}

void Worker::Acquire() {
    // TODO self invalidation
    std::atomic_thread_fence(std::memory_order_acquire);
}

void Worker::Barrier(uint64_t global_addr) {
    int target_rank;
    uint64_t local_addr;
    ASSERT(locate_global_addr(global_addr, target_rank, local_addr));

    uint64_t lock_variable;
    auto target = rdma::WorkBatch::PathDesc(worker_map[target_rank].LocalVirtAddr, worker_map[target_rank].MemRegionKey);
    auto wrapper = device->Wrap((uint8_t *) &lock_variable, sizeof(uint64_t));
    ASSERT(wrapper);

    // post a atomic add request to root node
    if (rank != target_rank) {
        if (endpoint_peer_rank != target_rank) {
            connect_peer(target_rank);
        }

        rdma::WorkBatch batch(endpoint);
        // When hook value is num_proc, it must be used last time -- reset it when necessary
        batch.CompareAndSwap(rdma::WorkBatch::PathDesc(wrapper, local_addr),
                             target,
                             (uint64_t) num_procs - 1, 0);

        batch.FetchAndAdd(rdma::WorkBatch::PathDesc(wrapper, local_addr),
                          target, 1);

        ASSERT(!batch.Commit());
        ASSERT(!device->Poll(-1));
    }

    // wait until lock value become
    lock_variable = 0xffffffff;

    while (lock_variable != num_procs - 1) {
        if (rank == target_rank) {
            lock_variable = __sync_fetch_and_add((uint64_t *) &memory->Get()[local_addr], 0);
        } else {
            rdma::WorkBatch batch(endpoint);
            batch.FetchAndAdd(rdma::WorkBatch::PathDesc(wrapper, local_addr),
                              target, 0);

            ASSERT(!batch.Commit());
            ASSERT(!device->Poll(-1));
        }

        if (lock_variable != num_procs - 1) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}


bool Worker::locate_global_addr(uint64_t global_addr, int &target_rank, uint64_t &local_addr) {
    local_addr = global_addr;
    target_rank = 0;

    for (; target_rank < num_procs; target_rank++) {
        if (local_addr >= worker_map[target_rank].MemSize) {
            local_addr -= worker_map[target_rank].MemSize;
        } else {
            return true;
        }
    }

    return false;
}


void Worker::Lock(uint64_t global_addr) {
    int target_rank;
    uint64_t local_addr;
    ASSERT(locate_global_addr(global_addr, target_rank, local_addr));

    uint64_t lock_variable;
    auto target = rdma::WorkBatch::PathDesc(worker_map[target_rank].LocalVirtAddr, worker_map[target_rank].MemRegionKey);
    auto wrapper = device->Wrap((uint8_t *) &lock_variable, sizeof(uint64_t));
    ASSERT(wrapper);

    if (target_rank != rank) {
        connect_peer(target_rank);
    }

    lock_variable = 0xffffffff;
    while (lock_variable) {
        if (target_rank == rank) {
            lock_variable = __sync_val_compare_and_swap((uint64_t *) &memory->Get()[local_addr], 0, 1);
        } else {
            rdma::WorkBatch batch(endpoint);
            // Set locked if unlocked
            batch.CompareAndSwap(rdma::WorkBatch::PathDesc(wrapper, local_addr), target, 0, 1);
            ASSERT(!batch.Commit());
            ASSERT(!device->Poll(-1));
        }

        if (lock_variable != 0) {
            // lock failed. Wait and retry
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

void Worker::Unlock(uint64_t global_addr) {
    int target_rank;
    uint64_t local_addr;
    ASSERT(locate_global_addr(global_addr, target_rank, local_addr));

    uint64_t lock_variable;
    auto target = rdma::WorkBatch::PathDesc(worker_map[target_rank].LocalVirtAddr, worker_map[target_rank].MemRegionKey);
    auto wrapper = device->Wrap((uint8_t *) &lock_variable, sizeof(uint64_t));
    ASSERT(wrapper);

    if (target_rank != rank) {
        connect_peer(target_rank);
    }

    lock_variable = 0;
    while (!lock_variable) {
        if (target_rank == rank) {
            lock_variable = __sync_val_compare_and_swap((uint64_t *) &memory->Get()[local_addr], 1, 0);
        } else {
            rdma::WorkBatch batch(endpoint);
            // Set locked if unlocked
            batch.CompareAndSwap(rdma::WorkBatch::PathDesc(wrapper, local_addr), target, 1, 0);
            ASSERT(!batch.Commit());
            ASSERT(!device->Poll(-1));
        }

        if (lock_variable == 0) {
            // lock failed. Wait and retry
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

void Worker::connect_peer(int target_rank) {
    if (target_rank == endpoint_peer_rank) {
        return;
    }

    ASSERT(!endpoint->Reset());
    ASSERT(!endpoint->RegisterRemote(worker_map[target_rank].LocalID, worker_map[target_rank].QueuePairNum));
    endpoint_peer_rank = target_rank;
}
