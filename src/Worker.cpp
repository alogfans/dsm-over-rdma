//
// Created by alogfans on 5/5/18.
//

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <thread>
#include "Worker.h"

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

    num_of_procs = reply.num_of_procs();
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
    if (num_of_procs <= 0) {
        return false;
    }

    grpc::ClientContext context;
    grpc::Status status;
    CacheWorkerMapRequest request;
    CacheWorkerMapReply reply;

    for (int i = 0; i < num_of_procs; i++) {
        request.add_want_rank(i);
    }

    status = stub->CacheWorkerMap(&context, request, &reply);
    if (!status.ok()) {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return false;
    }

    worker_map.clear();
    worker_map.resize(static_cast<ulong>(num_of_procs));
    for (auto &v : worker_map) {
        v.Valid = false;
    }

    for (auto &v : reply.worker()) {
        int rank = v.rank();
        if (rank < 0 || rank >= num_of_procs) {
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
    if (num_of_procs <= 0 || worker_map.size() != num_of_procs) {
        return false;
    }

    for (auto &v : worker_map) {
        if (!v.Valid)
            return false;
    }
    return true;
}

void Worker::map_dump() {
    if (num_of_procs <= 0 || worker_map.size() != num_of_procs) {
        std::cout << "map is broken" << std::endl << std::endl;
        return;
    }

    for (int i = 0; i < num_of_procs; i++) {
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

bool Worker::locate_global_addr(uint64_t global_addr, int &target_rank, uint64_t &local_addr) {
    local_addr = global_addr;
    target_rank = 0;

    for (; target_rank < num_of_procs; target_rank++) {
        if (local_addr >= worker_map[target_rank].MemSize) {
            local_addr -= worker_map[target_rank].MemSize;
        } else {
            return true;
        }
    }

    return false;
}