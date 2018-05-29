//
// Created by alogfans on 5/5/18.
//

#include <iostream>
#include <thread>
#include "Worker.h"
#include "Logger.h"

#define ENDPOINT_SELECTOR(i) ((i) > rank ? endpoint[(i) - 1] : endpoint[(i)])

using namespace universe;

bool Worker::Connect(const std::string &address, int rank, int num_of_procs, uint64_t size, uint64_t align) {
    if (this->rank >= 0) {
        return false;
    }

    channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    stub = Controller::NewStub(channel);
    device = rdma::Device::Open();
    if (!device) {
        return false;
    }

    num_procs = num_of_procs;
    for (int i = 0; i < num_of_procs; i++) {
        if (i != rank) {
            auto entry = device->CreateEndPoint(IBV_QPT_RC);
            if (!entry) {
                return false;
            }
            endpoint.push_back(entry);
        }
    }

    data_memory = device->Malloc(size, true, align);
    if (!data_memory) {
        return false;
    }

    grpc::ClientContext context;
    grpc::Status status;
    JoinGroupRequest request;
    JoinGroupReply reply;

    this->rank = rank;
    request.set_rank(rank);
    for (auto &v : endpoint) {
        request.add_qpn(v->QPN());
    }
    request.set_lid(device->LID());
    request.set_addr(data_memory->VirtualAddress());
    request.set_key(data_memory->Key());
    request.set_size(size);
    status = stub->JoinGroup(&context, request, &reply);
    if (!status.ok()) {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return false;
    }

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

    // Wait until all members leave. Max: 2 seconds to exit
    int attempt = 0;
    do {
        if (!UpdateWorkerMap()) {
            return false;
        }

        if (worker_map.empty()) {
            return true;
        }
        attempt++;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } while (attempt < 20);

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
        for (auto &u : v.qpn()) {
            worker_map[rank].QueuePairNum.push_back(u);
        }
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
            complete_connection();
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
            std::cout << " QPN ";
            for (auto &v : worker_map[i].QueuePairNum) {
                std::cout << v << " ";
            }
            std::cout << " LID " << worker_map[i].LocalID;
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

void Worker::Barrier(uint64_t global_addr) {
    int target_rank;
    uint64_t local_addr;
    ASSERT(locate_global_addr(global_addr, target_rank, local_addr));
    ASSERT((local_addr % sizeof(uint64_t)) == 0);

    cached_memory.clear();

    uint64_t lock_variable;
    auto wrapper = device->Wrap((uint8_t *) &lock_variable, sizeof(uint64_t));
    ASSERT(wrapper);

    auto target = rdma::WorkBatch::PathDesc(worker_map[target_rank].LocalVirtAddr + local_addr,
            worker_map[target_rank].MemRegionKey);

    if (rank != target_rank) {
        rdma::WorkBatch batch(ENDPOINT_SELECTOR(target_rank));
        batch.CompareAndSwap(rdma::WorkBatch::PathDesc(wrapper, 0), target, num_procs - 1, 0);
        batch.FetchAndAdd(rdma::WorkBatch::PathDesc(wrapper, 0), target, 1);
        ASSERT(!batch.Commit());
        ASSERT(!device->Poll(-1));
    }

    lock_variable = 0xffffffff;
    while (lock_variable != num_procs - 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        if (rank == target_rank) {
            uint64_t *cast_lock = (uint64_t *) &data_memory->Get()[local_addr];
            lock_variable = __sync_fetch_and_add(cast_lock, 0);
        } else {
            rdma::WorkBatch batch(ENDPOINT_SELECTOR(target_rank));
            batch.FetchAndAdd(rdma::WorkBatch::PathDesc(wrapper, 0), target, 0);

            ASSERT(!batch.Commit());
            ASSERT(!device->Poll(-1));
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
    ASSERT((local_addr % sizeof(uint64_t)) == 0);

    cached_memory.clear();

    uint64_t lock_variable;
    auto target = rdma::WorkBatch::PathDesc(worker_map[target_rank].LocalVirtAddr + local_addr,
            worker_map[target_rank].MemRegionKey);
    auto wrapper = device->Wrap((uint8_t *) &lock_variable, sizeof(uint64_t));
    ASSERT(wrapper);

    lock_variable = 0xffffffff;
    while (lock_variable) {
        if (target_rank == rank) {
            lock_variable = __sync_val_compare_and_swap((uint64_t *) &data_memory->Get()[local_addr], 0, 1);
        } else {
            rdma::WorkBatch batch(ENDPOINT_SELECTOR(target_rank));
            batch.CompareAndSwap(rdma::WorkBatch::PathDesc(wrapper, 0), target, 0, 1);
            ASSERT(!batch.Commit());
            ASSERT(!device->Poll(-1));
        }

        if (lock_variable != 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

void Worker::Unlock(uint64_t global_addr) {
    int target_rank;
    uint64_t local_addr;
    ASSERT(locate_global_addr(global_addr, target_rank, local_addr));
    ASSERT(local_addr % sizeof(uint64_t) == 0);

    cached_memory.clear();

    uint64_t lock_variable;
    auto target = rdma::WorkBatch::PathDesc(worker_map[target_rank].LocalVirtAddr + local_addr,
            worker_map[target_rank].MemRegionKey);
    auto wrapper = device->Wrap((uint8_t *) &lock_variable, sizeof(uint64_t));
    ASSERT(wrapper);

    lock_variable = 0;
    while (!lock_variable) {
        if (target_rank == rank) {
            lock_variable = __sync_val_compare_and_swap((uint64_t *) &data_memory->Get()[local_addr], 1, 0);
        } else {
            rdma::WorkBatch batch(ENDPOINT_SELECTOR(target_rank));
            batch.CompareAndSwap(rdma::WorkBatch::PathDesc(wrapper, 0), target, 1, 0);
            ASSERT(!batch.Commit());
            ASSERT(!device->Poll(-1));
        }

        if (lock_variable == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

void Worker::complete_connection() {
    for (int i = 0; i < num_procs; i++) {
        if (i == rank) {
            continue;
        }

        int target_selector = (i > rank) ? i - 1 : i;
        int source_selector = (rank > i) ? rank - 1 : rank;

        ASSERT(!endpoint[target_selector]->RegisterRemote(worker_map[i].LocalID, worker_map[i].QueuePairNum[source_selector]));
    }
}

bool Worker::post_fault(const std::string &type, uint64_t global_addr) {
    grpc::ClientContext context;
    grpc::Status status;
    FaultRequest request;
    request.set_rank(rank);
    request.set_addr(global_addr);
    FaultReply reply;

    if (type == "ReadFault") {
        status = stub->ReadFault(&context, request, &reply);
    } else if (type == "WriteFault") {
        status = stub->WriteFault(&context, request, &reply);
    } else if (type == "EndFault") {
        status = stub->EndFault(&context, request, &reply);
    } else {
        return false;
    }

    if (!status.ok()) {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return false;
    }

    return true;
}

void Worker::do_read_page(uint64_t global_addr) {
    int target_rank;
    uint64_t local_addr;
    ASSERT(locate_global_addr(global_addr, target_rank, local_addr));

    if (target_rank == rank) {
        return;
    }

    if (cached_memory.find(global_addr) != cached_memory.end()) {
        // we have read this page.
        return;
    }

    if (!post_fault("ReadFault", global_addr)) {
        return;
    }

    cached_memory[global_addr] = device->Malloc(PAGESIZE, true, 16);
    rdma::WorkBatch batch(ENDPOINT_SELECTOR(target_rank));
    batch.Read(rdma::WorkBatch::PathDesc(cached_memory[global_addr], 0),
               rdma::WorkBatch::PathDesc(worker_map[target_rank].LocalVirtAddr + local_addr,
                                         worker_map[target_rank].MemRegionKey),
               PAGESIZE);

    ASSERT(!batch.Commit());
    ASSERT(!device->Poll(-1));

    if (!post_fault("EndFault", global_addr)) {
        return;
    }
}

void Worker::do_write_page(uint64_t global_addr) {
    int target_rank;
    uint64_t local_addr;
    ASSERT(locate_global_addr(global_addr, target_rank, local_addr));

    if (target_rank != rank && cached_memory.find(global_addr) == cached_memory.end()) {
        std::cout << "global_addr not cached -- page not read before. maybe error." << std::endl;
        return;
    }

    if (!post_fault("WriteFault", global_addr)) {
        return;
    }

    /*
    for (auto &v : reply.cached_rank()) {
        if (v == rank) {
            continue;
        }

        std::cout << "Not implemented -- must erase entry " << global_addr << " in rank " << v << std::endl;
    }
    */

    if (target_rank != rank) {
        rdma::WorkBatch batch(ENDPOINT_SELECTOR(target_rank));
        batch.Write(rdma::WorkBatch::PathDesc(cached_memory[global_addr], 0),
                    rdma::WorkBatch::PathDesc(worker_map[target_rank].LocalVirtAddr + local_addr,
                                              worker_map[target_rank].MemRegionKey),
                    PAGESIZE);

        ASSERT(!batch.Commit());
        ASSERT(!device->Poll(-1));

        cached_memory.erase(global_addr);
    }

    if (!post_fault("EndFault", global_addr)) {
        return;
    }
}

void Worker::naive_load(uint8_t *object, size_t size, uint64_t global_addr) {
    uint64_t local_addr;
    int target_rank;
    if (!locate_global_addr(global_addr, target_rank, local_addr)) {
        return;
    }

    if (local_addr + size > worker_map[target_rank].MemSize) {
        std::cout << "object exceeded the boundary of chunk" << std::endl;
        return;
    }

    if (target_rank == rank) {
        memcpy(object, &data_memory->Get()[local_addr], size);
    } else {
        auto wrapper = device->Wrap(object, size);
        ASSERT(wrapper);

        rdma::WorkBatch batch(ENDPOINT_SELECTOR(target_rank));
        batch.Read(rdma::WorkBatch::PathDesc(wrapper, 0),
                   rdma::WorkBatch::PathDesc(worker_map[target_rank].LocalVirtAddr + local_addr,
                                             worker_map[target_rank].MemRegionKey),
                   (uint32_t) wrapper->Size());

        ASSERT(!batch.Commit());
        ASSERT(!device->Poll(-1));
    }
}

void Worker::naive_store(uint8_t *object, size_t size, uint64_t global_addr) {
    uint64_t local_addr;
    int target_rank;
    if (!locate_global_addr(global_addr, target_rank, local_addr)) {
        return;
    }

    if (local_addr + size > worker_map[target_rank].MemSize) {
        std::cout << "object exceeded the boundary of chunk" << std::endl;
        return;
    }

    if (target_rank == rank) {
        memcpy(&data_memory->Get()[local_addr], object, size);
    } else {
        auto wrapper = device->Wrap(object, size);
        ASSERT(wrapper);

        rdma::WorkBatch batch(ENDPOINT_SELECTOR(target_rank));
        batch.Write(rdma::WorkBatch::PathDesc(wrapper, 0),
                    rdma::WorkBatch::PathDesc(worker_map[target_rank].LocalVirtAddr + local_addr,
                                             worker_map[target_rank].MemRegionKey),
                    (uint32_t) wrapper->Size());

        ASSERT(!batch.Commit());
        ASSERT(!device->Poll(-1));
    }
}


void Worker::fast_load(uint8_t *object, size_t size, uint64_t global_addr) {
    uint64_t local_addr;
    int target_rank;
    if (!locate_global_addr(global_addr, target_rank, local_addr)) {
        return;
    }

    if (local_addr + size > worker_map[target_rank].MemSize) {
        std::cout << "object exceeded the boundary of chunk" << std::endl;
        return;
    }

    if (target_rank == rank) {
        memcpy(object, &data_memory->Get()[local_addr], size);
    } else {
        uint64_t aligned_addr = global_addr - global_addr % PAGESIZE;
        do_read_page(aligned_addr);
        memcpy(object, &cached_memory[aligned_addr]->Get()[local_addr - aligned_addr], size);
    }
}

void Worker::fast_store(uint8_t *object, size_t size, uint64_t global_addr) {
    uint64_t local_addr;
    int target_rank;
    if (!locate_global_addr(global_addr, target_rank, local_addr)) {
        return;
    }

    if (local_addr + size > worker_map[target_rank].MemSize) {
        std::cout << "object exceeded the boundary of chunk" << std::endl;
        return;
    }

    uint64_t aligned_addr = global_addr - global_addr % PAGESIZE;
    if (target_rank == rank) {
        memcpy(&data_memory->Get()[local_addr], object, size);
    } else {
        if (cached_memory.find(aligned_addr) == cached_memory.end()) {
            do_read_page(aligned_addr);
        }

        memcpy(&cached_memory[aligned_addr]->Get()[local_addr - aligned_addr], object, size);
    }
    do_write_page(aligned_addr);
}
