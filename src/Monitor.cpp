//
// Created by alogfans on 5/5/18.
//

#include <iostream>
#include "Monitor.h"

using namespace universe;

void Monitor::Start(const std::string &address, int num_of_procs) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    monitor_impl = std::make_shared<MonitorImpl>(num_of_procs);
    builder.RegisterService(monitor_impl.get());
    server = builder.BuildAndStart();
    std::cout << "Server listening on " << address << std::endl;
}

void Monitor::Wait() {
    if (server) {
        server->Wait();
    }
}

void Monitor::Shutdown() {
    if (server) {
        server->Shutdown();
        server = nullptr;
        // Free monitor_impl too.
        monitor_impl.reset();
    }
}

MonitorImpl::MonitorImpl(int num_of_procs) {
    worker_map = std::make_shared<WorkerMap>(num_of_procs);
}

grpc::Status MonitorImpl::JoinGroup(grpc::ServerContext *context,
                                    const JoinGroupRequest *request,
                                    JoinGroupReply *response) {

    std::vector<uint32_t> qpn_list;
    for (auto &v : request->qpn()) {
        qpn_list.push_back(v);
    }

    bool result = worker_map->insert(request->rank(),
                                     qpn_list,
                                     static_cast<uint16_t>(request->lid() & 0xffff),
                                     request->addr(),
                                     request->key(),
                                     request->size());
    if (!result) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "rank has been registered");
    }

    response->set_num_of_procs(worker_map->get_num_of_procs());
    return grpc::Status::OK;
}

grpc::Status MonitorImpl::LeaveGroup(grpc::ServerContext* context,
                                     const LeaveGroupRequest* request,
                                     LeaveGroupReply* response) {
    bool result = worker_map->erase(request->rank());
    if (!result) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "invalid rank, or rank has been leave");
    }
    return grpc::Status::OK;
}

grpc::Status MonitorImpl::CacheWorkerMap(grpc::ServerContext* context,
                                         const CacheWorkerMapRequest* request,
                                         CacheWorkerMapReply* response) {
    std::vector<WorkerEntry> worker_map_copy;
    worker_map->snapshot(worker_map_copy);
    int num_of_procs = static_cast<int>(worker_map_copy.size());
    for (auto &rank : request->want_rank()) {
        if (rank < 0 || rank >= num_of_procs) {
            continue;
        }
        if (!worker_map_copy[rank].Valid) {
            continue;
        }
        auto entry = response->add_worker();
        entry->set_rank(rank);
        for (auto &v : worker_map_copy[rank].QueuePairNum) {
            entry->add_qpn(v);
        }
        entry->set_lid(worker_map_copy[rank].LocalID);
        entry->set_addr(worker_map_copy[rank].LocalVirtAddr);
        entry->set_key(worker_map_copy[rank].MemRegionKey);
        entry->set_size(worker_map_copy[rank].MemSize);
    }
    return grpc::Status::OK;
}

grpc::Status MonitorImpl::ReadFault(grpc::ServerContext* context,
                       const FaultRequest* request,
                       FaultReply* response) {
    int rank = request->rank();
    uint64_t addr = request->addr();

    if (rank < 0 || rank >= worker_map->get_num_of_procs()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "invalid rank");
    }

    if (addr % PAGESIZE) {
        printf("Warning: addr %lx is not aligned.\n", addr);
        addr &= (PAGESIZE - 1);
    }

    bool expected = false;
    while (!page_states[addr].onFaultProgress.compare_exchange_strong(expected, true)) {
        expected = false;
    }

    page_states[addr].copySet.insert(rank);

    for (auto &v : page_states[addr].copySet) {
        response->add_cached_rank(v);
    }

    return grpc::Status::OK;
}

grpc::Status MonitorImpl::WriteFault(grpc::ServerContext* context,
                        const FaultRequest* request,
                        FaultReply* response) {
    int rank = request->rank();
    uint64_t addr = request->addr();

    if (rank < 0 || rank >= worker_map->get_num_of_procs()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "invalid rank");
    }

    if (addr % PAGESIZE) {
        printf("Warning: addr %lx is not aligned.\n", addr);
        addr &= (PAGESIZE - 1);
    }

    bool expected = false;
    while (!page_states[addr].onFaultProgress.compare_exchange_strong(expected, true)) {
        expected = false;
    }

    for (auto &v : page_states[addr].copySet) {
        response->add_cached_rank(v);
    }

    page_states[addr].copySet.clear();

    return grpc::Status::OK;
}

grpc::Status MonitorImpl::EndFault(grpc::ServerContext* context,
                      const FaultRequest* request,
                      FaultReply* response) {
    int rank = request->rank();
    uint64_t addr = request->addr();

    if (rank < 0 || rank >= worker_map->get_num_of_procs()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "invalid rank");
    }

    if (addr % PAGESIZE) {
        printf("Warning: addr %lx is not aligned.\n", addr);
        addr &= (PAGESIZE - 1);
    }

    page_states[addr].onFaultProgress.store(false);
    return grpc::Status::OK;
}
