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

    bool result = worker_map->insert(request->rank(),
                                     request->qpn(),
                                     static_cast<uint16_t>(request->lid() & 0xffff),
                                     request->addr(),
                                     request->key());
    if (!result) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "rank has been registered");
    }

    response->set_num_of_procs(worker_map->get_num_of_procs());
    return grpc::Status::OK;
}

grpc::Status MonitorImpl::LeaveGroup(grpc::ServerContext* context, const LeaveGroupRequest* request,
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
        entry->set_qpn(worker_map_copy[rank].QueuePairNum);
        entry->set_lid(worker_map_copy[rank].LocalID);
        entry->set_addr(worker_map_copy[rank].LocalVirtAddr);
        entry->set_key(worker_map_copy[rank].MemRegionKey);
    }
    return grpc::Status::OK;
}
