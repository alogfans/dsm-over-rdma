//
// Created by alogfans on 5/5/18.
//

#include <iostream>
#include "Monitor.h"

using namespace universe;

void Monitor::Run(const std::string &address) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    MonitorImpl monitor_impl;
    builder.RegisterService(&monitor_impl);
    server = builder.BuildAndStart();
    std::cout << "Server listening on " << address << std::endl;
    server->Wait();
}

void Monitor::Shutdown() {
    if (server) {
        server->Shutdown();
        server = nullptr;
    }
}

MonitorImpl::MonitorImpl() {
    next_page_id.store(0);
    next_rank.store(0);
}

grpc::Status MonitorImpl::JoinGroup(grpc::ServerContext *context, const JoinGroupRequest *request,
                                    JoinGroupReply *response) {
    int rank = next_rank.fetch_add(1);
    global_map->AddWorker(rank, request->qpn(), static_cast<uint16_t>(request->lid() & 0xffff));
    response->set_rank(rank);
    return grpc::Status::OK;
}

grpc::Status MonitorImpl::AllocPage(grpc::ServerContext *context, const universe::AllocPageRequest *request,
                                    universe::AllocPageReply *response) {
    const int page_id = next_page_id.fetch_add(1);
    const int owner_rank = request->owner_rank();

    {
        std::unique_lock<std::mutex> guard(global_map->lock);
        if (global_map->worker_map.find(owner_rank) == global_map->worker_map.end()) {
            printf("peer %d not joined before.\n", request->owner_rank());
            return grpc::Status::CANCELLED;
        }
    }

    global_map->AddPage(page_id, request->size(), request->align(), owner_rank);
    global_map->AddPageReplica(page_id, owner_rank, request->owner_addr(), request->owner_key());

    response->set_page_id(page_id);
    return grpc::Status::OK;
}

grpc::Status MonitorImpl::SyncMap(grpc::ServerContext *context, const universe::SyncMapRequest *request,
                                  universe::SyncMapReply *response) {
    std::unique_lock<std::mutex> guard(global_map->lock);
    for (auto &v : global_map->worker_map) {
        WorkerDesc *worker = response->add_workers();
        worker->set_rank(v.first);
        worker->set_qpn(v.second.qpn);
        worker->set_lid(v.second.lid);
    }

    for (auto &v : global_map->page_map) {
        PageDesc *page = response->add_pages();
        page->set_page_id(v.first);
        page->set_size(v.second.size);
        page->set_align(v.second.align);
        page->set_owner_rank(v.second.owner_rank);

        for (auto &u : v.second.rep_list) {
            PageRepList *entry = page->add_rep_list();
            entry->set_rank(u.first);
            entry->set_key(u.second.key);
            entry->set_addr(u.second.addr);
        }
    }

    return grpc::Status::OK;
}
