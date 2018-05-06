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
    if (server != NULL) {
        server->Shutdown();
        server = NULL;
    }
}

MonitorImpl::MonitorImpl() {
    global_map = std::make_shared<GlobalMap>();
    page_counter.store(0);
}

grpc::Status MonitorImpl::JoinGroup(grpc::ServerContext *context, const JoinGroupRequest *request,
                                    JoinGroupReply *response) {
    const std::string &peer = request->peer();

    if (global_map->worker_map.find(peer) != global_map->worker_map.end()) {
        printf("peer %s has joined before.\n", peer.c_str());
        return grpc::Status::CANCELLED;
    }

    global_map->AddWorker(peer, request->qpn(), static_cast<uint16_t>(request->lid() & 0xffff));

    return grpc::Status::OK;
}

grpc::Status MonitorImpl::AllocPage(grpc::ServerContext *context, const universe::AllocPageRequest *request,
                                    universe::AllocPageReply *response) {
    const uint64_t shared_addr = page_counter.fetch_add(request->size());
    if (global_map->page_map.find(shared_addr) != global_map->page_map.end()) {
        printf("(internal error) shared page with address %016X has joined before.\n", shared_addr);
        return grpc::Status::CANCELLED;
    }

    const std::string &owner_peer = request->owner_peer();
    if (global_map->worker_map.find(owner_peer) == global_map->worker_map.end()) {
        printf("peer %s not joined before.\n", owner_peer.c_str());
        return grpc::Status::CANCELLED;
    }

    global_map->AddPage(shared_addr, request->size(), request->align(), owner_peer);
    global_map->AddPageRepInfo(shared_addr, owner_peer, request->owner_addr(), request->owner_key());

    response->set_shared_addr(shared_addr);
    return grpc::Status::OK;
}

grpc::Status MonitorImpl::SyncMap(grpc::ServerContext *context, const universe::SyncMapRequest *request,
                                  universe::SyncMapReply *response) {
    for (auto &v : global_map->worker_map) {
        WorkerDesc *worker = response->add_workers();
        worker->set_peer(v.first);
        worker->set_qpn(v.second.qpn);
        worker->set_lid(v.second.lid);
    }

    for (auto &v : global_map->page_map) {
        PageDesc *page = response->add_pages();
        page->set_shared_addr(v.first);
        page->set_size(v.second.size);
        page->set_align(v.second.align);
        page->set_owner_peer(v.second.owner_peer);

        for (auto &u : v.second.rep_list) {
            PageRepList *entry = page->add_rep_list();
            entry->set_peer(u.first);
            entry->set_key(u.second.key);
            entry->set_addr(u.second.addr);
        }
    }

    return grpc::Status::OK;
}
