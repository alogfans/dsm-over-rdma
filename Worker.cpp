//
// Created by alogfans on 5/5/18.
//

#include <grpcpp/grpcpp.h>
#include <iostream>
#include "Worker.h"

using namespace universe;

Worker::Worker(const std::string &address) : address(address) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    stub = Controller::NewStub(channel);
    global_map = std::make_shared<GlobalMap>();
}

bool Worker::JoinGroup() {
    grpc::ClientContext context;
    grpc::Status status;
    JoinGroupRequest request;
    JoinGroupReply reply;

    request.set_peer(address);
    request.set_qpn(0); // used in RDMA invocation
    request.set_lid(0); // used in RDMA invocation
    status = stub->JoinGroup(&context, request, &reply);
    if (!status.ok()) {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return false;
    }
    return true;
}

uint64_t Worker::AllocPage(uint64_t size, uint64_t align) {
    grpc::ClientContext context;
    grpc::Status status;
    AllocPageRequest request;
    AllocPageReply reply;

    request.set_size(size);
    request.set_align(align);
    request.set_owner_peer(address);
    request.set_owner_addr(0);
    request.set_owner_key(0);

    status = stub->AllocPage(&context, request, &reply);
    if (!status.ok()) {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return 0xffffffff;
    }

    return reply.shared_addr();
}

bool Worker::SyncMap() {
    grpc::ClientContext context;
    grpc::Status status;
    SyncMapRequest request;
    SyncMapReply reply;

    status = stub->SyncMap(&context, request, &reply);
    if (!status.ok()) {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return false;
    }

    global_map->Clear();

    for (auto &v : reply.workers()) {
        global_map->AddWorker(v.peer(), v.qpn(), static_cast<uint16_t>(v.lid() & 0xffff));
    }

    for (auto &v : reply.pages()) {
        global_map->AddPage(v.shared_addr(), v.size(), v.align(), v.owner_peer());
        for (auto &u : v.rep_list()) {
            global_map->AddPageRepInfo(v.shared_addr(), u.peer(), u.addr(), u.key());
        }
    }

    return true;
}
