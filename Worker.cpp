//
// Created by alogfans on 5/5/18.
//

#include <grpcpp/grpcpp.h>
#include <iostream>
#include "Worker.h"

using namespace universe;

int Worker::JoinGroup(const std::string &address)  {
    channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    stub = Controller::NewStub(channel);
    global_map = std::make_shared<GlobalMap>();

    device = rdma::Device::Open();
    if (!device) {
        return -1;
    }

    endpoint = device->CreateEndPoint(IBV_QPT_RC);
    if (!endpoint) {
        return -1;
    }

    grpc::ClientContext context;
    grpc::Status status;
    JoinGroupRequest request;
    JoinGroupReply reply;

    request.set_qpn(endpoint->QPN()); // used in RDMA invocation
    request.set_lid(device->LID());   // used in RDMA invocation
    status = stub->JoinGroup(&context, request, &reply);
    if (!status.ok()) {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return -1;
    }

    rank = reply.rank();
    return rank;
}

bool Worker::LeaveGroup() {
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

    return true;
}

int Worker::AllocPage(uint64_t size, uint64_t align) {
    auto mr = device->Malloc(size, true, align);
    if (!endpoint) {
        return -1;
    }

    grpc::ClientContext context;
    grpc::Status status;
    AllocPageRequest request;
    AllocPageReply reply;

    request.set_size(size);
    request.set_align(align);
    request.set_owner_rank(rank);
    request.set_owner_addr(mr->VirtualAddress());
    request.set_owner_key(mr->Key());

    status = stub->AllocPage(&context, request, &reply);
    if (!status.ok()) {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return -1;
    }

    {
        std::unique_lock<std::mutex> guard(managed_memory_lock);
        managed_memory[reply.page_id()] = mr;
    }

    return reply.page_id();
}

bool Worker::FreePage(int page_id) {
    grpc::ClientContext context;
    grpc::Status status;
    FreePageRequest request;
    FreePageReply reply;

    request.set_page_id(page_id);
    status = stub->FreePage(&context, request, &reply);
    if (!status.ok()) {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return false;
    }

    return true;
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
        global_map->AddWorker(v.rank(), v.qpn(), static_cast<uint16_t>(v.lid() & 0xffff));
    }

    for (auto &v : reply.pages()) {
        global_map->AddPage(v.page_id(), v.size(), v.align(), v.owner_rank());
        for (auto &u : v.rep_list()) {
            global_map->AddPageReplica(v.page_id(), u.rank(), u.addr(), u.key());
        }
    }

    return true;
}

bool Worker::Barrier(int barrier_id) {
    grpc::ClientContext context;
    grpc::Status status;
    GlobalBarrierRequest request;
    GlobalBarrierReply reply;

    request.set_barrier_id(barrier_id);
    status = stub->GlobalBarrier(&context, request, &reply);
    if (!status.ok()) {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return false;
    }

    return true;
}