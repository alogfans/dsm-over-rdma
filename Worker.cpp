//
// Created by alogfans on 5/5/18.
//

#include <grpcpp/grpcpp.h>
#include <iostream>
#include "Worker.h"

using namespace universe;

Worker::Worker(const std::string &address) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    stub = Controller::NewStub(channel);
}

std::string Worker::JoinGroup(const std::string &name) {
    JoinGroupRequest request;
    request.set_name(name);

    JoinGroupResponse reply;
    grpc::ClientContext context;
    grpc::Status status = stub->JoinGroup(&context, request, &reply);
    if (status.ok()) {
        return reply.message();
    } else {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return "RPC failed";
    }
}
