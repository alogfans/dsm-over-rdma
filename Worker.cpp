//
// Created by alogfans on 5/5/18.
//

#include <grpcpp/grpcpp.h>
#include <iostream>
#include "Worker.h"

using namespace universe;
using namespace grpc;

Worker::Worker(const std::string &address) {
    std::shared_ptr<Channel> channel = CreateChannel(address, InsecureChannelCredentials());
    stub = Controller::NewStub(channel);
}

std::string Worker::JoinGroup(const std::string &name) {
    JoinGroupRequest request;
    request.set_name(name);

    JoinGroupResponse reply;
    ClientContext context;
    Status status = stub->JoinGroup(&context, request, &reply);
    if (status.ok()) {
        return reply.message();
    } else {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return "RPC failed";
    }
}
