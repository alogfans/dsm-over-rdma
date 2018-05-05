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

grpc::Status MonitorImpl::JoinGroup(grpc::ServerContext *context,
                                const JoinGroupRequest *request,
                                JoinGroupResponse *response) {
    std::cout << request->name() << std::endl;
    response->set_message("hello " + request->name());
    return grpc::Status::OK;
}
