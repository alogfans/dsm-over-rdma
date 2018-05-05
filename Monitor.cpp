//
// Created by alogfans on 5/5/18.
//

#include "Monitor.h"
#include <iostream>
#include "MonitorImpl.h"

using universe::Monitor;

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