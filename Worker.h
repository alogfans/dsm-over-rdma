//
// Created by alogfans on 5/5/18.
//

#ifndef UNIVERSE_WORKER_H
#define UNIVERSE_WORKER_H

#include <string>
#include <memory>

#include "protos/controller.pb.h"
#include "protos/controller.grpc.pb.h"

namespace universe {
    class Worker {
    public:
        Worker(const std::string &address);
        bool JoinGroup();
        uint64_t AllocPage(uint64_t size, uint64_t align);
        bool SyncMap();

    private:
        std::unique_ptr<Controller::Stub> stub;
        const std::string address;
    };
}




#endif //UNIVERSE_WORKER_H
