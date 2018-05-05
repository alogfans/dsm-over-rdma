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
        std::string JoinGroup(const std::string &name);
    private:
        std::unique_ptr<Controller::Stub> stub;
    };

}




#endif //UNIVERSE_WORKER_H
