//
// Created by alogfans on 5/5/18.
//

#ifndef UNIVERSE_MONITOR_H
#define UNIVERSE_MONITOR_H

#include <string>
#include <grpcpp/grpcpp.h>

#include "protos/controller.grpc.pb.h"
#include "protos/controller.pb.h"

namespace universe {
    class MonitorImpl final : public Controller::Service {
        grpc::Status JoinGroup(grpc::ServerContext* context,
                               const JoinGroupRequest* request,
                               JoinGroupResponse* response);

    };

    class Monitor {
    public:
        virtual ~Monitor() { Shutdown(); }
        void Run(const std::string &address);
        void Shutdown();

    private:
        std::unique_ptr<grpc::Server> server;
    };
}

#endif //UNIVERSE_MONITOR_H
