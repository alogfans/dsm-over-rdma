//
// Created by alogfans on 5/5/18.
//

#ifndef UNIVERSE_MONITORIMPL_H
#define UNIVERSE_MONITORIMPL_H

#include "protos/controller.grpc.pb.h"
#include "protos/controller.pb.h"

namespace universe {

class MonitorImpl final : public Controller::Service {
    grpc::Status JoinGroup(grpc::ServerContext* context,
                           const JoinGroupRequest* request,
                           JoinGroupResponse* response);

};

}

#endif //UNIVERSE_MONITORIMPL_H
