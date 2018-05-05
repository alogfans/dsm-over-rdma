//
// Created by alogfans on 5/5/18.
//

#include "MonitorImpl.h"

using namespace universe;

grpc::Status MonitorImpl::JoinGroup(grpc::ServerContext *context,
                                    const JoinGroupRequest *request,
                                    JoinGroupResponse *response) {
    std::cout << request->name() << std::endl;
    response->set_message("hello " + request->name());
    return grpc::Status::OK;
}
