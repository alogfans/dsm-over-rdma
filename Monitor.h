//
// Created by alogfans on 5/5/18.
//

#ifndef UNIVERSE_MONITOR_H
#define UNIVERSE_MONITOR_H

#include <string>
#include <grpcpp/grpcpp.h>

namespace universe {

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
