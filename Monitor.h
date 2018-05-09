//
// Created by alogfans on 5/5/18.
//

#ifndef UNIVERSE_MONITOR_H
#define UNIVERSE_MONITOR_H

#include <string>
#include <grpcpp/grpcpp.h>
#include <atomic>

#include "protos/controller.grpc.pb.h"
#include "protos/controller.pb.h"
#include "GlobalMap.h"

namespace universe {
    class MonitorImpl final : public Controller::Service {
    public:
        MonitorImpl();
        virtual ~MonitorImpl() = default;

    private:
        grpc::Status JoinGroup(grpc::ServerContext* context, const JoinGroupRequest* request,
                               JoinGroupReply* response) override;

        grpc::Status AllocPage(grpc::ServerContext* context, const AllocPageRequest* request,
                               AllocPageReply* response) override;

        grpc::Status SyncMap(grpc::ServerContext* context, const SyncMapRequest* request,
                             SyncMapReply* response) override ;

        grpc::Status LeaveGroup(grpc::ServerContext* context, const LeaveGroupRequest* request,
                                LeaveGroupReply* response);

        grpc::Status FreePage(grpc::ServerContext* context, const FreePageRequest* request,
                              FreePageReply* response);

    private:
        std::atomic<int> next_page_id;
        std::atomic<int> next_rank;
        std::shared_ptr<GlobalMap> global_map;
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
