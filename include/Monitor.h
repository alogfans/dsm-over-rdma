//
// Created by alogfans on 5/5/18.
//

#ifndef UNIVERSE_MONITOR_H
#define UNIVERSE_MONITOR_H

#include <string>
#include <grpc++/grpc++.h>
#include <atomic>
#include <bitset>

#include "message.grpc.pb.h"
#include "message.pb.h"
#include "WorkerMap.h"

#define PAGESIZE (4 * 1024 * 1024)

namespace universe {
    class MonitorImpl final : public Controller::Service {
    public:
        MonitorImpl(int num_of_procs);
        virtual ~MonitorImpl() = default;

    private:

        grpc::Status JoinGroup(grpc::ServerContext* context,
                               const JoinGroupRequest* request,
                               JoinGroupReply* response) override;

        grpc::Status LeaveGroup(grpc::ServerContext* context,
                                const LeaveGroupRequest* request,
                                LeaveGroupReply* response) override;

        grpc::Status CacheWorkerMap(grpc::ServerContext* context,
                                    const CacheWorkerMapRequest* request,
                                    CacheWorkerMapReply* response) override;

        grpc::Status ReadFault(grpc::ServerContext* context,
                               const FaultRequest* request,
                               FaultReply* response) override;

        grpc::Status WriteFault(grpc::ServerContext* context,
                                const FaultRequest* request,
                                FaultReply* response) override;

        grpc::Status EndFault(grpc::ServerContext* context,
                              const FaultRequest* request,
                              FaultReply* response) override;

    private:
        std::shared_ptr<WorkerMap> worker_map;

        struct PageState {
            std::atomic<bool> onFaultProgress;
            std::set<int> copySet;
        };

        std::map<uint64_t, PageState> page_states;
    };

    class Monitor {
    public:
        virtual ~Monitor() { Shutdown(); }
        void Start(const std::string &address, int num_of_procs);
        void Wait();
        void Shutdown();

    private:
        std::shared_ptr<MonitorImpl>  monitor_impl;
        std::shared_ptr<grpc::Server> server;
    };
}

#endif //UNIVERSE_MONITOR_H
