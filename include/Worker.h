//
// Created by alogfans on 5/5/18.
//

#ifndef UNIVERSE_WORKER_H
#define UNIVERSE_WORKER_H

#include <string>
#include <memory>
#include <grpcpp/grpcpp.h>

#include "controller.pb.h"
#include "controller.grpc.pb.h"
#include "WorkerMap.h"
#include "RDMA.h"

namespace universe {
    class Worker {
    public:
        Worker() : rank(-1), num_of_procs(0) { }
        bool Connect(const std::string &address, int rank, uint64_t size, uint64_t align);
        bool WaitUntilReady(int interval_ms, int max_attempt, bool dump = false);
        bool UpdateWorkerMap();
        bool Disconnect();
        int Rank() const { return rank; }
        int NumOfProcs() const { return num_of_procs; }

    private:
        bool map_ready();
        void map_dump();

    private:
        std::shared_ptr<grpc::Channel>      channel;
        std::unique_ptr<Controller::Stub>   stub;
        int                                 rank, num_of_procs;
        std::shared_ptr<rdma::Device>       device;
        std::shared_ptr<rdma::EndPoint>     endpoint;
        std::shared_ptr<rdma::MemoryRegion> memory;
        std::vector<WorkerEntry>            worker_map;
    };
}

#endif //UNIVERSE_WORKER_H
