//
// Created by alogfans on 5/5/18.
//

#ifndef UNIVERSE_WORKER_H
#define UNIVERSE_WORKER_H

#include <string>
#include <memory>

#include "protos/controller.pb.h"
#include "protos/controller.grpc.pb.h"
#include "GlobalMap.h"
#include "RdmaService.h"

namespace universe {
    class Worker {
    public:
        int JoinGroup(const std::string &address);
        bool LeaveGroup();
        int Rank() const { return rank; }
        int AllocPage(uint64_t size, uint64_t align);
        bool FreePage(int page_id);
        bool Barrier(int barrier_id);
        // uint8_t *GetPage(int page_id);
        bool SyncMap();
        void DumpGlobalMap() { global_map->DumpGlobalMap(); }

    private:
        std::shared_ptr<grpc::Channel> channel;
        std::unique_ptr<Controller::Stub> stub;
        int rank = -1; // undefined
        std::shared_ptr<GlobalMap> global_map;
        std::shared_ptr<rdma::Device> device;
        std::shared_ptr<rdma::EndPoint> endpoint;
        std::map< int, std::shared_ptr<rdma::MemoryRegion> > managed_memory;
        std::mutex managed_memory_lock;
    };
}




#endif //UNIVERSE_WORKER_H
