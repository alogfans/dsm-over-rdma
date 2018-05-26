//
// Created by alogfans on 5/5/18.
//

#ifndef UNIVERSE_WORKER_H
#define UNIVERSE_WORKER_H

#include <string>
#include <memory>
#include <grpc++/grpc++.h>

#include "message.pb.h"
#include "message.grpc.pb.h"
#include "WorkerMap.h"
#include "RDMA.h"

#define PAGESIZE (4 * 1024 * 1024)

namespace universe {
    class Worker {
    public:
        Worker() : rank(-1), num_procs(0) { }
        bool Connect(const std::string &address, int rank, uint64_t size, uint64_t align);
        bool WaitUntilReady(int interval_ms, int max_attempt, bool dump = false);
        bool UpdateWorkerMap();
        bool Disconnect();
        int Rank() const { return rank; }
        int NumOfProcs() const { return num_procs; }
        uint64_t GlobalAddress(uint64_t local_addr, int target_rank = -1) const;

        void Barrier(uint64_t global_addr);
        void Lock(uint64_t global_addr);
        void Unlock(uint64_t global_addr);

        template <typename T>
        void Store(const T& object, uint64_t global_addr) {
            naive_store((uint8_t *) &object, sizeof(T), global_addr);
        }

        template <typename T>
        T Load(uint64_t global_addr) {
            T out_buffer;
            naive_load((uint8_t *) &out_buffer, sizeof(T), global_addr);
            return out_buffer;
        }

    private:
        bool map_ready();
        void map_dump();
        bool locate_global_addr(uint64_t global_addr, int &target_rank, uint64_t &local_addr);
        void complete_connection();

        void do_read_page(uint64_t global_addr);
        void do_write_page(uint64_t global_addr);

        void naive_load(uint8_t *object, size_t size, uint64_t global_addr);
        void naive_store(uint8_t *object, size_t size, uint64_t global_addr);

        void fast_load(uint8_t *object, size_t size, uint64_t global_addr);
        void fast_store(uint8_t *object, size_t size, uint64_t global_addr);
    private:
        std::shared_ptr<grpc::Channel>                           channel;
        std::unique_ptr<Controller::Stub>                        stub;
        int                                                      rank, num_procs;
        std::shared_ptr<rdma::Device>                            device;
        std::vector< std::shared_ptr<rdma::EndPoint> >           endpoint;
        std::vector<WorkerEntry>                                 worker_map;
        std::shared_ptr<rdma::MemoryRegion>                      data_memory;
        std::map<uint64_t, std::shared_ptr<rdma::MemoryRegion> > cached_memory;
    };
}

#endif //UNIVERSE_WORKER_H
