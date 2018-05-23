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

namespace universe {
    class Worker {
    public:
        Worker() : rank(-1), num_procs(0), endpoint_peer_rank(-1) { }
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

        void Acquire();
        void Release();

        template <typename T>
        void Store(const T& object, uint64_t global_addr, std::memory_order order = std::memory_order::memory_order_release) {
            if (order == std::memory_order::memory_order_acq_rel || order == std::memory_order::memory_order_release)
                Release();

            uint64_t local_addr;
            int target_rank;
            if (!locate_global_addr(global_addr, target_rank, local_addr)) {
                return;
            }

            if (target_rank == rank) {
                memcpy(&memory->Get()[local_addr], &object, sizeof(T));
            } else {
                connect_peer(target_rank);
                auto wrapper = device->Wrap((uint8_t *) &object, sizeof(T));
                ASSERT(wrapper);

                rdma::WorkBatch batch(endpoint);
                batch.Write(rdma::WorkBatch::PathDesc(wrapper, 0),
                            rdma::WorkBatch::PathDesc(worker_map[target_rank].LocalVirtAddr,
                                                      worker_map[target_rank].MemRegionKey),
                            (uint32_t) wrapper->Size());

                ASSERT(!batch.Commit());
                ASSERT(!device->Poll(-1));
            }

            if (order == std::memory_order::memory_order_acq_rel || order == std::memory_order::memory_order_acquire)
                Acquire();
        }


        template <typename T>
        T Load(uint64_t global_addr, std::memory_order order = std::memory_order::memory_order_acquire) {
            T out_buffer;

            if (order == std::memory_order::memory_order_acq_rel || order == std::memory_order::memory_order_release)
                Release();

            uint64_t local_addr;
            int target_rank;
            if (!locate_global_addr(global_addr, target_rank, local_addr)) {
                return out_buffer;
            }

            if (target_rank == rank) {
                memcpy(&out_buffer, &memory->Get()[local_addr], sizeof(T));
            } else {
                connect_peer(target_rank);
                auto wrapper = device->Wrap((uint8_t *) &out_buffer, sizeof(T));
                ASSERT(wrapper);

                rdma::WorkBatch batch(endpoint);
                batch.Read(rdma::WorkBatch::PathDesc(wrapper, 0),
                           rdma::WorkBatch::PathDesc(worker_map[target_rank].LocalVirtAddr,
                                                     worker_map[target_rank].MemRegionKey),
                           (uint32_t) wrapper->Size());

                ASSERT(!batch.Commit());
                ASSERT(!device->Poll(-1));
            }

            if (order == std::memory_order::memory_order_acq_rel || order == std::memory_order::memory_order_acquire)
                Acquire();

            return out_buffer;
        }

    private:
        bool map_ready();
        void map_dump();
        bool locate_global_addr(uint64_t global_addr, int &target_rank, uint64_t &local_addr);
        void connect_peer(int target_rank);

    private:
        std::shared_ptr<grpc::Channel>      channel;
        std::unique_ptr<Controller::Stub>   stub;
        int                                 rank, num_procs;
        std::shared_ptr<rdma::Device>       device;
        std::shared_ptr<rdma::EndPoint>     endpoint;
        int                                 endpoint_peer_rank;
        std::shared_ptr<rdma::MemoryRegion> memory;
        std::vector<WorkerEntry>            worker_map;
        // std::vector< std::shared_ptr<rdma::MemoryRegion> > cached_memory;
    };
}

#endif //UNIVERSE_WORKER_H
