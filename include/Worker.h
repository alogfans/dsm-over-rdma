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
        uint64_t GlobalAddress(uint64_t local_addr, int target_rank = -1) const;

        // void Barrier(int at_least_procs = 1);
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
                endpoint->Reset();
                int rc = endpoint->RegisterRemote(worker_map[target_rank].LocalID, worker_map[target_rank].QueuePairNum);
                if (rc != 0) {
                    return;
                }

                auto wrapper = device->Wrap((uint8_t *) &object, sizeof(T));
                if (!wrapper) {
                    return;
                }

                rdma::WorkBatch batch(endpoint);
                batch.Write(rdma::WorkBatch::PathDesc(wrapper, 0),
                            rdma::WorkBatch::PathDesc(worker_map[target_rank].LocalVirtAddr, worker_map[target_rank].MemRegionKey),
                            (uint32_t) wrapper->Size());

                batch.Commit();
                device->Poll(-1);
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
                endpoint->Reset();
                int rc = endpoint->RegisterRemote(worker_map[target_rank].LocalID, worker_map[target_rank].QueuePairNum);
                if (rc != 0) {
                    return out_buffer;
                }

                auto wrapper = device->Wrap((uint8_t *) &out_buffer, sizeof(T));
                if (!wrapper) {
                    return out_buffer;
                }

                rdma::WorkBatch batch(endpoint);
                batch.Read(rdma::WorkBatch::PathDesc(wrapper, 0),
                           rdma::WorkBatch::PathDesc(worker_map[target_rank].LocalVirtAddr, worker_map[target_rank].MemRegionKey),
                           (uint32_t) wrapper->Size());

                rc = batch.Commit();
                if (rc != 0) {
                    return out_buffer;
                }

                rc = device->Poll(-1);
                if (rc != 0) {
                    return out_buffer;
                }
            }

            if (order == std::memory_order::memory_order_acq_rel || order == std::memory_order::memory_order_acquire)
                Acquire();

            return out_buffer;
        }


    private:
        bool map_ready();
        void map_dump();
        bool locate_global_addr(uint64_t global_addr, int &target_rank, uint64_t &local_addr);

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
