//
// Created by alogfans on 5/6/18.
//

#ifndef UNIVERSE_WORKER_MAP_H
#define UNIVERSE_WORKER_MAP_H

#include <cstdint>
#include <string>
#include <map>

namespace universe {
    class WorkerEntry {
    public:
        WorkerEntry() : Valid(false) { }

        WorkerEntry(const std::vector<uint32_t> &queue_pair_num,
                    uint16_t local_id,
                    uint64_t local_virt_addr,
                    uint32_t mem_region_key,
                    uint64_t mem_size) :
                QueuePairNum(queue_pair_num),
                LocalID(local_id),
                LocalVirtAddr(local_virt_addr),
                MemRegionKey(mem_region_key),
                MemSize(mem_size),
                Valid(true) { }

    public:
        bool                  Valid;
        std::vector<uint32_t> QueuePairNum;
        uint16_t              LocalID;
        uint64_t              LocalVirtAddr;
        uint32_t              MemRegionKey;
        uint64_t              MemSize;
    };

    class WorkerMap {
    public:
        explicit WorkerMap(int num_of_procs) : num_of_procs(num_of_procs) {
            worker_map.clear();
            worker_map.resize(static_cast<ulong>(num_of_procs));
            for (auto &v : worker_map) {
                v.Valid = false;
            }
        }

        bool insert(int rank,
                    const std::vector<uint32_t> &queue_pair_num,
                    uint16_t local_id,
                    uint64_t local_virt_addr,
                    uint32_t mem_region_key,
                    uint64_t mem_size) {

            if (rank < 0 || rank >= num_of_procs) {
                return false;
            }

            std::lock_guard<std::mutex> guard(mutex);
            if (worker_map[rank].Valid) {
                return false;
            }

            worker_map[rank] = WorkerEntry(queue_pair_num, local_id, local_virt_addr, mem_region_key, mem_size);
            return true;
        }

        bool erase(int rank) {
            if (rank < 0 || rank >= num_of_procs) {
                return false;
            }

            std::lock_guard<std::mutex> guard(mutex);
            worker_map[rank].Valid = false;
            return true;
        }

        int get_num_of_procs() {
            return num_of_procs;
        }

        bool is_ready() {
            std::lock_guard<std::mutex> guard(mutex);
            for (auto &v : worker_map) {
                if (!v.Valid)
                    return false;
            }
            return true;
        }

        void dump() {
            std::cout << "Not implemented." << std::endl;
        }

        void snapshot(std::vector<WorkerEntry> &worker_map_copy) {
            std::lock_guard<std::mutex> guard(mutex);
            worker_map_copy.clear();
            worker_map_copy = worker_map;
        }

    private:
        const int num_of_procs;
        std::mutex mutex;
        std::vector<WorkerEntry> worker_map;
    };
}

#endif //UNIVERSE_WORKER_MAP_H
