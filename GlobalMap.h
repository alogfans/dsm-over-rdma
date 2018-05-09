//
// Created by alogfans on 5/6/18.
//

#ifndef UNIVERSE_GLOBAL_MAP_H
#define UNIVERSE_GLOBAL_MAP_H

#include <cstdint>
#include <string>
#include <map>

struct GlobalMap {
    struct worker_desc {
        uint32_t qpn;
        uint16_t lid;
    };

    struct page_rep_desc {
        uint64_t addr;
        uint32_t key;
    };

    struct page_desc {
        uint64_t size;
        uint64_t align;
        int owner_rank;
        std::map<int, page_rep_desc> rep_list;
    };

    std::mutex lock;
    std::map<int, worker_desc> worker_map;  // peer -> worker_desc
    std::map<int, page_desc> page_map;      // shared_addr -> page_desc

public:
    void Clear() {
        std::unique_lock<std::mutex> guard(lock);
        worker_map.clear();
        page_map.clear();
    }

    void AddWorker(int rank, uint32_t qpn, uint16_t lid) {
        std::unique_lock<std::mutex> guard(lock);
        worker_map[rank].qpn = qpn;
        worker_map[rank].lid = lid;
    }

    void AddPage(int page_id, uint64_t size, uint64_t align, int owner_rank) {
        std::unique_lock<std::mutex> guard(lock);
        page_map[page_id].size = size;
        page_map[page_id].align = align;
        page_map[page_id].owner_rank = owner_rank;
    }

    void AddPageReplica(int page_id, int replica_rank, uint64_t addr, uint32_t key) {
        std::unique_lock<std::mutex> guard(lock);
        page_map[page_id].rep_list[replica_rank].addr = addr;
        page_map[page_id].rep_list[replica_rank].key = key;
    }

    void DumpGlobalMap() {
        std::unique_lock<std::mutex> guard(lock);
        std::cout << "Global Map dump:" << std::endl;
        std::cout << "*** Workers ***" << std::endl;
        for (auto &v : worker_map) {
            std::cout << "rank " << v.first << ", QPN " << v.second.qpn << ", LID " << v.second.lid << std::endl;
        }

        std::cout << "*** Pages ***" << std::endl;
        for (auto &v : page_map) {
            std::cout << "page_id " << v.first << ", size " << v.second.size << ", align " << v.second.align
                      << ", owner_rank " << v.second.owner_rank << std::endl;

            for (auto &u : v.second.rep_list) {
                std::cout << "\t replica_rank " << u.first << ", key "
                          << u.second.key << ", addr " << u.second.addr << std::endl;
            }
        }

        std::cout << "*** Dump Completed ***" << std::endl;
    }
};

#endif //UNIVERSE_GLOBAL_MAP_H
