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
        std::string owner_peer;

        std::map<std::string, page_rep_desc> rep_list;
    };

    std::map<std::string, worker_desc> worker_map;  // peer -> worker_desc
    std::map<uint64_t, page_desc> page_map;         // shared_addr -> page_desc

public:
    void AddWorker(const std::string &peer, uint32_t qpn, uint16_t lid) {
        worker_map[peer] = worker_desc{qpn, lid};
    }

    void AddPage(uint64_t shared_addr, uint64_t size, uint64_t align, const std::string &owner_peer) {
        page_map[shared_addr].size = size;
        page_map[shared_addr].align = align;
        page_map[shared_addr].owner_peer = owner_peer;
    }

    void AddPageRepInfo(uint64_t shared_addr, const std::string &peer, uint64_t addr, uint32_t key) {
        page_map[shared_addr].rep_list[peer] = page_rep_desc{addr, key};
    }
};

#endif //UNIVERSE_GLOBAL_MAP_H
