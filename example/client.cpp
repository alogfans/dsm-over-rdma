//
// Created by alogfans on 5/5/18.
//

#include <iostream>
#include "../Worker.h"
#include "../RdmaService.h"

using namespace universe;
using namespace rdma;

int main() {
    /*
    Worker worker("127.0.0.1:8080");
    worker.JoinGroup();
    auto shared_addr = worker.AllocPage(1024, 64);
    std::cout << shared_addr << std::endl;
    worker.SyncMap();
    */
    auto device = Device::Open();
    if (!device) {
        perror("open device failed");
        return -1;
    }

    auto endpoint = device->CreateEndPoint(RC);
    if (!endpoint) {
        perror("create endpoint failed");
        return -1;
    }

    std::cout << "LID " << device->LID() << ", QPN " << endpoint->QPN() << std::endl;

    uint16_t lid;
    uint32_t qpn;
    std::cin >> lid >> qpn;

    endpoint->ReadyToSend(device, lid, qpn);

    auto mr = MemoryRegion::Malloc(device, 1024);
    std::cout << "VADDR " << mr->VirtualAddress() << ", KEY " << mr->Key() << std::endl;

    uint64_t vaddr;
    uint32_t key;
    std::cin >> vaddr >> key;

    strcpy((char *) mr->Raw(), "hello world!");

    WorkRequest work(endpoint);
    work.Write(mr, 0, 12, vaddr, key);
    device->Poll();

    return 0;
}