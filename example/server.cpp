#include <iostream>
#include <unistd.h>
#include "../Monitor.h"
#include "../RdmaService.h"

using namespace universe;
using namespace rdma;

int main() {
    /*
    Monitor monitor;
    monitor.Run("0.0.0.0:8080");
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

    sleep(1);

    printf("%s\n", (char *) mr->Raw());

    return 0;
}