//
// Created by alogfans on 5/5/18.
//

#include <iostream>
#include <unistd.h>
#include "../Worker.h"

using namespace universe;
using namespace rdma;

int main() {
    Worker worker;
    int rank = worker.JoinGroup("tstore04:8080");
    if (rank % 2 == 0) {
        worker.AllocPage(1024, 64);
    }

    int delay = 10;
    while (delay--) {
        worker.SyncMap();
        worker.DumpGlobalMap();
        sleep(1);
    }

    worker.LeaveGroup();
    return 0;
}