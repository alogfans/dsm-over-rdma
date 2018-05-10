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
    int rank = worker.JoinGroup("tstore04:12580");
    if (rank % 2 == 0) {
        worker.AllocPage(1024, 64);
    }

    worker.Barrier(0);
    worker.SyncMap();
    worker.Barrier(1);
    worker.DumpGlobalMap();
    worker.LeaveGroup();
    return 0;
}