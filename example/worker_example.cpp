//
// Created by alogfans on 5/5/18.
//

#include <iostream>
#include <unistd.h>
#include <thread>
#include "Worker.h"
#include "CmdParser.h"

using namespace universe;
using namespace rdma;

static int rank = 0;
static uint64_t memory_size = 16;
static int num_of_procs = 1;
static std::string backend_server = "localhost:10086";

int parse_arguments(int argc, char **argv) {
    CLI::App app{"Distributed shared CPU/GPU memory (Worker)"};
    app.add_option("-s,--monitor-host", backend_server, "Monitor hostname [default localhost:10086]");
    app.add_option("-m,--memory", memory_size, "Memory size in MB [default 16]");
    app.add_option("-n,--num-worker", num_of_procs, "Number of workers in the group [default 1]");
    app.add_option("-r,--rank", rank, "Rank of this node, must be distinct non-negative value and lower than num_of_procs in the monitor side");

    try {
        app.parse(argc, argv);
    } catch (CLI::ParseError &e) {
        std::exit(app.exit(e));
    }

    return 0;
}

int prepare_worker(Worker &worker, int argc, char **argv) {
    parse_arguments(argc, argv);
    if (!worker.Connect(backend_server, rank, num_of_procs, memory_size * 1024 * 1024, 64)) {
        std::cout << "execution error." << std::endl;
        exit(-1);
    }

    if (!worker.WaitUntilReady(200, 100, false)) {
        std::cout << "execution error." << std::endl;
        exit(-1);
    }
}

int main(int argc, char **argv) {
    Worker worker;
    prepare_worker(worker, argc, argv);

    std::cout << "Worker ready." << std::endl;

    const uint64_t lock_area = memory_size / 2 * 1024 * 1024;

    int var = 1000 + rank;
    worker.Store(var, worker.GlobalAddress(0, 1 - rank));
    worker.Barrier(worker.GlobalAddress(lock_area + 0, 0));
    uint64_t result = worker.Load<uint64_t>(worker.GlobalAddress(0));
    std::cout << rank << ":" << result << std::endl;

    auto mutex = worker.GlobalAddress(lock_area + 8, 1);
    worker.Lock(mutex);

    std::cout << "enter lock region" << std::endl;
    uint64_t data = worker.Load<uint64_t>(worker.GlobalAddress(40, 0));
    std::cout << rank << ":" << data << std::endl;
    worker.Store(data + 512, worker.GlobalAddress(40, 0));

    worker.Unlock(mutex);

    result = worker.Load<uint64_t>(worker.GlobalAddress(40, 0));
    std::cout << rank << ":" << result << std::endl;
    std::cout << "Worker terminated." << std::endl;

    worker.Disconnect();
    return 0;
}
