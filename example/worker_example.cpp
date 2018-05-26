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
static std::string backend_server = "localhost:10086";

int parse_arguments(int argc, char **argv) {
    CLI::App app{"Distributed shared CPU/GPU memory (Worker)"};
    app.add_option("-s,--server", backend_server, "Backend server hostname");
    app.add_option("-m,--mem-size", memory_size, "Memory size in megabytes");
    app.add_option("-r,--rank", rank, "Rank of this node, must be lower than num_of_procs in monitor side");
    app.set_help_flag("-h,--help", "Print help information");
    CLI11_PARSE(app, argc, argv);
    return 0;
}

int prepare_worker(Worker &worker, int argc, char **argv) {
    parse_arguments(argc, argv);
    if (!worker.Connect(backend_server, rank, memory_size * 1024 * 1024, 64)) {
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

    int var = 1000 + rank;
    worker.Store(var, worker.GlobalAddress(0, 1 - rank));
    worker.Barrier(worker.GlobalAddress(24, 0));
    uint64_t result = worker.Load<uint64_t>(worker.GlobalAddress(0));
    std::cout << rank << ":" << result << std::endl;

    worker.Lock(worker.GlobalAddress(32, 0));
    std::cout << "enter lock region" << std::endl;
    uint64_t data = worker.Load<uint64_t>(worker.GlobalAddress(40, 0));
    std::cout << rank << ":" << data << std::endl;
    worker.Store(data + 512, worker.GlobalAddress(40, 0));
    worker.Unlock(worker.GlobalAddress(32, 0));

    result = worker.Load<uint64_t>(worker.GlobalAddress(40, 0));
    std::cout << rank << ":" << result << std::endl;

    std::cout << "Worker terminated." << std::endl;

    worker.Disconnect();
    return 0;
}
