//
// Created by alogfans on 5/5/18.
//

#include <iostream>
#include <unistd.h>
#include <thread>
#include "Worker.h"
#include "cxxopts.h"

using namespace universe;
using namespace rdma;

int rank = 0;
uint64_t memory_size = 16;
std::string backend_server = "localhost:10086";

int parse_arguments(int argc, char **argv) {
    try {
        cxxopts::Options options(argv[0], "Distributed shared CPU/GPU memory (Worker)");
        options.add_options()
                ("s,server", "Backend server hostname", cxxopts::value<std::string>(backend_server))
                ("m,memory-size", "Memory size in MB", cxxopts::value<uint64_t>(memory_size))
                ("r,rank", "Rank of this node, must be lower than num_of_procs in monitor side", cxxopts::value<int>(rank))
                ("h,help", "Print help information");

        auto result = options.parse(argc, argv);

        if (result.count("help")) {
            std::cout << options.help() << std::endl;
            exit(0);
        }

        if (result.count("server")) {
            backend_server = result["server"].as<std::string>();
        }

        if (result.count("rank")) {
            rank = result["rank"].as<int>();
        }

        if (result.count("memory")) {
            memory_size = result["memory"].as<uint64_t>();
        }

    } catch (const cxxopts::OptionException& e) {
        std::cout << "error parsing options: " << e.what() << std::endl;
        exit(1);
    }

    return 0;
}

int main(int argc, char **argv) {
    parse_arguments(argc, argv);
    Worker worker;
    if (!worker.Connect(backend_server, rank, memory_size * 1024 * 1024, 64)) {
        std::cout << "execution error." << std::endl;
        exit(-1);
    }

    std::cout << "rank " << worker.Rank() << ", num_of_procs " << worker.NumOfProcs() << std::endl;

    if (!worker.WaitUntilReady(1000, 100, true)) {
        std::cout << "execution error." << std::endl;
        exit(-1);
    }

    std::this_thread::sleep_for(std::chrono::seconds(2));

    int var = 1000 + rank;
    worker.Store(var, worker.GlobalAddress(0, 1 - rank));

    std::this_thread::sleep_for(std::chrono::seconds(2));

    int result = worker.Load<int>(worker.GlobalAddress(0));
    std::cout << rank << ":" << result << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(2));

    worker.Disconnect();
    return 0;
}