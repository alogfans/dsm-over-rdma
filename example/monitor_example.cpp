#include <iostream>
#include <unistd.h>
#include "Monitor.h"
#include "cxxopts.h"

using namespace universe;

int num_of_procs = 1;
std::string backend_server = "0.0.0.0:10086";

int parse_arguments(int argc, char **argv) {
    try {
        cxxopts::Options options(argv[0], "Distributed shared CPU/GPU memory (Monitor)");
        options.add_options()
                ("n,num-procs", "Number of processes", cxxopts::value<int>(num_of_procs))
                ("s,server", "Backend server hostname", cxxopts::value<std::string>(backend_server))
                ("h,help", "Print help information");
        auto result = options.parse(argc, argv);

        if (result.count("help")) {
            std::cout << options.help() << std::endl;
            exit(0);
        }

        if (result.count("server")) {
            backend_server = result["server"].as<std::string>();
        }

        if (result.count("num")) {
            num_of_procs = result["num"].as<int>();
        }
    } catch (const cxxopts::OptionException& e) {
        std::cout << "error parsing options: " << e.what() << std::endl;
        exit(1);
    }

    return 0;
}

int main(int argc, char **argv) {
    parse_arguments(argc, argv);
    Monitor monitor;
    monitor.Start(backend_server, num_of_procs);
    monitor.Wait();
    return 0;
}