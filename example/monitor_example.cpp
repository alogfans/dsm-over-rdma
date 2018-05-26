#include <iostream>
#include <string>
#include <unistd.h>
#include "Monitor.h"
#include "CmdParser.h"

using namespace universe;

static int num_of_procs = 1;
static std::string backend_server("0.0.0.0:10086");

int parse_arguments(int argc, char **argv) {
    CLI::App app{"Distributed shared CPU/GPU memory (Monitor)"};
    app.add_option("-s,--server", backend_server, "Backend server hostname");
    app.add_option("-n,--num-proc", num_of_procs, "Memory size in megabytes");
    app.set_help_flag("-h,--help", "Print help information");
    CLI11_PARSE(app, argc, argv);
    return 0;
}

int main(int argc, char **argv) {
    parse_arguments(argc, argv);
    Monitor monitor;
    monitor.Start(backend_server, num_of_procs);
    monitor.Wait();
    return 0;
}