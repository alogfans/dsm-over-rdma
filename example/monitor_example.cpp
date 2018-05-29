#include <iostream>
#include <string>
#include <unistd.h>
#include "Monitor.h"
#include "CmdParser.h"

using namespace universe;

static int num_of_procs = 1;
static std::string port("10086");

int parse_arguments(int argc, char **argv) {
    CLI::App app{"Distributed Shared CPU/GPU Memory"};
    app.add_option("-p,--port", port, "Port over TCP [default 10086]");
    app.add_option("-n,--num-worker", num_of_procs, "Number of workers in the group [default 1]");

    try {
        app.parse(argc, argv);
    } catch (CLI::ParseError &e) {
        std::exit(app.exit(e));
    }

    return 0;
}

int main(int argc, char **argv) {
    parse_arguments(argc, argv);
    Monitor monitor;
    monitor.Start("0.0.0.0:" + port, num_of_procs);
    monitor.Wait();
    return 0;
}