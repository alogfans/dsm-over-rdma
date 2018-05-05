#include <iostream>
#include "../Monitor.h"

using namespace universe;

int main() {
    Monitor monitor;
    monitor.Run("0.0.0.0:8080");
    return 0;
}