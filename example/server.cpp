#include <iostream>
#include <unistd.h>
#include "../Monitor.h"

using namespace universe;

int main() {
    Monitor monitor;
    monitor.Run("0.0.0.0:12580");
    return 0;
}