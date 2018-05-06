//
// Created by alogfans on 5/5/18.
//

#include <iostream>
#include "../Worker.h"

using namespace universe;

int main() {
    Worker worker("127.0.0.1:8080");
    worker.JoinGroup();
    auto shared_addr = worker.AllocPage(1024, 64);
    std::cout << shared_addr << std::endl;
    worker.SyncMap();
    return 0;
}