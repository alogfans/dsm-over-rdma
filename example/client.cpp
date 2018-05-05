//
// Created by alogfans on 5/5/18.
//

#include <iostream>
#include "../Worker.h"

using namespace universe;

int main() {
    Worker worker("127.0.0.1:8080");
    std::cout << worker.JoinGroup("world") << std::endl;
    return 0;
}