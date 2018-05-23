//
// Created by alogfans on 5/23/18.
//

#ifndef UNIVERSE_LOGGER_H
#define UNIVERSE_LOGGER_H

#define DEBUG(format,...) printf("ERROR " __FILE__":%03d " format " (errno %d %s)\n", __LINE__, ##__VA_ARGS__, errno, strerror(errno))

#define ASSERT(func) do { if (!(func)) { DEBUG("assertion '%s' failed", #func); } } while (0)

#endif //UNIVERSE_LOGGER_H
