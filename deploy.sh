#!/bin/bash
rm -rf build
mkdir build && cd build
cmake ..
make -j4
cp /usr/local/lib/libgpr.so.6 .
cp /usr/local/lib/libgrpc++.so.1 .
cp /usr/local/lib/libgrpc.so.6 .
tar czvf runtime.tar.gz client server lib*
# tstore04
scp runtime.tar.gz root@166.111.69.87:~/renfeng
