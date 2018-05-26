cmake_minimum_required(VERSION 2.8)
project(universe)

find_package(Protobuf REQUIRED)

include_directories(include protos)

set(CMAKE_CXX_FLAGS "-std=c++11 -O3")
link_libraries(protobuf grpc++ ibverbs)

set(PROTO_GENERATOR /usr/bin/grpc_cpp_plugin)

set(PROTO_PATH "${CMAKE_SOURCE_DIR}/protos")
set(PROTO_FILES "${PROTO_PATH}/message")

add_custom_command(
        OUTPUT  "${PROTO_FILES}.pb.h"
                "${PROTO_FILES}.pb.cc"
                "${PROTO_FILES}.grpc.pb.h"
                "${PROTO_FILES}.grpc.pb.cc"
        COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
        ARGS    "--proto_path=${PROTO_PATH}"
                "--cpp_out=${PROTO_PATH}"
                "${PROTO_FILES}.proto"
        COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
        ARGS    "--proto_path=${PROTO_PATH}"
                "--grpc_out=${PROTO_PATH}"
                "--plugin=protoc-gen-grpc=${PROTO_GENERATOR}"
                "${PROTO_FILES}.proto"
)

set(SHARED_SOURCES
        src/Monitor.cpp
        src/Worker.cpp
        ${PROTO_FILES}.pb.cc
        ${PROTO_FILES}.grpc.pb.cc
        src/RDMA.cpp)

set(SHARED_HEADERS
        include/Monitor.h
        include/Worker.h
        ${PROTO_FILES}.pb.h
        ${PROTO_FILES}.grpc.pb.h
        include/CmdParser.h
        include/WorkerMap.h
        include/RDMA.h include/Logger.h)

set(SHARED_FILES ${SHARED_SOURCES} ${SHARED_HEADERS})

add_library(universe ${SHARED_FILES} ${SHARED_HEADERS})
link_libraries(universe)

add_executable(monitor example/monitor_example.cpp)
add_executable(worker example/worker_example.cpp)
