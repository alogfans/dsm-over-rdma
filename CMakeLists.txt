cmake_minimum_required(VERSION 3.10)
project(universe)

find_package(Protobuf REQUIRED)

set(CMAKE_CXX_STANDARD 11)
link_libraries(protobuf grpc++)

set(PROTO_PATH "${CMAKE_SOURCE_DIR}/protos")
set(PROTO_FILES "${PROTO_PATH}/controller")

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
                "--plugin=protoc-gen-grpc=/usr/local/bin/grpc_cpp_plugin"
                "${PROTO_FILES}.proto"
)

set(SHARED_SOURCES Monitor.cpp Worker.cpp ${PROTO_FILES}.pb.cc ${PROTO_FILES}.grpc.pb.cc)
set(SHARED_HEADERS Monitor.h Worker.h ${PROTO_FILES}.pb.h ${PROTO_FILES}.grpc.pb.h)
set(SHARED_FILES ${SHARED_SOURCES} ${SHARED_HEADERS})

add_library(universe ${SHARED_FILES} ${SHARED_HEADERS})
link_libraries(universe)
add_executable(server example/server.cpp)
add_executable(client example/client.cpp)
