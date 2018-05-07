//
// Created by alogfans on 5/7/18.
//

#ifndef UNIVERSE_RDMASERVICE_H
#define UNIVERSE_RDMASERVICE_H

#include <string>
#include <memory>

#include <infiniband/verbs.h>
#include <vector>
#include <atomic>

#define DEBUG_ENABLED
#ifdef DEBUG_ENABLED
#define DEBUG(format,...) printf("ERR " __FILE__":%04d " format " (errno %d %s)\n", __LINE__, ##__VA_ARGS__, errno, strerror(errno))
#else
#define DEBUG(format,...)
#endif

#define RIO_DEFAULT_QKEY       (0x11111111)
#define RIO_MULTICAST_QPN      (0xffffffff)
#define RIO_FULL_ACCESS_PERM   (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC)

namespace rdma {
    class EndPoint;
    enum EndPointType {
        RC = 2, UC, UD
    };

    class Device {
    public:
        static std::shared_ptr<Device> Open(const std::string &device_name = "", uint8_t port = 1);
        virtual ~Device();

        Device(const Device &) = delete;
        Device &operator=(const Device &) = delete;

        int Poll(int timeout_ms = -1);

        std::shared_ptr<EndPoint> CreateEndPoint(EndPointType type,
                                                 uint32_t max_cqe = 4,
                                                 uint32_t max_wr = 4,
                                                 uint32_t max_inline_data = 64);

        uint16_t LID();
        uint8_t Port() { return ib_port; }
        struct ibv_pd *UnsafePD() { return pd; }

    public:
        explicit Device(struct ibv_context *ib_ctx, struct ibv_pd *pd,
                struct ibv_comp_channel *channel,
                int epoll_fd, uint8_t ib_port):
                ib_ctx(ib_ctx), pd(pd), channel(channel), epoll_fd(epoll_fd), ib_port(ib_port) { }

    private:
        struct ibv_context *ib_ctx;
        struct ibv_pd *pd;
        struct ibv_comp_channel *channel;
        int epoll_fd;
        uint8_t ib_port;
    };

    class WorkRequest;
    class EndPoint {
    public:
        virtual ~EndPoint();

        EndPoint(const EndPoint &) = delete;
        EndPoint &operator=(const EndPoint &) = delete;

        uint32_t QPN() { return qp->qp_num; }
        int ReadyToSend(std::shared_ptr<Device> device, uint16_t remote_lid, uint32_t remote_qpn);

        // int JoinMulticast(GID, LID)

    public:
        explicit EndPoint(EndPointType type, struct ibv_cq *cq, struct ibv_qp *qp)
                : type(type), cq(cq), qp(qp), ah(NULL) { }

        struct ibv_qp *UnsafeQP() { return qp; }
        struct ibv_ah *UnsafeAH() { return ah; }

    private:
        EndPointType type;
        struct ibv_cq *cq;
        struct ibv_qp *qp;

        struct ibv_ah *ah;
    };

    class MemoryRegion {
    public:
        static std::shared_ptr<MemoryRegion> Malloc(std::shared_ptr<Device> device, size_t n,
                                                    bool zero = false, uint64_t align = 16);

    public:
        explicit MemoryRegion(void *addr, size_t n, struct ibv_mr *mr) : addr(addr), n(n), mr(mr) { }
        virtual ~MemoryRegion();

        MemoryRegion(const MemoryRegion &) = delete;
        MemoryRegion &operator=(const MemoryRegion &) = delete;

        uint64_t VirtualAddress() { return (uintptr_t) addr; }
        uint32_t Key() { return mr->lkey; }
        void *Raw() { return addr; }

    private:
        void *addr;
        size_t n;
        struct ibv_mr *mr;
    };

    class WorkRequest {
    public:
        explicit WorkRequest(std::shared_ptr<EndPoint> endpoint) : endpoint(endpoint) { counter.store(0); }

        int Write(std::shared_ptr<MemoryRegion> memory, uint64_t offset, uint32_t length,
                  uint64_t remote_address, uint32_t remote_key);

        int WriteImm(std::shared_ptr<MemoryRegion> memory, uint64_t offset, uint32_t length, uint32_t imm_data,
                     uint64_t remote_address, uint32_t remote_key);

        int Read(std::shared_ptr<MemoryRegion> memory, uint64_t offset, uint32_t length,
                 uint64_t remote_address, uint32_t remote_key);

        int FetchAndAdd(std::shared_ptr<MemoryRegion> memory, uint64_t offset,
                        uint64_t add_val, uint64_t remote_address, uint32_t remote_key);

        int CompareAndSwap(std::shared_ptr<MemoryRegion> memory, uint64_t offset,
                           uint64_t compare_val, uint64_t swap_value,
                           uint64_t remote_address, uint32_t remote_key);

        int Send(std::shared_ptr<MemoryRegion> memory, uint64_t offset, uint32_t length);

        int Receive(std::shared_ptr<MemoryRegion> memory, uint64_t offset, uint32_t length);

    private:
        std::shared_ptr<EndPoint> endpoint;
        std::atomic<uint64_t> counter;
    };
}

#endif //UNIVERSE_RDMASERVICE_H
