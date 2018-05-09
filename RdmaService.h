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

#define DEBUG(format,...) printf("ERR " __FILE__":%04d " format " (errno %d %s)\n", __LINE__, ##__VA_ARGS__, errno, strerror(errno))

namespace rdma {
    class EndPoint;
    class WorkBatch;
    class MemoryRegion;

    const uint32_t MultiCastQPN = 0xffffffff;
    const uint32_t MulticastKey = 0x11111111;
    const int FullAccessPermFlags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

    class Device {
    public:
        static std::shared_ptr<Device> Open(const std::string &device_name = "", uint8_t port = 1);

        virtual ~Device();

        Device(const Device &) = delete;
        Device &operator=(const Device &) = delete;

        std::shared_ptr<EndPoint> CreateEndPoint(ibv_qp_type type, uint32_t max_cqe = 4, uint32_t max_wr = 4, uint32_t max_inline_data = 64);
        int Poll(int timeout_ms = -1);
        int Poll(std::vector<ibv_wc> &replies, int timeout_ms = -1);

        std::shared_ptr<MemoryRegion> Malloc(size_t n, bool zero = false, uint64_t align = 16);

        uint16_t LID() const;
        const std::vector<uint8_t> GID(int gid_index = 0) const;
        uint8_t Port() const { return ib_port; }

    public:
        explicit Device(ibv_context *ib_ctx, ibv_pd *pd, ibv_comp_channel *channel, int epoll_fd, uint8_t ib_port):
                ib_ctx(ib_ctx), pd(pd), channel(channel), epoll_fd(epoll_fd), ib_port(ib_port) { }

    private:
        static ibv_context *getContext(const std::string &device_name);

    private:
        ibv_context *ib_ctx;
        ibv_pd *pd;
        ibv_comp_channel *channel;
        int epoll_fd;
        uint8_t ib_port;
    };

    class EndPoint {
    public:
        virtual ~EndPoint();

        EndPoint(const EndPoint &) = delete;
        EndPoint &operator=(const EndPoint &) = delete;

        int RegisterRemote(uint16_t remote_lid, uint32_t remote_qpn);
        int JoinMulticast(const std::vector<uint8_t> &remote_gid, uint16_t remote_lid);
        int LeaveMulticast(const std::vector<uint8_t> &remote_gid, uint16_t remote_lid);

        uint32_t QPN() const { return qp->qp_num; }

    public:
        explicit EndPoint(uint8_t ib_port, ibv_cq *cq, ibv_qp *qp) : ib_port(ib_port), cq(cq), qp(qp), ah(NULL) { }

        struct ibv_qp *UnsafeQP() { return qp; }
        struct ibv_ah *UnsafeAH() { return ah; }

    private:
        int setInit(ibv_qp *qp, uint8_t ib_port);
        int SetRTR(ibv_qp *qp, uint32_t remote_qpn, uint16_t remote_lid, uint8_t ib_port);
        int setRTS(ibv_qp *qp);

    private:
        struct ibv_cq *cq;
        struct ibv_qp *qp;
        struct ibv_ah *ah;
        uint8_t ib_port;
    };

    class MemoryRegion {
    public:
        explicit MemoryRegion(uint8_t *addr, size_t n, struct ibv_mr *mr) : addr(addr), n(n), mr(mr) { }

        virtual ~MemoryRegion() {
            if (mr) { ibv_dereg_mr(mr); }
            if (addr) { free(addr); }
        }

        MemoryRegion(const MemoryRegion &) = delete;
        MemoryRegion &operator=(const MemoryRegion &) = delete;

        uint64_t VirtualAddress() const { return (uintptr_t) addr; }
        uint32_t Key() const { return mr->lkey; }
        uint8_t *Get() const { return addr; }
        uint8_t &operator[](int i) { return addr[i]; }

    private:
        uint8_t *addr;
        size_t n;
        struct ibv_mr *mr;
    };

    class WorkBatch {
    public:
        struct PathDesc {
            PathDesc(const std::shared_ptr<MemoryRegion> memory, uint64_t offset) :
                    Address(memory->VirtualAddress() + offset), Key(memory->Key()) { }
            PathDesc(uint64_t address, uint32_t key) : Address(address), Key(key) { }

            uint64_t Address;
            uint32_t Key;
        };

        WorkBatch(std::shared_ptr<EndPoint> endpoint) : counter(0), endpoint(std::move(endpoint)) { }

        void Write(const PathDesc &local, const PathDesc &target, uint32_t length);
        void WriteImm(const PathDesc &local, const PathDesc &target, uint32_t length, uint32_t imm_data);
        void Read(const PathDesc &local, const PathDesc &target, uint32_t length);
        void FetchAndAdd(const PathDesc &local, const PathDesc &target, uint64_t add_val);
        void CompareAndSwap(const PathDesc &local, const PathDesc &target, uint64_t compare_val, uint64_t swap_val);
        void Send(const PathDesc &local, uint32_t length);
        void Receive(const PathDesc &local, uint32_t length);

        int Commit();

    private:
        std::atomic<uint64_t> counter;
        std::vector<ibv_send_wr> send_wr;
        std::vector<ibv_sge> send_sge;
        std::vector<ibv_recv_wr> recv_wr;
        std::vector<ibv_sge> recv_sge;
        std::shared_ptr<EndPoint> endpoint;

        void genericNewOp(ibv_send_wr &wr, const PathDesc &local, uint32_t length);
    };
}

#endif //UNIVERSE_RDMASERVICE_H
