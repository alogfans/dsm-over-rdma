//
// Created by alogfans on 5/7/18.
//

#include <sys/epoll.h>
#include <fcntl.h>
#include <unistd.h>
#include <infiniband/verbs.h>

#include "RdmaService.h"

using namespace rdma;

struct ibv_device* search_device(const std::string &device_name, struct ibv_device **device_list, int num_devices) {
    if (!device_name.empty()) {
        int i;
        for (i = 0; i < num_devices; ++i) {
            const char * current_device_name = ibv_get_device_name(device_list[i]);
            if (current_device_name && strcmp(current_device_name, device_name.c_str()) == 0) {
                return device_list[i];
            }
        }
    } else if (num_devices > 0) {
        return device_list[0];
    }

    return NULL;
}

struct ibv_context *get_context(const std::string &device_name) {
    struct ibv_device **device_list = NULL;
    struct ibv_device *device = NULL;
    struct ibv_context *context = NULL;
    int num_devices = 0;

    device_list = ibv_get_device_list(&num_devices);
    if (!device_list || num_devices <= 0) {
        DEBUG("unable to get device list.");
        goto failure;
    }

    device = search_device(device_name, device_list, num_devices);
    if (!device) {
        DEBUG("device not found.");
        goto failure;
    }

    context = ibv_open_device(device);
    return context;

failure:
    if (device_list) {
        ibv_free_device_list(device_list);
    }

    return NULL;
}

std::shared_ptr<Device> Device::Open(const std::string &device_name, uint8_t port) {
    struct ibv_port_attr port_attr;
    struct epoll_event event;

    struct ibv_context *ib_ctx = NULL;
    struct ibv_pd *pd = NULL;
    struct ibv_comp_channel *channel = NULL;
    struct rio_ctx_t *ctx = NULL;
    int epoll_fd = -1;
    int flags;

    ib_ctx = get_context(device_name);
    if (!ib_ctx) {
        errno = EINVAL;
        goto failure;
    }

    flags = fcntl(ib_ctx->async_fd, F_GETFL);
    if (fcntl(ib_ctx->async_fd, F_SETFL, flags | O_NONBLOCK)) {
        DEBUG("change blocking mode failed.");
        goto failure;
    }

    if (ibv_query_port(ib_ctx, port, &port_attr)) {
        DEBUG("ibv_query_port failed.");
        goto failure;
    }


    pd = ibv_alloc_pd(ib_ctx);
    if (!pd) {
        DEBUG("ibv_alloc_pd failed.");
        goto failure;
    }

    channel = ibv_create_comp_channel(ib_ctx);
    if (!channel) {
        DEBUG("ibv_create_comp_channel failed.");
        goto failure;
    }

    flags = fcntl(channel->fd, F_GETFL);
    if (fcntl(channel->fd, F_SETFL, flags | O_NONBLOCK)) {
        DEBUG("change blocking mode failed.");
        goto failure;
    }

    epoll_fd = epoll_create(1);
    if (epoll_fd < 0) {
        DEBUG("epoll_create failed.");
        goto failure;
    }

    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN;
    event.data.fd = ib_ctx->async_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, ib_ctx->async_fd, &event)) {
        DEBUG("epoll_create failed.");
        goto failure;
    }

    event.data.fd = channel->fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, channel->fd, &event)) {
        DEBUG("epoll_create failed.");
        goto failure;
    }

    return std::make_shared<Device>(ib_ctx, pd, channel, port, epoll_fd);

failure:
    if (epoll_fd >= 0) {
        close(epoll_fd);
    }

    if (channel) {
        ibv_destroy_comp_channel(channel);
    }

    if (pd) {
        ibv_dealloc_pd(pd);
    }

    if (ib_ctx) {
        ibv_close_device(ib_ctx);
    }

    return NULL;
}

Device::~Device() {
    if (epoll_fd >= 0) {
        close(epoll_fd);
    }

    if (channel) {
        ibv_destroy_comp_channel(channel);
    }

    if (pd) {
        ibv_dealloc_pd(pd);
    }

    if (ib_ctx) {
        ibv_close_device(ib_ctx);
    }
}

int Device::Poll(int timeout_ms) {
    struct epoll_event event;
    int rc = epoll_wait(epoll_fd, &event, 1, timeout_ms);
    if (rc <= 0) {
        return rc;
    }

    if (event.data.fd == channel->fd) {
        struct ibv_cq *cq;
        struct rio_ctx_t *cq_ctx;
        struct ibv_wc wc;

        if (ibv_get_cq_event(channel, &cq, (void **) &cq_ctx)) {
            DEBUG("ibv_get_cq_event failed");
            return -1;
        }

        ibv_ack_cq_events(cq, 1);

        if (ibv_req_notify_cq(cq, 0)) {
            DEBUG("ibv_req_notify_cq failed.");
            return -1;
        }

        do {
            rc = ibv_poll_cq(cq, 1, &wc);
            if (rc < 0) {
                DEBUG("ibv_poll_cq failed.");
                return -1;
            } else if (rc == 0) {
                continue;
            }

            if (wc.status != IBV_WC_SUCCESS) {
                DEBUG("found a failed working request %s", ibv_wc_status_str(wc.status));
                return -1;
            }
        } while (rc);

        return rc;
    }

    if (event.data.fd == ib_ctx->async_fd) {
        struct ibv_async_event event;

        if (ibv_get_async_event(ib_ctx, &event)) {
            DEBUG("ibv_get_async_event failed");
            return -1;
        }

        ibv_ack_async_event(&event);

        DEBUG("event: %s", ibv_event_type_str(event.event_type));
        return 1;
    }
}

uint16_t Device::LID() {
    struct ibv_port_attr port_attr;
    if (ibv_query_port(ib_ctx, ib_port, &port_attr)) {
        DEBUG("ibv_query_port failed.");
        return 0xffff;
    }
    return port_attr.lid;
}

std::shared_ptr<EndPoint> Device::CreateEndPoint(EndPointType type,
                                                 uint32_t max_cqe,
                                                 uint32_t max_wr,
                                                 uint32_t max_inline_data) {
    struct ibv_qp *qp = NULL;
    struct ibv_cq *cq = NULL;

    cq = ibv_create_cq(ib_ctx, max_cqe, NULL, channel, 0);
    if (ibv_req_notify_cq(cq, 0)) {
        DEBUG("ibv_req_notify_cq failed.");
        goto failure;
    }

    if (!cq) {
        DEBUG("ibv_create_cq failed.");
        goto failure;
    }

    struct ibv_qp_init_attr qp_init_attr;
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.send_cq = cq;
    qp_init_attr.recv_cq = cq;
    qp_init_attr.sq_sig_all = false;
    qp_init_attr.qp_type = (enum ibv_qp_type) type;
    qp_init_attr.cap.max_send_wr = max_wr;
    qp_init_attr.cap.max_recv_wr = max_wr;
    qp_init_attr.cap.max_send_sge = max_wr;
    qp_init_attr.cap.max_recv_sge = max_wr;
    qp_init_attr.cap.max_inline_data = max_inline_data;

    qp = ibv_create_qp(pd, &qp_init_attr);
    if (!qp) {
        DEBUG("ibv_create_qp failed.");
        goto failure;
    }

    return std::make_shared<EndPoint>(type, cq, qp);

failure:
    if (cq) { free(cq); }
    if (qp) { ibv_destroy_qp(qp); }
    return NULL;
}

EndPoint::~EndPoint() {
    if (ah) { ibv_destroy_ah(ah); }
    if (cq) { free(cq); }
    if (qp) { ibv_destroy_qp(qp); }
}


int set_queue_pair_init(struct ibv_qp *qp, uint8_t ib_port) {
    struct ibv_qp_attr attr;
    int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = ib_port;
    attr.pkey_index = 0;

    if (qp->qp_type == IBV_QPT_RC || qp->qp_type == IBV_QPT_UC) {
        flags |= IBV_QP_ACCESS_FLAGS;
        attr.qp_access_flags = RIO_FULL_ACCESS_PERM;
    }

    if (qp->qp_type == IBV_QPT_UD) {
        flags |= IBV_QP_QKEY;
        attr.qkey = RIO_DEFAULT_QKEY;
    }

    return ibv_modify_qp(qp, &attr, flags);
}

int set_queue_pair_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t remote_lid, uint8_t ib_port) {
    struct ibv_qp_attr attr;
    int flags = IBV_QP_STATE ;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;

    if (qp->qp_type == IBV_QPT_RC || qp->qp_type == IBV_QPT_UC) {
        flags |= IBV_QP_PATH_MTU | IBV_QP_AV | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;
        attr.path_mtu = IBV_MTU_512;
        attr.ah_attr.is_global = 0;
        attr.ah_attr.dlid = remote_lid;
        attr.ah_attr.sl = 0;
        attr.ah_attr.src_path_bits = 0;
        attr.ah_attr.port_num = ib_port;

        attr.dest_qp_num = remote_qpn;
        attr.rq_psn = 0;
    }

    if (qp->qp_type == IBV_QPT_RC) {
        flags |= IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
        attr.max_dest_rd_atomic = 1;
        attr.min_rnr_timer = 0x12;
    }

    return ibv_modify_qp(qp, &attr, flags);
}

int set_queue_pair_rts(struct ibv_qp *qp) {
    struct ibv_qp_attr attr;
    int flags = IBV_QP_STATE | IBV_QP_SQ_PSN;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = 0;

    if (qp->qp_type == IBV_QPT_RC) {
        flags |= IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;
        attr.timeout = 0x12;
        attr.retry_cnt = 7;
        attr.rnr_retry = 7;
        attr.max_rd_atomic = 1;
    }

    return ibv_modify_qp(qp, &attr, flags);
}

int EndPoint::ReadyToSend(std::shared_ptr<Device> device, uint16_t remote_lid, uint32_t remote_qpn){
    if (set_queue_pair_init(qp, device->Port())) {
        DEBUG("set_queue_pair_init failed.");
        return -1;
    }

    if (set_queue_pair_rtr(qp, remote_qpn, remote_lid, device->Port())) {
        DEBUG("set_queue_pair_rtr failed.");
        return -1;
    }

    if (qp->qp_type == IBV_QPT_UD) {
        struct ibv_ah_attr ah_attr;
        memset(&ah_attr, 0, sizeof(struct ibv_ah_attr));
        ah_attr.dlid = remote_lid;
        ah_attr.port_num = device->Port();
        ah = ibv_create_ah(device->UnsafePD(), &ah_attr);
        if (!ah) {
            DEBUG("ibv_create_ah failed.");
            return -1;
        }
    }

    if (set_queue_pair_rts(qp)) {
        DEBUG("set_queue_pair_rts failed.");
        return -1;
    }

    return 0;
}

MemoryRegion::~MemoryRegion() {
    if (mr) { ibv_dereg_mr(mr); }
    if (addr) { free(addr); }
}

std::shared_ptr<MemoryRegion> MemoryRegion::Malloc(std::shared_ptr<rdma::Device> device, size_t n, bool zero, uint64_t align) {
    void *addr = NULL;
    posix_memalign(&addr, align, n);
    if (!addr) {
        DEBUG("rio_malloc failed.");
        return NULL;
    }

    if (zero) {
        memset(addr, 0, n);
    }

    struct ibv_mr *mr = ibv_reg_mr(device->UnsafePD(), addr, n, RIO_FULL_ACCESS_PERM);
    if (!mr) {
        DEBUG("ibv_reg_mr failed.");
        return NULL;
    }

    return std::make_shared<MemoryRegion>(addr, n, mr);
}

int WorkRequest::Write(std::shared_ptr<rdma::MemoryRegion> memory, uint64_t offset, uint32_t length,
                       uint64_t remote_address, uint32_t remote_key) {

    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));
    memset(&sge, 0, sizeof(sge));

    sge.addr = memory->VirtualAddress() + offset;
    sge.lkey = memory->Key();
    sge.length = length;

    wr.wr_id = counter.fetch_add(1);
    wr.send_flags = IBV_SEND_SIGNALED;

    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.wr.rdma.remote_addr = remote_address;
    wr.wr.rdma.rkey = remote_key;

    wr.num_sge = 1;
    wr.sg_list = &sge;

    int rc = ibv_post_send(endpoint->UnsafeQP(), &wr, &bad_wr);
    if (rc) {
        DEBUG("ibv_post_send error. first bad_wr %lud", bad_wr ? bad_wr->wr_id : 0xffffffff);
    }

    return rc;
}

int WorkRequest::WriteImm(std::shared_ptr<rdma::MemoryRegion> memory, uint64_t offset, uint32_t length,
                          uint32_t imm_data, uint64_t remote_address, uint32_t remote_key) {
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));
    memset(&sge, 0, sizeof(sge));

    sge.addr = memory->VirtualAddress() + offset;
    sge.lkey = memory->Key();
    sge.length = length;

    wr.wr_id = counter.fetch_add(1);
    wr.send_flags = IBV_SEND_SIGNALED;

    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.wr.rdma.remote_addr = remote_address;
    wr.wr.rdma.rkey = remote_key;
    wr.imm_data = imm_data;

    wr.num_sge = 1;
    wr.sg_list = &sge;

    int rc = ibv_post_send(endpoint->UnsafeQP(), &wr, &bad_wr);
    if (rc) {
        DEBUG("ibv_post_send error. first bad_wr %lud", bad_wr ? bad_wr->wr_id : 0xffffffff);
    }

    return rc;
}

int WorkRequest::Read(std::shared_ptr<rdma::MemoryRegion> memory, uint64_t offset, uint32_t length,
                      uint64_t remote_address, uint32_t remote_key) {
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));
    memset(&sge, 0, sizeof(sge));

    sge.addr = memory->VirtualAddress() + offset;
    sge.lkey = memory->Key();
    sge.length = length;

    wr.wr_id = counter.fetch_add(1);
    wr.send_flags = IBV_SEND_SIGNALED;

    wr.opcode = IBV_WR_RDMA_READ;
    wr.wr.rdma.remote_addr = remote_address;
    wr.wr.rdma.rkey = remote_key;

    wr.num_sge = 1;
    wr.sg_list = &sge;

    int rc = ibv_post_send(endpoint->UnsafeQP(), &wr, &bad_wr);
    if (rc) {
        DEBUG("ibv_post_send error. first bad_wr %lud", bad_wr ? bad_wr->wr_id : 0xffffffff);
    }

    return rc;
}

int WorkRequest::FetchAndAdd(std::shared_ptr<rdma::MemoryRegion> memory, uint64_t offset, uint64_t add_val,
                             uint64_t remote_address, uint32_t remote_key) {
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));
    memset(&sge, 0, sizeof(sge));

    sge.addr = memory->VirtualAddress() + offset;
    sge.lkey = memory->Key();
    sge.length = sizeof(uint64_t);

    wr.wr_id = counter.fetch_add(1);
    wr.send_flags = IBV_SEND_SIGNALED;

    wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.wr.atomic.remote_addr = remote_address;
    wr.wr.atomic.rkey = remote_key;
    wr.wr.atomic.compare_add = add_val;

    wr.num_sge = 1;
    wr.sg_list = &sge;

    int rc = ibv_post_send(endpoint->UnsafeQP(), &wr, &bad_wr);
    if (rc) {
        DEBUG("ibv_post_send error. first bad_wr %lud", bad_wr ? bad_wr->wr_id : 0xffffffff);
    }

    return rc;
}

int WorkRequest::CompareAndSwap(std::shared_ptr<rdma::MemoryRegion> memory, uint64_t offset, uint64_t compare_val,
                                uint64_t swap_value, uint64_t remote_address, uint32_t remote_key) {
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));
    memset(&sge, 0, sizeof(sge));

    sge.addr = memory->VirtualAddress() + offset;
    sge.lkey = memory->Key();
    sge.length = sizeof(uint64_t);

    wr.wr_id = counter.fetch_add(1);
    wr.send_flags = IBV_SEND_SIGNALED;

    wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.wr.atomic.remote_addr = remote_address;
    wr.wr.atomic.rkey = remote_key;
    wr.wr.atomic.compare_add = compare_val;
    wr.wr.atomic.swap = swap_value;

    wr.num_sge = 1;
    wr.sg_list = &sge;

    int rc = ibv_post_send(endpoint->UnsafeQP(), &wr, &bad_wr);
    if (rc) {
        DEBUG("ibv_post_send error. first bad_wr %lud", bad_wr ? bad_wr->wr_id : 0xffffffff);
    }

    return rc;
}

int WorkRequest::Send(std::shared_ptr<rdma::MemoryRegion> memory, uint64_t offset, uint32_t length) {
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));
    memset(&sge, 0, sizeof(sge));

    sge.addr = memory->VirtualAddress() + offset;
    sge.lkey = memory->Key();
    sge.length = length;

    wr.wr_id = counter.fetch_add(1);
    wr.send_flags = IBV_SEND_SIGNALED;

    wr.opcode = IBV_WR_SEND;
    if (endpoint->UnsafeQP()->qp_type == IBV_QPT_UD) {
        wr.wr.ud.ah = endpoint->UnsafeAH();
        wr.wr.ud.remote_qkey = RIO_DEFAULT_QKEY;
        wr.wr.ud.remote_qpn = endpoint->QPN();
    }

    wr.num_sge = 1;
    wr.sg_list = &sge;

    int rc = ibv_post_send(endpoint->UnsafeQP(), &wr, &bad_wr);
    if (rc) {
        DEBUG("ibv_post_send error. first bad_wr %lud", bad_wr ? bad_wr->wr_id : 0xffffffff);
    }

    return rc;
}

int WorkRequest::Receive(std::shared_ptr<rdma::MemoryRegion> memory, uint64_t offset, uint32_t length) {
    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));
    memset(&sge, 0, sizeof(sge));

    sge.addr = memory->VirtualAddress() + offset;
    sge.lkey = memory->Key();
    sge.length = length;

    wr.wr_id = counter.fetch_add(1);
    wr.num_sge = 1;
    wr.sg_list = &sge;

    int rc = ibv_post_recv(endpoint->UnsafeQP(), &wr, &bad_wr);
    if (rc) {
        DEBUG("ibv_post_send error. first bad_wr %lud", bad_wr ? bad_wr->wr_id : 0xffffffff);
    }

    return rc;
}
