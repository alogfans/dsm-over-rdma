//
// Created by alogfans on 5/7/18.
//

#include <sys/epoll.h>
#include <fcntl.h>
#include <unistd.h>
#include <infiniband/verbs.h>

#include "RDMA.h"

using namespace rdma;

ibv_context *Device::getContext(const std::string &device_name) {
    ibv_device **device_list = nullptr;
    ibv_context *context = nullptr;
    int num_devices = 0;
    int choose_device = 0;

    device_list = ibv_get_device_list(&num_devices);
    if (!device_list || num_devices <= 0) {
        DEBUG("unable to get device list.");
        goto failure;
    }

    if (!device_name.empty()) {
        for (int i = 0; i < num_devices; ++i) {
            const char * current_device_name = ibv_get_device_name(device_list[i]);
            if (current_device_name && strcmp(current_device_name, device_name.c_str()) == 0) {
                choose_device = i;
                break;
            }
        }
    }

    context = ibv_open_device(device_list[choose_device]);

failure:
    if (device_list) {
        ibv_free_device_list(device_list);
    }

    return context;
}

std::shared_ptr<Device> Device::Open(const std::string &device_name, uint8_t port) {
    ibv_port_attr port_attr = { };
    epoll_event event = { };
    ibv_context *ib_ctx = nullptr;
    ibv_pd *pd = nullptr;
    ibv_comp_channel *channel = nullptr;
    int epoll_fd = -1, flags = 0;

    ib_ctx = getContext(device_name);
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

    return std::make_shared<Device>(ib_ctx, pd, channel, epoll_fd, port);

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

    return nullptr;
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
    std::vector<ibv_wc> replies;
    return Poll(replies, timeout_ms);
}

int Device::Poll(std::vector<ibv_wc> &replies, int timeout_ms) {
    epoll_event event = { };
    int rc = epoll_wait(epoll_fd, &event, 1, timeout_ms);
    if (rc <= 0) {
        return rc;
    }

    if (event.data.fd == channel->fd) {
        ibv_cq *cq = nullptr;
        void *cq_ctx = nullptr;
        ibv_wc wc = { };

        if (ibv_get_cq_event(channel, &cq, &cq_ctx)) {
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

            replies.push_back(wc);

            if (wc.status != IBV_WC_SUCCESS) {
                DEBUG("found a failed working request %s", ibv_wc_status_str(wc.status));
                return -1;
            }
        } while (rc);

        return rc;
    }

    if (event.data.fd == ib_ctx->async_fd) {
        ibv_async_event async_event = { };
        if (ibv_get_async_event(ib_ctx, &async_event)) {
            DEBUG("ibv_get_async_event failed");
            return -1;
        }

        ibv_ack_async_event(&async_event);

        DEBUG("async event: %s", ibv_event_type_str(async_event.event_type));

        return 1;
    }
}

std::shared_ptr<MemoryRegion> Device::Malloc(size_t n, bool zero, uint64_t align) {
    uint8_t *byteBuffer = nullptr;
    posix_memalign((void **) &byteBuffer, align, n);
    if (!byteBuffer) {
        DEBUG("rio_malloc failed.");
        return nullptr;
    }

    if (zero) {
        memset(byteBuffer, 0, n);
    }

    ibv_mr *mr = ibv_reg_mr(pd, byteBuffer, n, FullAccessPermFlags);
    if (!mr) {
        DEBUG("ibv_reg_mr failed.");
        return nullptr;
    }

    return std::make_shared<MemoryRegion>(byteBuffer, n, mr);
}

std::shared_ptr<MemoryRegion> Device::Wrap(uint8_t *buf, size_t n) {
    ibv_mr *mr = ibv_reg_mr(pd, buf, n, FullAccessPermFlags);
    if (!mr) {
        DEBUG("ibv_reg_mr failed.");
        return nullptr;
    }

    return std::make_shared<MemoryRegion>(buf, n, mr, false);
}

uint16_t Device::LID() const {
    ibv_port_attr port_attr = { };
    if (ibv_query_port(ib_ctx, ib_port, &port_attr)) {
        DEBUG("ibv_query_port failed.");
        return 0xffff;
    }
    return port_attr.lid;
}

const std::vector<uint8_t> Device::GID(int gid_index) const {
    ibv_gid gid = { };
    std::vector<uint8_t> vec;
    if (ibv_query_gid(ib_ctx, ib_port, gid_index, &gid)) {
        DEBUG("ibv_query_gid failed.");
        return vec;
    }

    vec.reserve(16);
    vec.assign(gid.raw, gid.raw + 16);

    return vec;
}

std::shared_ptr<EndPoint> Device::CreateEndPoint(ibv_qp_type type, uint32_t max_cqe, uint32_t max_wr, uint32_t max_inline_data) {
    ibv_qp *qp = nullptr;
    ibv_cq *cq = nullptr;
    ibv_qp_init_attr qp_init_attr = { };

    cq = ibv_create_cq(ib_ctx, max_cqe, nullptr, channel, 0);
    if (!cq) {
        DEBUG("ibv_create_cq failed.");
        goto failure;
    }

    if (ibv_req_notify_cq(cq, 0)) {
        DEBUG("ibv_req_notify_cq failed.");
        goto failure;
    }

    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.send_cq = cq;
    qp_init_attr.recv_cq = cq;
    qp_init_attr.sq_sig_all = false;
    qp_init_attr.qp_type = type;
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

    return std::make_shared<EndPoint>(ib_port, cq, qp);

failure:
    if (cq) { free(cq); }
    if (qp) { ibv_destroy_qp(qp); }
    return nullptr;
}

EndPoint::~EndPoint() {
    if (ah) { ibv_destroy_ah(ah); }
    if (cq) { free(cq); }
    if (qp) { ibv_destroy_qp(qp); }
}

int EndPoint::JoinMulticast(const std::vector<uint8_t> &remote_gid, uint16_t remote_lid) {
    if (ibv_attach_mcast(qp, (ibv_gid *) remote_gid.data(), remote_lid)) {
        DEBUG("ibv_attach_mcast failed");
        return -1;
    }

    return 0;
}

int EndPoint::LeaveMulticast(const std::vector<uint8_t> &remote_gid, uint16_t remote_lid) {
    if (ibv_detach_mcast(qp, (ibv_gid *) remote_gid.data(), remote_lid)) {
        DEBUG("ibv_detach_mcast failed");
        return -1;
    }

    return 0;
}

int EndPoint::setInit(ibv_qp *qp, uint8_t ib_port) {
    ibv_qp_attr attr = { };
    int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = ib_port;
    attr.pkey_index = 0;

    if (qp->qp_type == IBV_QPT_RC || qp->qp_type == IBV_QPT_UC) {
        flags |= IBV_QP_ACCESS_FLAGS;
        attr.qp_access_flags = FullAccessPermFlags;
    }

    if (qp->qp_type == IBV_QPT_UD) {
        flags |= IBV_QP_QKEY;
        attr.qkey = MulticastKey;
    }

    return ibv_modify_qp(qp, &attr, flags);
}

int EndPoint::SetRTR(ibv_qp *qp, uint32_t remote_qpn, uint16_t remote_lid, uint8_t ib_port) {
    ibv_qp_attr attr = { };
    int flags = IBV_QP_STATE;
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

int EndPoint::setRTS(ibv_qp *qp) {
    ibv_qp_attr attr = { };
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

int EndPoint::Reset() {
    ibv_qp_attr attr = { };
    int flags = IBV_QP_STATE;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RESET;
    return ibv_modify_qp(qp, &attr, flags);
}

int EndPoint::RegisterRemote(uint16_t remote_lid, uint32_t remote_qpn) {
    if (setInit(qp, ib_port)) {
        DEBUG("set_queue_pair_init failed.");
        return -1;
    }

    if (SetRTR(qp, remote_qpn, remote_lid, ib_port)) {
        DEBUG("set_queue_pair_rtr failed.");
        return -1;
    }

    if (qp->qp_type == IBV_QPT_UD) {
        ibv_ah_attr ah_attr = { };
        memset(&ah_attr, 0, sizeof(ibv_ah_attr));
        ah_attr.dlid = remote_lid;
        ah_attr.port_num = ib_port;
        ah = ibv_create_ah(qp->pd, &ah_attr);
        if (!ah) {
            DEBUG("ibv_create_ah failed.");
            return -1;
        }
    }

    if (setRTS(qp)) {
        DEBUG("set_queue_pair_rts failed.");
        return -1;
    }

    return 0;
}

void WorkBatch::genericNewOp(ibv_send_wr &wr, const WorkBatch::PathDesc &local, uint32_t length) {
    ibv_sge sge = {};
    sge.length = length;
    sge.lkey = local.Key;
    sge.addr = local.Address;
    send_sge.push_back(sge);
    wr.wr_id = counter.fetch_add(1);
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.num_sge = 1;
}

void WorkBatch::Write(const rdma::WorkBatch::PathDesc &local, const rdma::WorkBatch::PathDesc &target, uint32_t length) {
    ibv_send_wr wr = {};
    genericNewOp(wr, local, length);
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.wr.rdma.remote_addr = target.Address;
    wr.wr.rdma.rkey = target.Key;
    send_wr.push_back(wr);
}

void WorkBatch::WriteImm(const rdma::WorkBatch::PathDesc &local, const rdma::WorkBatch::PathDesc &target,
                         uint32_t length, uint32_t imm_data) {
    ibv_send_wr wr = {};
    genericNewOp(wr, local, length);
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.wr.rdma.remote_addr = target.Address;
    wr.wr.rdma.rkey = target.Key;
    wr.imm_data = imm_data;
    send_wr.push_back(wr);
}

void WorkBatch::Read(const rdma::WorkBatch::PathDesc &local, const rdma::WorkBatch::PathDesc &target, uint32_t length) {
    ibv_send_wr wr = {};
    genericNewOp(wr, local, length);
    wr.opcode = IBV_WR_RDMA_READ;
    wr.wr.rdma.remote_addr = target.Address;
    wr.wr.rdma.rkey = target.Key;
    send_wr.push_back(wr);
}

void WorkBatch::FetchAndAdd(const rdma::WorkBatch::PathDesc &local, const WorkBatch::PathDesc &target, uint64_t add_val) {
    ibv_send_wr wr = {};
    genericNewOp(wr, local, sizeof(uint64_t));
    wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.wr.atomic.remote_addr = target.Address;
    wr.wr.atomic.rkey = target.Key;
    wr.wr.atomic.compare_add = add_val;
    send_wr.push_back(wr);
}

void WorkBatch::CompareAndSwap(const WorkBatch::PathDesc &local, const WorkBatch::PathDesc &target,
                               uint64_t compare_val, uint64_t swap_val) {
    ibv_send_wr wr = {};
    genericNewOp(wr, local, sizeof(uint64_t));
    wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.wr.atomic.remote_addr = target.Address;
    wr.wr.atomic.rkey = target.Key;
    wr.wr.atomic.compare_add = compare_val;
    wr.wr.atomic.swap = swap_val;
    send_wr.push_back(wr);
}

void WorkBatch::Send(const rdma::WorkBatch::PathDesc &local, uint32_t length) {
    ibv_send_wr wr = {};
    genericNewOp(wr, local, length);
    wr.opcode = IBV_WR_SEND;

    if (endpoint->UnsafeQP()->qp_type == IBV_QPT_UD) {
        wr.wr.ud.ah = endpoint->UnsafeAH();
        wr.wr.ud.remote_qkey = MulticastKey;
        wr.wr.ud.remote_qpn = MultiCastQPN;
    }

    send_wr.push_back(wr);
}

void WorkBatch::Receive(const rdma::WorkBatch::PathDesc &local, uint32_t length) {
    ibv_recv_wr wr = {};
    ibv_sge sge = {};
    sge.length = length;
    sge.lkey = local.Key;
    sge.addr = local.Address;
    recv_sge.push_back(sge);

    wr.wr_id = counter.fetch_add(1);
    wr.num_sge = 1;
    recv_wr.push_back(wr);
}

int WorkBatch::Commit() {
    int rc = 0;
    if (!send_wr.empty()) {
        int sge_index = 0;
        for (int i = 0; i < send_wr.size(); i++) {
            send_wr[i].sg_list = &send_sge[sge_index];
            sge_index += send_wr[i].num_sge;
            send_wr[i].next = (i + 1 == send_wr.size()) ? nullptr : &send_wr[i + 1];
        }

        ibv_send_wr *wr = send_wr.data(), *bad_wr = nullptr;
        rc = ibv_post_send(endpoint->UnsafeQP(), wr, &bad_wr);
        send_wr.clear();
        send_sge.clear();

        if (rc) {
            DEBUG("ibv_post_send error. first bad_wr %lud", bad_wr ? bad_wr->wr_id : 0xffffffff);
            return rc;
        }
    }

    if (!recv_wr.empty()) {
        int sge_index = 0;
        for (int i = 0; i < recv_wr.size(); i++) {
            recv_wr[i].sg_list = &recv_sge[sge_index];
            sge_index += recv_wr[i].num_sge;
            recv_wr[i].next = (i + 1 == recv_wr.size()) ? nullptr : &recv_wr[i + 1];
        }

        ibv_recv_wr *wr = recv_wr.data(), *bad_wr = nullptr;
        rc = ibv_post_recv(endpoint->UnsafeQP(), wr, &bad_wr);
        recv_wr.clear();
        recv_sge.clear();

        if (rc) {
            DEBUG("ibv_post_recv error. first bad_wr %lud", bad_wr ? bad_wr->wr_id : 0xffffffff);
            return rc;
        }
    }

    return 0;
}
