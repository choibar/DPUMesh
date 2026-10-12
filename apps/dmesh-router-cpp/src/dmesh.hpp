// Thin C++ view of the DPUMesh DMA datapath.
//
// The C datapath is used exactly as the Rust driver uses it: through the shim
// entry points (linkerd/doca/src/shim.c) plus the control-path state machine in
// comch_server.c. Nothing here reimplements the datapath — this header only
// declares the C surface and adds the tx-staging write cursor, which is the C++
// counterpart of the write side of linkerd/doca/src/io.rs.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

extern "C" {
#include <doca_error.h>

#include "comch_server.h" // dmesh_doca_ctrl_*, enum dmesh_doca_init_state

struct objects;

// shim.c ships without a header; these are its exported entry points, matching
// the `extern "C"` block in linkerd/doca/src/driver.rs one for one.
int32_t dmesh_doca_init(const char *dev_pci_addr, const char *rep_pci_addr, const char *server_name,
                        struct objects **handle);
void dmesh_doca_comch_destroy(struct objects *handle);

int32_t dmesh_doca_data_get_fd(struct objects *objs, int *out_fd);
int32_t dmesh_doca_data_arm(struct objects *objs);
int32_t dmesh_doca_data_clear_and_drain(struct objects *objs, int fd, int budget, int *out_drained);
int32_t dmesh_doca_data_drain_only(struct objects *objs, int budget, int *out_drained);

int32_t dmesh_doca_max_conns(void);
int32_t dmesh_doca_conn_state_get(struct objects *objs, int32_t slot);
int32_t dmesh_doca_conn_mode_get(struct objects *objs, int32_t slot);
int32_t dmesh_doca_conn_flow_get(struct objects *objs, int32_t slot, uint32_t *src_ip,
                                 uint16_t *src_port, uint32_t *dst_ip, uint16_t *dst_port,
                                 char *workload, int32_t workload_len);
int32_t dmesh_doca_conn_staging_base(struct objects *objs, int32_t slot, const uint8_t **out_base,
                                     size_t *out_len);
int32_t dmesh_doca_conn_recv_pop(struct objects *objs, int32_t slot, uint32_t *out_pos,
                                 uint32_t *out_len);
int32_t dmesh_doca_conn_tx_staging(struct objects *objs, int32_t slot, uintptr_t *out_base,
                                   size_t *out_len);
int32_t dmesh_doca_conn_update_dpu_rx_consumed_pos(struct objects *objs, int32_t slot, uint32_t pos);
int32_t dmesh_doca_conn_send_staged(struct objects *objs, int32_t slot, uint32_t pos, uint32_t len);
void dmesh_doca_stats_get(struct objects *objs, int64_t *sent, int64_t *recv, int64_t *recv_bytes,
                          int64_t *dma_pending, int64_t *dma_dropped);
}

namespace dmesh {

// Per-connection state values reported by dmesh_doca_conn_state_get (mirrors
// enum dmesh_conn_state in object.h; kept as plain constants so this header
// does not need object.h).
enum ConnState : int32_t {
    kConnFree = 0,
    kConnNew = 1,
    kConnAwaitMetadata = 2,
    kConnRunning = 3,
    kConnError = 4,
    kConnConsumerStarting = 5,
    kConnClosing = 6,
};

constexpr int32_t kFlowModeClient = 0;
constexpr int32_t kFlowModeBackend = 1;

// Write cursor over a connection's mapped tx_staging region.
//
// Bytes are copied into staging once and then published as DMA descriptors by
// slot; cursors are cumulative so the physical offset is `cursor % len`, and
// unpublished bytes are never overwritten. Identical accounting to
// DmeshIoHandle::{take_staged, advance_publish} on the Rust side.
class TxRing {
  public:
    void bind(uint8_t *base, size_t len) {
        base_ = base;
        len_ = len;
    }
    void kill() {
        base_ = nullptr;
        len_ = 0;
        dead_ = true;
    }
    bool ready() const { return base_ != nullptr && len_ > 0; }
    bool dead() const { return dead_; }

    size_t room() const { return len_ == 0 ? 0 : len_ - static_cast<size_t>(write_ - publish_); }

    // Copy up to `n` bytes into staging, wrapping at the end of the region.
    // Returns the number of bytes accepted (0 when full).
    size_t push(const uint8_t *data, size_t n) {
        if (!ready()) {
            return 0;
        }
        n = n < room() ? n : room();
        if (n == 0) {
            return 0;
        }
        const size_t off = static_cast<size_t>(write_ % len_);
        const size_t first = n < (len_ - off) ? n : (len_ - off);
        std::memcpy(base_ + off, data, first);
        if (first < n) {
            std::memcpy(base_, data + first, n - first);
        }
        write_ += n;
        return n;
    }

    // Next contiguous unpublished run, never crossing the wrap. False if none.
    bool next_run(uint32_t *pos, uint32_t *len) const {
        if (!ready() || write_ == publish_) {
            return false;
        }
        const size_t off = static_cast<size_t>(publish_ % len_);
        const size_t pending = static_cast<size_t>(write_ - publish_);
        const size_t run = pending < (len_ - off) ? pending : (len_ - off);
        *pos = static_cast<uint32_t>(off);
        *len = static_cast<uint32_t>(run);
        return true;
    }

    void advance(uint32_t n) { publish_ += n; }
    bool drained() const { return write_ == publish_; }

  private:
    uint8_t *base_ = nullptr;
    size_t len_ = 0;
    uint64_t write_ = 0;
    uint64_t publish_ = 0;
    bool dead_ = false;
};

} // namespace dmesh
