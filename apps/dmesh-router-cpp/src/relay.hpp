// Relay modes for the selective-decoding experiment.
//
//   l4    — bytes moved verbatim between a client channel and a backend channel;
//           no HTTP/2 parsing at all (the "speed of light" bound).
//   relay — the proxy LOGICALLY TERMINATES both HTTP/2 connections (own SETTINGS
//           and ACKs on each leg, PING answered locally, GOAWAY/RST translated,
//           per-leg flow control with WINDOW_UPDATE generation, per-stream state
//           and stream-id mapping) but never decodes->re-encodes header blocks:
//           every HEADERS/CONTINUATION block is forwarded byte-for-byte, and the
//           request direction is walked with the connection-scoped HPACK mirror
//           (hpack_walk.h) to materialize only the policy fields. Because the
//           mirror must equal the client encoder's table, a client connection is
//           pinned 1:1 to a backend connection for its whole life and a backend
//           connection is never re-paired.
//
// Proxy-generated header blocks (403 on deny) use static indices and
// literal-without-indexing only, so they decode under any dynamic-table state.

#pragma once

#include <cstdint>
#include <deque>
#include <string>
#include <unordered_map>
#include <vector>

#include "dmesh.hpp"

#define HW_VAL_CAP 256
#define HW_TBL_CAP 128
extern "C" {
#include "hpack_walk.h"
}

namespace dmesh {

struct RelayPolicy {
    std::vector<std::string> path_prefixes; // ":path" must start with one; empty ⇒ any "/…"
    std::string authority;                  // if non-empty, :authority must equal this
    bool match(const char *path, int plen, const char *auth, int alen) const;
};

struct RelayStats {
    uint64_t c2b_bytes = 0, b2c_bytes = 0;
    uint64_t frames_in = 0, data_frames = 0, header_blocks = 0, continuation = 0;
    uint64_t walk_fail = 0, allow = 0, deny = 0, no_path = 0;
    uint64_t streams_opened = 0, streams_closed = 0;
    uint64_t settings_in = 0, pings = 0, goaway = 0, rst_in = 0, rst_out = 0;
    uint64_t window_updates_out = 0, window_held = 0, reframed = 0;
    uint64_t proto_errors = 0, stalls = 0;
    void add(const RelayStats &o);
    std::string line() const;
};

// One DMA connection in l4/relay mode: rx segments in, tx staging out. In l4
// mode the bytes are shuttled directly to the peer; in relay mode the paired
// H2Relay engine owns both channels and drives them.
class RelayChannel {
  public:
    RelayChannel(struct objects *objs, int slot, bool is_backend, uint64_t key);
    void wire();
    bool rx_ready() const { return rx_base_ != nullptr; }
    // Pop the next received segment (consumed at pop time: the DPA may reuse it
    // once the consumed position is published in pump_send). False when empty.
    bool pop(const uint8_t **seg, uint32_t *len);
    // Publish staged bytes as DMA descriptors and the DPU RX consumed position to the DPA.
    void pump_send();

    // l4 byte relay
    size_t l4_pump_recv();

    void pair(RelayChannel *peer) { peer_ = peer; peer->peer_ = this; used_ = peer->used_ = true; }
    void unpair() {
        if (peer_ != nullptr) { peer_->peer_ = nullptr; }
        peer_ = nullptr;
    }
    RelayChannel *peer() const { return peer_; }
    bool paired() const { return peer_ != nullptr; }
    bool used() const { return used_; }
    bool is_backend() const { return is_backend_; }
    uint64_t key() const { return key_; }
    int slot() const { return slot_; }
    TxRing &tx() { return tx_; }
    RelayStats &stats() { return stats_; }

  private:
    struct objects *objs_;
    int slot_;
    bool is_backend_;
    uint64_t key_;
    RelayChannel *peer_ = nullptr;
    bool used_ = false;
    const uint8_t *rx_base_ = nullptr;
    size_t rx_len_ = 0;
    TxRing tx_;
    std::vector<uint8_t> pending_; // l4: bytes the peer staging could not take yet
    size_t pending_off_ = 0;
    uint32_t dpu_rx_consumed_pos_ = 0;
    bool dpu_rx_consumed_pos_changed_ = false;
    RelayStats stats_;
};

// One HTTP/2 connection owned by the proxy (server role on the client leg,
// client role on the backend leg).
struct Leg {
    RelayChannel *ch = nullptr;
    bool is_client_leg = false;
    // What the peer advertised.
    uint32_t peer_max_frame = 16384;
    uint32_t peer_init_window = 65535;
    uint32_t peer_table_size = 4096;
    bool peer_settings_seen = false;
    // Our connection-level send window toward the peer / receive accounting.
    int64_t send_window = 65535;
    int64_t recv_unacked = 0;
    // Bytes we could not place in staging yet (FIFO, already framed).
    std::vector<uint8_t> outq;
    size_t outq_off = 0;
    // Frames received from the peer that could not be forwarded because the
    // OTHER leg's window is exhausted (kept in order; head-of-line by design).
    std::deque<std::vector<uint8_t>> held;
    // Parser state.
    size_t preface_left = 0; // 24 on the client leg
    std::vector<uint8_t> acc; // partial frame accumulation (header + payload)
    bool goaway_sent = false;
};

struct StreamState {
    int32_t c_sid = 0, b_sid = 0;
    bool denied = false;
    bool req_done = false, res_done = false;
    int64_t c_send_win = 65535; // our send window toward the client (responses)
    int64_t b_send_win = 65535; // our send window toward the backend (requests)
    int64_t c_recv_unacked = 0, b_recv_unacked = 0;
};

class H2Relay {
  public:
    H2Relay(RelayChannel *client, RelayChannel *backend, const RelayPolicy *policy);
    // Drive both legs once: drain rx, forward, flush.
    void pump();
    RelayStats &stats() { return stats_; }

  private:
    // ---- byte intake --------------------------------------------------------
    void intake(Leg &from);
    // Process a complete frame that arrived on `from`. Returns false if the
    // frame must be held (window) — the caller keeps it and stops intake.
    bool on_frame(Leg &from, const uint8_t *hdr, const uint8_t *payload, uint32_t plen);
    bool replay_held(Leg &from);
    // ---- frame handlers -------------------------------------------------------
    bool on_data(Leg &from, uint32_t sid, uint8_t flags, const uint8_t *p, uint32_t n);
    void on_headers(Leg &from, uint32_t sid, uint8_t type, uint8_t flags, const uint8_t *hdr,
                    const uint8_t *p, uint32_t n);
    void on_settings(Leg &from, uint8_t flags, const uint8_t *p, uint32_t n);
    void on_window_update(Leg &from, uint32_t sid, const uint8_t *p, uint32_t n);
    void on_rst(Leg &from, uint32_t sid, const uint8_t *p, uint32_t n);
    void on_ping(Leg &from, uint8_t flags, const uint8_t *p, uint32_t n);
    void on_goaway(Leg &from, const uint8_t *p, uint32_t n);
    // ---- emission -------------------------------------------------------------
    void emit(Leg &to, uint8_t type, uint8_t flags, uint32_t sid, const uint8_t *payload,
              uint32_t plen);
    void emit_raw(Leg &to, const uint8_t *bytes, size_t n);
    void send_settings(Leg &to);
    void send_window_update(Leg &to, uint32_t sid, uint32_t inc);
    void send_rst(Leg &to, uint32_t sid, uint32_t code);
    void send_goaway(Leg &to, uint32_t last_sid, uint32_t code);
    void send_403(Leg &to, uint32_t sid);
    void flush(Leg &to);
    void protocol_error(Leg &from, const char *why);
    // ---- helpers --------------------------------------------------------------
    Leg &other(Leg &l) { return &l == &client_ ? backend_ : client_; }
    StreamState *stream_by_client(int32_t c_sid);
    StreamState *stream_by_backend(int32_t b_sid);
    void close_stream(StreamState *st);
    void policy_check(StreamState *st);

    Leg client_, backend_;
    const RelayPolicy *policy_;
    RelayStats stats_;
    struct hw_state hw_;
    struct hw_out out_;
    bool hw_lost_ = false;
    std::unordered_map<int32_t, StreamState> streams_; // by client sid
    std::unordered_map<int32_t, int32_t> b2c_;        // backend sid -> client sid
    int32_t next_b_sid_ = 1;
    int32_t last_c_sid_ = 0;
    // header block in flight (HEADERS..CONTINUATION) on the client leg
    std::vector<uint8_t> block_;
    bool in_block_ = false;
    int32_t block_sid_ = 0;
    // header block in flight on the backend leg (only END_HEADERS bookkeeping)
    bool b_in_block_ = false;
    bool dead_ = false;

    static constexpr uint32_t kOurMaxFrame = 16384;
    static constexpr uint32_t kOurInitWindow = 1u << 20;
    static constexpr uint32_t kOurConnWindow = 4u << 20;
    static constexpr uint32_t kOurTableSize = 4096;
    static constexpr uint32_t kOurMaxStreams = 1000;
};

} // namespace dmesh
