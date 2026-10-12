#include "relay.hpp"

#include <cstdio>
#include <cstring>

#include "router.hpp"

namespace dmesh {

// ---------------------------------------------------------------------------
// frame constants
// ---------------------------------------------------------------------------
static constexpr uint8_t kData = 0x0, kHeaders = 0x1, kPriority = 0x2, kRstStream = 0x3,
                         kSettings = 0x4, kPushPromise = 0x5, kPing = 0x6, kGoaway = 0x7,
                         kWindowUpdate = 0x8, kContinuation = 0x9;
static constexpr uint8_t kFlagEndStream = 0x1, kFlagAck = 0x1, kFlagEndHeaders = 0x4,
                         kFlagPadded = 0x8, kFlagPriority = 0x20;
static constexpr uint16_t kSetTableSize = 1, kSetMaxStreams = 3, kSetInitWindow = 4,
                          kSetMaxFrame = 5;
static constexpr uint32_t kErrProtocol = 0x1, kErrFlowControl = 0x3, kErrCancel = 0x8;
static const uint8_t kPreface[24] = {'P', 'R', 'I', ' ', '*', ' ', 'H', 'T', 'T', 'P', '/', '2',
                                     '.', '0', '\r', '\n', '\r', '\n', 'S', 'M', '\r', '\n', '\r', '\n'};

static inline uint32_t rd24(const uint8_t *p) {
    return (static_cast<uint32_t>(p[0]) << 16) | (static_cast<uint32_t>(p[1]) << 8) | p[2];
}
static inline uint32_t rd32(const uint8_t *p) {
    return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
           (static_cast<uint32_t>(p[2]) << 8) | p[3];
}
static inline void wr32(uint8_t *p, uint32_t v) {
    p[0] = static_cast<uint8_t>(v >> 24);
    p[1] = static_cast<uint8_t>(v >> 16);
    p[2] = static_cast<uint8_t>(v >> 8);
    p[3] = static_cast<uint8_t>(v);
}
static inline void wr_hdr(uint8_t *h, uint32_t len, uint8_t type, uint8_t flags, uint32_t sid) {
    h[0] = static_cast<uint8_t>(len >> 16);
    h[1] = static_cast<uint8_t>(len >> 8);
    h[2] = static_cast<uint8_t>(len);
    h[3] = type;
    h[4] = flags;
    wr32(h + 5, sid & 0x7fffffffu);
}

// ---------------------------------------------------------------------------
// policy / stats
// ---------------------------------------------------------------------------

bool RelayPolicy::match(const char *path, int plen, const char *auth, int alen) const {
    if (!authority.empty() &&
        (static_cast<size_t>(alen) != authority.size() ||
         std::memcmp(auth, authority.data(), static_cast<size_t>(alen)) != 0)) {
        return false;
    }
    if (path_prefixes.empty()) {
        return plen >= 1 && path[0] == '/';
    }
    for (const std::string &pre : path_prefixes) {
        if (static_cast<size_t>(plen) >= pre.size() &&
            std::memcmp(path, pre.data(), pre.size()) == 0) {
            return true;
        }
    }
    return false;
}

void RelayStats::add(const RelayStats &o) {
    c2b_bytes += o.c2b_bytes; b2c_bytes += o.b2c_bytes; frames_in += o.frames_in;
    data_frames += o.data_frames; header_blocks += o.header_blocks; continuation += o.continuation;
    walk_fail += o.walk_fail; allow += o.allow; deny += o.deny; no_path += o.no_path;
    streams_opened += o.streams_opened; streams_closed += o.streams_closed;
    settings_in += o.settings_in; pings += o.pings; goaway += o.goaway; rst_in += o.rst_in;
    rst_out += o.rst_out; window_updates_out += o.window_updates_out; window_held += o.window_held;
    reframed += o.reframed; proto_errors += o.proto_errors; stalls += o.stalls;
}

std::string RelayStats::line() const {
    char buf[512];
    std::snprintf(buf, sizeof(buf),
                  "relay stats c2b_bytes=%llu b2c_bytes=%llu frames=%llu data=%llu header_blocks=%llu "
                  "continuation=%llu walk_fail=%llu allow=%llu deny=%llu no_path=%llu streams=%llu/%llu "
                  "settings=%llu pings=%llu goaway=%llu rst_in=%llu rst_out=%llu wu_out=%llu held=%llu "
                  "reframed=%llu proto_err=%llu stalls=%llu",
                  (unsigned long long)c2b_bytes, (unsigned long long)b2c_bytes,
                  (unsigned long long)frames_in, (unsigned long long)data_frames,
                  (unsigned long long)header_blocks, (unsigned long long)continuation,
                  (unsigned long long)walk_fail, (unsigned long long)allow, (unsigned long long)deny,
                  (unsigned long long)no_path, (unsigned long long)streams_opened,
                  (unsigned long long)streams_closed, (unsigned long long)settings_in,
                  (unsigned long long)pings, (unsigned long long)goaway, (unsigned long long)rst_in,
                  (unsigned long long)rst_out, (unsigned long long)window_updates_out,
                  (unsigned long long)window_held, (unsigned long long)reframed,
                  (unsigned long long)proto_errors, (unsigned long long)stalls);
    return buf;
}

// ---------------------------------------------------------------------------
// RelayChannel
// ---------------------------------------------------------------------------

RelayChannel::RelayChannel(struct objects *objs, int slot, bool is_backend, uint64_t key)
    : objs_(objs), slot_(slot), is_backend_(is_backend), key_(key) {}

void RelayChannel::wire() {
    if (rx_base_ == nullptr) {
        const uint8_t *base = nullptr;
        size_t len = 0;
        if (dmesh_doca_conn_staging_base(objs_, slot_, &base, &len) == DOCA_SUCCESS && base != nullptr) {
            rx_base_ = base;
            rx_len_ = len;
        }
    }
    if (!tx_.ready() && !tx_.dead()) {
        uintptr_t base = 0;
        size_t len = 0;
        if (dmesh_doca_conn_tx_staging(objs_, slot_, &base, &len) == 0 && base != 0 && len > 0) {
            tx_.bind(reinterpret_cast<uint8_t *>(base), len);
        }
    }
}

bool RelayChannel::pop(const uint8_t **seg, uint32_t *len) {
    if (rx_base_ == nullptr) {
        return false;
    }
    for (;;) {
        uint32_t pos = 0;
        uint32_t l = 0;
        if (dmesh_doca_conn_recv_pop(objs_, slot_, &pos, &l) != DOCA_SUCCESS) {
            return false;
        }
        if (static_cast<size_t>(pos) + l > rx_len_) {
            log_warn("slot %d: recv segment out of range (pos=%u len=%u)", slot_, pos, l);
            continue;
        }
        dpu_rx_consumed_pos_ = pos + l;
        dpu_rx_consumed_pos_changed_ = true;
        *seg = rx_base_ + pos;
        *len = l;
        if (is_backend_) {
            stats_.b2c_bytes += l;
        } else {
            stats_.c2b_bytes += l;
        }
        return true;
    }
}

size_t RelayChannel::l4_pump_recv() {
    if (peer_ == nullptr) {
        return 0;
    }
    TxRing &ptx = peer_->tx_;
    size_t moved = 0;
    if (pending_off_ < pending_.size()) {
        const size_t n = ptx.dead() ? 0 : ptx.push(pending_.data() + pending_off_, pending_.size() - pending_off_);
        pending_off_ += n;
        moved += n;
        if (pending_off_ < pending_.size()) {
            return moved;
        }
        pending_.clear();
        pending_off_ = 0;
    }
    const uint8_t *seg = nullptr;
    uint32_t len = 0;
    while (pop(&seg, &len)) {
        const size_t n = ptx.dead() ? 0 : ptx.push(seg, len);
        moved += n;
        if (n < len) {
            stats_.stalls++;
            pending_.assign(seg + n, seg + len);
            pending_off_ = 0;
            break;
        }
    }
    return moved;
}

void RelayChannel::pump_send() {
    wire();
    if (dpu_rx_consumed_pos_changed_) {
        dpu_rx_consumed_pos_changed_ = false;
        dmesh_doca_conn_update_dpu_rx_consumed_pos(objs_, slot_, dpu_rx_consumed_pos_);
    }
    for (;;) {
        uint32_t pos = 0;
        uint32_t len = 0;
        if (!tx_.next_run(&pos, &len)) {
            break;
        }
        const int32_t rv = dmesh_doca_conn_send_staged(objs_, slot_, pos, len);
        if (rv <= 0) {
            break;
        }
        tx_.advance(static_cast<uint32_t>(rv));
        if (static_cast<uint32_t>(rv) < len) {
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// H2Relay
// ---------------------------------------------------------------------------

H2Relay::H2Relay(RelayChannel *client, RelayChannel *backend, const RelayPolicy *policy)
    : policy_(policy) {
    client_.ch = client;
    client_.is_client_leg = true;
    client_.preface_left = sizeof(kPreface);
    backend_.ch = backend;
    backend_.is_client_leg = false;
    hw_init(&hw_);
    // Own both connections: server SETTINGS toward the client, preface + client
    // SETTINGS toward the backend, then raise both connection windows.
    send_settings(client_);
    emit_raw(backend_, kPreface, sizeof(kPreface));
    send_settings(backend_);
}

void H2Relay::send_settings(Leg &to) {
    uint8_t p[6 * 4];
    const struct { uint16_t id; uint32_t v; } iv[4] = {{kSetTableSize, kOurTableSize},
                                                     {kSetMaxStreams, kOurMaxStreams},
                                                     {kSetInitWindow, kOurInitWindow},
                                                     {kSetMaxFrame, kOurMaxFrame}};
    for (int i = 0; i < 4; i++) {
        p[i * 6] = static_cast<uint8_t>(iv[i].id >> 8);
        p[i * 6 + 1] = static_cast<uint8_t>(iv[i].id);
        wr32(p + i * 6 + 2, iv[i].v);
    }
    emit(to, kSettings, 0, 0, p, sizeof(p));
    send_window_update(to, 0, kOurConnWindow - 65535);
}

void H2Relay::send_window_update(Leg &to, uint32_t sid, uint32_t inc) {
    uint8_t p[4];
    wr32(p, inc & 0x7fffffffu);
    emit(to, kWindowUpdate, 0, sid, p, 4);
    stats_.window_updates_out++;
}

void H2Relay::send_rst(Leg &to, uint32_t sid, uint32_t code) {
    uint8_t p[4];
    wr32(p, code);
    emit(to, kRstStream, 0, sid, p, 4);
    stats_.rst_out++;
}

void H2Relay::send_goaway(Leg &to, uint32_t last_sid, uint32_t code) {
    if (to.goaway_sent) {
        return;
    }
    uint8_t p[8];
    wr32(p, last_sid);
    wr32(p + 4, code);
    emit(to, kGoaway, 0, 0, p, 8);
    to.goaway_sent = true;
}

// ":status 403" as literal-without-indexing with static name index 8 (:status)
// and a raw (non-Huffman) value: valid regardless of the dynamic table state.
void H2Relay::send_403(Leg &to, uint32_t sid) {
    static const uint8_t blk[] = {0x08, 0x03, '4', '0', '3'};
    emit(to, kHeaders, kFlagEndHeaders | kFlagEndStream, sid, blk, sizeof(blk));
}

void H2Relay::emit_raw(Leg &to, const uint8_t *bytes, size_t n) {
    if (to.ch == nullptr || to.ch->tx().dead()) {
        return;
    }
    if (to.outq_off < to.outq.size()) {
        to.outq.insert(to.outq.end(), bytes, bytes + n);
        return;
    }
    const size_t k = to.ch->tx().push(bytes, n);
    if (k < n) {
        stats_.stalls++;
        to.outq.assign(bytes + k, bytes + n);
        to.outq_off = 0;
    }
}

// Emits one frame; splits DATA and plain HEADERS/CONTINUATION blocks that
// exceed the peer's MAX_FRAME_SIZE.
void H2Relay::emit(Leg &to, uint8_t type, uint8_t flags, uint32_t sid, const uint8_t *payload,
                   uint32_t plen) {
    uint8_t h[9];
    if (plen > to.peer_max_frame) {
        if (type == kData || ((type == kHeaders || type == kContinuation) &&
                              (flags & (kFlagPadded | kFlagPriority)) == 0)) {
            stats_.reframed++;
            uint32_t off = 0;
            while (off < plen) {
                const uint32_t n = (plen - off) < to.peer_max_frame ? (plen - off) : to.peer_max_frame;
                const bool last = off + n == plen;
                uint8_t f = 0, t = type;
                if (type == kData) {
                    f = last ? (flags & kFlagEndStream) : 0;
                } else {
                    t = off == 0 ? type : kContinuation;
                    f = last ? (flags & kFlagEndHeaders) : 0;
                    if (off == 0 && type == kHeaders) {
                        f |= flags & kFlagEndStream;
                    }
                }
                wr_hdr(h, n, t, f, sid);
                emit_raw(to, h, 9);
                emit_raw(to, payload + off, n);
                off += n;
            }
            return;
        }
        protocol_error(to, "frame exceeds peer MAX_FRAME_SIZE and cannot be split");
        return;
    }
    wr_hdr(h, plen, type, flags, sid);
    emit_raw(to, h, 9);
    if (plen > 0) {
        emit_raw(to, payload, plen);
    }
}

void H2Relay::flush(Leg &to) {
    if (to.ch == nullptr || to.ch->tx().dead()) {
        to.outq.clear();
        to.outq_off = 0;
        return;
    }
    if (to.outq_off < to.outq.size()) {
        const size_t k = to.ch->tx().push(to.outq.data() + to.outq_off, to.outq.size() - to.outq_off);
        to.outq_off += k;
        if (to.outq_off >= to.outq.size()) {
            to.outq.clear();
            to.outq_off = 0;
        }
    }
}

void H2Relay::protocol_error(Leg &from, const char *why) {
    stats_.proto_errors++;
    if (!dead_) {
        log_warn("relay slot=%d/%d: %s (%s leg)", client_.ch ? client_.ch->slot() : -1,
                 backend_.ch ? backend_.ch->slot() : -1, why, from.is_client_leg ? "client" : "backend");
    }
    send_goaway(client_, static_cast<uint32_t>(last_c_sid_), kErrProtocol);
    send_goaway(backend_, static_cast<uint32_t>(next_b_sid_ - 2 > 0 ? next_b_sid_ - 2 : 0), kErrProtocol);
    dead_ = true;
}

StreamState *H2Relay::stream_by_client(int32_t c_sid) {
    auto it = streams_.find(c_sid);
    return it == streams_.end() ? nullptr : &it->second;
}

StreamState *H2Relay::stream_by_backend(int32_t b_sid) {
    auto it = b2c_.find(b_sid);
    return it == b2c_.end() ? nullptr : stream_by_client(it->second);
}

void H2Relay::close_stream(StreamState *st) {
    b2c_.erase(st->b_sid);
    streams_.erase(st->c_sid);
    stats_.streams_closed++;
}

// ---- intake ---------------------------------------------------------------

void H2Relay::pump() {
    if (dead_) {
        flush(client_);
        flush(backend_);
        return;
    }
    // Drain leftovers first so intake never has to block on staging room.
    flush(backend_);
    flush(client_);
    if (replay_held(client_)) {
        intake(client_);
    }
    if (replay_held(backend_)) {
        intake(backend_);
    }
    flush(backend_);
    flush(client_);
}

bool H2Relay::replay_held(Leg &from) {
    while (!from.held.empty()) {
        std::vector<uint8_t> &fr = from.held.front();
        if (!on_frame(from, fr.data(), fr.data() + 9, static_cast<uint32_t>(fr.size() - 9))) {
            return false; // still blocked
        }
        from.held.pop_front();
    }
    return true;
}

void H2Relay::intake(Leg &from) {
    const uint8_t *seg = nullptr;
    uint32_t len = 0;
    while (!dead_ && from.ch->pop(&seg, &len)) {
        const uint8_t *p = seg;
        size_t n = len;
        if (from.preface_left > 0) {
            const size_t k = n < from.preface_left ? n : from.preface_left;
            if (std::memcmp(p, kPreface + (sizeof(kPreface) - from.preface_left), k) != 0) {
                protocol_error(from, "bad connection preface");
                return;
            }
            from.preface_left -= k;
            p += k;
            n -= k;
        }
        while (n > 0 && !dead_) {
            if (!from.acc.empty()) {
                // Complete the accumulated frame first.
                const size_t need = from.acc.size() < 9 ? 9 - from.acc.size()
                                                         : (9 + rd24(from.acc.data())) - from.acc.size();
                const size_t k = n < need ? n : need;
                from.acc.insert(from.acc.end(), p, p + k);
                p += k;
                n -= k;
                if (from.acc.size() < 9) {
                    continue;
                }
                const uint32_t plen = rd24(from.acc.data());
                if (plen > kOurMaxFrame) {
                    protocol_error(from, "frame larger than our MAX_FRAME_SIZE");
                    return;
                }
                if (from.acc.size() < 9 + plen) {
                    continue;
                }
                std::vector<uint8_t> fr;
                fr.swap(from.acc);
                if (!on_frame(from, fr.data(), fr.data() + 9, plen)) {
                    from.held.push_back(std::move(fr));
                }
                continue;
            }
            if (n < 9) {
                from.acc.assign(p, p + n);
                break;
            }
            const uint32_t plen = rd24(p);
            if (plen > kOurMaxFrame) {
                protocol_error(from, "frame larger than our MAX_FRAME_SIZE");
                return;
            }
            if (n < 9 + plen) {
                from.acc.assign(p, p + n);
                break;
            }
            // Fast path: the whole frame lies inside this segment — no copy.
            if (from.held.empty()) {
                if (!on_frame(from, p, p + 9, plen)) {
                    from.held.emplace_back(p, p + 9 + plen);
                }
            } else {
                from.held.emplace_back(p, p + 9 + plen); // keep order behind held frames
            }
            p += 9 + plen;
            n -= 9 + plen;
        }
    }
}

// ---- dispatch ---------------------------------------------------------------

bool H2Relay::on_frame(Leg &from, const uint8_t *hdr, const uint8_t *payload, uint32_t plen) {
    const uint8_t type = hdr[3];
    const uint8_t flags = hdr[4];
    const uint32_t sid = rd32(hdr + 5) & 0x7fffffffu;
    stats_.frames_in++;
    // A header block in progress admits only CONTINUATION on the same stream.
    const bool block_open = from.is_client_leg ? in_block_ : b_in_block_;
    if (block_open && (type != kContinuation || sid != (from.is_client_leg ? static_cast<uint32_t>(block_sid_) : 0u) )) {
        if (from.is_client_leg || type != kContinuation) {
            protocol_error(from, "expected CONTINUATION");
            return true;
        }
    }
    switch (type) {
    case kData:
        return on_data(from, sid, flags, payload, plen);
    case kHeaders:
    case kContinuation:
        on_headers(from, sid, type, flags, hdr, payload, plen);
        return true;
    case kPriority:
        return true; // advisory; dropped
    case kRstStream:
        on_rst(from, sid, payload, plen);
        return true;
    case kSettings:
        on_settings(from, flags, payload, plen);
        return true;
    case kPushPromise:
        protocol_error(from, "PUSH_PROMISE not supported");
        return true;
    case kPing:
        on_ping(from, flags, payload, plen);
        return true;
    case kGoaway:
        on_goaway(from, payload, plen);
        return true;
    case kWindowUpdate:
        on_window_update(from, sid, payload, plen);
        return true;
    default:
        return true; // unknown extension frame: dropped
    }
}

bool H2Relay::on_data(Leg &from, uint32_t sid, uint8_t flags, const uint8_t *p, uint32_t n) {
    if (sid == 0) {
        protocol_error(from, "DATA on stream 0");
        return true;
    }
    Leg &to = other(from);
    StreamState *st = from.is_client_leg ? stream_by_client(static_cast<int32_t>(sid))
                                         : stream_by_backend(static_cast<int32_t>(sid));
    // Receive-side accounting happens whether or not we forward (the peer used
    // its window); replenish generously so the sender never stalls on us.
    from.recv_unacked += n;
    if (from.recv_unacked >= static_cast<int64_t>(kOurConnWindow / 4)) {
        send_window_update(from, 0, static_cast<uint32_t>(from.recv_unacked));
        from.recv_unacked = 0;
    }
    if (st == nullptr || st->denied) {
        stats_.data_frames++;
        if (st != nullptr && (flags & kFlagEndStream)) {
            close_stream(st);
        }
        return true; // closed or denied stream: dropped
    }
    int64_t &win = from.is_client_leg ? st->b_send_win : st->c_send_win;
    if (win < static_cast<int64_t>(n) || to.send_window < static_cast<int64_t>(n)) {
        stats_.window_held++;
        return false; // hold until the other leg's peer opens its window
    }
    stats_.data_frames++;
    win -= n;
    to.send_window -= n;
    int64_t &sun = from.is_client_leg ? st->c_recv_unacked : st->b_recv_unacked;
    sun += n;
    if (sun >= static_cast<int64_t>(kOurInitWindow / 2)) {
        send_window_update(from, sid, static_cast<uint32_t>(sun));
        sun = 0;
    }
    const uint32_t out_sid = from.is_client_leg ? static_cast<uint32_t>(st->b_sid)
                                                : static_cast<uint32_t>(st->c_sid);
    emit(to, kData, flags & kFlagEndStream, out_sid, p, n);
    if (flags & kFlagEndStream) {
        if (from.is_client_leg) {
            st->req_done = true;
        } else {
            st->res_done = true;
            close_stream(st);
        }
    }
    return true;
}

void H2Relay::on_headers(Leg &from, uint32_t sid, uint8_t type, uint8_t flags, const uint8_t *hdr,
                         const uint8_t *p, uint32_t n) {
    if (sid == 0) {
        protocol_error(from, "HEADERS on stream 0");
        return;
    }
    Leg &to = other(from);
    if (!from.is_client_leg) {
        // Response direction: forwarded verbatim, no decoding at all.
        StreamState *st = stream_by_backend(static_cast<int32_t>(sid));
        if (type == kHeaders) {
            b_in_block_ = (flags & kFlagEndHeaders) == 0;
        } else {
            b_in_block_ = (flags & kFlagEndHeaders) == 0;
        }
        (void)hdr;
        if (st == nullptr) {
            // Stream already closed on the client side (RST/deny): the block is
            // still delivered so the client's HPACK decoder stays in sync; the
            // client discards the frame as STREAM_CLOSED. Valid because the
            // 1:1 pairing makes the stream-id mapping the identity.
            emit(to, type, flags, sid, p, n);
            return;
        }
        emit(to, type, flags, static_cast<uint32_t>(st->c_sid), p, n);
        if ((flags & kFlagEndStream) && (flags & kFlagEndHeaders)) {
            st->res_done = true;
            close_stream(st);
        }
        return;
    }

    // Request direction.
    StreamState *st = stream_by_client(static_cast<int32_t>(sid));
    if (type == kHeaders) {
        if (st == nullptr) {
            if (static_cast<int32_t>(sid) <= last_c_sid_ || (sid & 1u) == 0) {
                protocol_error(from, "HEADERS on an old or even stream id");
                return;
            }
            StreamState ns;
            ns.c_sid = static_cast<int32_t>(sid);
            ns.b_sid = next_b_sid_;
            next_b_sid_ += 2;
            ns.c_send_win = client_.peer_init_window;
            ns.b_send_win = backend_.peer_init_window;
            last_c_sid_ = ns.c_sid;
            b2c_[ns.b_sid] = ns.c_sid;
            st = &streams_.emplace(ns.c_sid, ns).first->second;
            stats_.streams_opened++;
        }
        in_block_ = (flags & kFlagEndHeaders) == 0;
        block_sid_ = static_cast<int32_t>(sid);
        block_.clear();
    } else {
        stats_.continuation++;
        in_block_ = (flags & kFlagEndHeaders) == 0;
        if (st == nullptr) {
            protocol_error(from, "CONTINUATION on unknown stream");
            return;
        }
    }
    // Collect the block fragment for the HPACK mirror (padding/priority stripped
    // for the walk only; the frame itself is forwarded untouched).
    if (!hw_lost_) {
        const uint8_t *q = p;
        uint32_t left = n;
        uint32_t pad = 0;
        if (type == kHeaders && (flags & kFlagPadded)) {
            if (left < 1) { protocol_error(from, "bad padding"); return; }
            pad = *q++;
            left--;
        }
        if (type == kHeaders && (flags & kFlagPriority)) {
            if (left < 5) { protocol_error(from, "bad priority"); return; }
            q += 5;
            left -= 5;
        }
        if (pad > left) { protocol_error(from, "padding exceeds payload"); return; }
        left -= pad;
        block_.insert(block_.end(), q, q + left);
    }
    // Forward the frame bytes as they are. This happens even for a denied
    // stream: the backend decoder must process every block the client encoder
    // produced to stay in sync (RFC 7540 §4.3); it then ignores the frame as
    // STREAM_CLOSED since we reset the stream there.
    emit(to, type, flags, static_cast<uint32_t>(st->b_sid), p, n);
    if (flags & kFlagEndStream) {
        st->req_done = true;
        if (st->denied && (flags & kFlagEndHeaders)) {
            close_stream(st);
            return;
        }
    }
    if (flags & kFlagEndHeaders) {
        stats_.header_blocks++;
        if (!hw_lost_) {
            const int rc = hw_walk(block_.data(), static_cast<int>(block_.size()), &hw_, &out_, 0);
            block_.clear();
            if (rc != 0) {
                // Mirror may now disagree with the client's encoder: a real
                // implementation switches the connection to full termination.
                stats_.walk_fail++;
                hw_lost_ = true;
            } else if ((out_.have & (1u << (HW_PATH - 1))) == 0) {
                stats_.no_path++; // trailers
            } else if (!st->denied) {
                policy_check(st);
            }
        }
    }
}

void H2Relay::policy_check(StreamState *st) {
    if (policy_->match(out_.path, out_.plen, out_.auth, out_.alen)) {
        stats_.allow++;
        return;
    }
    stats_.deny++;
    st->denied = true;
    // The HEADERS already reached the backend (HPACK table sync); cancel the
    // stream there and answer the client ourselves.
    send_rst(backend_, static_cast<uint32_t>(st->b_sid), kErrCancel);
    send_403(client_, static_cast<uint32_t>(st->c_sid));
    if (st->req_done) {
        close_stream(st);
    }
}

void H2Relay::on_settings(Leg &from, uint8_t flags, const uint8_t *p, uint32_t n) {
    if (flags & kFlagAck) {
        return;
    }
    if (n % 6 != 0) {
        protocol_error(from, "bad SETTINGS length");
        return;
    }
    stats_.settings_in++;
    Leg &to = other(from);
    for (uint32_t i = 0; i < n; i += 6) {
        const uint16_t id = static_cast<uint16_t>((p[i] << 8) | p[i + 1]);
        const uint32_t v = rd32(p + i + 2);
        switch (id) {
        case kSetTableSize:
            if (v != from.peer_table_size) {
                from.peer_table_size = v;
                // Keep the encoder on the other leg within this decoder's limit:
                // the forwarded blocks are decoded by `from`'s peer.
                uint8_t s[6] = {0, kSetTableSize, 0, 0, 0, 0};
                wr32(s + 2, v);
                emit(to, kSettings, 0, 0, s, 6);
            }
            break;
        case kSetInitWindow: {
            if (v > 0x7fffffffu) {
                protocol_error(from, "INITIAL_WINDOW_SIZE too large");
                return;
            }
            const int64_t delta = static_cast<int64_t>(v) - static_cast<int64_t>(from.peer_init_window);
            from.peer_init_window = v;
            for (auto &e : streams_) {
                if (from.is_client_leg) {
                    e.second.c_send_win += delta;
                } else {
                    e.second.b_send_win += delta;
                }
            }
            break;
        }
        case kSetMaxFrame:
            if (v < 16384 || v > 16777215) {
                protocol_error(from, "bad MAX_FRAME_SIZE");
                return;
            }
            from.peer_max_frame = v;
            break;
        default:
            break; // MAX_CONCURRENT_STREAMS / ENABLE_PUSH / MAX_HEADER_LIST_SIZE: not needed
        }
    }
    from.peer_settings_seen = true;
    emit(from, kSettings, kFlagAck, 0, nullptr, 0);
}

void H2Relay::on_window_update(Leg &from, uint32_t sid, const uint8_t *p, uint32_t n) {
    if (n != 4) {
        protocol_error(from, "bad WINDOW_UPDATE");
        return;
    }
    const uint32_t inc = rd32(p) & 0x7fffffffu;
    if (inc == 0) {
        if (sid == 0) {
            protocol_error(from, "zero WINDOW_UPDATE");
        }
        return;
    }
    if (sid == 0) {
        from.send_window += inc;
        if (from.send_window > 0x7fffffff) {
            protocol_error(from, "connection window overflow");
        }
        return;
    }
    StreamState *st = from.is_client_leg ? stream_by_client(static_cast<int32_t>(sid))
                                         : stream_by_backend(static_cast<int32_t>(sid));
    if (st == nullptr) {
        return; // closed stream: ignored
    }
    if (from.is_client_leg) {
        st->c_send_win += inc;
    } else {
        st->b_send_win += inc;
    }
}

void H2Relay::on_rst(Leg &from, uint32_t sid, const uint8_t *p, uint32_t n) {
    if (n != 4 || sid == 0) {
        protocol_error(from, "bad RST_STREAM");
        return;
    }
    stats_.rst_in++;
    const uint32_t code = rd32(p);
    if (from.is_client_leg) {
        if (in_block_ && block_sid_ == static_cast<int32_t>(sid)) {
            protocol_error(from, "RST inside header block");
            return;
        }
        StreamState *st = stream_by_client(static_cast<int32_t>(sid));
        if (st == nullptr) {
            return;
        }
        if (!st->denied) {
            send_rst(backend_, static_cast<uint32_t>(st->b_sid), code);
        }
        close_stream(st);
    } else {
        StreamState *st = stream_by_backend(static_cast<int32_t>(sid));
        if (st == nullptr) {
            return;
        }
        if (!st->denied) {
            send_rst(client_, static_cast<uint32_t>(st->c_sid), code);
        }
        close_stream(st);
    }
}

void H2Relay::on_ping(Leg &from, uint8_t flags, const uint8_t *p, uint32_t n) {
    if (n != 8) {
        protocol_error(from, "bad PING");
        return;
    }
    if (flags & kFlagAck) {
        return;
    }
    stats_.pings++;
    emit(from, kPing, kFlagAck, 0, p, 8); // answered locally, never forwarded
}

void H2Relay::on_goaway(Leg &from, const uint8_t *p, uint32_t n) {
    if (n < 8) {
        protocol_error(from, "bad GOAWAY");
        return;
    }
    stats_.goaway++;
    const uint32_t code = rd32(p + 4);
    // Translate: the other side learns which of ITS streams were processed.
    if (from.is_client_leg) {
        send_goaway(backend_, static_cast<uint32_t>(next_b_sid_ >= 3 ? next_b_sid_ - 2 : 0), code);
    } else {
        send_goaway(client_, static_cast<uint32_t>(last_c_sid_), code);
    }
}

} // namespace dmesh
