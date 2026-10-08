#include "router.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace dmesh {

// ---------------------------------------------------------------------------
// logging / config
// ---------------------------------------------------------------------------

static void vlog(const char *level, const char *fmt, va_list ap) {
    std::fprintf(stderr, "[dmesh-router-cpp] %s ", level);
    std::vfprintf(stderr, fmt, ap);
    std::fputc('\n', stderr);
}

void log_info(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vlog("INFO", fmt, ap);
    va_end(ap);
}

void log_warn(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vlog("WARN", fmt, ap);
    va_end(ap);
}

std::string Key::str() const {
    char buf[32];
    struct in_addr a;
    a.s_addr = ip;
    std::snprintf(buf, sizeof(buf), "%s:%u", inet_ntoa(a), port);
    return buf;
}

bool Key::parse(const std::string &text, Key *out) {
    const size_t colon = text.rfind(':');
    if (colon == std::string::npos) {
        return false;
    }
    const std::string host = text.substr(0, colon);
    struct in_addr a;
    if (inet_aton(host.c_str(), &a) == 0) {
        return false;
    }
    const long port = std::strtol(text.c_str() + colon + 1, nullptr, 10);
    if (port <= 0 || port > 65535) {
        return false;
    }
    out->ip = a.s_addr;
    out->port = static_cast<uint16_t>(port);
    return true;
}

static std::string env_or(const char *name, const char *fallback) {
    const char *v = std::getenv(name);
    return (v != nullptr && *v != '\0') ? std::string(v) : std::string(fallback);
}

bool Config::from_env(Config *out, std::string *error) {
    out->dev_pci = env_or("DMESH_ROUTER_DEV_PCI", "03:00.1");
    out->rep_pci = env_or("DMESH_ROUTER_REP_PCI", "94:00.1");
    out->server_name = env_or("DMESH_ROUTER_SERVER", "DPUMesh0");
    out->backend_wait_ms = std::atoi(env_or("DMESH_ROUTER_BACKEND_WAIT_MS", "5000").c_str());
    out->max_streams =
        static_cast<uint32_t>(std::atoi(env_or("DMESH_ROUTER_MAX_STREAMS", "1000").c_str()));
    const std::string busy = env_or("DMESH_BUSY_POLL", "");
    out->busy_poll = !busy.empty() && busy != "0";

    const std::string mode = env_or("DMESH_ROUTER_MODE", "terminate");
    if (mode == "terminate") {
        out->mode = Config::Mode::kTerminate;
    } else if (mode == "l4") {
        out->mode = Config::Mode::kL4;
    } else if (mode == "relay") {
        out->mode = Config::Mode::kRelay;
    } else if (mode == "transcode") {
        out->mode = Config::Mode::kTranscode;
    } else {
        *error = "DMESH_ROUTER_MODE=" + mode + ": expected terminate|l4|relay|transcode";
        return false;
    }
    // "/ok,/api/" — allowed :path prefixes (lookup-only policy); empty ⇒ any "/…"
    const std::string paths = env_or("DMESH_ROUTER_POLICY_PATHS", "");
    size_t ps = 0;
    while (ps < paths.size()) {
        size_t c = paths.find(',', ps);
        if (c == std::string::npos) {
            c = paths.size();
        }
        if (c > ps) {
            out->policy.path_prefixes.push_back(paths.substr(ps, c - ps));
        }
        ps = c + 1;
    }
    out->policy.authority = env_or("DMESH_ROUTER_POLICY_AUTHORITY", "");

    const std::string proto = env_or("DMESH_ROUTER_BACKEND_PROTO", "h2");
    if (proto != "h2" && proto != "http2" && proto != "h2c") {
        *error = "DMESH_ROUTER_BACKEND_PROTO=" + proto +
                 ": this implementation speaks HTTP/2 to the backend only (nghttp2); "
                 "use the Rust dmesh-router for an HTTP/1.1 backend leg";
        return false;
    }

    const std::string def = env_or("DMESH_ROUTER_DEFAULT_BACKEND", "");
    if (!def.empty()) {
        if (!Key::parse(def, &out->default_backend)) {
            *error = "DMESH_ROUTER_DEFAULT_BACKEND '" + def + "' is not <ip:port>";
            return false;
        }
        out->have_default_backend = true;
    }

    // "svc.example.com=10.0.0.1:8086,other:8080=10.0.0.2:8086"
    const std::string spec = env_or("DMESH_ROUTER_ROUTES", "");
    size_t start = 0;
    while (start < spec.size()) {
        size_t comma = spec.find(',', start);
        if (comma == std::string::npos) {
            comma = spec.size();
        }
        const std::string entry = spec.substr(start, comma - start);
        start = comma + 1;
        if (entry.empty()) {
            continue;
        }
        const size_t eq = entry.find('=');
        if (eq == std::string::npos) {
            *error = "DMESH_ROUTER_ROUTES: '" + entry + "' is not <authority>=<ip:port>";
            return false;
        }
        Key key;
        if (!Key::parse(entry.substr(eq + 1), &key)) {
            *error = "DMESH_ROUTER_ROUTES: backend of '" + entry + "' is not <ip:port>";
            return false;
        }
        out->routes[entry.substr(0, eq)] = key;
    }
    return true;
}

bool Config::route(const std::string &authority, Key *out) const {
    auto it = routes.find(authority);
    if (it == routes.end()) {
        const size_t colon = authority.rfind(':');
        if (colon == std::string::npos) {
            return false;
        }
        it = routes.find(authority.substr(0, colon));
        if (it == routes.end()) {
            return false;
        }
    }
    *out = it->second;
    return true;
}

// ---------------------------------------------------------------------------
// header helpers
// ---------------------------------------------------------------------------

// Case-sensitive compare of an incoming (name, namelen) against a literal,
// without constructing a std::string (the header hot path runs this per field).
static inline bool header_eq(const uint8_t *name, size_t namelen, const char *lit, size_t litlen) {
    return namelen == litlen && std::memcmp(name, lit, namelen) == 0;
}

// Connection-scoped headers must not be forwarded (RFC 9110 §7.6.1); HTTP/2
// forbids most of them outright, and `host` is carried as :authority.
static bool hop_by_hop(const uint8_t *name, size_t namelen) {
    struct Lit {
        const char *s;
        size_t n;
    };
    static const Lit kNames[] = {
        {"connection", 10},        {"keep-alive", 10}, {"proxy-connection", 16},
        {"proxy-authenticate", 18}, {"proxy-authorization", 19}, {"te", 2},
        {"trailer", 7},            {"transfer-encoding", 17}, {"upgrade", 7},
        {"host", 4}};
    for (const Lit &l : kNames) {
        if (header_eq(name, namelen, l.s, l.n)) {
            return true;
        }
    }
    return false;
}

// Builds an nghttp2_nv array that POINTS AT caller-owned memory — no copy.
//
// nghttp2 copies each name/value into its own storage inside the submit call
// (NGHTTP2_NV_FLAG_NONE), so the pointed-at bytes only need to be valid for the
// duration of that call. Every source we use satisfies that: string literals,
// the Stream's own pseudo-header fields, and its stored header pairs all outlive
// the submit. So this holds no storage of its own (the earlier deque copy was
// redundant with nghttp2's own copy — pure allocation churn on the hot path).
class NvList {
  public:
    void add(const char *name, size_t namelen, const char *value, size_t valuelen) {
        nghttp2_nv nv;
        nv.name = reinterpret_cast<uint8_t *>(const_cast<char *>(name));
        nv.value = reinterpret_cast<uint8_t *>(const_cast<char *>(value));
        nv.namelen = namelen;
        nv.valuelen = valuelen;
        nv.flags = NGHTTP2_NV_FLAG_NONE;
        nva_.push_back(nv);
    }
    // name is always a string literal here; value may be a literal or a
    // Stream-owned std::string (both valid through submit).
    void add(const char *name, const char *value) {
        add(name, std::strlen(name), value, std::strlen(value));
    }
    void add(const char *name, const std::string &value) {
        add(name, std::strlen(name), value.data(), value.size());
    }
    void add(const std::string &name, const std::string &value) {
        add(name.data(), name.size(), value.data(), value.size());
    }
    const nghttp2_nv *data() const { return nva_.data(); }
    size_t size() const { return nva_.size(); }

  private:
    std::vector<nghttp2_nv> nva_;
};

// ---------------------------------------------------------------------------
// Channel: DMA <-> nghttp2
// ---------------------------------------------------------------------------

Channel::~Channel() {
    if (session_ != nullptr) {
        nghttp2_session_del(session_);
        session_ = nullptr;
    }
}

void Channel::wire() {
    if (rx_base_ == nullptr) {
        const uint8_t *base = nullptr;
        size_t len = 0;
        if (dmesh_doca_conn_staging_base(objs_, slot_, &base, &len) == DOCA_SUCCESS &&
            base != nullptr) {
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

int Channel::pump_recv() {
    if (!alive() || rx_base_ == nullptr) {
        return 0;
    }
    int total = 0;
    for (;;) {
        uint32_t pos = 0;
        uint32_t len = 0;
        if (dmesh_doca_conn_recv_pop(objs_, slot_, &pos, &len) != DOCA_SUCCESS) {
            break; // DOCA_ERROR_EMPTY
        }
        if (static_cast<size_t>(pos) + len > rx_len_) {
            log_warn("slot %d: recv segment out of range (pos=%u len=%u)", slot_, pos, len);
            continue;
        }
        const ssize_t rv = nghttp2_session_mem_recv(session_, rx_base_ + pos, len);
        dpu_rx_consumed_pos_ = pos + len; // fully consumed by mem_recv: DPA may reuse staging up to here
        dpu_rx_consumed_pos_changed_ = true;
        if (rv < 0) {
            log_warn("slot %d: nghttp2 recv failed: %s", slot_, nghttp2_strerror(rv));
            failed_ = true;
            return -1;
        }
        total += static_cast<int>(rv);
    }
    return total;
}

ssize_t Channel::send_cb(nghttp2_session *, const uint8_t *data, size_t length, int,
                         void *user_data) {
    auto *ch = static_cast<Channel *>(user_data);
    if (ch->tx_.dead()) {
        return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    const size_t n = ch->tx_.push(data, length);
    if (n == 0) {
        // Staging full (or the reverse path is not up yet): nghttp2 keeps the
        // bytes queued and we retry after publishing frees room.
        return NGHTTP2_ERR_WOULDBLOCK;
    }
    return static_cast<ssize_t>(n);
}

bool Channel::pump_send() {
    if (!alive()) {
        return false;
    }
    wire();
    if (dpu_rx_consumed_pos_changed_) {
        dpu_rx_consumed_pos_changed_ = false;
        dmesh_doca_conn_update_dpu_rx_consumed_pos(objs_, slot_, dpu_rx_consumed_pos_);
    }

    // Publish first: anything already staged frees room for this round.
    for (;;) {
        uint32_t pos = 0;
        uint32_t len = 0;
        if (!tx_.next_run(&pos, &len)) {
            break;
        }
        const int32_t rv = dmesh_doca_conn_send_staged(objs_, slot_, pos, len);
        if (rv < 0) {
            break; // reverse path not ready yet, or ring full: retry next tick
        }
        if (rv == 0) {
            break;
        }
        tx_.advance(static_cast<uint32_t>(rv));
        if (static_cast<uint32_t>(rv) < len) {
            break;
        }
    }

    if (nghttp2_session_want_write(session_)) {
        const int rv = nghttp2_session_send(session_);
        if (rv != 0) {
            log_warn("slot %d: nghttp2 send failed: %s", slot_, nghttp2_strerror(rv));
            failed_ = true;
            return false;
        }
        // Publish whatever the session just staged.
        for (;;) {
            uint32_t pos = 0;
            uint32_t len = 0;
            if (!tx_.next_run(&pos, &len)) {
                break;
            }
            const int32_t rv2 = dmesh_doca_conn_send_staged(objs_, slot_, pos, len);
            if (rv2 <= 0) {
                break;
            }
            tx_.advance(static_cast<uint32_t>(rv2));
            if (static_cast<uint32_t>(rv2) < len) {
                break;
            }
        }
    }
    return true;
}

// Settings shared by both session directions. The default 64KB connection
// window throttles a proxy hard, so both the stream and connection windows are
// raised (hyper does the equivalent by default).
static void apply_settings(nghttp2_session *session, uint32_t max_streams) {
    const nghttp2_settings_entry iv[] = {
        {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, max_streams},
        {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, 1u << 20},
    };
    nghttp2_submit_settings(session, NGHTTP2_FLAG_NONE, iv,
                            sizeof(iv) / sizeof(nghttp2_settings_entry));
    nghttp2_session_set_local_window_size(session, NGHTTP2_FLAG_NONE, 0, 8 << 20);
}

// ---------------------------------------------------------------------------
// H2Server: the client (h2load) side
// ---------------------------------------------------------------------------

H2Server::H2Server(Router *router, struct objects *objs, int slot, Key dst)
    : Channel(objs, slot), router_(router), dst_(dst) {
    nghttp2_session_callbacks *cbs = nullptr;
    nghttp2_session_callbacks_new(&cbs);
    nghttp2_session_callbacks_set_send_callback(cbs, Channel::send_cb);
    nghttp2_session_callbacks_set_on_begin_headers_callback(cbs, on_begin_headers);
    nghttp2_session_callbacks_set_on_header_callback(cbs, on_header);
    nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, on_frame_recv);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs, on_data_chunk);
    nghttp2_session_callbacks_set_on_stream_close_callback(cbs, on_stream_close);
    nghttp2_session_server_new(&session_, cbs, this);
    nghttp2_session_callbacks_del(cbs);
    apply_settings(session_, router->config().max_streams);
}

H2Server::~H2Server() {
    for (auto &entry : streams_) {
        entry.second->client_gone = true;
        entry.second->client = nullptr;
    }
}

int H2Server::on_begin_headers(nghttp2_session *s, const nghttp2_frame *f, void *ud) {
    if (f->hd.type != NGHTTP2_HEADERS || f->headers.cat != NGHTTP2_HCAT_REQUEST) {
        return 0;
    }
    auto *self = static_cast<H2Server *>(ud);
    auto st = std::make_shared<Stream>();
    st->client_sid = f->hd.stream_id;
    st->client = self;
    st->key = self->dst_;
    st->arrived = std::chrono::steady_clock::now();
    self->streams_[st->client_sid] = st;
    nghttp2_session_set_stream_user_data(s, st->client_sid, st.get());
    return 0;
}

int H2Server::on_header(nghttp2_session *s, const nghttp2_frame *f, const uint8_t *name,
                        size_t namelen, const uint8_t *value, size_t valuelen, uint8_t, void *) {
    auto *st = static_cast<Stream *>(nghttp2_session_get_stream_user_data(s, f->hd.stream_id));
    if (st == nullptr) {
        return 0;
    }
    // Compare pseudo-header names against literals directly (no std::string
    // built for the compare); only materialize a string when a value is stored.
    const char *vc = reinterpret_cast<const char *>(value);
    if (header_eq(name, namelen, ":method", 7)) {
        st->method.assign(vc, valuelen);
    } else if (header_eq(name, namelen, ":path", 5)) {
        st->path.assign(vc, valuelen);
    } else if (header_eq(name, namelen, ":authority", 10)) {
        st->authority.assign(vc, valuelen);
    } else if (header_eq(name, namelen, ":scheme", 7)) {
        st->scheme.assign(vc, valuelen);
    } else if (namelen > 0 && name[0] != ':' && !hop_by_hop(name, namelen)) {
        st->req_headers.emplace_back(std::string(reinterpret_cast<const char *>(name), namelen),
                                     std::string(vc, valuelen));
    }
    return 0;
}

int H2Server::on_frame_recv(nghttp2_session *s, const nghttp2_frame *f, void *ud) {
    auto *self = static_cast<H2Server *>(ud);
    if (f->hd.type != NGHTTP2_HEADERS && f->hd.type != NGHTTP2_DATA) {
        return 0;
    }
    auto it = self->streams_.find(f->hd.stream_id);
    if (it == self->streams_.end()) {
        return 0;
    }
    const StreamPtr st = it->second;
    if ((f->hd.flags & NGHTTP2_FLAG_END_STREAM) == 0) {
        return 0;
    }
    st->req_eof = true;
    if (!st->dispatched) {
        st->dispatched = true;
        self->router_->dispatch(st);
    }
    (void)s;
    return 0;
}

int H2Server::on_data_chunk(nghttp2_session *s, uint8_t, int32_t sid, const uint8_t *data,
                            size_t len, void *) {
    auto *st = static_cast<Stream *>(nghttp2_session_get_stream_user_data(s, sid));
    if (st != nullptr) {
        st->req_body.append(reinterpret_cast<const char *>(data), len);
    }
    return 0;
}

int H2Server::on_stream_close(nghttp2_session *, int32_t sid, uint32_t, void *ud) {
    auto *self = static_cast<H2Server *>(ud);
    auto it = self->streams_.find(sid);
    if (it == self->streams_.end()) {
        return 0;
    }
    const StreamPtr st = it->second;
    self->streams_.erase(it);
    st->client_gone = true;
    st->client = nullptr;
    return 0;
}

ssize_t H2Server::res_read_cb(nghttp2_session *, int32_t, uint8_t *buf, size_t length,
                              uint32_t *data_flags, nghttp2_data_source *source, void *) {
    auto *st = static_cast<Stream *>(source->ptr);
    const size_t avail = st->res_body.size() - st->res_sent;
    if (avail == 0) {
        if (st->res_eof) {
            *data_flags |= NGHTTP2_DATA_FLAG_EOF;
            return 0;
        }
        return NGHTTP2_ERR_DEFERRED; // resumed when backend data arrives
    }
    const size_t n = std::min(avail, length);
    std::memcpy(buf, st->res_body.data() + st->res_sent, n);
    st->res_sent += n;
    if (st->res_sent == st->res_body.size()) {
        st->res_body.clear();
        st->res_sent = 0;
        if (st->res_eof) {
            *data_flags |= NGHTTP2_DATA_FLAG_EOF;
        }
    }
    return static_cast<ssize_t>(n);
}

void H2Server::respond(const StreamPtr &st) {
    if (st->res_submitted || st->client_gone || !alive()) {
        return;
    }
    NvList nva;
    nva.add(":status", st->status.empty() ? "502" : st->status.c_str());
    for (const auto &h : st->res_headers) {
        nva.add(h.first, h.second);
    }
    nghttp2_data_provider prd;
    prd.source.ptr = st.get();
    prd.read_callback = res_read_cb;
    const int rv = nghttp2_submit_response(session_, st->client_sid, nva.data(), nva.size(), &prd);
    if (rv != 0) {
        log_warn("slot %d: submit_response failed: %s", slot_, nghttp2_strerror(rv));
        return;
    }
    st->res_submitted = true;
}

void H2Server::resume(Stream *st) {
    if (st->res_submitted && !st->client_gone && alive()) {
        nghttp2_session_resume_data(session_, st->client_sid);
    }
}

void H2Server::fail(Stream *st, const char *status) {
    if (st->res_submitted || st->client_gone || !alive()) {
        return;
    }
    NvList nva;
    nva.add(":status", status);
    if (nghttp2_submit_response(session_, st->client_sid, nva.data(), nva.size(), nullptr) == 0) {
        st->res_submitted = true;
    }
}

// ---------------------------------------------------------------------------
// H2Backend: the nginx side
// ---------------------------------------------------------------------------

H2Backend::H2Backend(Router *router, struct objects *objs, int slot, Key key)
    : Channel(objs, slot), router_(router), key_(key) {
    nghttp2_session_callbacks *cbs = nullptr;
    nghttp2_session_callbacks_new(&cbs);
    nghttp2_session_callbacks_set_send_callback(cbs, Channel::send_cb);
    nghttp2_session_callbacks_set_on_header_callback(cbs, on_header);
    nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, on_frame_recv);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs, on_data_chunk);
    nghttp2_session_callbacks_set_on_stream_close_callback(cbs, on_stream_close);
    nghttp2_session_client_new(&session_, cbs, this);
    nghttp2_session_callbacks_del(cbs);
    apply_settings(session_, router->config().max_streams);
}

H2Backend::~H2Backend() {
    // Streams still waiting on this channel will never get a response.
    for (auto &entry : streams_) {
        const StreamPtr &st = entry.second;
        st->backend = nullptr;
        if (st->client != nullptr) {
            st->client->fail(st.get(), "502");
        }
    }
}

ssize_t H2Backend::req_read_cb(nghttp2_session *, int32_t, uint8_t *buf, size_t length,
                               uint32_t *data_flags, nghttp2_data_source *source, void *) {
    auto *st = static_cast<Stream *>(source->ptr);
    const size_t avail = st->req_body.size() - st->req_sent;
    if (avail == 0) {
        if (st->req_eof) {
            *data_flags |= NGHTTP2_DATA_FLAG_EOF;
            return 0;
        }
        return NGHTTP2_ERR_DEFERRED;
    }
    const size_t n = std::min(avail, length);
    std::memcpy(buf, st->req_body.data() + st->req_sent, n);
    st->req_sent += n;
    if (st->req_sent == st->req_body.size() && st->req_eof) {
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
    }
    return static_cast<ssize_t>(n);
}

bool H2Backend::submit(const StreamPtr &st) {
    if (!alive()) {
        return false;
    }
    const std::string authority = st->authority.empty() ? key_.str() : st->authority;
    NvList nva;
    nva.add(":method", st->method.empty() ? "GET" : st->method.c_str());
    nva.add(":scheme", st->scheme.empty() ? "http" : st->scheme.c_str());
    nva.add(":authority", authority);
    nva.add(":path", st->path.empty() ? "/" : st->path.c_str());
    for (const auto &h : st->req_headers) {
        nva.add(h.first, h.second);
    }

    const bool has_body = !st->req_body.empty() || !st->req_eof;
    nghttp2_data_provider prd;
    prd.source.ptr = st.get();
    prd.read_callback = req_read_cb;

    const int32_t sid = nghttp2_submit_request(session_, nullptr, nva.data(), nva.size(),
                                               has_body ? &prd : nullptr, st.get());
    if (sid < 0) {
        log_warn("slot %d: submit_request failed: %s", slot_, nghttp2_strerror(sid));
        return false;
    }
    st->backend = this;
    st->backend_sid = sid;
    streams_[sid] = st;
    return true;
}

int H2Backend::on_header(nghttp2_session *s, const nghttp2_frame *f, const uint8_t *name,
                         size_t namelen, const uint8_t *value, size_t valuelen, uint8_t, void *) {
    auto *st = static_cast<Stream *>(nghttp2_session_get_stream_user_data(s, f->hd.stream_id));
    if (st == nullptr) {
        return 0;
    }
    const char *vc = reinterpret_cast<const char *>(value);
    if (header_eq(name, namelen, ":status", 7)) {
        st->status.assign(vc, valuelen);
    } else if (namelen > 0 && name[0] != ':' && !hop_by_hop(name, namelen)) {
        st->res_headers.emplace_back(std::string(reinterpret_cast<const char *>(name), namelen),
                                     std::string(vc, valuelen));
    }
    return 0;
}

int H2Backend::on_frame_recv(nghttp2_session *, const nghttp2_frame *f, void *ud) {
    auto *self = static_cast<H2Backend *>(ud);
    if (f->hd.type != NGHTTP2_HEADERS || f->headers.cat != NGHTTP2_HCAT_RESPONSE) {
        return 0;
    }
    auto it = self->streams_.find(f->hd.stream_id);
    if (it == self->streams_.end()) {
        return 0;
    }
    const StreamPtr st = it->second;
    if (st->client != nullptr) {
        st->client->respond(st);
    }
    return 0;
}

int H2Backend::on_data_chunk(nghttp2_session *s, uint8_t, int32_t sid, const uint8_t *data,
                             size_t len, void *) {
    auto *st = static_cast<Stream *>(nghttp2_session_get_stream_user_data(s, sid));
    if (st == nullptr) {
        return 0;
    }
    st->res_body.append(reinterpret_cast<const char *>(data), len);
    if (st->client != nullptr) {
        st->client->resume(st);
    }
    return 0;
}

int H2Backend::on_stream_close(nghttp2_session *, int32_t sid, uint32_t, void *ud) {
    auto *self = static_cast<H2Backend *>(ud);
    auto it = self->streams_.find(sid);
    if (it == self->streams_.end()) {
        return 0;
    }
    const StreamPtr st = it->second;
    self->streams_.erase(it);
    st->res_eof = true;
    st->backend = nullptr;
    if (st->client == nullptr) {
        return 0;
    }
    if (!st->res_submitted) {
        // Closed before any response headers: nothing to relay.
        st->client->fail(st.get(), "502");
    } else {
        st->client->resume(st.get());
    }
    return 0;
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

void Router::poll_slots() {
    const int max_conns = dmesh_doca_max_conns();
    for (int slot = 0; slot < max_conns; slot++) {
        const int32_t state = dmesh_doca_conn_state_get(objs_, slot);
        const int32_t prev = states_.count(slot) ? states_[slot] : kConnFree;
        if (state == prev) {
            continue;
        }
        states_[slot] = state;

        if (state == kConnRunning) {
            open_slot(slot);
        } else if (state == kConnFree || state == kConnError || state == kConnClosing) {
            close_slot(slot);
        }
    }
}

void Router::open_slot(int slot) {
    uint32_t src_ip = 0;
    uint32_t dst_ip = 0;
    uint16_t src_port = 0;
    uint16_t dst_port = 0;
    char workload[64] = {0};
    dmesh_doca_conn_flow_get(objs_, slot, &src_ip, &src_port, &dst_ip, &dst_port, workload,
                             static_cast<int32_t>(sizeof(workload)));
    Key key;
    key.ip = dst_ip;
    key.port = dst_port;

    const bool is_backend = dmesh_doca_conn_mode_get(objs_, slot) == kFlowModeBackend;
    if (cfg_.mode != Config::Mode::kTerminate) {
        open_relay_slot(slot, is_backend, key);
        return;
    }
    if (is_backend) {
        auto backend = std::make_unique<H2Backend>(this, objs_, slot, key);
        H2Backend *raw = backend.get();
        raw->wire();
        raw->pump_send(); // client preface + SETTINGS
        backends_[key.packed()].push_back(raw);
        channels_[slot] = std::move(backend);
        log_info("backend channel registered slot=%d backend=%s channels=%zu proto=h2", slot,
                 key.str().c_str(), backends_[key.packed()].size());
        retry_pending();
        return;
    }

    auto server = std::make_unique<H2Server>(this, objs_, slot, key);
    server->wire();
    server->pump_send(); // server SETTINGS
    channels_[slot] = std::move(server);
    log_info("client connection ready slot=%d dst=%s workload=%s", slot, key.str().c_str(),
             workload);
}

void Router::close_slot(int slot) {
    auto rit = relays_.find(slot);
    if (rit != relays_.end()) {
        rit->second->tx().kill();
        if (mux_) {
            mux_->remove_leg(slot);
        }
        RelayChannel *peer = rit->second->peer();
        const int cslot = rit->second->is_backend() ? (peer ? peer->slot() : -1) : slot;
        auto eit = engines_.find(cslot);
        if (eit != engines_.end()) {
            relay_closed_.add(eit->second->stats());
            engines_.erase(eit);
        }
        rit->second->unpair();
        relay_closed_.add(rit->second->stats());
        relays_.erase(rit);
        log_info("relay connection closed slot=%d", slot);
        return;
    }
    auto it = channels_.find(slot);
    if (it == channels_.end()) {
        return;
    }
    it->second->tx().kill();
    // Drop the channel from the backend registry before destroying it.
    for (auto &entry : backends_) {
        auto &vec = entry.second;
        vec.erase(std::remove(vec.begin(), vec.end(), static_cast<H2Backend *>(it->second.get())),
                  vec.end());
    }
    channels_.erase(it);
    log_info("connection closed slot=%d", slot);
}

H2Backend *Router::pick(const Key &key) {
    auto it = backends_.find(key.packed());
    if (it == backends_.end() || it->second.empty()) {
        return nullptr;
    }
    auto &vec = it->second;
    const size_t idx = rr_[key.packed()]++ % vec.size();
    return vec[idx];
}

void Router::dispatch(const StreamPtr &st) {
    Key key = st->key;
    if (!st->authority.empty()) {
        Key routed;
        if (cfg_.route(st->authority, &routed)) {
            key = routed;
        }
    }

    H2Backend *backend = pick(key);
    if (backend == nullptr && cfg_.have_default_backend) {
        backend = pick(cfg_.default_backend);
        if (backend != nullptr) {
            key = cfg_.default_backend;
        }
    }
    if (backend == nullptr) {
        // The host's backend bridge may not have connected yet; hold the
        // request until it does (bounded by DMESH_ROUTER_BACKEND_WAIT_MS).
        st->key = key;
        pending_.push_back(st);
        return;
    }
    st->key = key;
    if (!backend->submit(st) && st->client != nullptr) {
        st->client->fail(st.get(), "502");
    }
}

void Router::retry_pending() {
    if (pending_.empty()) {
        return;
    }
    const auto now = std::chrono::steady_clock::now();
    std::vector<StreamPtr> still_waiting;
    still_waiting.reserve(pending_.size());
    for (const StreamPtr &st : pending_) {
        if (st->client_gone) {
            continue;
        }
        H2Backend *backend = pick(st->key);
        if (backend != nullptr) {
            if (!backend->submit(st) && st->client != nullptr) {
                st->client->fail(st.get(), "502");
            }
            continue;
        }
        const auto waited =
            std::chrono::duration_cast<std::chrono::milliseconds>(now - st->arrived).count();
        if (waited >= cfg_.backend_wait_ms) {
            log_warn("no dmesh backend channel for %s after %lldms", st->key.str().c_str(),
                     static_cast<long long>(waited));
            if (st->client != nullptr) {
                st->client->fail(st.get(), "503");
            }
            continue;
        }
        still_waiting.push_back(st);
    }
    pending_.swap(still_waiting);
}

void Router::pump() {
    if (!relays_.empty()) {
        pair_relays();
        for (auto &entry : relays_) {
            entry.second->wire();
        }
        if (cfg_.mode == Config::Mode::kRelay) {
            for (auto &entry : engines_) {
                entry.second->pump();
            }
        } else if (cfg_.mode == Config::Mode::kTranscode) {
            mux_->pump();
        } else {
            for (auto &entry : relays_) {
                entry.second->l4_pump_recv();
            }
        }
        for (auto &entry : relays_) {
            entry.second->pump_send();
        }
    }
    retry_pending();
    for (auto &entry : channels_) {
        Channel *ch = entry.second.get();
        ch->wire();
        ch->pump_recv();
    }
    // Send after every session has consumed its input: a request received on a
    // client channel produces output on a backend channel in the same tick.
    for (auto &entry : channels_) {
        entry.second->pump_send();
    }
}

// ---------------------------------------------------------------------------
// l4 / relay modes
// ---------------------------------------------------------------------------

void Router::open_relay_slot(int slot, bool is_backend, const Key &key) {

    auto ch = std::make_unique<RelayChannel>(objs_, slot, is_backend, key.packed());
    ch->wire();
    RelayChannel *raw = ch.get();
    relays_[slot] = std::move(ch);
    if (cfg_.mode == Config::Mode::kTranscode) {
        if (!mux_) {
            mux_ = std::make_unique<H2Mux>(&cfg_.policy, cfg_.have_default_backend, cfg_.default_backend.packed());
        }
        mux_->add_leg(raw);
    }
    log_info("%s %s channel slot=%d key=%s (mode=%s)", is_backend ? "backend" : "client",
             is_backend ? "registered" : "ready", slot, key.str().c_str(),
             cfg_.mode == Config::Mode::kRelay ? "relay" : cfg_.mode == Config::Mode::kTranscode ? "transcode" : "l4");
    if (cfg_.mode != Config::Mode::kTranscode) {
        pair_relays();
    }
}

// Pin every unpaired client channel to a never-used backend channel with the
// same key (or the default backend). Backend channels are single-use: their
// HPACK decoder table belongs to the first client that spoke through them.
void Router::pair_relays() {
    for (auto &centry : relays_) {
        RelayChannel *c = centry.second.get();
        if (c->is_backend() || c->paired() || c->used()) {
            continue;
        }
        RelayChannel *pick = nullptr;
        for (auto &bentry : relays_) {
            RelayChannel *b = bentry.second.get();
            if (!b->is_backend() || b->used()) {
                continue;
            }
            if (b->key() == c->key() ||
                (cfg_.have_default_backend && b->key() == cfg_.default_backend.packed())) {
                pick = b;
                break;
            }
        }
        if (pick == nullptr) {
            continue; // backend bridge not up yet; retried every tick
        }
        c->pair(pick);
        if (cfg_.mode == Config::Mode::kRelay) {
            engines_[c->slot()] = std::make_unique<H2Relay>(c, pick, &cfg_.policy);
        }
        log_info("relay pair: client slot=%d <-> backend slot=%d", c->slot(), pick->slot());
    }
}

std::string Router::stats_line() {
    if (cfg_.mode == Config::Mode::kTerminate) {
        return std::string();
    }
    RelayStats t = relay_closed_;
    for (auto &entry : relays_) {
        t.add(entry.second->stats());
    }
    for (auto &entry : engines_) {
        t.add(entry.second->stats());
    }
    if (mux_) {
        t.add(mux_->stats());
        return t.line() + " | " + mux_->tstats_line();
    }
    return t.line();
}

} // namespace dmesh
