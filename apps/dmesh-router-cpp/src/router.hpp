// HTTP/2 router over the DPUMesh DMA datapath, on libnghttp2.
//
// Same shape as the Rust/hyper `dmesh-router`: a client channel arriving over
// DMA gets an nghttp2 *server* session, a backend channel gets an nghttp2
// *client* session, and each request stream is bridged from one to the other.
// Everything runs on one thread in the event loop of main.cpp — no executor, no
// service abstraction; the callbacks below are the whole proxy.

#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <nghttp2/nghttp2.h>

#include "dmesh.hpp"
#include "relay.hpp"
#include "relay3.hpp"

namespace dmesh {

// Backend service key: the flow destination an intercepted connection carried
// (the SO_ORIGINAL_DST analogue), packed for cheap map lookups.
struct Key {
    uint32_t ip = 0; // network byte order, as inet_addr produced it
    uint16_t port = 0;

    uint64_t packed() const { return (static_cast<uint64_t>(ip) << 16) | port; }
    bool operator<(const Key &o) const { return packed() < o.packed(); }
    bool operator==(const Key &o) const { return packed() == o.packed(); }
    std::string str() const;
    static bool parse(const std::string &text, Key *out); // "10.0.0.1:8086"
};

struct Config {
    std::string dev_pci = "03:00.1";
    std::string rep_pci = "94:00.1";
    std::string server_name = "DPUMesh0";
    // authority -> backend key
    std::map<std::string, Key> routes;
    bool have_default_backend = false;
    Key default_backend;
    int backend_wait_ms = 5000;
    uint32_t max_streams = 1000;
    bool busy_poll = false;
    // terminate (nghttp2 h2 termination, default) | l4 | relay — see relay.hpp
    enum class Mode { kTerminate, kL4, kRelay, kTranscode } mode = Mode::kTerminate;
    RelayPolicy policy;

    static bool from_env(Config *out, std::string *error);
    // Backend key for an authority, if the route table names one.
    bool route(const std::string &authority, Key *out) const;
};

class H2Server;
class H2Backend;
class Router;

using HeaderList = std::vector<std::pair<std::string, std::string>>;

// One in-flight request: a stream on a client session bridged to a stream on a
// backend session. Held by both sides; whichever outlives the other drops it.
struct Stream {
    int32_t client_sid = -1;
    int32_t backend_sid = -1;
    H2Server *client = nullptr;
    H2Backend *backend = nullptr;
    Key key;

    // request
    std::string method, path, authority, scheme;
    HeaderList req_headers;
    std::string req_body;
    size_t req_sent = 0;
    bool req_eof = false;
    bool dispatched = false;
    std::chrono::steady_clock::time_point arrived;

    // response
    std::string status;
    HeaderList res_headers;
    std::string res_body;
    size_t res_sent = 0;
    bool res_eof = false;
    bool res_submitted = false;

    bool client_gone = false;
};

using StreamPtr = std::shared_ptr<Stream>;

// A dmesh connection with an nghttp2 session on top: DMA'd bytes are fed to the
// session, and the session's output is staged for the reverse DMA path.
class Channel {
  public:
    Channel(struct objects *objs, int slot) : objs_(objs), slot_(slot) {}
    virtual ~Channel();

    // Bind the mapped staging regions; the reverse path appears a tick or two
    // after the connection is ready, so this is retried until it succeeds.
    void wire();
    // DMA'd segments -> nghttp2. Returns bytes fed, or -1 on session error.
    int pump_recv();
    // nghttp2 output -> tx staging -> DMA descriptors. False on session error.
    bool pump_send();

    bool alive() const { return session_ != nullptr && !failed_; }
    int slot() const { return slot_; }
    TxRing &tx() { return tx_; }

  protected:
    struct objects *objs_;
    int slot_;
    nghttp2_session *session_ = nullptr;
    const uint8_t *rx_base_ = nullptr;
    size_t rx_len_ = 0;
    TxRing tx_;
    bool failed_ = false;
    uint32_t dpu_rx_consumed_pos_ = 0;
    bool dpu_rx_consumed_pos_changed_ = false;

    static ssize_t send_cb(nghttp2_session *session, const uint8_t *data, size_t length, int flags,
                           void *user_data);
};

// Client channel: terminates HTTP/2 for the host's h2 traffic.
class H2Server : public Channel {
  public:
    H2Server(Router *router, struct objects *objs, int slot, Key dst);
    ~H2Server() override;

    void respond(const StreamPtr &st);   // backend headers arrived
    void resume(Stream *st);             // more backend body / EOF
    void fail(Stream *st, const char *status);

    Router *router() { return router_; }
    Key dst() const { return dst_; }

  private:
    Router *router_;
    Key dst_;
    std::unordered_map<int32_t, StreamPtr> streams_;

    static int on_begin_headers(nghttp2_session *s, const nghttp2_frame *f, void *ud);
    static int on_header(nghttp2_session *s, const nghttp2_frame *f, const uint8_t *name,
                         size_t namelen, const uint8_t *value, size_t valuelen, uint8_t flags,
                         void *ud);
    static int on_frame_recv(nghttp2_session *s, const nghttp2_frame *f, void *ud);
    static int on_data_chunk(nghttp2_session *s, uint8_t flags, int32_t sid, const uint8_t *data,
                             size_t len, void *ud);
    static int on_stream_close(nghttp2_session *s, int32_t sid, uint32_t code, void *ud);
    static ssize_t res_read_cb(nghttp2_session *s, int32_t sid, uint8_t *buf, size_t length,
                               uint32_t *data_flags, nghttp2_data_source *source, void *ud);
};

// Backend channel: speaks HTTP/2 to the server the host provides over DMA.
class H2Backend : public Channel {
  public:
    H2Backend(Router *router, struct objects *objs, int slot, Key key);
    ~H2Backend() override;

    // Submit a request; false if nghttp2 refused it.
    bool submit(const StreamPtr &st);
    Key key() const { return key_; }

  private:
    Router *router_;
    Key key_;
    std::unordered_map<int32_t, StreamPtr> streams_;

    static int on_header(nghttp2_session *s, const nghttp2_frame *f, const uint8_t *name,
                         size_t namelen, const uint8_t *value, size_t valuelen, uint8_t flags,
                         void *ud);
    static int on_frame_recv(nghttp2_session *s, const nghttp2_frame *f, void *ud);
    static int on_data_chunk(nghttp2_session *s, uint8_t flags, int32_t sid, const uint8_t *data,
                             size_t len, void *ud);
    static int on_stream_close(nghttp2_session *s, int32_t sid, uint32_t code, void *ud);
    static ssize_t req_read_cb(nghttp2_session *s, int32_t sid, uint8_t *buf, size_t length,
                               uint32_t *data_flags, nghttp2_data_source *source, void *ud);
};

// Owns the channels, the backend registry and the routing decision.
class Router {
  public:
    Router(struct objects *objs, const Config &cfg) : objs_(objs), cfg_(cfg) {}

    // Diff per-slot connection states and create/destroy channels accordingly.
    void poll_slots();
    // Feed received bytes to every session and publish their output.
    void pump();
    // Route a request; queues it if its backend channel has not registered yet.
    void dispatch(const StreamPtr &st);

    const Config &config() const { return cfg_; }
    // One-line counters for the relay modes (empty string in terminate mode).
    std::string stats_line();

  private:
    void open_slot(int slot);
    void close_slot(int slot);
    H2Backend *pick(const Key &key);
    void retry_pending();

    void open_relay_slot(int slot, bool is_backend, const Key &key);
    void pair_relays();
    std::string relay_stats_line();

    struct objects *objs_;
    Config cfg_;
    std::map<int, std::unique_ptr<Channel>> channels_;
    std::map<int, std::unique_ptr<RelayChannel>> relays_;
    std::map<int, std::unique_ptr<H2Relay>> engines_; // by client slot (relay mode)
    std::unique_ptr<H2Mux> mux_;                        // transcode mode
    RelayStats relay_closed_; // totals of channels/engines already closed
    std::map<int, int32_t> states_;
    // Backend channels per service key, round-robin.
    std::map<uint64_t, std::vector<H2Backend *>> backends_;
    std::map<uint64_t, size_t> rr_;
    std::vector<StreamPtr> pending_;
};

void log_info(const char *fmt, ...);
void log_warn(const char *fmt, ...);

} // namespace dmesh
