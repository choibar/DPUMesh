#ifndef BROKER_IPC_H
#define BROKER_IPC_H

/*
 * Wire between an application's channel and the host broker that owns the
 * DOCA device for it (ported from ~/DPUmesh src/broker/dmesh_ipc.h and
 * dmesh_brokerlink.c). One SOCK_SEQPACKET connection per channel; every
 * request gets one reply, and a reply may carry memfds over SCM_RIGHTS:
 *
 *   HELLO          open the device and the Comch session  -> status page memfd
 *   MEM_ALLOC      register a region                       -> region memfd
 *   CONN_OPEN      open a flow on registered regions       -> forward ring memfd
 *   CONN_CLOSE     close a flow
 *   SESSION_CLOSE  close the Comch session
 *
 * No data bytes cross the socket: the application maps the memfds and runs the
 * forward ring and the push window on shared memory. Flow outcomes the DPU
 * reports asynchronously (CLOSED, errors) are published in the status page.
 * No DOCA types appear here.
 */

#include <stddef.h>
#include <stdint.h>

#define BROKER_IPC_MAGIC "DPMBRK01"
#define BROKER_IPC_VERSION 1
#define BROKER_DEFAULT_SOCKET "/run/dpumesh/broker.sock"
#define BROKER_FLOWS 33          /* flow ids 1..32, the channel's slots */
#define BROKER_MAX_FDS 1
#define BROKER_NAME_LEN 64

enum broker_msg_type {
	BROKER_HELLO = 1,
	BROKER_MEM_ALLOC,
	BROKER_CONN_OPEN,
	BROKER_CONN_CLOSE,
	BROKER_SESSION_CLOSE,
	BROKER_REPLY,
};

struct broker_request {
	char magic[8];
	uint8_t type;
	uint8_t version;
	uint16_t reserved;
	uint32_t flow_id;             /* CONN_OPEN, CONN_CLOSE */
	uint64_t bytes;               /* MEM_ALLOC */
	uint32_t mode;                /* CONN_OPEN: CHANNEL_MODE_* */
	uint32_t src_ip, dst_ip;      /* CONN_OPEN: network byte order */
	uint16_t src_port, dst_port;  /* CONN_OPEN: host order */
	uint32_t tx_id, rx_id;        /* CONN_OPEN: region ids from MEM_ALLOC */
	uint64_t rx_offset;           /* CONN_OPEN: the flow's window inside rx */
	char name[BROKER_NAME_LEN];   /* HELLO: Comch server; CONN_OPEN: workload label */
};

struct broker_reply {
	char magic[8];
	uint8_t type;
	uint8_t version;
	uint16_t fd_count;
	int32_t status;               /* 0 or an errno */
	uint32_t id;                  /* MEM_ALLOC: region id; CONN_OPEN: flow generation */
	uint32_t dpa;                 /* MEM_ALLOC: the region's DPA handle on the forward device */
	uint64_t bytes;               /* size of the memfd sent with the reply */
	uint64_t base;                /* MEM_ALLOC: the broker's address of the region */
};

_Static_assert(sizeof(struct broker_request) == 120, "broker request wire ABI changed");
_Static_assert(sizeof(struct broker_reply) == 40, "broker reply wire ABI changed");

/* Written by the broker, read by the application. An entry describes the flow
 * incarnation `generation`; `state` is the broker's channel_conn_progress
 * result (0 open, 1 CLOSED, -1 failed with `error`), stored after `error`. */
struct broker_flow_status {
	uint32_t generation;
	int32_t state;
	int32_t error;
	uint32_t reserved;
};

struct broker_status {
	struct broker_flow_status flow[BROKER_FLOWS];
};

/* Sends one message with up to BROKER_MAX_FDS descriptors (still owned by the caller). */
int broker_ipc_send(int sock, const void *msg, size_t len, const int *fds, int nfds);
/* Receives exactly `len` bytes and up to `max_fds` descriptors (close-on-exec).
 * Returns the descriptor count, or -1 (EBADMSG for a short or truncated message,
 * ECONNRESET when the peer closed); received descriptors are closed on failure. */
int broker_ipc_recv(int sock, void *msg, size_t len, int *fds, int max_fds);

/* A memfd the broker registered: exactly `bytes` long, and sealed against
 * shrink/grow and further sealing so its size cannot change under the NIC. */
int broker_ipc_check_memfd(int fd, size_t bytes);

/* Connects to the broker. The socket must be owned by root or by this user. */
int broker_ipc_connect(const char *path);
/* Binds and listens on `path` (a stale socket there is replaced), mode 0666. */
int broker_ipc_listen(const char *path);

#endif /* BROKER_IPC_H */
