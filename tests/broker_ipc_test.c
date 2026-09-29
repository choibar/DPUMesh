/* broker_ipc: framing, memfd hand-over and the checks that keep a client from
 * mapping a region whose size could change under the NIC. No DOCA. */
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>
#include "src/transport/host/broker_ipc.h"

static int sealed_memfd(size_t bytes, int seals)
{
    int fd = memfd_create("broker-ipc-test", MFD_CLOEXEC | MFD_ALLOW_SEALING);
    assert(fd >= 0 && ftruncate(fd, (off_t)bytes) == 0);
    if (seals) assert(fcntl(fd, F_ADD_SEALS, seals) == 0);
    return fd;
}

static int fd_open(int fd) { return fcntl(fd, F_GETFD) != -1; }

static void test_roundtrip(void)
{
    int sv[2];
    assert(socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, sv) == 0);

    /* A reply with a sealed memfd: the receiver maps the sender's bytes. */
    int memfd = sealed_memfd(8192, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL);
    char *a = mmap(NULL, 8192, PROT_READ | PROT_WRITE, MAP_SHARED, memfd, 0);
    assert(a != MAP_FAILED);
    struct broker_reply out = { .type = BROKER_REPLY, .version = BROKER_IPC_VERSION, .fd_count = 1, .bytes = 8192 };
    assert(broker_ipc_send(sv[0], &out, sizeof(out), &memfd, 1) == 0);
    struct broker_reply in;
    int fd = -1;
    assert(broker_ipc_recv(sv[1], &in, sizeof(in), &fd, 1) == 1);
    assert(in.type == BROKER_REPLY && in.bytes == 8192 && fd >= 0 && fd != memfd);
    assert(fcntl(fd, F_GETFD) & FD_CLOEXEC);
    assert(broker_ipc_check_memfd(fd, 8192) == 0);
    char *b = mmap(NULL, 8192, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    assert(b != MAP_FAILED);
    a[4096] = 0x5a;
    assert(b[4096] == 0x5a);
    munmap(a, 8192); munmap(b, 8192); close(fd); close(memfd);

    /* A message of the wrong size is rejected, and its descriptors closed. */
    memfd = sealed_memfd(4096, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL);
    assert(broker_ipc_send(sv[0], &out, sizeof(out) - 1, &memfd, 1) == 0);
    fd = -1;
    errno = 0;
    assert(broker_ipc_recv(sv[1], &in, sizeof(in), &fd, 1) == -1 && errno == EBADMSG);
    close(memfd);

    /* Descriptors beyond max_fds are closed, not leaked. */
    memfd = sealed_memfd(4096, 0);
    assert(broker_ipc_send(sv[0], &out, sizeof(out), &memfd, 1) == 0);
    int before = dup(0); close(before); /* the next free descriptor number */
    assert(broker_ipc_recv(sv[1], &in, sizeof(in), NULL, 0) == 0);
    assert(!fd_open(before));
    close(memfd);

    /* A request is 120 bytes and carries no descriptors. */
    struct broker_request req = { .type = BROKER_CONN_CLOSE, .flow_id = 7 };
    assert(broker_ipc_send(sv[1], &req, sizeof(req), NULL, 0) == 0);
    struct broker_request got;
    assert(broker_ipc_recv(sv[0], &got, sizeof(got), NULL, 0) == 0 && got.flow_id == 7);

    /* A closed peer is ECONNRESET, not an empty message. */
    close(sv[0]);
    errno = 0;
    assert(broker_ipc_recv(sv[1], &in, sizeof(in), &fd, 1) == -1 && errno == ECONNRESET);
    close(sv[1]);
}

static void test_memfd_checks(void)
{
    int fd = sealed_memfd(4096, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL);
    errno = 0;
    assert(broker_ipc_check_memfd(fd, 8192) == -1 && errno == EBADMSG);
    close(fd);

    /* Resizable: a client could not trust the registered size. */
    fd = sealed_memfd(4096, F_SEAL_SHRINK);
    errno = 0;
    assert(broker_ipc_check_memfd(fd, 4096) == -1 && errno == EPERM);
    close(fd);

    /* Write-sealed: the data path could not write it. */
    fd = sealed_memfd(4096, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE | F_SEAL_SEAL);
    errno = 0;
    assert(broker_ipc_check_memfd(fd, 4096) == -1 && errno == EPERM);
    close(fd);
}

static void test_connect_listen(void)
{
    char dir[] = "/tmp/broker-ipc-XXXXXX";
    assert(mkdtemp(dir));
    char path[128], file[128];
    snprintf(path, sizeof(path), "%s/broker.sock", dir);
    snprintf(file, sizeof(file), "%s/plain", dir);

    int lfd = broker_ipc_listen(path);
    assert(lfd >= 0);
    struct stat st;
    assert(stat(path, &st) == 0 && (st.st_mode & 0777) == 0666);
    int c = broker_ipc_connect(path);
    assert(c >= 0);
    int s = accept(lfd, NULL, NULL);
    assert(s >= 0);
    close(c); close(s);
    /* A second broker must not take over a socket that is being served. */
    errno = 0;
    assert(broker_ipc_listen(path) == -1 && errno == EADDRINUSE);
    close(lfd);

    /* A stale socket is replaced; a regular file is never removed. */
    lfd = broker_ipc_listen(path);
    assert(lfd >= 0);
    close(lfd);
    int f = open(file, O_CREAT | O_WRONLY, 0600);
    assert(f >= 0);
    close(f);
    errno = 0;
    assert(broker_ipc_listen(file) == -1 && errno == EEXIST);
    errno = 0;
    assert(broker_ipc_connect(file) == -1 && errno == EPERM);
    unlink(file); unlink(path); rmdir(dir);
}

int main(void)
{
    test_roundtrip();
    test_memfd_checks();
    test_connect_listen();
    printf("broker_ipc_test: ok\n");
    return 0;
}
