#ifndef _GNU_SOURCE
#define _GNU_SOURCE /* F_SEAL_*, MSG_CMSG_CLOEXEC */
#endif
#include "broker_ipc.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

int broker_ipc_send(int sock, const void *msg, size_t len, const int *fds, int nfds)
{
	union {
		struct cmsghdr align;
		unsigned char bytes[CMSG_SPACE(sizeof(int) * BROKER_MAX_FDS)];
	} control;
	struct iovec iov = { .iov_base = (void *)msg, .iov_len = len };
	struct msghdr hdr = { .msg_iov = &iov, .msg_iovlen = 1 };

	if (nfds < 0 || nfds > BROKER_MAX_FDS) { errno = EINVAL; return -1; }
	if (nfds > 0) {
		memset(&control, 0, sizeof(control));
		hdr.msg_control = control.bytes;
		hdr.msg_controllen = CMSG_SPACE(sizeof(int) * (size_t)nfds);
		struct cmsghdr *cmsg = CMSG_FIRSTHDR(&hdr);
		cmsg->cmsg_level = SOL_SOCKET;
		cmsg->cmsg_type = SCM_RIGHTS;
		cmsg->cmsg_len = CMSG_LEN(sizeof(int) * (size_t)nfds);
		memcpy(CMSG_DATA(cmsg), fds, sizeof(int) * (size_t)nfds);
	}
	ssize_t sent;
	do {
		sent = sendmsg(sock, &hdr, MSG_NOSIGNAL);
	} while (sent < 0 && errno == EINTR);
	if (sent < 0) return -1;
	if ((size_t)sent != len) { errno = EMSGSIZE; return -1; }
	return 0;
}

int broker_ipc_recv(int sock, void *msg, size_t len, int *fds, int max_fds)
{
	union {
		struct cmsghdr align;
		unsigned char bytes[CMSG_SPACE(sizeof(int) * BROKER_MAX_FDS)];
	} control;
	struct iovec iov = { .iov_base = msg, .iov_len = len };
	struct msghdr hdr = {
		.msg_iov = &iov,
		.msg_iovlen = 1,
		.msg_control = control.bytes,
		.msg_controllen = sizeof(control.bytes),
	};
	ssize_t n;
	int count = 0;

	do {
		n = recvmsg(sock, &hdr, MSG_CMSG_CLOEXEC);
	} while (n < 0 && errno == EINTR);
	if (n < 0) return -1;
	if (n == 0) { errno = ECONNRESET; return -1; }
	for (struct cmsghdr *c = CMSG_FIRSTHDR(&hdr); c != NULL; c = CMSG_NXTHDR(&hdr, c)) {
		if (c->cmsg_level != SOL_SOCKET || c->cmsg_type != SCM_RIGHTS) continue;
		int k = (int)((c->cmsg_len - CMSG_LEN(0)) / sizeof(int));
		for (int i = 0; i < k; i++) {
			int fd;
			memcpy(&fd, CMSG_DATA(c) + sizeof(int) * (size_t)i, sizeof(fd));
			if (count < max_fds) fds[count++] = fd;
			else close(fd);
		}
	}
	if ((size_t)n != len || (hdr.msg_flags & (MSG_TRUNC | MSG_CTRUNC))) {
		while (count > 0) close(fds[--count]);
		errno = EBADMSG;
		return -1;
	}
	return count;
}

int broker_ipc_check_memfd(int fd, size_t bytes)
{
	const int required = F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL;
	struct stat st;
	int seals = fcntl(fd, F_GET_SEALS);

	if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || (size_t)st.st_size != bytes) {
		errno = EBADMSG;
		return -1;
	}
	if (seals < 0 || (seals & required) != required || (seals & F_SEAL_WRITE)) {
		errno = EPERM;
		return -1;
	}
	return 0;
}

int broker_ipc_check_eventfd(int fd)
{
	char link[32], target[32];
	ssize_t n;

	snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
	n = readlink(link, target, sizeof(target) - 1);
	if (n < 0)
		return -1;
	target[n] = '\0';
	if (strcmp(target, "anon_inode:[eventfd]") != 0) {
		errno = EBADMSG;
		return -1;
	}
	return 0;
}

static int socket_address(const char *path, struct sockaddr_un *addr)
{
	if (path == NULL || strlen(path) >= sizeof(addr->sun_path)) { errno = EINVAL; return -1; }
	memset(addr, 0, sizeof(*addr));
	addr->sun_family = AF_UNIX;
	memcpy(addr->sun_path, path, strlen(path) + 1);
	return 0;
}

int broker_ipc_connect(const char *path)
{
	struct sockaddr_un addr;
	struct stat st;

	if (socket_address(path, &addr) != 0) return -1;
	if (lstat(path, &st) != 0) return -1;
	/* Nobody but an administrator or this user may stand in for the broker. */
	if (!S_ISSOCK(st.st_mode) || (st.st_uid != 0 && st.st_uid != geteuid())) { errno = EPERM; return -1; }
	int fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
	if (fd < 0) return -1;
	if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
		int saved = errno;
		close(fd);
		errno = saved;
		return -1;
	}
	return fd;
}

int broker_ipc_listen(const char *path)
{
	struct sockaddr_un addr;
	struct stat st;

	if (socket_address(path, &addr) != 0) return -1;
	if (lstat(path, &st) == 0) {
		if (!S_ISSOCK(st.st_mode)) { errno = EEXIST; return -1; }
		/* Replace a stale socket, never one a running broker still serves. */
		int probe = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
		int live = probe >= 0 && connect(probe, (struct sockaddr *)&addr, sizeof(addr)) == 0;
		if (probe >= 0) close(probe);
		if (live) { errno = EADDRINUSE; return -1; }
		(void)unlink(path);
	}
	int fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
	if (fd < 0) return -1;
	if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 || chmod(path, 0666) != 0 ||
	    listen(fd, 64) != 0) {
		int saved = errno;
		close(fd);
		errno = saved;
		return -1;
	}
	return fd;
}
