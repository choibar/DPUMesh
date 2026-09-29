/*
 * dpumesh_broker — the host process that owns the DOCA device for DPUmesh
 * applications (docs/2026-09-29_host-broker-plan.md).
 *
 *   dpumesh_broker --listen <socket> [--pci <addr>]
 *
 * An administrator starts it (sudo or a systemd unit) with access to the
 * BlueField function; applications need only the socket. Every accepted
 * connection is served by its own forked child, which opens the device, the
 * Comch session and the registered memory for that one application and hands
 * the memory over as memfds. The parent never touches DOCA. A child exits when
 * its application disconnects, and is killed with the parent.
 *
 * The PCI address defaults to DPUMESH_PCI_ADDR. Pod authentication is not
 * decided yet: the peer credentials are only logged, at the point where a
 * check would go.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <unistd.h>

#include "broker_ipc.h"
#include "channel.h"

static volatile sig_atomic_t stop;

static void on_signal(int signo)
{
	(void)signo;
	stop = 1;
}

static int usage(void)
{
	fprintf(stderr, "usage: dpumesh_broker --listen <socket> [--pci <addr>]  (default pci: $DPUMESH_PCI_ADDR)\n");
	return 2;
}

int main(int argc, char **argv)
{
	const char *path = NULL, *pci = getenv("DPUMESH_PCI_ADDR");
	struct sigaction action = { .sa_handler = on_signal };
	int listener;

	for (int i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--listen") == 0 && i + 1 < argc)
			path = argv[++i];
		else if (strcmp(argv[i], "--pci") == 0 && i + 1 < argc)
			pci = argv[++i];
		else
			return usage();
	}
	if (path == NULL || pci == NULL || *pci == '\0')
		return usage();

	listener = broker_ipc_listen(path);
	if (listener < 0) {
		fprintf(stderr, "dpumesh_broker: listen on %s: %s\n", path, strerror(errno));
		return 1;
	}
	sigemptyset(&action.sa_mask);
	sigaction(SIGTERM, &action, NULL);
	sigaction(SIGINT, &action, NULL);
	signal(SIGCHLD, SIG_IGN); /* children are reaped by the kernel */
	fprintf(stderr, "dpumesh_broker: pid %d serving %s on %s\n", (int)getpid(), pci, path);

	const pid_t self = getpid();
	while (!stop) {
		int sock = accept4(listener, NULL, NULL, SOCK_CLOEXEC);
		if (sock < 0) {
			if (errno == EINTR || errno == ECONNABORTED) continue;
			fprintf(stderr, "dpumesh_broker: accept: %s\n", strerror(errno));
			/* Out of fds or memory for a moment: exiting would take every
			 * served application down with the children. */
			if (errno == EMFILE || errno == ENFILE || errno == ENOBUFS || errno == ENOMEM) {
				usleep(100 * 1000);
				continue;
			}
			break;
		}
		struct ucred peer = {0};
		socklen_t len = sizeof(peer);
		(void)getsockopt(sock, SOL_SOCKET, SO_PEERCRED, &peer, &len);

		pid_t pid = fork();
		if (pid == 0) {
			close(listener);
			/* A broker must not outlive the daemon an administrator stopped.
			 * Until the child's own handlers are in, SIGTERM stays blocked
			 * (compare with the saved pid: the daemon may be PID 1). */
			sigset_t term;
			sigemptyset(&term);
			sigaddset(&term, SIGTERM);
			sigaddset(&term, SIGINT);
			sigprocmask(SIG_BLOCK, &term, NULL);
			if (prctl(PR_SET_PDEATHSIG, SIGTERM) != 0 || getppid() != self)
				_exit(1);
			signal(SIGCHLD, SIG_DFL);
			fprintf(stderr, "dpumesh_broker[%d]: client pid %d uid %d\n", (int)getpid(), (int)peer.pid, (int)peer.uid);
			int rc = channel_broker_serve(sock, pci, &term);
			fprintf(stderr, "dpumesh_broker[%d]: client pid %d done (%s)\n", (int)getpid(), (int)peer.pid,
				rc == 0 ? "clean" : "cleanup failed");
			fflush(NULL); /* DOCA logs to stdout */
			_exit(rc == 0 ? 0 : 1);
		}
		if (pid < 0)
			fprintf(stderr, "dpumesh_broker: fork: %s\n", strerror(errno));
		close(sock);
	}
	close(listener);
	unlink(path);
	return 0;
}
