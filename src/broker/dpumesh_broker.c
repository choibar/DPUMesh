/*
 * dpumesh_broker — the host process that owns the DOCA device for DPUmesh
 * applications (docs/2026-09-29_host-broker-plan.md).
 *
 *   dpumesh_broker --listen <socket> [--pci <addr>] [--verify none|addr|pod]
 *                  [--client-mem-mib N] [--max-clients N]
 *                  [--trust-domain D] [--kubelet-root DIR] [--proc-root DIR]
 *
 * An administrator starts it (sudo or a systemd unit) with access to the
 * BlueField function; applications need only the socket. Every accepted
 * connection is served by its own forked child, which opens the device, the
 * Comch session and the registered memory for that one application and hands
 * the memory over as memfds. The parent never touches DOCA. A child exits when
 * its application disconnects, and is killed with the parent.
 *
 * The PCI address defaults to DPUMESH_PCI_ADDR. --verify sets what the broker
 * proves about each client before serving it (peer_identity.h):
 *   none  the client's claims are trusted (bare metal, benchmarks; the default)
 *   addr  its LISTEN and source addresses must be its own network namespace's
 *   pod   addr, it must run in a Pod, and its flows carry the Pod's mesh
 *         identity from the kubelet's token instead of its own claim
 * A client registers at most --client-mem-mib of pinned memory (256; 0 = no
 * limit), and at most --max-clients (64) are served at once.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <errno.h>
#include <arpa/inet.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include "broker_ipc.h"
#include "channel.h"
#include "peer_identity.h"

#define CLIENT_MEM_MIB_DEFAULT 256   /* the carrier registers ~96 MiB per channel */
#define MAX_CLIENTS_DEFAULT 64       /* Comch sessions per DPU server name */

static volatile sig_atomic_t stop, children;

static void on_signal(int signo)
{
	(void)signo;
	stop = 1;
}

static void on_child(int signo)
{
	int saved = errno;
	(void)signo;
	while (waitpid(-1, NULL, WNOHANG) > 0)
		--children;
	errno = saved;
}

static int usage(void)
{
	fprintf(stderr, "usage: dpumesh_broker --listen <socket> [--pci <addr>] [--verify none|addr|pod]\n"
			"                      [--client-mem-mib N] [--max-clients N] [--trust-domain D]\n"
			"                      [--kubelet-root DIR] [--proc-root DIR]  (default pci: $DPUMESH_PCI_ADDR)\n");
	return 2;
}

/* Child: establish the client, then serve it under the rules that follow. */
static int serve_client(int sock, const char *pci, const sigset_t *term, const struct peer_policy *policy,
			uint64_t mem_limit)
{
	struct peer_identity id;
	struct broker_peer_rules rules = { .mem_limit = mem_limit };

	if (peer_identity_read(sock, policy, &id) != 0) {
		rules.refuse = errno == EPERM ? EPERM : EACCES;
		fprintf(stderr, "dpumesh_broker[%d]: client not established (%s): refused\n", (int)getpid(),
			strerror(errno));
	} else {
		char addrs[PEER_MAX_ADDRS * 16 + 1] = "";
		for (int i = 0; i < id.naddrs; ++i) {
			char a[INET_ADDRSTRLEN];
			struct in_addr in = { .s_addr = id.addrs[i] };
			inet_ntop(AF_INET, &in, a, sizeof(a));
			snprintf(addrs + strlen(addrs), sizeof(addrs) - strlen(addrs), "%s%s", i ? "," : "", a);
		}
		rules.verify = (enum broker_verify)policy->verify;
		rules.addrs = id.addrs;
		rules.naddrs = id.naddrs;
		rules.workload = id.workload;
		fprintf(stderr, "dpumesh_broker[%d]: client pid %d uid %d verify %s%s%s%s%s%s%s\n", (int)getpid(),
			(int)id.pid, (int)id.uid, peer_verify_name(policy->verify), *addrs ? " addrs " : "", addrs,
			id.pod_uid[0] ? " pod " : "", id.pod_uid, id.workload[0] ? " workload " : "", id.workload);
	}
	return channel_broker_serve(sock, pci, term, &rules);
}

int main(int argc, char **argv)
{
	const char *path = NULL, *pci = getenv("DPUMESH_PCI_ADDR");
	struct sigaction action = { .sa_handler = on_signal }, reap = { .sa_handler = on_child };
	struct peer_policy policy = { .verify = PEER_VERIFY_NONE, .proc_root = "/proc",
				      .kubelet_root = "/var/lib/kubelet", .trust_domain = "cluster.local" };
	long mem_mib = CLIENT_MEM_MIB_DEFAULT, max_clients = MAX_CLIENTS_DEFAULT;
	int listener;

	for (int i = 1; i + 1 < argc; i += 2) {
		const char *arg = argv[i], *val = argv[i + 1];
		char *end = "";
		int verify = 0;
		if (strcmp(arg, "--listen") == 0)
			path = val;
		else if (strcmp(arg, "--pci") == 0)
			pci = val;
		else if (strcmp(arg, "--verify") == 0 && (verify = peer_verify_parse(val)) >= 0)
			policy.verify = (enum peer_verify)verify;
		else if (strcmp(arg, "--client-mem-mib") == 0)
			mem_mib = strtol(val, &end, 10);
		else if (strcmp(arg, "--max-clients") == 0)
			max_clients = strtol(val, &end, 10);
		else if (strcmp(arg, "--trust-domain") == 0)
			policy.trust_domain = val;
		else if (strcmp(arg, "--kubelet-root") == 0)
			policy.kubelet_root = val;
		else if (strcmp(arg, "--proc-root") == 0)
			policy.proc_root = val;
		else
			return usage();
		if (*end)
			return usage();
	}
	if (argc % 2 == 0 || mem_mib < 0 || max_clients < 1)
		return usage();
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
	sigemptyset(&reap.sa_mask);
	reap.sa_flags = SA_NOCLDSTOP;
	sigaction(SIGCHLD, &reap, NULL);
	fprintf(stderr, "dpumesh_broker: pid %d serving %s on %s (verify %s, %ld MiB and %ld clients at most)\n",
		(int)getpid(), pci, path, peer_verify_name(policy.verify), mem_mib, max_clients);

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
		/* The count changes only with SIGCHLD blocked, in the handler or here. */
		sigset_t chld, saved;
		sigemptyset(&chld);
		sigaddset(&chld, SIGCHLD);
		sigprocmask(SIG_BLOCK, &chld, &saved);
		if (children >= max_clients) {
			sigprocmask(SIG_SETMASK, &saved, NULL);
			fprintf(stderr, "dpumesh_broker: %ld clients served already: refusing one\n", max_clients);
			close(sock);
			continue;
		}
		pid_t pid = fork();
		if (pid == 0) {
			close(listener);
			/* A broker must not outlive the daemon an administrator stopped.
			 * Until the child's own handlers are in, SIGTERM stays blocked
			 * (compare with the saved pid: the daemon may be PID 1). */
			sigprocmask(SIG_SETMASK, &saved, NULL);
			sigset_t term;
			sigemptyset(&term);
			sigaddset(&term, SIGTERM);
			sigaddset(&term, SIGINT);
			sigprocmask(SIG_BLOCK, &term, NULL);
			if (prctl(PR_SET_PDEATHSIG, SIGTERM) != 0 || getppid() != self)
				_exit(1);
			signal(SIGCHLD, SIG_DFL);
			int rc = serve_client(sock, pci, &term, &policy, (uint64_t)mem_mib << 20);
			fprintf(stderr, "dpumesh_broker[%d]: client done (%s)\n", (int)getpid(),
				rc == 0 ? "clean" : "cleanup failed");
			fflush(NULL); /* DOCA logs to stdout */
			_exit(rc == 0 ? 0 : 1);
		}
		if (pid < 0)
			fprintf(stderr, "dpumesh_broker: fork: %s\n", strerror(errno));
		else
			++children;
		sigprocmask(SIG_SETMASK, &saved, NULL);
		close(sock);
	}
	close(listener);
	unlink(path);
	return 0;
}
