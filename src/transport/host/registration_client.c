#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "registration_client.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

int dmesh_registration_client_begin(struct dmesh_registration_client *c, const uint8_t nonce[32])
{
    if (!c || !nonce || c->state)
        return EINVAL;

    const char *path = getenv("DPUMESH_REGISTRATION_SOCKET");
    if (!path || !*path)
        return EACCES;
    struct sockaddr_un address = {.sun_family = AF_UNIX};
    if (strlen(path) >= sizeof(address.sun_path))
        return ENAMETOOLONG;
    strcpy(address.sun_path, path);

    c->fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (c->fd < 0)
        return errno;
    if (connect(c->fd, (struct sockaddr *)&address, sizeof(address))) {
        int error = errno;
        close(c->fd);
        c->fd = -1;
        return error;
    }

    struct ucred cred;
    socklen_t length = sizeof(cred);
    if (getsockopt(c->fd, SOL_SOCKET, SO_PEERCRED, &cred, &length) || cred.uid != 0) {
        close(c->fd);
        c->fd = -1;
        return EACCES;
    }
    memcpy(c->nonce, nonce, sizeof(c->nonce));
    c->state = 1;
    return 0;
}

int dmesh_registration_client_progress(struct dmesh_registration_client *c)
{
    if (!c || !c->state)
        return -EINVAL;
    if (c->state == 3)
        return 1;

    if (c->state == 1) {
        struct {
            uint8_t magic[8], nonce[32];
            char service[64];
        } request = {.magic = {'D','M','N','A','T','I','V','1'}};
        const char *service = getenv("DPUMESH_SERVICE_NAME");
        if (service && strlen(service) >= sizeof(request.service))
            return -EINVAL;
        if (service)
            strcpy(request.service, service);
        memcpy(request.nonce, c->nonce, sizeof(request.nonce));

        ssize_t n = send(c->fd, &request, sizeof(request), MSG_NOSIGNAL);
        if (n < 0)
            return errno == EAGAIN || errno == EINTR ? 0 : -errno;
        if ((size_t)n != sizeof(request))
            return -EPROTO;
        c->state = 2;
    }

    ssize_t n = recv(c->fd, &c->assertion, sizeof(c->assertion), MSG_TRUNC);
    if (n < 0)
        return errno == EAGAIN || errno == EINTR ? 0 : -errno;
    if (n != sizeof(c->assertion) || c->assertion.type != DMESH_MSG_REG_ASSERTION ||
        c->assertion.version != DMESH_LOCAL_VERSION || c->assertion.reserved[0] ||
        c->assertion.reserved[1] || memcmp(c->nonce, c->assertion.identity.nonce, 32))
        return -EACCES;

    c->state = 3;
    /* Keep the socket until the session is destroyed. The agent monitors
     * EOF/process exit/Pod deletion to revoke this exact registration. */
    return 1;
}

void dmesh_registration_client_close(struct dmesh_registration_client *c)
{
    if (!c)
        return;
    if (c->state)
        close(c->fd);
    memset(c, 0, sizeof(*c));
    c->fd = -1;
}
