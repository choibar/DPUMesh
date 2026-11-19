/* Pure signed-topology body parser, imported from jukebox. The caller owns
 * immutable tables and controls their publication/lifetime. */
#include "peer_topology.h"
#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const char *
dmesh_topology_result_name(enum dmesh_topology_result result)
{
    switch (result) {
    case DMESH_TOPOLOGY_UNCHANGED: return "unchanged";
    case DMESH_TOPOLOGY_SAME_VERSION: return "unchanged";
    case DMESH_TOPOLOGY_ADOPTED: return "adopted";
    case DMESH_TOPOLOGY_UNREADABLE: return "unreadable";
    case DMESH_TOPOLOGY_MALFORMED: return "malformed";
    case DMESH_TOPOLOGY_ROLLBACK: return "rollback";
    case DMESH_TOPOLOGY_OVERFLOW: return "overflow";
    case DMESH_TOPOLOGY_UNSIGNED: return "unsigned";
    case DMESH_TOPOLOGY_BAD_KEY_ID: return "bad-key-id";
    case DMESH_TOPOLOGY_BAD_SIG: return "bad-sig";
    }
    return "unknown";
}

/* ---- span validators; a record field is a (ptr, len) slice of one line ---- */

static int
span_dns_label_char(unsigned char c)
{
    return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-';
}

static int
span_dns_subdomain(const char *s, size_t len, size_t max_len)
{
    if (len == 0 || len > max_len ||
        !span_dns_label_char((unsigned char)s[0]) ||
        !span_dns_label_char((unsigned char)s[len - 1]) ||
        s[0] == '-' || s[len - 1] == '-')
        return 0;
    int label_start = 1;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c == '.') {
            if (label_start || i == len - 1 || s[i - 1] == '-')
                return 0;
            label_start = 1;
            continue;
        }
        if (!span_dns_label_char(c) || (label_start && c == '-'))
            return 0;
        label_start = 0;
    }
    return 1;
}

/* A single DNS label: a subdomain with no dots. */
static int
span_dns_label(const char *s, size_t len, size_t max_len)
{
    return span_dns_subdomain(s, len, max_len) &&
           memchr(s, '.', len) == NULL;
}

/* Lowercase RFC 4122 text form, exactly 36 characters. */
static int
span_pod_uid(const char *s, size_t len)
{
    if (len != 36)
        return 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)s[i];
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (c != '-')
                return 0;
        } else if (!((c >= 'a' && c <= 'f') || (c >= '0' && c <= '9'))) {
            return 0;
        }
    }
    return 1;
}

static int
span_ipv4(const char *s, size_t len, uint32_t *out_be)
{
    unsigned int octets[4];
    size_t i = 0;
    for (int o = 0; o < 4; o++) {
        unsigned int value = 0;
        size_t digits = 0;
        while (i < len && s[i] >= '0' && s[i] <= '9') {
            value = value * 10u + (unsigned int)(s[i] - '0');
            digits++;
            i++;
        }
        if (digits == 0 || digits > 3 || value > 255 ||
            (digits > 1 && s[i - digits] == '0'))
            return 0;
        octets[o] = value;
        if (o < 3) {
            if (i >= len || s[i] != '.')
                return 0;
            i++;
        }
    }
    if (i != len)
        return 0;
    /* The value carries the first octet in the most significant byte — the
     * same integer a consumer rebuilds from network-order bytes. */
    if (out_be != NULL)
        *out_be = (uint32_t)((octets[0] << 24) | (octets[1] << 16) |
                             (octets[2] << 8) | octets[3]);
    return 1;
}

static int
span_port(const char *s, size_t len, uint16_t *out)
{
    unsigned int value = 0;
    if (len == 0 || len > 5 || (len > 1 && s[0] == '0'))
        return 0;
    for (size_t i = 0; i < len; i++) {
        if (s[i] < '0' || s[i] > '9')
            return 0;
        value = value * 10u + (unsigned int)(s[i] - '0');
    }
    if (value == 0 || value > 65535)
        return 0;
    *out = (uint16_t)value;
    return 1;
}

static int
span_u64(const char *s, size_t len, uint64_t *out)
{
    uint64_t value = 0;
    if (len == 0 || len > 20 || (len > 1 && s[0] == '0'))
        return 0;
    for (size_t i = 0; i < len; i++) {
        if (s[i] < '0' || s[i] > '9')
            return 0;
        if (value > (UINT64_MAX - (uint64_t)(s[i] - '0')) / 10u)
            return 0;
        value = value * 10u + (uint64_t)(s[i] - '0');
    }
    *out = value;
    return 1;
}


static int
span_hex_bytes(const char *s, size_t len, uint8_t *out, size_t out_size)
{
    if (len != 2 * out_size)
        return 0;
    for (size_t i = 0; i < out_size; i++) {
        int hi, lo;
        unsigned char h = (unsigned char)s[2 * i], l = (unsigned char)s[2 * i + 1];
        hi = (h >= '0' && h <= '9') ? h - '0' :
             (h >= 'a' && h <= 'f') ? h - 'a' + 10 : -1;
        lo = (l >= '0' && l <= '9') ? l - '0' :
             (l >= 'a' && l <= 'f') ? l - 'a' + 10 : -1;
        if (hi < 0 || lo < 0)
            return 0;
        out[i] = (uint8_t)((hi << 4) | lo);
    }
    return 1;
}

/* Copy a span into a fixed NUL-terminated field. */
static int
span_copy(char *dst, size_t cap, const char *s, size_t len)
{
    if (len >= cap)
        return 0;
    memcpy(dst, s, len);
    dst[len] = '\0';
    return 1;
}

/* "namespace/name": both DNS labels of at most 63. */
static int
span_service_key(const char *s, size_t len, char *dst, size_t cap)
{
    const char *slash = memchr(s, '/', len);
    if (slash == NULL)
        return 0;
    size_t ns_len = (size_t)(slash - s);
    size_t name_len = len - ns_len - 1;
    if (!span_dns_label(s, ns_len, 63) ||
        !span_dns_label(slash + 1, name_len, 63))
        return 0;
    return span_copy(dst, cap, s, len);
}

/* Split one line into comma-separated fields. Returns the count, or -1 when
 * there are more than `max`. No spaces around separators, per the grammar. */
static int
split_fields(const char *line, size_t len, const char **f, size_t *fl, int max)
{
    int n = 0;
    size_t start = 0;
    for (size_t i = 0; i <= len; i++) {
        if (i == len || line[i] == ',') {
            if (n == max)
                return -1;
            f[n] = line + start;
            fl[n] = i - start;
            n++;
            start = i + 1;
        }
    }
    return n;
}

static int
compare_pod_uid(const void *a, const void *b)
{
    return strcmp(((const struct dmesh_gen_pod *)a)->uid,
                  ((const struct dmesh_gen_pod *)b)->uid);
}

static int
compare_service_key(const void *a, const void *b)
{
    return strcmp(((const struct dmesh_gen_service *)a)->key,
                  ((const struct dmesh_gen_service *)b)->key);
}

static int
compare_endpoint(const void *a, const void *b)
{
    const struct dmesh_gen_endpoint *x = a, *y = b;
    if (x->service != y->service)
        return x->service < y->service ? -1 : 1;
    return x->pod < y->pod ? -1 : x->pod > y->pod ? 1 : 0;
}

void
dmesh_topology_tables_free(struct dmesh_topology_tables *tables)
{
    if (tables == NULL)
        return;
    free(tables->nodes);
    free(tables->services);
    free(tables->pods);
    free(tables->endpoints);
    free(tables);
}

/* Re-derive the stable interning after an adoption: a Service keeps its id
 * while it persists, a departed Service frees its id, and a new Service takes
 * the lowest free one. Ids past the 128-id space stay -1 until one frees. */
static void
intern_services(struct dmesh_topology_intern *interned,
                struct dmesh_topology_tables *tables)
{
    for (int id = 0; id < DMESH_TOPOLOGY_INTERN_MAX; id++) {
        if (!interned[id].in_use)
            continue;
        int held = 0;
        for (size_t s = 0; s < tables->service_count; s++) {
            if (strcmp(tables->services[s].key, interned[id].key) == 0) {
                tables->services[s].interned = (int16_t)id;
                held = 1;
                break;
            }
        }
        if (!held) {
            interned[id].in_use = 0;
            interned[id].key[0] = '\0';
        }
    }
    for (size_t s = 0; s < tables->service_count; s++) {
        if (tables->services[s].interned >= 0)
            continue;
        for (int id = 0; id < DMESH_TOPOLOGY_INTERN_MAX; id++) {
            if (interned[id].in_use)
                continue;
            snprintf(interned[id].key, sizeof(interned[id].key), "%s",
                     tables->services[s].key);
            interned[id].in_use = 1;
            tables->services[s].interned = (int16_t)id;
            break;
        }
    }
}

enum dmesh_topology_result
dmesh_topology_parse(const char *document, size_t length,
                     struct dmesh_topology_intern *interned,
                     struct dmesh_topology_tables **out)
{
    enum dmesh_topology_result result = DMESH_TOPOLOGY_MALFORMED;
    struct dmesh_topology_tables *tables = NULL;
    size_t node_seen = 0, service_seen = 0, pod_seen = 0, endpoint_seen = 0;
    size_t protected_seen = 0;
    int have_version = 0;

    /* Pass 1: count records per kind so the tables allocate exactly, and
     * refuse anything over its bound before touching memory. */
    size_t line_start = 0;
    for (size_t i = 0; i <= length; i++) {
        if (i != length && document[i] != '\n')
            continue;
        if (i == length && line_start == i)
            break;
        /* The signed prefix ends in '\n', so a final unterminated line means
         * the document was truncated mid-line. */
        if (i == length)
            return DMESH_TOPOLOGY_MALFORMED;
        const char *line = document + line_start;
        size_t len = i - line_start;
        line_start = i + 1;
        if (len == 0)
            return DMESH_TOPOLOGY_MALFORMED;
        if (line[0] == '#') {
            if (have_version)
                return DMESH_TOPOLOGY_MALFORMED;
            continue;
        }
        if (!have_version) {
            if (len <= 8 || memcmp(line, "version=", 8) != 0)
                return DMESH_TOPOLOGY_MALFORMED;
            have_version = 1;
            continue;
        }
        if (len > 5 && memcmp(line, "node=", 5) == 0)
            node_seen++;
        else if (len > 4 && memcmp(line, "pod=", 4) == 0)
            pod_seen++;
        else if (len > 8 && memcmp(line, "service=", 8) == 0)
            service_seen++;
        else if (len > 9 && memcmp(line, "endpoint=", 9) == 0)
            endpoint_seen++;
        else if (len > 10 && memcmp(line, "protected=", 10) == 0)
            protected_seen++;
        else
            return DMESH_TOPOLOGY_MALFORMED;
    }
    if (!have_version)
        return DMESH_TOPOLOGY_MALFORMED;
    if (node_seen > DMESH_GEN_NODE_MAX || pod_seen > DMESH_GEN_POD_MAX ||
        service_seen > DMESH_GEN_SERVICE_MAX ||
        endpoint_seen > DMESH_GEN_ENDPOINT_MAX)
        return DMESH_TOPOLOGY_OVERFLOW;

    tables = calloc(1, sizeof(*tables));
    if (tables == NULL)
        return DMESH_TOPOLOGY_UNREADABLE;
    if (node_seen > 0)
        tables->nodes = calloc(node_seen, sizeof(*tables->nodes));
    if (service_seen > 0)
        tables->services = calloc(service_seen, sizeof(*tables->services));
    if (pod_seen > 0)
        tables->pods = calloc(pod_seen, sizeof(*tables->pods));
    if (endpoint_seen > 0)
        tables->endpoints = calloc(endpoint_seen, sizeof(*tables->endpoints));
    if ((node_seen > 0 && tables->nodes == NULL) ||
        (service_seen > 0 && tables->services == NULL) ||
        (pod_seen > 0 && tables->pods == NULL) ||
        (endpoint_seen > 0 && tables->endpoints == NULL)) {
        result = DMESH_TOPOLOGY_UNREADABLE;
        goto fail;
    }

    /* Endpoint and protected lines resolve against the tables, so they are
     * parsed after them; their spans are remembered here. */
    const char **endpoint_lines = NULL;
    size_t *endpoint_lens = NULL;
    const char **protected_lines = NULL;
    size_t *protected_lens = NULL;
    size_t protected_count = 0;
    if (endpoint_seen > 0) {
        endpoint_lines = calloc(endpoint_seen, sizeof(*endpoint_lines));
        endpoint_lens = calloc(endpoint_seen, sizeof(*endpoint_lens));
    }
    if (protected_seen > 0) {
        protected_lines = calloc(protected_seen, sizeof(*protected_lines));
        protected_lens = calloc(protected_seen, sizeof(*protected_lens));
    }
    if ((endpoint_seen > 0 && (endpoint_lines == NULL || endpoint_lens == NULL)) ||
        (protected_seen > 0 && (protected_lines == NULL || protected_lens == NULL))) {
        free(endpoint_lines);
        free(endpoint_lens);
        free(protected_lines);
        free(protected_lens);
        result = DMESH_TOPOLOGY_UNREADABLE;
        goto fail;
    }

    /* Pass 2: parse and validate every field. */
    line_start = 0;
    have_version = 0;
    for (size_t i = 0; i < length; i++) {
        if (document[i] != '\n')
            continue;
        const char *line = document + line_start;
        size_t len = i - line_start;
        line_start = i + 1;
        if (line[0] == '#')
            continue;
        const char *body = memchr(line, '=', len);
        size_t body_len;
        body++;
        body_len = len - (size_t)(body - line);

        const char *f[8];
        size_t fl[8];
        if (!have_version) {
            have_version = 1;
            if (!span_u64(body, body_len, &tables->version) ||
                tables->version == 0)
                goto malformed;
            continue;
        }
        if (memcmp(line, "node=", 5) == 0) {
            /* node-name,rdma-ip:port,dpu-static-pub */
            if (split_fields(body, body_len, f, fl, 8) != 3)
                goto malformed;
            struct dmesh_gen_node *node = &tables->nodes[tables->node_count];
            const char *colon = memchr(f[1], ':', fl[1]);
            if (!span_dns_subdomain(f[0], fl[0], 253) ||
                !span_copy(node->name, sizeof(node->name), f[0], fl[0]) ||
                colon == NULL ||
                !span_ipv4(f[1], (size_t)(colon - f[1]), &node->rdma_ip_be) ||
                !span_port(colon + 1, fl[1] - (size_t)(colon - f[1]) - 1,
                           &node->rdma_port) ||
                !span_hex_bytes(f[2], fl[2], node->dpu_static_public_key,
                                sizeof(node->dpu_static_public_key)))
                goto malformed;
            tables->node_count++;
        } else if (memcmp(line, "pod=", 4) == 0) {
            /* pod-uid,node-name,namespace,service-account,pod-ipv4 */
            if (split_fields(body, body_len, f, fl, 8) != 5)
                goto malformed;
            struct dmesh_gen_pod *pod = &tables->pods[tables->pod_count];
            if (!span_pod_uid(f[0], fl[0]) ||
                !span_copy(pod->uid, sizeof(pod->uid), f[0], fl[0]) ||
                !span_dns_subdomain(f[1], fl[1], 253) ||
                !span_copy(pod->node_name, sizeof(pod->node_name), f[1], fl[1]) ||
                !span_dns_label(f[2], fl[2], 63) ||
                !span_copy(pod->namespace_name, sizeof(pod->namespace_name),
                           f[2], fl[2]) ||
                !span_dns_subdomain(f[3], fl[3], 253) ||
                !span_copy(pod->service_account, sizeof(pod->service_account),
                           f[3], fl[3]) ||
                !span_ipv4(f[4], fl[4], &pod->ip_be))
                goto malformed;
            tables->pod_count++;
        } else if (memcmp(line, "service=", 8) == 0) {
            /* namespace/name,cluster-ipv4:port */
            if (split_fields(body, body_len, f, fl, 8) != 2)
                goto malformed;
            struct dmesh_gen_service *service =
                &tables->services[tables->service_count];
            const char *colon = memchr(f[1], ':', fl[1]);
            if (!span_service_key(f[0], fl[0], service->key,
                                  sizeof(service->key)) ||
                colon == NULL ||
                !span_ipv4(f[1], (size_t)(colon - f[1]),
                           &service->cluster_ip_be) ||
                !span_port(colon + 1, fl[1] - (size_t)(colon - f[1]) - 1,
                           &service->port))
                goto malformed;
            service->interned = -1;
            tables->service_count++;
        } else if (memcmp(line, "endpoint=", 9) == 0) {
            endpoint_lines[tables->endpoint_count] = body;
            endpoint_lens[tables->endpoint_count] = body_len;
            tables->endpoint_count++;
        } else {
            /* protected= */
            protected_lines[protected_count] = body;
            protected_lens[protected_count] = body_len;
            protected_count++;
        }
    }

    /* Sort, then refuse duplicates: a duplicate pod= for one UID (and one
     * service= for one Service) would make lookups ambiguous. */
    if (tables->pod_count > 1)
        qsort(tables->pods, tables->pod_count, sizeof(*tables->pods),
              compare_pod_uid);
    for (size_t p = 1; p < tables->pod_count; p++)
        if (strcmp(tables->pods[p - 1].uid, tables->pods[p].uid) == 0)
            goto malformed;
    if (tables->service_count > 1)
        qsort(tables->services, tables->service_count,
              sizeof(*tables->services), compare_service_key);
    for (size_t s = 1; s < tables->service_count; s++)
        if (strcmp(tables->services[s - 1].key, tables->services[s].key) == 0)
            goto malformed;

    /* Endpoints and protected entries name only defined Services and Pods. */
    size_t endpoint_total = tables->endpoint_count;
    tables->endpoint_count = 0;
    for (size_t e = 0; e < endpoint_total; e++) {
        const char *f2[4];
        size_t fl2[4];
        char key[sizeof(((struct dmesh_gen_service *)0)->key)];
        char uid[DMESH_POD_UID_MAX];
        if (split_fields(endpoint_lines[e], endpoint_lens[e], f2, fl2, 4) != 2 ||
            !span_service_key(f2[0], fl2[0], key, sizeof(key)) ||
            !span_pod_uid(f2[1], fl2[1]) ||
            !span_copy(uid, sizeof(uid), f2[1], fl2[1]))
            goto malformed;
        struct dmesh_gen_service probe_service;
        snprintf(probe_service.key, sizeof(probe_service.key), "%s", key);
        struct dmesh_gen_service *service = tables->service_count == 0 ? NULL :
            bsearch(&probe_service, tables->services, tables->service_count,
                    sizeof(*tables->services), compare_service_key);
        struct dmesh_gen_pod probe_pod;
        snprintf(probe_pod.uid, sizeof(probe_pod.uid), "%s", uid);
        struct dmesh_gen_pod *pod = tables->pod_count == 0 ? NULL :
            bsearch(&probe_pod, tables->pods, tables->pod_count,
                    sizeof(*tables->pods), compare_pod_uid);
        if (service == NULL || pod == NULL)
            goto malformed;
        tables->endpoints[tables->endpoint_count].service =
            (uint32_t)(service - tables->services);
        tables->endpoints[tables->endpoint_count].pod =
            (uint32_t)(pod - tables->pods);
        tables->endpoint_count++;
    }
    if (tables->endpoint_count > 1)
        qsort(tables->endpoints, tables->endpoint_count,
              sizeof(*tables->endpoints), compare_endpoint);
    for (size_t s = 0, e = 0; s < tables->service_count; s++) {
        tables->services[s].endpoint_first = (uint32_t)e;
        while (e < tables->endpoint_count &&
               tables->endpoints[e].service == s)
            e++;
        tables->services[s].endpoint_count =
            (uint32_t)(e - tables->services[s].endpoint_first);
    }

    for (size_t p = 0; p < protected_count; p++) {
        char key[sizeof(((struct dmesh_gen_service *)0)->key)];
        if (!span_service_key(protected_lines[p], protected_lens[p], key,
                              sizeof(key)))
            goto malformed;
        struct dmesh_gen_service probe_service;
        snprintf(probe_service.key, sizeof(probe_service.key), "%s", key);
        struct dmesh_gen_service *service = tables->service_count == 0 ? NULL :
            bsearch(&probe_service, tables->services, tables->service_count,
                    sizeof(*tables->services), compare_service_key);
        if (service == NULL)
            goto malformed;
        service->is_protected = 1;
    }

    if (interned != NULL)
        intern_services(interned, tables);

    free(endpoint_lines);
    free(endpoint_lens);
    free(protected_lines);
    free(protected_lens);
    *out = tables;
    return DMESH_TOPOLOGY_ADOPTED;

malformed:
    result = DMESH_TOPOLOGY_MALFORMED;
    free(endpoint_lines);
    free(endpoint_lens);
    free(protected_lines);
    free(protected_lens);
fail:
    dmesh_topology_tables_free(tables);
    return result;
}

