#include "dpa_runtime.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <doca_dev.h>
#include <doca_log.h>

DOCA_LOG_REGISTER(DPA_RUNTIME);

pthread_mutex_t dmesh_dpa_sdk_mutex = PTHREAD_MUTEX_INITIALIZER;

struct dmesh_dpa_runtime {
    char pci[DOCA_DEVINFO_PCI_ADDR_SIZE];
    struct doca_dpa_app *app;
    struct doca_dev *dev; /* Own open reference, independent of every worker. */
    struct doca_dpa *dpa;
    unsigned refs;
    bool started, retiring;
    struct dmesh_dpa_runtime *next;
};
static struct dmesh_dpa_runtime *runtimes;

static doca_error_t retire(struct dmesh_dpa_runtime *r)
{
    doca_error_t result;
    r->retiring = true;
    if (r->started) {
        result = doca_dpa_stop(r->dpa);
        if (result != DOCA_SUCCESS) return result;
        r->started = false;
    }
    if (r->dpa) {
        result = doca_dpa_destroy(r->dpa);
        if (result != DOCA_SUCCESS) return result;
        r->dpa = NULL;
    }
    if (r->dev) {
        result = doca_dev_close(r->dev);
        if (result != DOCA_SUCCESS) return result;
        r->dev = NULL;
    }
    return DOCA_SUCCESS;
}

doca_error_t dmesh_dpa_runtime_acquire(struct doca_dev *dev, struct doca_dpa_app *app,
                                      struct dmesh_dpa_runtime **out)
{
    struct dmesh_dpa_runtime *r, **link;
    char pci[DOCA_DEVINFO_PCI_ADDR_SIZE] = {0};
    doca_error_t result;
    if (!dev || !app || !out || *out) return DOCA_ERROR_INVALID_VALUE;
    pthread_mutex_lock(&dmesh_dpa_sdk_mutex);
    result = doca_devinfo_get_pci_addr_str(doca_dev_as_devinfo(dev), pci);
    if (result != DOCA_SUCCESS) goto done;
    for (link = &runtimes; (r = *link) != NULL; link = &r->next) {
        if (strcmp(r->pci, pci) != 0 || r->app != app) continue;
        /* An unsuccessful initialization may have retained SDK resources. */
        if (r->refs == 0) {
            result = retire(r);
            if (result != DOCA_SUCCESS) goto done;
            *link = r->next;
            free(r);
            break;
        }
        if (r->retiring) { result = DOCA_ERROR_AGAIN; goto done; }
        result = doca_dpa_peek_at_last_error(r->dpa);
        if (result != DOCA_SUCCESS) goto done;
        ++r->refs;
        *out = r;
        DOCA_LOG_INFO("Shared DPA context %s acquired: workers=%u", pci, r->refs);
        goto done;
    }
    r = calloc(1, sizeof(*r));
    if (!r) { result = DOCA_ERROR_NO_MEMORY; goto done; }
    memcpy(r->pci, pci, sizeof(pci));
    r->app = app;
    r->next = runtimes;
    runtimes = r;
    result = doca_dev_open(doca_dev_as_devinfo(dev), &r->dev);
    if (result == DOCA_SUCCESS) result = doca_dpa_create(r->dev, &r->dpa);
    if (result == DOCA_SUCCESS) result = doca_dpa_set_app(r->dpa, app);
    if (result == DOCA_SUCCESS) result = doca_dpa_start(r->dpa);
    if (result != DOCA_SUCCESS) {
        if (retire(r) == DOCA_SUCCESS) {
            runtimes = r->next;
            free(r);
        } /* Otherwise keep the failed resource domain for the next retry. */
        goto done;
    }
    r->started = true;
    r->refs = 1;
    *out = r;
    DOCA_LOG_INFO("Created shared DPA context %s: workers=1", pci);
done:
    pthread_mutex_unlock(&dmesh_dpa_sdk_mutex);
    return result;
}

struct doca_dpa *dmesh_dpa_runtime_context(struct dmesh_dpa_runtime *r)
{
    /* The caller's lease prevents destruction while this pointer is in use. */
    return r ? r->dpa : NULL;
}

doca_error_t dmesh_dpa_runtime_release(struct dmesh_dpa_runtime **handle)
{
    struct dmesh_dpa_runtime *r, **link;
    doca_error_t result = DOCA_SUCCESS;
    if (!handle || !*handle) return DOCA_SUCCESS;
    pthread_mutex_lock(&dmesh_dpa_sdk_mutex);
    r = *handle;
    if (r->refs == 1) {
        result = retire(r);
        if (result != DOCA_SUCCESS) goto done;
        for (link = &runtimes; *link != r; link = &(*link)->next) {}
        *link = r->next;
        DOCA_LOG_INFO("Destroyed shared DPA context %s: workers=0", r->pci);
        free(r);
    } else {
        --r->refs;
    }
    *handle = NULL;
done:
    pthread_mutex_unlock(&dmesh_dpa_sdk_mutex);
    return result;
}
