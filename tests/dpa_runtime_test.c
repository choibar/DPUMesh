#include <assert.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include "../src/transport/common/dpa_runtime.c"

/* Exercise the actual registry concurrently without creating device resources. */
struct fake_dev { char pci[DOCA_DEVINFO_PCI_ADDR_SIZE]; unsigned opens, closes; };
struct fake_dpa { struct fake_dev *dev; bool started; };
static unsigned creates, starts, stops, destroys;
static bool fail_start, fail_stop, fail_destroy, fail_close, fatal;
static struct fake_dev devices[2] = {{.pci="0000:03:00.0"}, {.pci="0000:03:00.1"}};
static char app_storage;
static struct doca_dpa_app *app = (void *)&app_storage;

struct doca_devinfo *doca_dev_as_devinfo(const struct doca_dev *dev) { return (void *)dev; }
doca_error_t doca_devinfo_get_pci_addr_str(const struct doca_devinfo *info, char *pci)
{
    memcpy(pci, ((const struct fake_dev *)info)->pci, DOCA_DEVINFO_PCI_ADDR_SIZE);
    return DOCA_SUCCESS;
}
doca_error_t doca_dev_open(struct doca_devinfo *info, struct doca_dev **dev)
{
    ++((struct fake_dev *)info)->opens;
    *dev = (void *)info;
    return DOCA_SUCCESS;
}
doca_error_t doca_dev_close(struct doca_dev *dev)
{
    if (fail_close) return DOCA_ERROR_IN_USE;
    ++((struct fake_dev *)dev)->closes;
    return DOCA_SUCCESS;
}
doca_error_t doca_dpa_create(struct doca_dev *dev, struct doca_dpa **out)
{
    struct fake_dpa *dpa = calloc(1, sizeof(*dpa));
    assert(dpa);
    dpa->dev = (void *)dev;
    *out = (void *)dpa;
    ++creates;
    return DOCA_SUCCESS;
}
doca_error_t doca_dpa_set_app(struct doca_dpa *dpa, struct doca_dpa_app *a)
{ assert(dpa && a == app); return DOCA_SUCCESS; }
doca_error_t doca_dpa_start(struct doca_dpa *dpa)
{
    if (fail_start) return DOCA_ERROR_DRIVER;
    ((struct fake_dpa *)dpa)->started = true;
    ++starts;
    return DOCA_SUCCESS;
}
doca_error_t doca_dpa_stop(struct doca_dpa *dpa)
{
    if (fail_stop) return DOCA_ERROR_IN_USE;
    assert(((struct fake_dpa *)dpa)->started);
    ((struct fake_dpa *)dpa)->started = false;
    ++stops;
    return DOCA_SUCCESS;
}
doca_error_t doca_dpa_destroy(struct doca_dpa *dpa)
{
    if (fail_destroy) return DOCA_ERROR_DRIVER;
    assert(!((struct fake_dpa *)dpa)->started);
    ++destroys;
    free(dpa);
    return DOCA_SUCCESS;
}
doca_error_t doca_dpa_peek_at_last_error(const struct doca_dpa *dpa)
{ assert(dpa); return fatal ? DOCA_ERROR_BAD_STATE : DOCA_SUCCESS; }

#define WORKERS 16
static pthread_barrier_t barrier;
static struct doca_dpa *contexts[WORKERS];
static atomic_uint in_sdk, ops;
static doca_error_t mock_operation(void)
{
    assert(atomic_fetch_add(&in_sdk, 1) == 0);
    atomic_fetch_add(&ops, 1);
    assert(atomic_fetch_sub(&in_sdk, 1) == 1);
    return DOCA_SUCCESS;
}
static void *worker(void *arg)
{
    size_t id = (size_t)arg;
    struct dmesh_dpa_runtime *r = NULL;
    assert(dmesh_dpa_runtime_acquire((void *)&devices[0], app, &r) == DOCA_SUCCESS);
    contexts[id] = dmesh_dpa_runtime_context(r);
    pthread_barrier_wait(&barrier);
    for (unsigned i = 0; i < 1000; ++i) assert(DMESH_DPA_CALL(mock_operation()) == DOCA_SUCCESS);
    pthread_barrier_wait(&barrier);
    assert(dmesh_dpa_runtime_release(&r) == DOCA_SUCCESS && !r);
    return NULL;
}
int main(void)
{
    pthread_t threads[WORKERS];
    pthread_barrier_init(&barrier, NULL, WORKERS);
    for (size_t i = 0; i < WORKERS; ++i) assert(pthread_create(&threads[i], NULL, worker, (void *)i) == 0);
    for (unsigned i = 0; i < WORKERS; ++i) pthread_join(threads[i], NULL);
    pthread_barrier_destroy(&barrier);
    for (unsigned i = 1; i < WORKERS; ++i) assert(contexts[i] == contexts[0]);
    assert(creates == 1 && starts == 1 && stops == 1 && destroys == 1);
    assert(ops == WORKERS * 1000 && runtimes == NULL);
    assert(devices[0].opens == 1 && devices[0].closes == 1);

    struct fake_dev alias = {.pci="0000:03:00.0"};
    struct dmesh_dpa_runtime *a = NULL, *b = NULL, *c = NULL;
    assert(dmesh_dpa_runtime_acquire((void *)&devices[0], app, &a) == DOCA_SUCCESS);
    assert(dmesh_dpa_runtime_acquire((void *)&alias, app, &b) == DOCA_SUCCESS);
    assert(a == b && a->refs == 2);
    assert(dmesh_dpa_runtime_release(&a) == DOCA_SUCCESS);
    assert(((struct fake_dpa *)b->dpa)->started); /* first worker cannot stop peer */
    assert(dmesh_dpa_runtime_acquire((void *)&devices[1], app, &c) == DOCA_SUCCESS);
    assert(c->dpa != b->dpa); /* PFs never alias */
    assert(dmesh_dpa_runtime_release(&c) == DOCA_SUCCESS);
    fatal = true;
    assert(dmesh_dpa_runtime_acquire((void *)&devices[0], app, &a) == DOCA_ERROR_BAD_STATE && !a);
    fatal = false;

    fail_stop = true;
    assert(dmesh_dpa_runtime_release(&b) == DOCA_ERROR_IN_USE && b);
    assert(dmesh_dpa_runtime_acquire((void *)&devices[0], app, &a) == DOCA_ERROR_AGAIN && !a);
    fail_stop = false;
    fail_destroy = true;
    assert(dmesh_dpa_runtime_release(&b) == DOCA_ERROR_DRIVER && b && !b->started);
    fail_destroy = false;
    fail_close = true;
    assert(dmesh_dpa_runtime_release(&b) == DOCA_ERROR_IN_USE && b && !b->dpa);
    fail_close = false;
    assert(dmesh_dpa_runtime_release(&b) == DOCA_SUCCESS && !b && !runtimes);

    fail_start = fail_destroy = true;
    assert(dmesh_dpa_runtime_acquire((void *)&devices[0], app, &a) == DOCA_ERROR_DRIVER && !a);
    assert(runtimes && runtimes->refs == 0 && runtimes->retiring);
    unsigned before = creates;
    assert(dmesh_dpa_runtime_acquire((void *)&devices[0], app, &a) == DOCA_ERROR_DRIVER);
    assert(creates == before); /* no duplicate context over an orphan */
    fail_start = fail_destroy = false;
    assert(dmesh_dpa_runtime_acquire((void *)&devices[0], app, &a) == DOCA_SUCCESS);
    assert(dmesh_dpa_runtime_release(&a) == DOCA_SUCCESS && !runtimes);
    assert(creates == destroys && starts == stops);
    for (unsigned i = 0; i < 2; ++i) assert(devices[i].opens == devices[i].closes);
    puts("dpa_runtime_test: PASS (16 concurrent workers, SDK exclusion, PF isolation, cleanup retries)");
}
