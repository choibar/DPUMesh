#ifndef DMESH_DPA_RUNTIME_H
#define DMESH_DPA_RUNTIME_H

#include <pthread.h>
#include <doca_dpa.h>

/* One SDK lock across the DPU's shared contexts. Never hold this across PE
 * progress, callbacks or completion waits. Worker/flow ownership remains local.
 * The registry uses this same lock, so its implementation calls the SDK raw. */
extern pthread_mutex_t dmesh_dpa_sdk_mutex;
#define DMESH_DPA_CALL(call) __extension__ ({ \
    pthread_mutex_lock(&dmesh_dpa_sdk_mutex); \
    doca_error_t dmesh_sdk_result_ = (call); \
    pthread_mutex_unlock(&dmesh_dpa_sdk_mutex); \
    dmesh_sdk_result_; \
})

struct dmesh_dpa_runtime;
doca_error_t dmesh_dpa_runtime_acquire(struct doca_dev *dev, struct doca_dpa_app *app,
                                      struct dmesh_dpa_runtime **out);
struct doca_dpa *dmesh_dpa_runtime_context(struct dmesh_dpa_runtime *runtime);
/* Caller must destroy all its children first. A failed last release preserves
 * the lease and rejects new acquisitions until cleanup succeeds. */
doca_error_t dmesh_dpa_runtime_release(struct dmesh_dpa_runtime **runtime);

#endif
