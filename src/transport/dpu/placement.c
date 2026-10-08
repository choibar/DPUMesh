#include "placement.h"
#include <limits.h>

static int choose(const struct dmesh_worker_load *workers, size_t count,
                  size_t cursor, bool least)
{
    int selected = -1;
    unsigned best = UINT_MAX;
    for (size_t offset = 0; offset < count; ++offset) {
        size_t i = (cursor + offset) % count;
        const struct dmesh_worker_load *w = &workers[i];
        if (!w->eligible || w->assigned >= w->capacity)
            continue;
        /* assigned includes opening reservations and demand-created backends. */
        if (w->assigned < best) {
            selected = (int)i;
            best = w->assigned;
            if (!least) break;
        }
    }
    return selected;
}
static int least(const struct dmesh_flow_spec *f, const struct dmesh_worker_load *w,
                 size_t n, size_t cursor, void *context)
{
    (void)f; (void)context;
    return choose(w, n, cursor, true);
}
static int round_robin(const struct dmesh_flow_spec *f, const struct dmesh_worker_load *w,
                       size_t n, size_t cursor, void *context)
{
    (void)f; (void)context;
    return choose(w, n, cursor, false);
}
const struct dmesh_placement_policy dmesh_placement_least_flows = {.select = least};
const struct dmesh_placement_policy dmesh_placement_round_robin = {.select = round_robin};
