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
/* The worker whose client flows retire the fewest instructions per second,
 * measured plus assumed for flows not yet measured
 * (DMESH_FLOW_PLACEMENT=least-instr); ties, such as before the first sample
 * or without instruction counting, go to the fewest assigned flows. */
static int least_instructions(const struct dmesh_flow_spec *f, const struct dmesh_worker_load *w,
                              size_t n, size_t cursor, void *context)
{
    (void)f; (void)context;
    int selected = -1;
    uint64_t best = 0;
    for (size_t offset = 0; offset < n; ++offset) {
        size_t i = (cursor + offset) % n;
        if (!w[i].eligible || w[i].assigned >= w[i].capacity) continue;
        uint64_t load = w[i].flow_instr_ps + w[i].pending_instr_ps;
        if (selected < 0 || load < best ||
            (load == best && w[i].assigned < w[selected].assigned)) {
            selected = (int)i;
            best = load;
        }
    }
    return selected;
}
/* Test policy: the first eligible worker, whatever its load. It packs every
 * flow onto one worker, which shows the dispatcher's svc-to-svc pair rule
 * spreading a pair regardless of the policy beneath it. */
static int first_eligible(const struct dmesh_flow_spec *f, const struct dmesh_worker_load *w,
                          size_t n, size_t cursor, void *context)
{
    (void)f; (void)cursor; (void)context;
    return choose(w, n, 0, false);
}
const struct dmesh_placement_policy dmesh_placement_least_flows = {.select = least};
const struct dmesh_placement_policy dmesh_placement_round_robin = {.select = round_robin};
const struct dmesh_placement_policy dmesh_placement_least_instructions = {.select = least_instructions};
const struct dmesh_placement_policy dmesh_placement_first_eligible = {.select = first_eligible};
