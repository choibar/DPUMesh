/* Server-only exports referenced by shared DPUMesh host sources. A host
 * process never owns a Comch server, so these paths cannot run here. */
#include "object.h"
#include "dma.h"

doca_error_t cleanup_dma_tasks(struct dmesh_conn *conn) { (void)conn; return DOCA_SUCCESS; }
