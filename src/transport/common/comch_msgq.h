#ifndef COMCH_MSGQ_H
#define COMCH_MSGQ_H

#include <doca_comch.h>
#include <doca_error.h>
#include <doca_comch_msgq.h>

struct doca_dev;
struct dmesh_dpa_endpoint;
doca_error_t dmesh_dpa_endpoint_init_comch(struct dmesh_dpa_endpoint *ep, struct doca_dev *dev);
struct dmesh_conn;
struct doca_pe;

doca_error_t
init_comch_dpa_msgq(struct dmesh_conn *conn, struct doca_pe *pe);

#endif // COMCH_MSGQ_H