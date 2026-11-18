#ifndef DMESH_TEST_PEER_DPA_SDK_H
#define DMESH_TEST_PEER_DPA_SDK_H

/* Host substitutes for the device calls used by peer_write_imm_dpa.c.
 * Only this test's compile command places these headers before the SDK. */
#include <stdint.h>

typedef uint64_t doca_dpa_dev_completion_element_t;
enum {
    DOCA_DPA_DEV_COMP_SEND = 0,
    DOCA_DPA_DEV_COMP_RECV_RDMA_WRITE_IMM = 1,
    DOCA_DPA_DEV_COMP_RECV_SEND = 2,
    DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH = 1u,
    DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS = 2u,
};

void doca_dpa_dev_thread_notify(uint64_t);
void doca_dpa_dev_thread_reschedule(void);
void __dpa_thread_window_writeback(void);
void __dpa_thread_window_read_inv(void);
void doca_dpa_dev_device_set(uint64_t device);
uint64_t doca_dpa_dev_buf_array_get_buf(uint64_t array, unsigned index);
uint64_t doca_dpa_dev_buf_get_external_ptr(uint64_t buffer);
int doca_dpa_dev_get_completion(uint64_t cq, doca_dpa_dev_completion_element_t *element);
void doca_dpa_dev_completion_ack(uint64_t cq, unsigned count);
uint32_t doca_dpa_dev_completion_element_get_qpn(doca_dpa_dev_completion_element_t element);
uint32_t doca_dpa_dev_completion_element_get_received_bytes(doca_dpa_dev_completion_element_t element);
uint32_t doca_dpa_dev_get_completion_type(doca_dpa_dev_completion_element_t element);
uint32_t doca_dpa_dev_get_completion_immediate(doca_dpa_dev_completion_element_t element);
uint32_t doca_dpa_dev_rdma_completion_get_wr_index(doca_dpa_dev_completion_element_t element);
int doca_dpa_dev_comch_producer_is_consumer_empty(uint64_t producer, unsigned consumer);
void doca_dpa_dev_comch_producer_post_send_imm_only(uint64_t producer, unsigned consumer,
                                                 const uint8_t *data, uint32_t bytes,
                                                 unsigned flags);
void doca_dpa_dev_rdma_post_write(uint64_t qp, uint32_t connection, uint32_t rkey,
                                 uint64_t remote, uint32_t key, uint64_t source,
                                 uint32_t bytes, unsigned flags);
void doca_dpa_dev_rdma_post_write_imm(uint64_t qp, uint32_t connection, uint32_t rkey,
                                     uint64_t remote, uint32_t key, uint64_t source,
                                     uint32_t bytes, uint32_t immediate, unsigned flags);
void doca_dpa_dev_rdma_post_receive(uint64_t qp, uint32_t key, uint64_t source, uint32_t bytes);
void doca_dpa_dev_rdma_receive_ack(uint64_t qp, unsigned count);
void doca_dpa_dev_rdma_post_send(uint64_t qp, uint32_t connection, uint32_t key,
                                uint64_t source, uint32_t bytes, unsigned flags);

#endif
