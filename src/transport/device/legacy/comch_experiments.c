/* Archived Comch experiments, previously unused in dpa_kernel.c.
 * Kept for reference; deliberately excluded from the device build.
 * These helpers are not the completion-fenced DPUMesh datapath.
 */
#include "doca_dpa_dev.h"
#include "doca_dpa_dev_comch_msgq.h"
#include "doca_dpa_dev_buf.h"
#include "dpaintrin.h"
#include "dpa_common.h"

struct legacy_comch_arg {
    uint64_t dpa_consumer_comp, dpa_producer_comp;
    uint64_t dpa_producer, dpa_consumer;
};

static inline uint32_t bswap32(uint32_t x) {
    return ((x & 0x000000FF) << 24) |
           ((x & 0x0000FF00) << 8)  |
           ((x & 0x00FF0000) >> 8)  |

           ((x & 0xFF000000) >> 24);
}

static void send_msgs(struct legacy_comch_arg *thread_arg, int num_msg)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    doca_dpa_dev_completion_element_t comp;
    struct comch_msg msg;
    uint64_t start, end, tick;
    doca_dpa_dev_completion_type_t t;
    tick = __dpa_thread_time();

    DOCA_DPA_DEV_LOG_INFO("tick frequency: %lu\n", tick);
    for (int i = 0; i < num_msg; i++) {
        start =  __dpa_thread_time();
        doca_dpa_dev_comch_producer_post_send_imm_only(producer,
                                                   /*consumer_id=*/1,
                                                   (uint8_t *)&msg,
                                                   sizeof(struct comch_msg),
                                                   DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
                                    //    DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS);

        while (doca_dpa_dev_get_completion(thread_arg->dpa_producer_comp, &comp) == 0) {
        }

        end =  __dpa_thread_time();
        t = doca_dpa_dev_get_completion_type(comp);

        DOCA_DPA_DEV_LOG_INFO("type: %d, cycles: %lu\n", t, end - start);
    }
}
static void handle_dpu_msg(struct legacy_comch_arg *thread_arg, const struct comch_msg *msg)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    // doca_dpa_dev_comch_producer_t producer;
    doca_dpa_dev_completion_element_t comp;
    uint64_t start, end;

    switch(msg->type) {
        case COMCH_MSG_TYPE_DMA_REQ:
            struct comch_dma_req_msg *dma_msg = (struct comch_dma_req_msg *)msg;
            if (dma_msg->dpa_producer)
                producer = dma_msg->dpa_producer;
            // producer = dma_msg->dpa_producer;
            DOCA_DPA_DEV_LOG_INFO("Received DMA REQ msg from host: producer=0x%lx, src_mmap=%u, dst_mmap=%u, src_addr=0x%lx, dst_addr=0x%lx, length=%u\n",
                                producer,
                                dma_msg->src_mmap,
                                 dma_msg->dst_mmap,
                                 dma_msg->src_addr,
                                 dma_msg->dst_addr,
                                 dma_msg->length);

            if (doca_dpa_dev_comch_producer_is_consumer_empty(producer, /*consumer_id=*/1)) {
                DOCA_DPA_DEV_LOG_INFO("Host consumer is empty, cannot send DMA completion\n");
                break;
            }

            doca_dpa_dev_comch_producer_dma_copy(producer,
                                    /*consumer_id=*/1,
                                    dma_msg->dst_mmap,
                                    dma_msg->dst_addr,
                                    dma_msg->src_mmap,
                                    dma_msg->src_addr,
                                    dma_msg->length,
                                    "test_dma_imm",
                                    sizeof("test_dma_imm"),
                                    DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS | DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
            break;
        default:
            DOCA_DPA_DEV_LOG_INFO("Unknown msg type received from host: %d\n", msg->type);
            break;
    }
}

static void handle_msgs(struct legacy_comch_arg *thread_arg)
{
    doca_dpa_dev_comch_consumer_completion_element_t completion;
    struct comch_msg *msg;
    uint32_t msg_size;
    doca_dpa_dev_comch_consumer_t consumer = thread_arg->dpa_consumer;
	doca_dpa_dev_comch_consumer_completion_t consumer_comp = thread_arg->dpa_consumer_comp;
    uint32_t num_msgs = 0;

    while (doca_dpa_dev_comch_consumer_get_completion(consumer_comp, &completion) != 0) {
        msg = (struct comch_msg *)doca_dpa_dev_comch_consumer_get_completion_imm(completion, &msg_size);
        handle_dpu_msg(thread_arg, msg);
        num_msgs++;
    }

    // send_msgs(thread_arg, num_msgs);
    if (num_msgs != 0) {
        doca_dpa_dev_comch_consumer_completion_ack(consumer_comp, num_msgs);
		doca_dpa_dev_comch_consumer_completion_request_notification(consumer_comp);
		doca_dpa_dev_comch_consumer_ack(consumer, num_msgs);
    }
    // DOCA_DPA_DEV_LOG_INFO("Handled %u msgs from host\n", num_msgs);
}
