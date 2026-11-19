# Host library, façades, tests and examples. The DOCA transport is
# `src/transport/` (its own Meson project, the authority on transport sources);
# the host library compiles the host-side subset listed in TRANSPORT_SRCS so it
# builds with plain make on any host that has the DOCA headers.
CC ?= cc
CXX ?= c++
PYTHON ?= python3
BUILD := build
LIBDIR := $(BUILD)/lib
TESTDIR := $(BUILD)/test
BINDIR := $(BUILD)/bin
ABI_MAJOR := 6
TRANSPORT := src/transport
DOCA_INC := /opt/mellanox/doca/include
# The library runs no DPA: Comch and registered memory only. Tests that
# compile DPU sources link the DPA libraries too.
LIB_DOCA_LIBS := $(shell pkg-config --libs doca-common doca-comch libcrypto)
DOCA_LIBS := $(shell pkg-config --libs doca-common doca-comch doca-dma doca-dpa libflexio libcrypto libssl)
HOST_CFLAGS := -std=gnu11 -O2 -g -Wall -Wextra -D_GNU_SOURCE -DDOCA_ALLOW_EXPERIMENTAL_API \
    -Iinclude -I. -I$(TRANSPORT)/common -I$(TRANSPORT)/dpu -I$(TRANSPORT)/host -I$(TRANSPORT)/peer -I$(DOCA_INC)
# Transport sources the host library needs: the common set, the Comch client
# and the channel layer. Keep in step with src/transport/meson.build.
TRANSPORT_SRCS := $(addprefix $(TRANSPORT)/common/,object.c buffer.c common.c comch_common.c ring.c) \
    $(addprefix $(TRANSPORT)/host/,comch_client.c channel.c registration_client.c host_stubs.c)
TRANSPORT_HDRS := $(wildcard $(TRANSPORT)/common/*.h $(TRANSPORT)/host/*.h $(TRANSPORT)/dpu/*.h $(TRANSPORT)/peer/*.h)
LIB_SRCS := src/core/dmesh_core.c src/core/carrier.c src/core/service_resolve.c \
    src/facade/dmesh_api.c $(TRANSPORT_SRCS)
PEER_AUTH_SRCS := $(addprefix $(TRANSPORT)/peer/,peer_authority.c peer_topology.c peer_identity.c local_control_session.c)
PEER_LAYOUT_TESTS := peer_rdma_layout_test peer_rdma_layout_2_1_test peer_rdma_layout_8_8_test peer_rdma_layout_129_16_test
HOST_TESTS := peer_imm_bootstrap_test peer_dpa_runtime_test $(PEER_LAYOUT_TESTS) peer_write_completion_dpa_test peer_dmesh4_test peer_write_completion_test peer_connection_test peer_channel_retire_test peer_authority_test carrier_logic_test service_resolve_test native_writable_test native_polling_test native_signaled_test native_loss_test native_core_transport_test \
    topology_test native_api_contract_test preload_api_contract_test session_protocol_test session_flow_test \
    channel_session_test comch_client_test session_server_test backend_dispatch_test dma_cleanup_test dpa_cleanup_test dpa_runtime_test \
    dispatch_request_test placement_instr_test rx_consumed_pos_test native_listen_test channel_flow_wake_test
EXAMPLES := hello_dpumesh hello_dpumesh_server tcp_echo tcp_client

.PHONY: all lib test test-native-headers test-abi test-peer-config examples clean
all: lib

lib: $(LIBDIR)/libdpumesh.so.$(ABI_MAJOR) $(LIBDIR)/libdpumesh_preload.so

$(LIBDIR) $(TESTDIR) $(BINDIR):
	mkdir -p $@

# Calls within the library bind directly and calls into libc and DOCA go
# through the GOT without a PLT stub; the exported symbols are unchanged.
LIB_CFLAGS := -fno-plt -fno-semantic-interposition
LIB_LDFLAGS := -Wl,-Bsymbolic-functions

$(LIBDIR)/libdpumesh.so.$(ABI_MAJOR): $(LIB_SRCS) include/dpumesh/*.h src/core/*.h $(TRANSPORT_HDRS) | $(LIBDIR)
	$(CC) $(HOST_CFLAGS) $(LIB_CFLAGS) -fPIC -shared -Wl,-soname,libdpumesh.so.$(ABI_MAJOR) -Wl,--no-undefined \
	    $(LIB_LDFLAGS) $(LIB_SRCS) -pthread $(LIB_DOCA_LIBS) -o $@
	ln -sfn libdpumesh.so.$(ABI_MAJOR) $(LIBDIR)/libdpumesh.so

$(LIBDIR)/libdpumesh_preload.so: src/facade/dmesh_preload.c $(LIBDIR)/libdpumesh.so.$(ABI_MAJOR)
	$(CC) $(HOST_CFLAGS) -U_FILE_OFFSET_BITS -fPIC -shared $< -L$(LIBDIR) -ldpumesh -ldl -pthread -o $@

test: test-native-headers $(addprefix $(TESTDIR)/,$(HOST_TESTS)) test-peer-config test-abi
	@set -e; for test in $(HOST_TESTS); do $(TESTDIR)/$$test; done
	CC="$(CC)" $(PYTHON) tests/dpa_poll_test.py
	$(PYTHON) tests/peer_fence_stats_test.py
	PYTHONPATH=. $(PYTHON) tests/native_registration_test.py
	CC="$(CC)" $(PYTHON) tests/ec_rev_poll_test.py

test-native-headers:
	CC="$(CC)" CXX="$(CXX)" $(PYTHON) tests/native_header_contract_test.py

test-abi: lib
	sh tests/abi_contract_test.sh $(LIBDIR)/libdpumesh.so.$(ABI_MAJOR) $(LIBDIR)/libdpumesh_preload.so $(ABI_MAJOR)

$(TESTDIR)/peer_rdma_layout_2_1_test: PEER_LAYOUT_FLAGS := -DPEER_RDMA_CLUSTER_NODES=2u -DPEER_RDMA_MAX_WORKERS=1u
$(TESTDIR)/peer_rdma_layout_8_8_test: PEER_LAYOUT_FLAGS := -DPEER_RDMA_CLUSTER_NODES=8u -DPEER_RDMA_MAX_WORKERS=8u
$(TESTDIR)/peer_rdma_layout_129_16_test: PEER_LAYOUT_FLAGS := -DPEER_RDMA_CLUSTER_NODES=129u -DPEER_RDMA_MAX_WORKERS=16u
$(addprefix $(TESTDIR)/,$(PEER_LAYOUT_TESTS)): tests/peer_rdma_layout_test.c $(TRANSPORT)/peer/peer_pair_transport.c $(TRANSPORT_HDRS) $(TRANSPORT)/peer/device/peer_write_imm_dpa_internal.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -U_GNU_SOURCE $(PEER_LAYOUT_FLAGS) -Werror -ffunction-sections -fdata-sections $< -Wl,--gc-sections -o $@

test-peer-config: $(TRANSPORT)/peer/peer_config.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -x c -fsyntax-only $<
	@set -e; for setting in PEER_RDMA_CLUSTER_NODES=1u PEER_RDMA_CLUSTER_NODES=130u PEER_RDMA_MAX_WORKERS=0u PEER_RDMA_MAX_WORKERS=17u; do \
	    if $(CC) $(HOST_CFLAGS) -D$$setting -x c -fsyntax-only $< >$(TESTDIR)/peer_config_invalid.log 2>&1; then \
	        echo "invalid peer configuration accepted: $$setting" >&2; exit 1; \
	    fi; \
	done

$(TESTDIR)/peer_imm_bootstrap_test: tests/peer_imm_bootstrap_test.c $(TRANSPORT)/peer/peer_carrier_write_imm.c $(TRANSPORT_HDRS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -U_GNU_SOURCE -Werror -ffunction-sections -fdata-sections $< -Wl,--gc-sections -libverbs -o $@

$(TESTDIR)/peer_dpa_runtime_test: tests/peer_dpa_runtime_test.c $(TRANSPORT)/peer/device/peer_runtime.c $(TRANSPORT_HDRS) $(wildcard tests/support/dpa_sdk/*.h) | $(TESTDIR)
	$(CC) -Itests/support/dpa_sdk $(HOST_CFLAGS) -Werror $< -o $@

$(TESTDIR)/peer_write_completion_dpa_test: tests/peer_write_completion_dpa_test.c $(TRANSPORT)/peer/device/peer_write_imm_dpa.c $(TRANSPORT)/peer/peer_write_imm_dpa.c $(TRANSPORT_HDRS) $(wildcard tests/support/dpa_sdk/*.h) $(TRANSPORT)/peer/device/peer_write_imm_dpa_internal.h | $(TESTDIR)
	$(CC) -Itests/support/dpa_sdk $(HOST_CFLAGS) $(WRITE_COMPLETION_TEST_CFLAGS) -Werror -ffunction-sections -fdata-sections $< -Wl,--gc-sections -o $@

$(TESTDIR)/peer_write_completion_test: tests/peer_write_completion_test.c $(TRANSPORT)/peer/peer_carrier_write_imm.c $(TRANSPORT)/peer/peer_write_imm_dpa.c $(TRANSPORT_HDRS) $(TRANSPORT)/peer/device/peer_write_imm_dpa_internal.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< -Wl,--gc-sections -o $@

$(TESTDIR)/session_protocol_test: tests/session_protocol_test.c $(TRANSPORT)/common/session_protocol.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) $< -o $@

$(TESTDIR)/session_flow_test: tests/session_flow_test.c $(TRANSPORT)/common/object.c $(TRANSPORT)/common/dpa.c $(TRANSPORT)/common/dpa_runtime.c $(TRANSPORT_HDRS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $(filter %.c,$^) -Wl,--gc-sections -pthread $(DOCA_LIBS) -o $@

$(TESTDIR)/channel_session_test: tests/channel_session_test.c $(TRANSPORT)/host/channel.c $(TRANSPORT_HDRS) $(TRANSPORT)/host/registration_client.c | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -DCHANNEL_REPLY_MS=20 -ffunction-sections -fdata-sections $< $(TRANSPORT)/host/registration_client.c -Wl,--gc-sections -pthread $(DOCA_LIBS) -o $@

$(TESTDIR)/channel_flow_wake_test: tests/channel_flow_wake_test.c $(TRANSPORT)/host/channel.c $(TRANSPORT_HDRS) $(TRANSPORT)/host/registration_client.c | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< $(TRANSPORT)/host/registration_client.c -Wl,--gc-sections -pthread $(DOCA_LIBS) -o $@

$(TESTDIR)/comch_client_test: tests/comch_client_test.c $(TRANSPORT)/host/comch_client.c $(TRANSPORT_HDRS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< -Wl,--gc-sections $(DOCA_LIBS) -o $@

$(TESTDIR)/session_server_test: tests/session_server_test.c $(TRANSPORT)/common/object.c $(TRANSPORT)/dpu/comch_server.c $(TRANSPORT)/dpu/dispatcher.c $(TRANSPORT)/dpu/placement.c $(TRANSPORT)/dpu/dpa_mux.c $(TRANSPORT)/common/dpa_runtime.c $(TRANSPORT)/common/buffer.c $(TRANSPORT_HDRS) $(PEER_AUTH_SRCS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< $(TRANSPORT)/dpu/dispatcher.c $(TRANSPORT)/dpu/placement.c $(TRANSPORT)/dpu/dpa_mux.c $(TRANSPORT)/common/dpa_runtime.c $(TRANSPORT)/common/object.c $(TRANSPORT)/common/buffer.c $(PEER_AUTH_SRCS) -Wl,--gc-sections $(DOCA_LIBS) -pthread -o $@

$(TESTDIR)/backend_dispatch_test: tests/backend_dispatch_test.c $(TRANSPORT)/dpu/dispatcher.c $(TRANSPORT)/dpu/placement.c $(TRANSPORT_HDRS) $(PEER_AUTH_SRCS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< $(TRANSPORT)/dpu/placement.c $(PEER_AUTH_SRCS) -Wl,--gc-sections -pthread $(DOCA_LIBS) -o $@

$(TESTDIR)/dispatch_request_test: tests/dispatch_request_test.c $(TRANSPORT)/dpu/dispatcher.c $(TRANSPORT)/dpu/placement.c $(TRANSPORT_HDRS) $(PEER_AUTH_SRCS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< $(TRANSPORT)/dpu/placement.c $(PEER_AUTH_SRCS) -Wl,--gc-sections -pthread $(DOCA_LIBS) -o $@

$(TESTDIR)/placement_instr_test: tests/placement_instr_test.c $(TRANSPORT)/dpu/dispatcher.c $(TRANSPORT)/dpu/placement.c $(TRANSPORT_HDRS) $(PEER_AUTH_SRCS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< $(TRANSPORT)/dpu/placement.c $(PEER_AUTH_SRCS) -Wl,--gc-sections -pthread $(DOCA_LIBS) -o $@

$(TESTDIR)/dma_cleanup_test: tests/dma_cleanup_test.c $(TRANSPORT)/dpu/dma.c $(TRANSPORT_HDRS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< -Wl,--gc-sections $(DOCA_LIBS) -o $@

$(TESTDIR)/dpa_runtime_test: tests/dpa_runtime_test.c $(TRANSPORT)/common/dpa_runtime.c $(TRANSPORT_HDRS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) $< -pthread $(DOCA_LIBS) -o $@

$(TESTDIR)/dpa_cleanup_test: tests/dpa_cleanup_test.c $(TRANSPORT)/common/dpa.c $(TRANSPORT)/common/comch_msgq.c $(TRANSPORT)/common/dpa_runtime.c $(TRANSPORT)/common/object.c $(TRANSPORT_HDRS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -DDMESH_DPA_QUIESCE_TIMEOUT_MS=20 -ffunction-sections -fdata-sections $< $(TRANSPORT)/common/dpa_runtime.c $(TRANSPORT)/common/object.c -Wl,--gc-sections $(DOCA_LIBS) -pthread -o $@

$(TESTDIR)/rx_consumed_pos_test: tests/rx_consumed_pos_test.c linkerd2-proxy/linkerd/doca/src/shim.c $(TRANSPORT_HDRS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< -Wl,--gc-sections $(DOCA_LIBS) -o $@


$(TESTDIR)/carrier_logic_test: tests/carrier_logic_test.c src/core/carrier.c src/core/carrier_logic.h $(TRANSPORT)/host/channel.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< -Wl,--gc-sections -pthread -o $@

$(TESTDIR)/topology_test: tests/topology_test.c include/dpumesh/dmesh_topology.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) $< -o $@

$(TESTDIR)/native_api_contract_test: tests/native_api_contract_test.c src/facade/dmesh_api.c src/core/dmesh_core.h include/dpumesh/dmesh.h include/dpumesh/dmesh_common.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections -Wl,--gc-sections tests/native_api_contract_test.c src/facade/dmesh_api.c -o $@

$(TESTDIR)/preload_api_contract_test: tests/preload_api_contract_test.c src/facade/dmesh_preload.c src/core/dmesh_core.h include/dpumesh/dmesh.h include/dpumesh/dmesh_common.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections -Wl,--gc-sections $< -ldl -lpthread -o $@

$(TESTDIR)/native_writable_test: tests/native_writable_test.c src/core/dmesh_core.c src/core/native_transport.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< -Wl,--gc-sections -pthread -o $@

$(TESTDIR)/native_polling_test: tests/native_polling_test.c src/core/dmesh_core.c src/core/native_transport.h tests/support/native_memory_transport.c src/facade/dmesh_api.c src/core/service_resolve.c | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< src/facade/dmesh_api.c src/core/service_resolve.c -Wl,--gc-sections -pthread -o $@

$(TESTDIR)/native_signaled_test: tests/native_signaled_test.c src/core/dmesh_core.c src/core/native_transport.h tests/support/native_memory_transport.c src/facade/dmesh_api.c src/core/service_resolve.c | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< src/facade/dmesh_api.c src/core/service_resolve.c -Wl,--gc-sections -pthread -o $@

$(TESTDIR)/native_loss_test: tests/native_loss_test.c src/core/dmesh_core.c src/core/native_transport.h tests/support/native_memory_transport.c src/facade/dmesh_api.c src/core/service_resolve.c | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< src/facade/dmesh_api.c src/core/service_resolve.c -Wl,--gc-sections -pthread -o $@

$(TESTDIR)/native_listen_test: tests/native_listen_test.c src/core/dmesh_core.c src/core/native_transport.h tests/support/native_memory_transport.c src/facade/dmesh_api.c src/core/service_resolve.c | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< src/facade/dmesh_api.c src/core/service_resolve.c -Wl,--gc-sections -pthread -o $@

$(TESTDIR)/native_core_transport_test: tests/native_core_transport_test.c src/core/dmesh_core.c src/core/native_transport.h tests/support/native_memory_transport.c src/facade/dmesh_api.c src/core/service_resolve.c | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< src/facade/dmesh_api.c src/core/service_resolve.c -Wl,--gc-sections -pthread -o $@

$(TESTDIR)/service_resolve_test: tests/service_resolve_test.c src/core/service_resolve.c src/core/service_resolve.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -DDMESH_RESOLVE_TEST $(filter %.c,$^) -pthread -o $@

examples: lib $(addprefix $(BINDIR)/,$(EXAMPLES))

$(BINDIR)/hello_dpumesh $(BINDIR)/hello_dpumesh_server: $(BINDIR)/%: examples/native/%.c $(LIBDIR)/libdpumesh.so.$(ABI_MAJOR) | $(BINDIR)
	$(CC) $(HOST_CFLAGS) $< -L$(LIBDIR) -ldpumesh -Wl,-rpath,$(abspath $(LIBDIR)) -lpthread -o $@

$(BINDIR)/tcp_echo $(BINDIR)/tcp_client: $(BINDIR)/%: examples/preload/%.c | $(BINDIR)
	$(CC) $(HOST_CFLAGS) $< -lpthread -o $@

# The DMA benchmark (dpumesh_host over this library) is apps/dma_bench, a meson
# project that links build/lib/libdpumesh.so: run `make lib` first.

clean:
	rm -rf $(BUILD)

$(TESTDIR)/peer_connection_test: tests/peer_connection_test.c $(TRANSPORT)/peer/peer_connection_io.c $(TRANSPORT)/peer/peer_carrier_write_imm.c $(TRANSPORT)/peer/peer_pair_protocol.c $(TRANSPORT)/peer/peer_control_protocol.c $(TRANSPORT_HDRS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< $(TRANSPORT)/peer/peer_pair_protocol.c $(TRANSPORT)/peer/peer_control_protocol.c -Wl,--gc-sections -lcrypto -o $@

$(TESTDIR)/peer_dmesh4_test: tests/peer_dmesh4_test.c $(TRANSPORT)/peer/peer_channel.c $(TRANSPORT)/peer/peer_pair_protocol.c $(TRANSPORT)/peer/peer_control_protocol.c $(TRANSPORT_HDRS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< $(TRANSPORT)/peer/peer_pair_protocol.c $(TRANSPORT)/peer/peer_control_protocol.c -Wl,--gc-sections -lcrypto -o $@

$(TESTDIR)/peer_authority_test: tests/peer_authority_test.c $(PEER_AUTH_SRCS) $(wildcard $(TRANSPORT)/peer/*.h) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) $(filter %.c,$^) -pthread -lssl -lcrypto -o $@

$(TESTDIR)/peer_channel_retire_test: tests/peer_channel_retire_test.c $(TRANSPORT)/peer/peer_channel.c $(TRANSPORT_HDRS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $(filter %.c,$^) -Wl,--gc-sections -o $@
