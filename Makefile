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
ABI_MAJOR := 5
TRANSPORT := src/transport
DOCA_INC := /opt/mellanox/doca/include
DOCA_LIBS := $(shell pkg-config --libs doca-common doca-comch doca-dma doca-dpa libflexio)
DPACC := $(TRANSPORT)/build_dpacc.sh
DPA_KERNEL := $(BUILD)/dpa/device/dpa_kernel.a
DPA_BENCH := $(TRANSPORT)/device/benchmarks/dpa_bench.c
HOST_CFLAGS := -std=gnu11 -O2 -g -Wall -Wextra -D_GNU_SOURCE -DDOCA_ALLOW_EXPERIMENTAL_API \
    -Iinclude -I. -I$(TRANSPORT)/common -I$(TRANSPORT)/dpu -I$(TRANSPORT)/host -I$(DOCA_INC)
# Transport sources the host library needs: the common set, the DPA
# management the host-dpa reverse path's host DPA thread uses, the Comch client and the
# channel layer. Keep in step with src/transport/meson.build.
TRANSPORT_SRCS := $(addprefix $(TRANSPORT)/common/,object.c buffer.c common.c comch_common.c \
    comch_consumer.c comch_producer.c comch_msgq.c dpa.c dpa_runtime.c ring.c) \
    $(addprefix $(TRANSPORT)/host/,comch_client.c comch_client_legacy.c channel.c host_stubs.c)
TRANSPORT_HDRS := $(wildcard $(TRANSPORT)/common/*.h $(TRANSPORT)/host/*.h $(TRANSPORT)/dpu/*.h)
LIB_SRCS := src/core/dmesh_core.c src/core/carrier.c src/core/service_resolve.c \
    src/facade/dmesh_api.c $(TRANSPORT_SRCS)
HOST_TESTS := carrier_logic_test service_resolve_test native_writable_test native_polling_test native_core_transport_test \
    topology_test native_api_contract_test preload_api_contract_test session_protocol_test session_flow_test \
    channel_session_test comch_client_test session_server_test backend_dispatch_test dma_cleanup_test dpa_cleanup_test dpa_runtime_test \
    rx_consumed_pos_test
EXAMPLES := hello_dpumesh hello_dpumesh_server tcp_echo tcp_client

.PHONY: all lib test test-native-headers test-abi examples clean
all: lib

lib: $(LIBDIR)/libdpumesh.so.$(ABI_MAJOR) $(LIBDIR)/libdpumesh_preload.so

$(LIBDIR) $(TESTDIR) $(BINDIR):
	mkdir -p $@

# The DPA kernel + its (PIC) host stub, compiled by dpacc: the host-dpa reverse path runs
# the same poll_desc_ring kernel on a host-owned DPA thread.
$(DPA_KERNEL): $(TRANSPORT)/device/dpa_kernel.c $(DPA_BENCH) $(TRANSPORT)/device/*.h $(TRANSPORT)/device/benchmarks/*.h $(TRANSPORT)/common/dpa_common.h $(TRANSPORT)/common/dpa_bench.h $(DPACC)
	$(DPACC) $(abspath $(BUILD)/dpa) $(abspath $(TRANSPORT)) $(abspath $<) dpa_kernel nv-dpa-bf3 \
	    $$(pkg-config --variable=libdir doca-dpa) $(abspath $(DPA_BENCH))

$(LIBDIR)/libdpumesh.so.$(ABI_MAJOR): $(LIB_SRCS) $(DPA_KERNEL) include/dpumesh/*.h src/core/*.h $(TRANSPORT_HDRS) | $(LIBDIR)
	$(CC) $(HOST_CFLAGS) -fPIC -shared -Wl,-soname,libdpumesh.so.$(ABI_MAJOR) -Wl,--no-undefined \
	    $(LIB_SRCS) $(DPA_KERNEL) -pthread $(DOCA_LIBS) -o $@
	ln -sfn libdpumesh.so.$(ABI_MAJOR) $(LIBDIR)/libdpumesh.so

$(LIBDIR)/libdpumesh_preload.so: src/facade/dmesh_preload.c $(LIBDIR)/libdpumesh.so.$(ABI_MAJOR)
	$(CC) $(HOST_CFLAGS) -U_FILE_OFFSET_BITS -fPIC -shared $< -L$(LIBDIR) -ldpumesh -ldl -pthread -o $@

test: test-native-headers $(addprefix $(TESTDIR)/,$(HOST_TESTS)) test-abi
	@set -e; for test in $(HOST_TESTS); do $(TESTDIR)/$$test; done
	CC="$(CC)" $(PYTHON) tests/dpa_poll_test.py

test-native-headers:
	CC="$(CC)" CXX="$(CXX)" $(PYTHON) tests/native_header_contract_test.py

test-abi: lib
	sh tests/abi_contract_test.sh $(LIBDIR)/libdpumesh.so.$(ABI_MAJOR) $(LIBDIR)/libdpumesh_preload.so $(ABI_MAJOR)

$(TESTDIR)/session_protocol_test: tests/session_protocol_test.c $(TRANSPORT)/common/session_protocol.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) $< -o $@

$(TESTDIR)/session_flow_test: tests/session_flow_test.c $(TRANSPORT)/common/object.c $(TRANSPORT)/common/dpa.c $(TRANSPORT)/common/dpa_runtime.c $(TRANSPORT_HDRS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $(filter %.c,$^) -Wl,--gc-sections -pthread $(DOCA_LIBS) -o $@

$(TESTDIR)/channel_session_test: tests/channel_session_test.c $(TRANSPORT)/host/channel.c $(TRANSPORT_HDRS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< -Wl,--gc-sections -pthread $(DOCA_LIBS) -o $@

$(TESTDIR)/comch_client_test: tests/comch_client_test.c $(TRANSPORT)/host/comch_client.c $(TRANSPORT_HDRS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< -Wl,--gc-sections $(DOCA_LIBS) -o $@

$(TESTDIR)/session_server_test: tests/session_server_test.c $(TRANSPORT)/common/object.c $(TRANSPORT)/dpu/comch_server.c $(TRANSPORT)/dpu/dispatcher.c $(TRANSPORT)/dpu/placement.c $(TRANSPORT)/common/dpa_runtime.c $(TRANSPORT_HDRS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< $(TRANSPORT)/dpu/dispatcher.c $(TRANSPORT)/dpu/placement.c $(TRANSPORT)/common/dpa_runtime.c $(TRANSPORT)/common/object.c -Wl,--gc-sections $(DOCA_LIBS) -pthread -o $@

$(TESTDIR)/backend_dispatch_test: tests/backend_dispatch_test.c $(TRANSPORT)/dpu/dispatcher.c $(TRANSPORT)/dpu/placement.c $(TRANSPORT_HDRS) | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< $(TRANSPORT)/dpu/placement.c -Wl,--gc-sections -pthread $(DOCA_LIBS) -o $@

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

$(TESTDIR)/native_core_transport_test: tests/native_core_transport_test.c tests/support/native_memory_transport.c src/core/dmesh_core.c src/core/service_resolve.c src/facade/dmesh_api.c src/core/native_transport.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) $(filter %.c,$^) -pthread -o $@

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
