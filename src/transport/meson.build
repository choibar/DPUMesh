project('dpumesh', 'c', 'cpp',
  version : '1.0.0',
  license : 'BSD-3',
  default_options : ['buildtype=debugoptimized'],
  meson_version : '>=0.61.2'
)

# The DOCA transport: the only tree that includes DOCA headers. It is split by
# the side that runs it and built into static archives that every consumer
# links (the `dpumesh` binary below, the Rust proxy's `linkerd/doca/build.rs`,
# `bench/dmesh-router-cpp`, the host library's root Makefile), so this file is
# the single list of transport sources.
#
#   common/  buffers, rings, per-worker objects, Comch control-path framing,
#            DPA thread/msgq management (both sides)
#   dpu/     Comch server, forward DPA pool + DMA push engine
#   host/    Comch client, and the channel layer the host library builds on
#   device/  the DPA kernel (compiled by dpacc, see build_dpacc.sh)
#
# The programs live elsewhere and link these archives: apps/dma_bench
# (`dpumesh`, the original worker pair, and `dpumesh-echo`),
# apps/dmesh-router-cpp, apps/dmeshgo (the old Go host library) and the Rust
# proxy (linkerd/doca/build.rs).

# Comment this line to restore warnings of experimental DOCA features
add_project_arguments('-D DOCA_ALLOW_EXPERIMENTAL_API', language: ['c', 'cpp'])

# Every subdirectory is on the include path so sources keep their bare
# `#include "x.h"` lines; a header lives next to its .c.
inc_dirs = include_directories('/opt/mellanox/doca/include/', 'common', 'dpu', 'host')

deps = []
deps += dependency('threads')
deps += dependency('doca-common')
deps += dependency('doca-comch')
deps += dependency('doca-dma')
deps += dependency('doca-dpa')
deps += dependency('libflexio')

# DPA management (common/dpa.c, and comch_msgq.c on top of it) references the
# dpacc host stub; the DPA-free set is what the host library's Makefile and
# apps/dmeshgo's shared library compile on their own.
common_nodpa_srcs = files(
  'common/buffer.c',
  'common/ring.c',
  'common/object.c',
  'common/common.c',
  'common/comch_common.c',
  'common/comch_consumer.c',
  'common/comch_producer.c',
)
common_srcs = common_nodpa_srcs + files('common/dpa.c', 'common/dpa_runtime.c', 'common/comch_msgq.c')
dpu_srcs = files(
  'dpu/comch_server.c',
  'dpu/dispatcher.c',
  'dpu/placement.c',
  'dpu/dma.c',
)
host_srcs = files(
  'host/comch_client.c',
  'host/comch_client_legacy.c',
)
# The host library's channel layer. host_stubs.c stubs the two server-side
# symbols the shared sources reference, so this archive must never be linked
# together with dmesh_dpu.
channel_srcs = files(
  'host/channel.c',
  'host/host_stubs.c',
)

# DPA device code: dpacc produces device/dpa_kernel.a (plus the host stub that
# registers the DPA app), linked into every DPU-side binary.
program_name = 'dpa_kernel'
dpacc_build_script = meson.project_source_root() / 'build_dpacc.sh'
dpacc_mcpu_flag = 'nv-dpa-bf3'
doca_lib_dir = dependency('doca-dpa').get_variable(pkgconfig : 'libdir')

# Kernel and argument-layout changes must rebuild the archive at build time,
# not only during meson setup/reconfigure.
subdir('device')

dmesh_common = static_library('dmesh_common', common_srcs,
  dependencies : deps,
  include_directories : inc_dirs,
)
dmesh_dpu = static_library('dmesh_dpu', dpu_srcs,
  dependencies : deps,
  include_directories : inc_dirs,
)
dmesh_host = static_library('dmesh_host', host_srcs,
  dependencies : deps,
  include_directories : inc_dirs,
)
dmesh_channel = static_library('dmesh_channel', channel_srcs,
  dependencies : deps,
  include_directories : inc_dirs,
)
