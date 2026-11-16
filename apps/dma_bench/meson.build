project('dma-bench', 'c',
  version : '1.0.0',
  license : 'BSD-3',
  default_options : ['buildtype=debugoptimized'],
  meson_version : '>=0.61.2'
)

# The DMA benchmark pair over the DOCA transport (src/transport, built first:
# `ninja -C src/transport/build`). The side is picked by the building
# machine's CPU (aarch64 = the DPU) unless -Dside=host|dpu says otherwise.
#
#   dpumesh_host   host/dpumesh_host.c over the host library (libdpumesh, the
#                  public API in include/dpumesh): one channel, one EQ + QP per
#                  thread, sink or echo mode, per-second rates, RTT histogram.
#                  Needs `make lib` at the repo root first.
#   dpumesh_dpu    dpu/dpumesh_dpu.c over the shim the proxy drives
#                  (linkerd/doca/src/shim.c + comch_server.c): serves the
#                  flows, counts every forward DMA completion per second and,
#                  in echo mode, pushes the bytes back.
#
#   dpumesh_v0_{host,dpu}   legacy/: the original worker pair on the old
#                  CLIENT-mode reverse path (fused completions, host DPA reverse). Kept
#                  for the h2load bridges (DMESH_BRIDGE_PORT, DMESH_PUSH_BRIDGE_
#                  PORT, DMESH_BACKEND_CONNECT) the proxy benchmark scripts use
#                  and for the host DPA microbench (DMESH_DPA_BENCH_MODE).
#                  ("v0", and <= 15 characters, so `pkill -x` still matches the
#                  kernel's truncated comm.)
#
# Everything DPU-side links the transport archives whole (dpu and common
# reference each other) plus the dpacc output dpa_kernel.a.
add_project_arguments('-D DOCA_ALLOW_EXPERIMENTAL_API', language : 'c')

fs = import('fs')
cc = meson.get_compiler('c')
repo_root = meson.project_source_root() / '..' / '..'
transport = repo_root / 'src' / 'transport'
transport_build = transport / 'build'

foreach a : ['libdmesh_dpu.a', 'libdmesh_common.a', 'libdmesh_host.a', 'device/dpa_kernel.a']
  if not fs.exists(transport_build / a)
    error(transport_build / a + ' not found — run `ninja -C src/transport/build` first')
  endif
endforeach
transport_link_args = ['-Wl,--whole-archive', transport_build / 'libdmesh_dpu.a',
                       transport_build / 'libdmesh_common.a', transport_build / 'peer/libdmesh_peer.a', '-Wl,--no-whole-archive',
                       transport_build / 'libdmesh_host.a']
dpa_program_lib = cc.find_library('dpa_kernel', dirs : transport_build / 'device', static : true)

inc_dirs = include_directories('/opt/mellanox/doca/include/', 'legacy',
                               '../../src/transport/common', '../../src/transport/dpu',
                               '../../src/transport/host')

deps = [
  dependency('threads'),
  dependency('doca-common'),
  dependency('doca-comch'),
  dependency('doca-dma'),
  dependency('doca-dpa'),
  dependency('libflexio'),
  dependency('doca-rdma'),
  dependency('doca-verbs'),
  dependency('libibverbs'),
  dependency('librdmacm'),
  dependency('libcrypto'),
  dependency('libssl'),
  cc.find_library('dl'),
]

side = get_option('side')
if side == 'auto'
  side = host_machine.cpu_family() == 'aarch64' ? 'dpu' : 'host'
endif
dpu_side = side == 'dpu'
message('building the ' + side + ' side (dpumesh_' + side + ', dpumesh_v0_' + side + ')')

if dpu_side
  # shim.c lives in the linkerd2-proxy submodule.
  shim_c = repo_root / 'linkerd2-proxy' / 'linkerd' / 'doca' / 'src' / 'shim.c'
  if not fs.exists(shim_c)
    error(shim_c + ' not found — check out the linkerd2-proxy submodule (dpumesh_dpu is built on the shim)')
  endif
  executable('dpumesh_dpu',
    files('dpu/dpumesh_dpu.c') + files(shim_c),
    dependencies : deps + [dependency('doca-aes-gcm'), dependency('doca-sha'), dpa_program_lib],
    include_directories : inc_dirs,
    link_args : transport_link_args,
    c_args : ['-DALLOW_EXPERIMENTAL_API', '-DFLEXIO_ALLOW_EXPERIMENTAL_API', '-Wno-unused-parameter'],
    install : false,
  )
else
  # The host library comes from the root Makefile (`make lib`).
  lib_dir = repo_root / 'build' / 'lib'
  if not fs.exists(lib_dir / 'libdpumesh.so')
    error(lib_dir / 'libdpumesh.so not found — run `make lib` at the repo root first')
  endif
  executable('dpumesh_host',
    files('host/dpumesh_host.c'),
    dependencies : [dependency('threads'), cc.find_library('dpumesh', dirs : lib_dir)],
    include_directories : include_directories('../../include'),
    c_args : ['-D_GNU_SOURCE'],
    build_rpath : lib_dir,
    install : false,
  )
endif

executable('dpumesh_v0_' + side,
  files('legacy/main.c', 'legacy/config.c') +
    (dpu_side ? files('legacy/dpu_worker.c') : files('legacy/host_worker.c')),
  c_args : ['-DDPUMESH_DPU_SIDE=' + (dpu_side ? '1' : '0')],
  dependencies : deps + [dependency('doca-argp'), dpa_program_lib],
  include_directories : inc_dirs,
  link_args : transport_link_args,
  install : false,
)
