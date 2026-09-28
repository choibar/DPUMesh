project('dmesh-hostlib', 'c',
  version : '1.0.0',
  license : 'BSD-3',
  default_options : ['buildtype=debugoptimized'],
  meson_version : '>=0.61.2'
)

# libdmesh_hostlib.so: the old Go host library (../dmesh.go links it through
# cgo; the C source lives in this subdirectory so cgo does not compile it itself)
# — the push channel as a byte stream. It has no host DPA, so it compiles the
# DPA-free transport sources directly instead of linking libdmesh_common.a
# (whose dpa.c would drag in the non-PIC dpa_kernel.a); the DPU-side symbols
# the shared sources reference are stubbed in host/host_stubs.c. Keep this list in
# step with common_nodpa_srcs in src/transport/meson.build.
add_project_arguments('-D DOCA_ALLOW_EXPERIMENTAL_API', language : 'c')

transport = meson.project_source_root() / '..' / '..' / '..' / 'src' / 'transport'
srcs = files(
  transport / 'common' / 'buffer.c',
  transport / 'common' / 'ring.c',
  transport / 'common' / 'object.c',
  transport / 'common' / 'common.c',
  transport / 'common' / 'comch_common.c',
  transport / 'common' / 'comch_consumer.c',
  transport / 'common' / 'comch_producer.c',
  transport / 'host' / 'comch_client.c',
  transport / 'host' / 'comch_client_legacy.c',
  transport / 'host' / 'host_stubs.c',
  'host_lib.c',
)
inc_dirs = include_directories('/opt/mellanox/doca/include/', '.',
                               '../../../src/transport/common', '../../../src/transport/dpu',
                               '../../../src/transport/host')
deps = [
  dependency('threads'),
  dependency('doca-common'),
  dependency('doca-comch'),
  dependency('doca-dma'),
  dependency('doca-dpa'),
]
shared_library('dmesh_hostlib', srcs,
  dependencies : deps,
  include_directories : inc_dirs,
  install : false,
)
