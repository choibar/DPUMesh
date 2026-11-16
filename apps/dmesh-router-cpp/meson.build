project('dmesh-router-cpp', 'c', 'cpp',
  version : '1.0.0',
  license : 'BSD-3',
  default_options : ['buildtype=release', 'cpp_std=c++17', 'warning_level=2'],
  meson_version : '>=0.61.2'
)

# The DOCA transport (src/transport) is built by its own meson project into
# static archives that this router, the `dpumesh` binary and the Rust proxy
# all link; only the shim is compiled here.
add_project_arguments('-D DOCA_ALLOW_EXPERIMENTAL_API', language: ['c', 'cpp'])
add_project_arguments('-D ALLOW_EXPERIMENTAL_API', language: ['c', 'cpp'])
add_project_arguments('-D FLEXIO_ALLOW_EXPERIMENTAL_API', language: ['c', 'cpp'])
add_project_arguments('-Wno-unused-parameter', language: 'c')

fs = import('fs')
cc = meson.get_compiler('c')

repo_root = meson.project_source_root() / '..' / '..'
transport_dir = repo_root / 'src' / 'transport'
transport_build = transport_dir / 'build'
shim_c = repo_root / 'linkerd2-proxy' / 'linkerd' / 'doca' / 'src' / 'shim.c'

if not fs.exists(shim_c)
  error('shim.c not found at ' + shim_c + ' — is the linkerd2-proxy submodule checked out?')
endif

inc_dirs = [
  include_directories('/opt/mellanox/doca/include/'),
  include_directories('../../src/transport/common'),
  include_directories('../../src/transport/dpu'),
  include_directories('../../src/transport/host'),
  include_directories('../../src/transport/device'),
  include_directories('src'),
]

deps = [
  dependency('threads'),
  dependency('doca-common'),
  dependency('doca-comch'),
  dependency('doca-dma'),
  dependency('doca-argp'),
  # shim.c's capability probe touches these two.
  dependency('doca-aes-gcm'),
  dependency('doca-sha'),
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

# The transport archives and the DPA kernel come from the transport's meson
# build; run `ninja -C src/transport/build` at least once before building this.
dpa_kernel_dir = transport_build / 'device'
foreach a : [transport_build / 'libdmesh_dpu.a', transport_build / 'libdmesh_common.a',
             transport_build / 'libdmesh_host.a', dpa_kernel_dir / 'dpa_kernel.a']
  if not fs.exists(a)
    error(a + ' not found — run `ninja -C src/transport/build` first')
  endif
endforeach
# dpu and common reference each other (object.c <-> dma.c), so pull them whole
# instead of relying on GNU ld's single pass; host (the Comch client the
# shared framing dispatches to) resolves normally after them.
transport_link_args = ['-Wl,--whole-archive', transport_build / 'libdmesh_dpu.a',
                       transport_build / 'libdmesh_common.a', transport_build / 'peer/libdmesh_peer.a', '-Wl,--no-whole-archive',
                       transport_build / 'libdmesh_host.a']
deps += cc.find_library('dpa_kernel', dirs: dpa_kernel_dir, static: true)

# libnghttp2: the testbed ships the runtime library but no -dev package, so
# fall back to the headers vendored under hpack-h2-bench/c and link the
# versioned SONAME directly (same approach as hpack-h2-bench/c/build.sh and
# linkerd/http/nghttp2/build.rs).
nghttp2_dep = dependency('libnghttp2', required : false)
nghttp2_link_args = []
if nghttp2_dep.found()
  deps += nghttp2_dep
else
  vendored_headers = meson.project_source_root() / '..' / 'hpack-h2-bench' / 'c'
  nghttp2_so = ''
  foreach cand : ['/lib/aarch64-linux-gnu/libnghttp2.so.14',
                  '/usr/lib/aarch64-linux-gnu/libnghttp2.so.14',
                  '/lib/x86_64-linux-gnu/libnghttp2.so.14',
                  '/usr/lib/x86_64-linux-gnu/libnghttp2.so.14']
    if nghttp2_so == '' and fs.exists(cand)
      nghttp2_so = cand
    endif
  endforeach
  if nghttp2_so == '' or not fs.exists(vendored_headers / 'nghttp2' / 'nghttp2.h')
    error('libnghttp2 not found: no pkg-config, no libnghttp2.so.14, or missing vendored headers')
  endif
  message('using vendored nghttp2 headers + ' + nghttp2_so)
  inc_dirs += include_directories('../hpack-h2-bench/c')
  nghttp2_link_args += nghttp2_so
endif

# jemalloc: the profile showed ~28% of cycles in glibc malloc/free from the
# per-request allocation churn. Interpose jemalloc (like the Rust proxy/router
# do via tikv-jemallocator) when present. Disable with -Duse_jemalloc=false.
# --no-as-needed keeps it linked though the program never references its symbols
# directly (it interposes malloc/free); listing it before the implicit libc lets
# its allocator win symbol resolution.
if get_option('use_jemalloc')
  jemalloc_so = ''
  foreach cand : ['/lib/aarch64-linux-gnu/libjemalloc.so.2',
                  '/usr/lib/aarch64-linux-gnu/libjemalloc.so.2',
                  '/lib/x86_64-linux-gnu/libjemalloc.so.2',
                  '/usr/lib/x86_64-linux-gnu/libjemalloc.so.2']
    if jemalloc_so == '' and fs.exists(cand)
      jemalloc_so = cand
    endif
  endforeach
  if jemalloc_so == ''
    warning('use_jemalloc set but libjemalloc.so.2 not found; using system malloc')
  else
    message('interposing jemalloc: ' + jemalloc_so)
    nghttp2_link_args += ['-Wl,--no-as-needed', jemalloc_so, '-Wl,--as-needed']
  endif
endif

datapath_srcs = files(shim_c)

router_srcs = files(
  'src/main.cpp',
  'src/router.cpp',
  'src/relay.cpp',
  'src/relay3.cpp',
)

executable('dmesh-router-cpp',
  datapath_srcs + router_srcs,
  dependencies : deps,
  include_directories : inc_dirs,
  link_args : transport_link_args + nghttp2_link_args,
  install : false,
)
