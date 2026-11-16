# ARM implementations and their authenticated control plane. The hardware
# crypto owner is a separate process; never link its main() into Linkerd.
peer_deps = deps + [dependency('libssl'), dependency('libcrypto'),
  dependency('libibverbs'), dependency('librdmacm'), dependency('doca-rdma'),
  dependency('doca-verbs'), meson.get_compiler('c').find_library('dl')]
peer_sources = files(
  'peer_channel.c', 'peer_datapath_config.c', 'peer_pair_wire.c',
  'peer_tls.c', 'peer_security_wire.c', 'peer_control.c', 'peer_association.c',
  'peer_manager.c', 'peer_crypto_ipc.c', 'peer_pair_transport.c',
  'peer_pair_verbs.c', 'peer_transport.c', 'peer_wire_tcp.c', 'peer_wire_rdma.c',
  'peer_wire_write_imm.c', 'peer_wire_write_sync.c', 'peer_sync_event.c',
  'peer_pair_dpa.c', 'peer_write_sync_dpa.c', 'peer_sync_native.c',
  'peer_dpa_owner.c', 'peer_topology.c', 'peer_workload_identity.c',
  'peer_local_control.c', 'peer_authority.c', 'peer_session_control.c', 'peer_stream.c',
  'peer_node.c',
)
dmesh_peer = static_library('dmesh_peer', peer_sources,
  dependencies : peer_deps,
  include_directories : include_directories('.'),
  c_args : ['-DDMESH_DPA_RDMA_OFFLOAD=1'],
)

# Optional standalone inline-IPsec owner. It is shared by native and migrated
# runtimes on a node; never start two owners on the same switch device.
flow_dep = dependency('doca-flow', required : false)
dpdk_dep = dependency('libdpdk', required : false)
bridge_dep = dependency('doca-dpdk-bridge', required : false)
if flow_dep.found() and dpdk_dep.found() and bridge_dep.found()
  owner_args = ['-Wno-deprecated-declarations']
  if flow_dep.version().version_compare('>=3.5')
    owner_args += '-DDMESH_DOCA35'
  endif
  executable('dpumesh_crypto_owner', ['peer_crypto_owner.c', 'peer_crypto_ipc.c'],
    dependencies : [dependency('doca-common'), flow_dep, bridge_dep, dpdk_dep, dependency('libcrypto')],
    c_args : owner_args,
    install : false,
  )
endif
