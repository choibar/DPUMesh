# ARM implementations and their authenticated control plane. The hardware
# crypto owner is a separate process; never link its main() into Linkerd.
peer_deps = deps + [dependency('libssl'), dependency('libcrypto'),
  dependency('libibverbs'), dependency('doca-rdma'),
  dependency('doca-verbs'), meson.get_compiler('c').find_library('dl')]
peer_sources = files(
  'peer_channel.c', 'peer_pair_protocol.c',
  'peer_tls.c', 'peer_control_protocol.c', 'peer_control_session.c', 'peer_association.c',
  'peer_manager.c', 'peer_crypto_ipc.c', 'peer_crypto_client.c',
  'peer_pair_transport.c',
  'peer_carrier_tcp.c',
  'peer_carrier_write_imm.c',
  'peer_write_imm_dpa.c', 'peer_imm_native.c',
  'peer_dpa_owner.c', 'peer_topology.c', 'peer_identity.c',
  'peer_authority.c', 'local_control_session.c', 'peer_connection_io.c',
  'peer_node.c',
)
dmesh_peer = static_library('dmesh_peer', peer_sources,
  dependencies : peer_deps,
  include_directories : include_directories('.'),
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
