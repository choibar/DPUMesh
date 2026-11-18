# Archived peer data paths

This directory preserves the former SEND (ARM and descriptor DPA),
WRITE+Sync (ARM and native DPA), and ARM-only WRITE_WITH_IMM implementations.
The mixed Sync/IMM files retain their pre-refactor code for reference.
Their configuration, protocol helpers and tests are archived alongside them.
They are excluded from both the ARM Meson library and the DPA app, and from
`make test`. There is no build or runtime selector to enable them.

The live path is WRITE_WITH_IMM+DPA:

- `../peer_carrier_write_imm.c`: ARM framing, bootstrap and source lifetime.
- `../peer_write_imm_dpa.c`: native setup, descriptor publication and status.
- `../peer_imm_native.c`: DOCA device/PD and native RDMA connection setup.
- `../device/peer_write_imm_dpa.c`: NIC WQEs, CQEs and IMM receive reposts.
- `../device/peer_runtime.c`: bounded scheduling of the attached IMM contexts.

`DPUMESH_PEER_RDMA_DEVICE` enables the peer runtime; it always constructs
WRITE_WITH_IMM+DPA. Authentication, topology, local registration and IPsec
configuration are still required. The old transport/offload/Sync-wait
selectors are no longer read. Native Host DMA remains a separate data path.

A single protected SEND is still used to exchange each 64-byte bootstrap;
application data and returned byte credits use WRITE_WITH_IMM. TCP/TLS remains
the authenticated control plane. These setup/control operations are not the
archived SEND data path.

The private ARM/DPA layouts changed. Rebuild the ARM library, peer DPA app
and proxy together. The bootstrap retains the existing native IMM wire format
(`DMWS`, version 4, mode 3, 64 bytes). The Host DMA/public library ABI is unchanged.

These archived sources are historical reference, not a separately supported
build target; shared interfaces may continue to evolve in the live code.
