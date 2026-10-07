#!/usr/bin/env bash
set -euo pipefail
if [[ $# != 2 ]]; then echo "usage: $0 <DPUMesh-checkout> <prepared-hotelReservation>" >&2; exit 2; fi
dpumesh=$(realpath "$1")
hotel=$(realpath "$2")
make -C "$dpumesh" -j"${BUILD_JOBS:-4}" lib
(cd "$dpumesh/integrations/grpc/go" && go test -race ./...)
(cd "$hotel" && go test -race ./dmesh ./dialer ./services/geo && go build -a -o bin/ ./cmd/...)
sha256sum "$dpumesh/build/lib/libdpumesh.so.5" "$hotel"/bin/*
