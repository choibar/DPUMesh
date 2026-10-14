#!/bin/bash
# build_wheel.sh [out dir]: builds grpcio 1.80.0 with gRPC over DPUMesh
# (patches/grpcio-1.80.0-dpumesh.patch) into <out dir>/wheels, default
# build/grpcio of this checkout. Needs a C++17 compiler and Python headers; the
# build compiles all of gRPC C-core, a few minutes with JOBS=32.
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
OUT=$(mkdir -p "${1:-$ROOT/build/grpcio}" && cd "${1:-$ROOT/build/grpcio}" && pwd)
VERSION=1.80.0
PYTHON=${PYTHON:-python3}

cd "$OUT"
[ -x venv/bin/python ] || "$PYTHON" -m venv venv
venv/bin/pip install -q --upgrade pip setuptools wheel "cython>=3.0,<4"
[ -f "grpcio-$VERSION.tar.gz" ] ||
    venv/bin/pip download -q "grpcio==$VERSION" --no-binary :all: --no-deps -d .
rm -rf src && mkdir src && tar xzf "grpcio-$VERSION.tar.gz" -C src
patch -s -d "src/grpcio-$VERSION" -p1 < "$HERE/patches/grpcio-$VERSION-dpumesh.patch"
# setup.py takes sources inside its tree only.
mkdir "src/grpcio-$VERSION/dpumesh"
cp "$HERE"/src/dpumesh_grpcio.{cc,h} "$ROOT"/integrations/grpc/cpp/include/dpumesh_stream.h \
    "$ROOT"/integrations/grpc/cpp/src/{dmesh_endpoint.cc,dmesh_endpoint.h,endpoint_transport.h,executor.h} \
    "src/grpcio-$VERSION/dpumesh/"
(cd "src/grpcio-$VERSION" &&
    GRPC_PYTHON_BUILD_WITH_CYTHON=1 \
    GRPC_PYTHON_BUILD_EXT_COMPILER_JOBS=${JOBS:-$(nproc)} \
    "$OUT/venv/bin/pip" wheel -q --no-build-isolation --no-deps -w "$OUT/wheels" .)
ls "$OUT"/wheels/grpcio-$VERSION-*.whl
