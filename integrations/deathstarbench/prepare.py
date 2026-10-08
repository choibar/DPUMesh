#!/usr/bin/env python3
"""Create an isolated HotelReservation source tree; never modify the input."""
import argparse
import pathlib
import re
import shutil
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('source', type=pathlib.Path, help='HotelReservation source directory (baseline 312855ac)')
p.add_argument('output', type=pathlib.Path)
p.add_argument('--dpumesh', required=True, type=pathlib.Path)
p.add_argument('--go', default='go')
a = p.parse_args()
root = pathlib.Path(__file__).resolve().parent
if a.output.exists():
    p.error('output already exists; use a fresh directory')
if not (a.dpumesh / 'integrations/grpc/go/go.mod').is_file():
    p.error('DPUMesh checkout with current Go integration is required')
if not re.search(r'ID:\s+id,', (a.source/'registry/registry.go').read_text()):
    p.error('source registry differs from clean baseline; use a clean git worktree (replica IDs must be unique)')
shutil.copytree(a.source, a.output, ignore=shutil.ignore_patterns('.git', 'vendor', 'bin', '*.orig'))
subprocess.run(['git', 'apply', '--check', str(root / 'hotelreservation.patch')], cwd=a.output, check=True)
subprocess.run(['git', 'apply', str(root / 'hotelreservation.patch')], cwd=a.output, check=True)
shutil.copytree(root / 'overlay', a.output, dirs_exist_ok=True)
subprocess.run([a.go, 'mod', 'edit', '-go=1.26', '-require=dmeshgo@v0.0.0',
                '-replace=dmeshgo=' + str((a.dpumesh / 'integrations/grpc/go').resolve())], cwd=a.output, check=True)
subprocess.run([a.go, 'mod', 'tidy'], cwd=a.output, check=True)
print(a.output.resolve())
