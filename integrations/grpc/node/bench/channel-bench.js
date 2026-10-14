#!/usr/bin/env node
// channel-bench server for grpc-js: the Node.js peer of
// integrations/grpc/go/cmd/channel-bench. It serves the same wire
// (/dmesh.ChannelBench/Echo, the raw request echoed unchanged) over DPUMesh
// or, with -tcp host:port, kernel TCP, and takes the same flags. grpc-js has
// no client transport hook, so there is no client mode: drive it with the Go
// or C++ client. Load flags (-connections, -duration, ...) are accepted and
// ignored, so a harness can pass one flag set to every implementation.
'use strict';

const grpc = require('@grpc/grpc-js');

function parseFlags(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (!arg.startsWith('-')) throw new Error(`unexpected argument ${arg}`);
    const body = arg.replace(/^--?/, '');
    const eq = body.indexOf('=');
    if (eq >= 0) out[body.slice(0, eq)] = body.slice(eq + 1);
    else if (i + 1 < argv.length) out[body] = argv[++i];
    else throw new Error(`flag -${body} needs a value`);
  }
  return out;
}

const flags = parseFlags(process.argv.slice(2));
if ((flags.mode || 'client') !== 'server') {
  console.error('grpc-js has no client transport hook: use the Go or C++ channel-bench client');
  process.exit(2);
}

const server = new grpc.Server();
const raw = (buffer) => buffer;
server.register('/dmesh.ChannelBench/Echo', (call, callback) => callback(null, call.request),
  raw, raw, 'unary');
const credentials = grpc.ServerCredentials.createInsecure();
let dpumesh = null;

function stop() {
  if (dpumesh) dpumesh.close();
  const force = setTimeout(() => server.forceShutdown(), 5000);
  server.tryShutdown(() => {
    clearTimeout(force);
    console.error('CHANNEL_BENCH_SERVER_CLOSED');
    // 'exit' closes the DPUMesh runtime.
    process.exit(0);
  });
}
process.once('SIGTERM', stop);
process.once('SIGINT', stop);

if (flags.tcp) {
  server.bindAsync(flags.tcp, credentials, (err) => {
    if (err) {
      console.error(err.message);
      process.exit(1);
    }
    console.error(`CHANNEL_BENCH_SERVER_READY service=${flags.tcp}`);
  });
} else {
  dpumesh = require('..').serve(server, credentials);
  console.error(`CHANNEL_BENCH_SERVER_READY service=${process.env.DPUMESH_SERVICE || ''}`);
}
