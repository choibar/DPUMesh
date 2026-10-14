'use strict';
// A grpc-js server over libdpumesh_stream_loopback.so, which routes the
// process's streams to its own listener. grpc-js has no client transport hook,
// so the client speaks gRPC over node:http2 on a DPUMesh socket. Build the
// loopback first: integrations/grpc/cpp into build/grpc.
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const http2 = require('node:http2');
const path = require('node:path');
const { test, before, after } = require('node:test');

function findLoopback() {
  for (let dir = __dirname; dir !== path.dirname(dir); dir = path.dirname(dir)) {
    const candidate = path.join(dir, 'build', 'grpc', 'libdpumesh_stream_loopback.so');
    if (fs.existsSync(candidate)) return candidate;
  }
  throw new Error('libdpumesh_stream_loopback.so not found');
}
process.env.DPUMESH_STREAM_LIBRARY ||= findLoopback();
process.env.DPUMESH_SERVICE = 'echo.test:50051';

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const dpumesh = require('..');

const definition = protoLoader.loadSync(path.join(__dirname, 'echo.proto'), {
  longs: Number, defaults: true,
});
const echo = definition['dpumesh.test.Echo'];

let server;
let serving;

before(() => {
  server = new grpc.Server({
    'grpc.max_receive_message_length': -1,
    'grpc.max_send_message_length': -1,
  });
  server.addService(echo, {
    Unary: (call, callback) => callback(null, call.request),
    Source: (call) => {
      for (let i = 0; i < call.request.count; i += 1) {
        call.write({ data: call.request.data, count: i });
      }
      call.end();
    },
    Sleep: (call, callback) => {
      const timer = setTimeout(() => callback(null, call.request), call.request.delayMs);
      call.on('cancelled', () => clearTimeout(timer));
    },
  });
  serving = dpumesh.serve(server, grpc.ServerCredentials.createInsecure());
});

after(() => {
  serving.close();
  server.forceShutdown();
});

// One HTTP/2 session over one DPUMesh stream.
async function session() {
  const socket = await dpumesh.connect('echo.test:50051');
  return http2.connect('http://echo.test:50051', { createConnection: () => socket });
}

function frame(message) {
  const header = Buffer.alloc(5);
  header.writeUInt32BE(message.length, 1);
  return Buffer.concat([header, message]);
}

// Sends one request message and collects the response messages and status.
function call(client, method, request, headers = {}) {
  const spec = echo[method];
  return new Promise((resolve, reject) => {
    const stream = client.request({
      ':method': 'POST',
      ':path': spec.path,
      'content-type': 'application/grpc',
      te: 'trailers',
      ...headers,
    });
    const chunks = [];
    let status = null;
    stream.on('response', (h) => { if ('grpc-status' in h) status = Number(h['grpc-status']); });
    stream.on('trailers', (t) => { status = Number(t['grpc-status']); });
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('error', reject);
    stream.on('end', () => {
      const body = Buffer.concat(chunks);
      const messages = [];
      for (let at = 0; at < body.length;) {
        const length = body.readUInt32BE(at + 1);
        messages.push(spec.responseDeserialize(body.subarray(at + 5, at + 5 + length)));
        at += 5 + length;
      }
      resolve({ status, messages });
    });
    stream.end(frame(spec.requestSerialize(request)));
  });
}

test('unary echoes across sizes', async () => {
  const client = await session();
  for (const size of [0, 1, 8063, 8064, 8065, 65536, 1 << 20, 4 << 20]) {
    const data = crypto.randomBytes(size);
    const { status, messages } = await call(client, 'Unary', { data });
    assert.equal(status, 0);
    assert.equal(messages.length, 1);
    assert.ok(data.equals(messages[0].data), `size ${size}`);
  }
  client.close();
});

test('server streaming delivers 16 MiB in order', async () => {
  const client = await session();
  const data = crypto.randomBytes(256 * 1024);
  const { status, messages } = await call(client, 'Source', { data, count: 64 });
  assert.equal(status, 0);
  assert.equal(messages.length, 64);
  messages.forEach((m, i) => {
    assert.equal(m.count, i);
    assert.ok(data.equals(m.data));
  });
  client.close();
});

test('concurrent calls share one connection', async () => {
  const client = await session();
  await Promise.all(Array.from({ length: 64 }, async (_, i) => {
    for (let j = 0; j < 20; j += 1) {
      const data = crypto.randomBytes(64 + i);
      const { status, messages } = await call(client, 'Unary', { data });
      assert.equal(status, 0);
      assert.ok(data.equals(messages[0].data));
    }
  }));
  client.close();
});

test('deadline ends the call and the connection stays usable', async () => {
  const client = await session();
  const late = await call(client, 'Sleep', { delayMs: 5000 }, { 'grpc-timeout': '100m' });
  assert.equal(late.status, grpc.status.DEADLINE_EXCEEDED);
  const data = crypto.randomBytes(100);
  const { status, messages } = await call(client, 'Unary', { data });
  assert.equal(status, 0);
  assert.ok(data.equals(messages[0].data));
  client.close();
});

test('many connections open and close', async () => {
  for (let i = 0; i < 20; i += 1) {
    const client = await session();
    const data = crypto.randomBytes(1000);
    const { status, messages } = await call(client, 'Unary', { data });
    assert.equal(status, 0);
    assert.ok(data.equals(messages[0].data));
    await new Promise((resolve) => client.close(resolve));
  }
});
