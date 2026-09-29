'use strict';
// DpumeshSocket pairs over the loopback, below HTTP/2: HTTP/2 flow control
// keeps a gRPC connection's input drained, so only a paused raw reader drives
// the receive credit hold and its resume.
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { test } = require('node:test');

function findLoopback() {
  for (let dir = __dirname; dir !== path.dirname(dir); dir = path.dirname(dir)) {
    const candidate = path.join(dir, 'build', 'grpc', 'libdpumesh_stream_loopback.so');
    if (fs.existsSync(candidate)) return candidate;
  }
  throw new Error('libdpumesh_stream_loopback.so not found');
}
process.env.DPUMESH_STREAM_LIBRARY ||= findLoopback();

const dpumesh = require('..');

async function pair() {
  let accepted;
  const arrived = new Promise((resolve) => { accepted = resolve; });
  const listener = dpumesh.listen((socket) => accepted(socket));
  try {
    const client = await dpumesh.connect('raw.test:1');
    const server = await arrived;
    return { client, server };
  } finally {
    listener.close();
  }
}

function readAll(socket) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    socket.on('data', (chunk) => chunks.push(chunk));
    socket.on('end', () => resolve(Buffer.concat(chunks)));
    socket.on('error', reject);
  });
}

test('a paused reader holds credit, then receives every byte', async () => {
  const { client, server } = await pair();
  const payload = crypto.randomBytes(8 << 20);
  server.pause();
  let written = false;
  client.end(payload, () => { written = true; });
  await new Promise((resolve) => setTimeout(resolve, 300));
  assert.equal(written, false, 'the writer finished although the reader held its credit');
  const received = readAll(server);
  server.resume();
  assert.ok(payload.equals(await received));
  server.destroy();
});

test('destroy resets the local side and ends the peer', async () => {
  const { client, server } = await pair();
  const ended = readAll(server);
  client.destroy(new Error('reset'));
  await assert.rejects(new Promise((resolve, reject) => {
    client.on('error', reject);
    client.on('close', () => reject(new Error('closed')));
  }));
  assert.equal((await ended).length, 0);
  server.destroy();
});
