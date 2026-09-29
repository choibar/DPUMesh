'use strict';
// gRPC over DPUMesh for @grpc/grpc-js. A DPUMesh stream is a Duplex socket;
// serve() injects the streams the DPU routes to DPUMESH_SERVICE into a
// grpc-js server, and connect() opens one to a "<host>:<port>" service
// address. With DPUMESH_ENABLE unset nothing is loaded.
const { Duplex } = require('node:stream');

const EAGAIN = 11;
const EVENT = { ACCEPT: 1, CONNECT: 2, DATA: 3, WRITABLE: 4, EOF: 5, ERROR: 6, RELEASED: 7 };

let addon = null;
let postMax = 0;
const sockets = new Map();
const pendingConnects = new Map();
let nextRequest = 1;
let onAccept = null;

function enabled() {
  return process.env.DPUMESH_ENABLE === '1';
}

function runtime() {
  if (addon !== null) return addon;
  const loaded = require('../build/Release/dpumesh_grpc.node');
  postMax = loaded.open(dispatch);
  addon = loaded;
  // A process closes its channel before it exits, or the DPU sees it vanish
  // mid-stream.
  process.once('exit', () => addon.close());
  // SIGTERM ends a Node.js process without 'exit'. When nothing else handles
  // it, close the channel first, then end the process as the default would.
  if (process.listenerCount('SIGTERM') === 0) {
    process.once('SIGTERM', () => {
      addon.close();
      if (process.listenerCount('SIGTERM') === 0) process.kill(process.pid, 'SIGTERM');
    });
  }
  return addon;
}

function dispatch(kind, id, value, extra) {
  switch (kind) {
    case EVENT.ACCEPT: {
      const socket = new DpumeshSocket(id);
      sockets.set(id, socket);
      if (onAccept !== null) onAccept(socket);
      else socket.destroy();
      break;
    }
    case EVENT.CONNECT: {
      const pending = pendingConnects.get(id);
      pendingConnects.delete(id);
      if (value === 0) {
        pending.reject(new Error(`DPUMesh connect failed: ${extra}`));
        break;
      }
      const socket = new DpumeshSocket(value);
      sockets.set(value, socket);
      pending.resolve(socket);
      break;
    }
    case EVENT.DATA:
      sockets.get(id)?._onData(value);
      break;
    case EVENT.WRITABLE:
      sockets.get(id)?._onWritable();
      break;
    case EVENT.EOF:
      sockets.get(id)?._onEof();
      break;
    case EVENT.ERROR:
      sockets.get(id)?._onError(value, extra);
      break;
    case EVENT.RELEASED:
      sockets.delete(id);
      break;
  }
}

// One DPUMesh stream. Writes are copied into native transmit space at once,
// or wait for the stream's `writable` event when it has none; reads follow
// the Readable protocol, and a full read buffer withholds receive credit.
class DpumeshSocket extends Duplex {
  constructor(id) {
    super();
    this._id = id;
    this._ended = false;
    this._pending = null;
    this.remoteAddress = undefined;
    this.remotePort = undefined;
    this.localAddress = undefined;
    this.localPort = undefined;
  }

  _read() {
    addon.pause(this._id, false);
  }

  _write(chunk, encoding, callback) {
    this._pending = { chunk, offset: 0, callback };
    this._flush();
  }

  _writev(chunks, callback) {
    const chunk = chunks.length === 1 ? chunks[0].chunk : Buffer.concat(chunks.map((c) => c.chunk));
    this._write(chunk, null, callback);
  }

  _flush() {
    const pending = this._pending;
    if (pending === null) return;
    while (pending.offset < pending.chunk.length) {
      const n = addon.write(this._id, pending.chunk, pending.offset);
      if (n > 0) {
        pending.offset += n;
      } else if (n === -EAGAIN) {
        return; // resumes on the stream's writable event
      } else {
        this._pending = null;
        pending.callback(new Error(`DPUMesh write failed: errno ${-n}`));
        return;
      }
    }
    this._pending = null;
    pending.callback();
  }

  _final(callback) {
    this._end(false);
    // A closed stream delivers nothing more, so the readable side ends too.
    this.push(null);
    callback();
  }

  _destroy(error, callback) {
    this._end(error != null);
    callback(error);
  }

  _end(abort) {
    if (this._ended) return;
    this._ended = true;
    addon.end(this._id, abort);
  }

  _onData(buffer) {
    if (!this.push(buffer)) addon.pause(this._id, true);
  }

  _onWritable() {
    this._flush();
  }

  _onEof() {
    this.push(null);
  }

  _onError(errno, message) {
    this.destroy(new Error(`DPUMesh stream failed: ${message} (errno ${errno})`));
  }

  // net.Socket methods HTTP/2 may call; a DPUMesh stream has no such knobs.
  setNoDelay() { return this; }
  setKeepAlive() { return this; }
  setTimeout(ms, callback) {
    if (callback) this.once('timeout', callback);
    return this;
  }
  ref() { return this; }
  unref() { return this; }
}

// Hands the streams the DPU routes to DPUMESH_SERVICE to `onSocket`. One
// listener at a time; close() stops accepting.
function listen(onSocket) {
  runtime();
  if (onAccept !== null) throw new Error('DPUMesh already has a listener');
  onAccept = onSocket;
  addon.listen(true);
  return {
    close() {
      if (onAccept !== onSocket) return;
      addon.listen(false);
      onAccept = null;
    },
  };
}

// Serves DPUMESH_SERVICE for a grpc-js server: the streams the DPU routes to
// the service become the server's HTTP/2 connections. close() stops
// accepting and closes the server's DPUMesh connections.
function serve(server, credentials) {
  const injector = server.createConnectionInjector(credentials);
  const listener = listen((socket) => injector.injectConnection(socket));
  return {
    close() {
      listener.close();
      injector.destroy();
    },
  };
}

// Opens a DPUMesh stream to a "<host>:<port>" service address.
function connect(service) {
  runtime();
  return new Promise((resolve, reject) => {
    const request = nextRequest++;
    pendingConnects.set(request, { resolve, reject });
    addon.connect(service, request);
  });
}

module.exports = {
  enabled,
  listen,
  serve,
  connect,
  DpumeshSocket,
  get postMax() {
    runtime();
    return postMax;
  },
};
