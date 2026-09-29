import { Duplex } from 'node:stream';
import type { Server, ServerCredentials } from '@grpc/grpc-js';

/** True when DPUMESH_ENABLE is "1". */
export function enabled(): boolean;
/** Hands the streams routed to DPUMESH_SERVICE to `onSocket`. */
export function listen(onSocket: (socket: DpumeshSocket) => void): { close(): void };
/** Serves DPUMESH_SERVICE for `server`; close() stops accepting. */
export function serve(server: Server, credentials: ServerCredentials): { close(): void };
/** Opens a DPUMesh stream to a "<host>:<port>" service address. */
export function connect(service: string): Promise<DpumeshSocket>;
export class DpumeshSocket extends Duplex {}
export const postMax: number;
