import { Duplex } from 'bare-stream'
import { Protocol } from 'bare-stow-protocol'

/** Options for `wrap()`. */
interface IPCOptions {
  /**
   * Stop the worker once it has ignored a `terminate` control frame for `grace` milliseconds.
   * Defaults to destroying `stream`.
   */
  kill?: () => unknown
  /**
   * The number of milliseconds to wait for the worker to signal ready before rejecting `ready` and
   * terminating the worker, or `null` to wait indefinitely. Defaults to `null`.
   */
  timeout?: number | null
  /**
   * The number of milliseconds a terminating worker gets to run `stop()` and exit before `kill` is
   * called. Defaults to `5000`.
   */
  grace?: number
}

/**
 * The host side of a stowed bundle's lifecycle, layered on a `Protocol`. It emits `exit` with the
 * code the worker signals, and `close` once the underlying transport closes. It emits `error` when
 * the worker reports a fault, when it exits without saying why, such as through `Bare.exit()`, a
 * signal, or a crash, or when the underlying transport errors, and is destroyed either way.
 *
 * The errors that it raises carry a `code`: `WORKER_ERROR` for a fault the worker reported,
 * `WORKER_EXITED` with `exitCode` for a worker that exited, `WORKER_KILLED` with `signalCode` for a
 * worker killed by a signal, `WORKER_CLOSED` for a transport that closed before ready, and
 * `WORKER_TIMEOUT` for a worker that timed out before ready, and `TRANSPORT_ERROR` with `cause` for
 * a transport that failed while reading or writing. A transport that is reset or broken by the
 * worker going away counts as closing rather than failing.
 *
 * Destroying it sends a `terminate` control frame to the worker and waits for the underlying
 * transport to close, calling `kill` if the worker hasn't exited within `grace`.
 */
interface IPC extends Protocol {
  /**
   * A promise that resolves once the worker has signaled ready, and rejects if the worker errors,
   * exits, closes, or times out before then. The worker is terminated when it rejects.
   */
  readonly ready: Promise<void>
  /**
   * Like `destroy()`, but resolve once the worker exits.
   * @returns The worker's exit `code`, or `undefined` if it closed without signaling one.
   */
  terminate(): Promise<number | undefined>
  /**
   * Report that the worker has exited, as observed by the transport. Whoever wraps the transport
   * must call it once the worker exits. If the worker exited without saying why and was not asked
   * to, the handle is destroyed with a `WORKER_EXITED` or `WORKER_KILLED` error carrying the reason
   * as `exitCode` and `signalCode`. Until then, the end of a transport that ends without the worker
   * saying why is held back. A transport that fails instead waits for the exit for `grace`
   * milliseconds, failing with the exit and the failure as its `cause`, or killing the worker and
   * failing with `TRANSPORT_ERROR` if it hasn't exited by then.
   * @param code - The exit code of the worker, or `null` if it was killed by a signal.
   * @param signal - The signal that killed the worker, or `null` (default `null`).
   */
  exited(code: number | null, signal?: string | null): void
}

declare class IPC {
  /**
   * Wrap the host side of a stowed bundle's transport `stream`, sending `grace` to the worker in a
   * `configure` control frame straight away.
   * @param stream - The host side of a stowed bundle's transport (any duplex byte stream).
   * @param opts - Options; see `IPCOptions`.
   */
  constructor(stream: Duplex, opts?: IPCOptions)
}

/**
 * Wrap the host side of a stowed bundle's transport `stream`, sending `grace` to the worker in a
 * `configure` control frame straight away.
 * @param stream - The host side of a stowed bundle's transport (any duplex byte stream).
 * @param opts - Options; see `IPCOptions`.
 * @returns An `IPC` handle wrapping `stream`.
 */
declare function wrap(stream: Duplex, opts?: IPCOptions): IPC

export { IPC, type IPCOptions, wrap }
