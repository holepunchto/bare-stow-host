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
 * signal, or a crash, or when the underlying transport errors, and is destroyed either way. For a
 * worker that exited without saying why, `err.exitCode` and `err.signalCode` carry the reason.
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
   * Report that the worker has exited, as observed by the transport. If the worker exited without
   * saying why and was not asked to, the handle is destroyed with an error carrying the reason as
   * `exitCode` and `signalCode`.
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
