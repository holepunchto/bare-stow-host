const { Protocol, DEFAULT_GRACE } = require('bare-stow-protocol')

class IPC extends Protocol {
  constructor(stream, opts = {}) {
    super(stream)

    const { kill = () => stream.destroy(), timeout = null, grace = DEFAULT_GRACE } = opts

    this._kill = kill
    this._grace = grace
    this._terminating = null
    this._exitReported = false
    this._timer = null
    this._failure = null
    this._failing = null

    this.once('exit', () => {
      this._exitReported = true
    })

    this.send('configure', { grace })

    this._ready = new Promise((resolve, reject) => {
      this._timer =
        timeout === null
          ? null
          : setTimeout(
              () => onerror(workerError('Worker timed out before ready', 'WORKER_TIMEOUT')),
              timeout
            )

      const cleanup = () => {
        clearTimeout(this._timer)
        this.off('ready', onready).off('error', onerror).off('exit', onexit).off('close', onclose)
      }

      const onready = () => {
        cleanup()
        resolve()
      }

      const onerror = (err) => {
        cleanup()
        reject(err)
      }

      const onexit = (code) => {
        // An error frame precedes the exit frame and is emitted on destroy
        if (this.destroying) return
        onerror(
          workerError(`Worker exited with code ${code} before ready`, 'WORKER_EXITED', {
            exitCode: code
          })
        )
      }

      const onclose = () => onerror(workerError('Worker closed before ready', 'WORKER_CLOSED'))

      this.on('ready', onready).on('error', onerror).on('exit', onexit).on('close', onclose)
    })

    this._ready.catch(() => this.destroy())
  }

  get ready() {
    return this._ready
  }

  terminate() {
    if (this._stream.destroyed) return Promise.resolve(undefined)

    return new Promise((resolve) => {
      this.once('exit', resolve)

      this._terminate().then(() => {
        this.off('exit', resolve)
        resolve(undefined)
      })
    })
  }

  exited(code, signal = null) {
    // The worker either said why it exited or was asked to.
    if (this._exitReported || this._terminating !== null || this.destroying) return

    const err =
      signal === null
        ? workerError(`Worker exited with code ${code}`, 'WORKER_EXITED', { exitCode: code })
        : workerError(`Worker was killed by ${signal}`, 'WORKER_KILLED', {
            exitCode: code,
            signalCode: signal
          })

    if (this._failure !== null) {
      clearTimeout(this._failing)
      err.cause = this._failure
    }

    this.destroy(err)
  }

  // A worker whose transport ends without it saying why has crashed, so the end
  // is held back until its exit is reported for readers to fail with. It can no
  // longer time out, as it already ended.
  _onend() {
    if (this._crashed()) clearTimeout(this._timer)
    else super._onend()
  }

  _onclose() {
    if (this._crashed()) clearTimeout(this._timer)
    else super._onclose()
  }

  // A peer that goes away resets or breaks the connection rather than ending
  // it, which is no different from it closing. Other failures of the transport
  // of a worker that may have crashed wait for its exit as an end does, killing
  // it if it hasn't exited within the grace period.
  _onerror(err) {
    if (err.code === 'ECONNRESET' || err.code === 'EPIPE') return this._onclose()

    const failure = workerError(err.message, 'TRANSPORT_ERROR', { cause: err })

    if (!this._crashed()) return this.destroy(failure)

    clearTimeout(this._timer)

    this._failure = failure
    this._failing = setTimeout(() => {
      this._kill()
      this.destroy(failure)
    }, this._grace)
  }

  _crashed() {
    return !this._exitReported && this._terminating === null && !this.destroying
  }

  _destroy(err, cb) {
    if (this._stream.destroyed) return cb(null)

    this._terminate().then(() => cb(null))
  }

  _terminate() {
    if (this._terminating !== null) return this._terminating

    const timer = setTimeout(() => this._kill(), this._grace)

    this.send('terminate')

    this._terminating = new Promise((resolve) => {
      this._stream.once('close', () => {
        clearTimeout(timer)
        resolve()
      })
    })

    return this._terminating
  }
}

function workerError(message, code, props) {
  const err = new Error(message)

  err.code = code

  return Object.assign(err, props)
}

exports.IPC = IPC

exports.wrap = function wrap(stream, opts) {
  return new IPC(stream, opts)
}
