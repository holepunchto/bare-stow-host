const test = require('brittle')
const { Duplex } = require('bare-stream')
const protocol = require('bare-stow-protocol')
const host = require('.')

test('host wrap resolves ready when worker signals', async (t) => {
  const [left, right] = pair()
  const ipc = host.wrap(left)
  const worker = protocol.attach(right)

  worker.send('ready')

  await ipc.ready

  t.pass('ready resolved')
})

test('host wrap rejects ready when worker errors before ready', async (t) => {
  const [left, right] = pair()
  const ipc = host.wrap(left)
  const worker = protocol.attach(right)

  worker.on('terminate', () => right.destroy())
  worker.send('error', { message: 'boom' })

  const err = await ipc.ready.catch((err) => err)

  t.is(err.message, 'boom')
  t.is(err.code, 'WORKER_ERROR')
})

test('host wrap rejects ready when worker exits before ready', async (t) => {
  const [left, right] = pair()
  const ipc = host.wrap(left)
  const worker = protocol.attach(right)

  worker.on('terminate', () => right.destroy())
  worker.send('exit', { code: 1 })

  const err = await ipc.ready.catch((err) => err)

  t.is(err.code, 'WORKER_EXITED')
  t.is(err.exitCode, 1)
})

test('host wrap rejects ready when destroyed before ready', async (t) => {
  const [left, right] = pair()
  const ipc = host.wrap(left)
  const worker = protocol.attach(right)

  worker.on('terminate', () => right.destroy())

  ipc.destroy()

  const err = await ipc.ready.catch((err) => err)

  t.is(err.code, 'WORKER_CLOSED')
})

test('host wrap rejects ready when the transport fails', async (t) => {
  const [left] = pair()
  const ipc = host.wrap(left, { grace: 10 })
  const failure = new Error('broken')

  left.destroy(failure)

  const err = await ipc.ready.catch((err) => err)

  t.is(err.code, 'TRANSPORT_ERROR')
  t.is(err.cause, failure)
})

test('host wrap rejects ready when writing to the transport fails', async (t) => {
  const failure = new Error('broken')
  const stream = new Duplex({
    write(data, encoding, cb) {
      cb(failure)
    }
  })
  const ipc = host.wrap(stream, { grace: 10 })

  const err = await ipc.ready.catch((err) => err)

  t.is(err.code, 'TRANSPORT_ERROR')
  t.is(err.cause, failure)
})

test('host wrap ignores exit after ready', async (t) => {
  const [left, right] = pair()
  const ipc = host.wrap(left)
  const worker = protocol.attach(right)

  worker.send('ready')
  await ipc.ready

  worker.send('exit', { code: 1 })
  right.destroy()

  t.is(await ipc.ready, undefined)
})

test('host wrap emits exit with code', (t) => {
  t.plan(1)

  const [left, right] = pair()
  const ipc = host.wrap(left)
  const worker = protocol.attach(right)

  ipc.on('exit', (code) => t.is(code, 7))

  worker.send('exit', { code: 7 })
})

test('host wrap is the user duplex', (t) => {
  t.plan(1)

  const [left, right] = pair()
  const ipc = host.wrap(left)
  const worker = protocol.attach(right)

  ipc.on('data', (data) => t.alike(data, Buffer.from('payload')))

  worker.write(Buffer.from('payload'))
})

test('host terminate resolves to exit code', async (t) => {
  const [left, right] = pair()
  const ipc = host.wrap(left)
  const worker = protocol.attach(right)

  worker.on('terminate', () => worker.send('exit', { code: 0 }))
  worker.send('ready')
  await ipc.ready

  const code = await ipc.terminate()

  t.is(code, 0)
})

test('host destroy sends terminate to worker', (t) => {
  t.plan(2)

  const [left, right] = pair()
  const ipc = host.wrap(left)
  const worker = protocol.attach(right)

  worker.on('terminate', () => {
    t.pass('worker received terminate')
    right.destroy()
  })
  ipc.on('close', () => t.pass('host closed'))

  ipc.destroy()
})

test('host emits close when underlying stream closes', async (t) => {
  t.plan(1)

  const [left, right] = pair()
  const ipc = host.wrap(left)
  const worker = protocol.attach(right)

  ipc.on('close', () => t.pass('close fired')).resume()

  worker.send('exit', { code: 0 })
  await new Promise((resolve) => ipc.once('exit', resolve))

  right.destroy()
})

test('host wrap rejects ready and terminates the worker on timeout', (t) => {
  t.plan(2)

  const [left, right] = pair()
  const ipc = host.wrap(left, { timeout: 10 })
  const worker = protocol.attach(right)

  worker.on('terminate', () => {
    t.pass('worker received terminate')
    right.destroy()
  })

  ipc.ready.catch((err) => t.is(err.code, 'WORKER_TIMEOUT'))
})

test('host terminate kills the worker after the grace period', async (t) => {
  const [left, right] = pair()
  const ipc = host.wrap(left, { grace: 10 })
  const worker = protocol.attach(right)

  worker.send('ready')
  await ipc.ready

  t.is(await ipc.terminate(), undefined)
  t.ok(left.destroyed)
})

test('host terminate calls kill after the grace period', async (t) => {
  t.plan(2)

  const [left, right] = pair()
  const ipc = host.wrap(left, {
    grace: 10,
    kill() {
      t.pass('kill called')
      right.destroy()
    }
  })
  const worker = protocol.attach(right)

  worker.send('ready')
  await ipc.ready

  t.is(await ipc.terminate(), undefined)
})

test('host destroy kills the worker after the grace period', (t) => {
  t.plan(1)

  const [left] = pair()
  const ipc = host.wrap(left, { grace: 10 })

  ipc.on('close', () => t.ok(left.destroyed))

  ipc.destroy()
})

test('host terminate resolves once the stream has closed', async (t) => {
  const [left, right] = pair()
  const ipc = host.wrap(left)
  const worker = protocol.attach(right)

  ipc.resume()

  worker.send('exit', { code: 0 })
  await new Promise((resolve) => ipc.once('exit', resolve))

  right.destroy()
  await new Promise((resolve) => ipc.once('close', resolve))

  t.is(await ipc.terminate(), undefined)
})

test('host exited before ready rejects ready with the exit code', async (t) => {
  const [left] = pair()
  const ipc = host.wrap(left, { grace: 10 })

  ipc.exited(3)

  const err = await ipc.ready.catch((err) => err)

  t.is(err.code, 'WORKER_EXITED')
  t.is(err.exitCode, 3)
})

test('host exited after ready emits an error with the signal', async (t) => {
  const [left, right] = pair()
  const ipc = host.wrap(left, { grace: 10 })
  const worker = protocol.attach(right)

  worker.send('ready')
  await ipc.ready

  const error = new Promise((resolve) => ipc.once('error', resolve))

  ipc.exited(null, 'SIGKILL')

  const err = await error

  t.is(err.code, 'WORKER_KILLED')
  t.is(err.signalCode, 'SIGKILL')
})

test('host exited is ignored after an exit frame', async (t) => {
  const [left, right] = pair()
  const ipc = host.wrap(left)
  const worker = protocol.attach(right)

  ipc.on('error', () => t.fail('reported as an error'))

  worker.send('ready')
  await ipc.ready

  worker.send('exit', { code: 0 })
  await new Promise((resolve) => ipc.once('exit', resolve))

  ipc.exited(0)

  t.absent(ipc.destroying)
})

test('host exited is ignored while terminating', async (t) => {
  const [left, right] = pair()
  const ipc = host.wrap(left, { grace: 10 })
  const worker = protocol.attach(right)

  ipc.on('error', () => t.fail('reported as an error'))

  worker.send('ready')
  await ipc.ready

  const terminated = ipc.terminate()

  ipc.exited(null, 'SIGKILL')

  t.is(await terminated, undefined)
})

test('host holds the end of a crashed worker until its exit is reported', async (t) => {
  const [left, right] = pair()
  const ipc = host.wrap(left)
  const worker = protocol.attach(right)

  worker.send('ready')
  await ipc.ready

  const error = new Promise((resolve) => ipc.once('error', resolve))

  right.destroy()
  await new Promise((resolve) => left.once('close', resolve))

  t.absent(ipc.destroyed)

  ipc.exited(null, 'SIGSEGV')

  const err = await error

  t.is(err.code, 'WORKER_KILLED')
  t.is(err.signalCode, 'SIGSEGV')
})

test('host holds the close of a worker that crashes before ready', async (t) => {
  const [left, right] = pair()
  const ipc = host.wrap(left)

  right.destroy()
  await new Promise((resolve) => left.once('close', resolve))

  ipc.exited(1)

  const err = await ipc.ready.catch((err) => err)

  t.is(err.code, 'WORKER_EXITED')
  t.is(err.exitCode, 1)
})

test('host fails a crashed worker with its exit rather than a timeout', async (t) => {
  const [left, right] = pair()
  const ipc = host.wrap(left, { timeout: 50 })

  right.destroy()
  await new Promise((resolve) => setTimeout(resolve, 100))

  ipc.exited(1)

  const err = await ipc.ready.catch((err) => err)

  t.is(err.code, 'WORKER_EXITED')
})

test('host fails a crashed worker with its exit rather than a transport failure', async (t) => {
  const [left] = pair()
  const ipc = host.wrap(left)
  const failure = new Error('broken')

  left.destroy(failure)
  await new Promise((resolve) => left.once('close', resolve))

  ipc.exited(null, 'SIGSEGV')

  const err = await ipc.ready.catch((err) => err)

  t.is(err.code, 'WORKER_KILLED')
  t.is(err.cause.cause, failure)
})

test('host kills a worker whose transport fails and that does not exit', async (t) => {
  t.plan(3)

  const [left] = pair()
  const failure = new Error('broken')
  const ipc = host.wrap(left, {
    grace: 10,
    kill: () => t.pass('killed')
  })

  left.destroy(failure)

  const err = await ipc.ready.catch((err) => err)

  t.is(err.code, 'TRANSPORT_ERROR')
  t.is(err.cause, failure)
})

test('host treats a reset transport as closed', async (t) => {
  const [left] = pair()
  const ipc = host.wrap(left)
  const reset = new Error('reset')

  reset.code = 'ECONNRESET'

  left.destroy(reset)
  await new Promise((resolve) => left.once('close', resolve))

  ipc.exited(1)

  const err = await ipc.ready.catch((err) => err)

  t.is(err.code, 'WORKER_EXITED')
  t.absent(err.cause)
})

test('host ends a worker that exits on its own without holding', async (t) => {
  const [left, right] = pair()
  const ipc = host.wrap(left)
  const worker = protocol.attach(right)

  worker.send('ready')
  await ipc.ready

  ipc.resume()

  const closed = new Promise((resolve) => ipc.once('close', resolve))

  worker.send('exit', { code: 0 })
  await new Promise((resolve) => ipc.once('exit', resolve))

  right.destroy()

  await closed

  t.pass('closed')
})

function pair() {
  let a, b

  a = new Duplex({
    write(data, encoding, cb) {
      b.push(Buffer.from(data))
      cb(null)
    },
    destroy(err, cb) {
      if (!b.destroyed) b.destroy()
      cb(null)
    }
  })

  b = new Duplex({
    write(data, encoding, cb) {
      a.push(Buffer.from(data))
      cb(null)
    },
    destroy(err, cb) {
      if (!a.destroyed) a.destroy()
      cb(null)
    }
  })

  return [a, b]
}
