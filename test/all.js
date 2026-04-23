const p = require('path')
const fs = require('fs')
const os = require('os')
const test = require('tape')
const Corestore = require('corestore')
const rimraf = require('rimraf')
const xattr = require('fs-xattr')
const Hyperdrive = require('hyperdrive')

const { HyperdriveFuse } = require('..')

function createDrive () {
  const dir = fs.mkdtempSync(p.join(os.tmpdir(), 'hyperdrive-fuse-'))
  return new Hyperdrive(new Corestore(dir))
}

test('can read/write a small file', async (t) => {
  const drive = createDrive()
  const fuse = new HyperdriveFuse(drive, './mnt')

  const onint = () => cleanup(fuse, true)
  process.on('SIGINT', onint)

  await fuse.mount()

  const NUM_SLICES = 100
  const SLICE_SIZE = 4096
  const READ_SIZE = Math.floor(4096 * 2.76)

  try {
    const content = await writeData(NUM_SLICES, SLICE_SIZE)
    await readData(content, NUM_SLICES, SLICE_SIZE, READ_SIZE)
    t.pass('all slices matched')
  } catch (err) {
    t.fail(err)
  }

  await cleanup(fuse)
  process.removeListener('SIGINT', onint)
  t.end()
})

test('can read/write a large file', async (t) => {
  const drive = createDrive()
  const fuse = new HyperdriveFuse(drive, './mnt')

  const onint = () => cleanup(fuse, true)
  process.on('SIGINT', onint)

  await fuse.mount()

  const NUM_SLICES = 10000
  const SLICE_SIZE = 4096
  const READ_SIZE = Math.floor(4096 * 2.76)

  try {
    const content = await writeData(NUM_SLICES, SLICE_SIZE)
    await readData(content, NUM_SLICES, SLICE_SIZE, READ_SIZE)
    t.pass('all slices matched')
  } catch (err) {
    t.fail(err)
  }

  await cleanup(fuse)
  process.removeListener('SIGINT', onint)
  t.end()
})

test('can read/write a huge file', async (t) => {
  const drive = createDrive()
  const fuse = new HyperdriveFuse(drive, './mnt')

  const onint = () => cleanup(fuse, true)
  process.on('SIGINT', onint)

  await fuse.mount()

  const NUM_SLICES = 100000
  const SLICE_SIZE = 4096
  const READ_SIZE = Math.floor(4096 * 2.76)

  try {
    const content = await writeData(NUM_SLICES, SLICE_SIZE)
    await readData(content, NUM_SLICES, SLICE_SIZE, READ_SIZE)
    t.pass('all slices matched')
  } catch (err) {
    t.fail(err)
  }

  await cleanup(fuse)
  process.removeListener('SIGINT', onint)
  t.end()
})

test('can list a directory', async (t) => {
  const drive = createDrive()
  const fuse = new HyperdriveFuse(drive, './mnt')

  const onint = () => cleanup(fuse, true)
  process.on('SIGINT', onint)

  await fuse.mount()

  try {
    await new Promise((resolve) => {
      fs.mkdir('./mnt/a', (err) => {
        t.error(err, 'no error')
        fs.writeFile('./mnt/a/1', '1', (err) => {
          t.error(err, 'no error')
          fs.writeFile('./mnt/a/2', '2', (err) => {
            t.error(err, 'no error')
            fs.readdir('./mnt/a', (err, list) => {
              t.error(err, 'no error')
              t.same(list.sort(), ['1', '2'].sort())
              return resolve()
            })
          })
        })
      })
    })
  } catch (err) {
    t.fail(err)
  }

  await cleanup(fuse)
  process.removeListener('SIGINT', onint)
  t.end()
})

test('can create and read from a symlink', async (t) => {
  const drive = createDrive()
  const fuse = new HyperdriveFuse(drive, './mnt')

  const onint = () => cleanup(fuse, true)
  process.on('SIGINT', onint)

  await fuse.mount()

  try {
    await new Promise((resolve) => {
      fs.writeFile('./mnt/a', 'hello', (err) => {
        t.error(err, 'no error')
        fs.symlink('a', './mnt/b', (err) => {
          t.error(err, 'no error')
          fs.readFile('./mnt/b', { encoding: 'utf-8' }, (err, content) => {
            t.error(err, 'no error')
            t.same(content, 'hello')
            return resolve()
          })
        })
      })
    })
  } catch (err) {
    t.fail(err)
  }

  await cleanup(fuse)
  process.removeListener('SIGINT', onint)
  t.end()
})

test('can get/set/list xattrs', async (t) => {
  const drive = createDrive()
  const fuse = new HyperdriveFuse(drive, './mnt')

  const onint = () => cleanup(fuse, true)
  process.on('SIGINT', onint)

  await fuse.mount()

  try {
    await fs.promises.writeFile('./mnt/a', 'hello')
    await xattr.set('./mnt/a', 'test', 'hello world')
    t.same(await xattr.get('./mnt/a', 'test'), Buffer.from('hello world'))
    let list = await xattr.list('./mnt/a')
    t.same(list.length, 1)
    t.same(list[0], 'test')
  } catch (err) {
    t.fail(err)
  }

  await cleanup(fuse)
  process.removeListener('SIGINT', onint)
  t.end()
})

test('uid/gid are normalized on read', async (t) => {
  const drive = createDrive()
  await drive.ready()
  await drive.put('/a', Buffer.from('hello'), { metadata: { uid: 0, gid: 0 } })

  const fuse = new HyperdriveFuse(drive, './mnt')
  const onint = () => cleanup(fuse, true)
  process.on('SIGINT', onint)

  await fuse.mount()

  try {
    const e = await drive.entry('/a', { follow: true, wait: true })
    t.is(e && e.value && e.value.metadata && e.value.metadata.uid, 0)
    t.is(e && e.value && e.value.metadata && e.value.metadata.gid, 0)
    await new Promise((resolve) => {
      fs.stat('./mnt/a', (err, stat) => {
        t.error(err, 'no error')
        t.is(stat.uid, process.getuid())
        t.is(stat.gid, process.getgid())
        return resolve()
      })
    })
  } catch (err) {
    t.fail(err)
  }

  await cleanup(fuse)
  process.removeListener('SIGINT', onint)
  t.end()
})

test('a relative symlink will not read files outside the sandbox', async (t) => {
  const drive = createDrive()
  await drive.ready()
  await drive.symlink('/test', '../test.txt')
  const fuse = new HyperdriveFuse(drive, './mnt')
  const onint = () => cleanup(fuse, true)
  process.on('SIGINT', onint)

  await fs.promises.writeFile('./test.txt', 'Hello world!')
  await fuse.mount()

  try {
    await new Promise((resolve) => setImmediate(resolve))
    try {
      const contents = await fs.promises.readFile('./mnt/test', { encoding: 'utf8' })
      t.false(contents)
    } catch (err) {
      t.true(err)
    }
  } catch (err) {
    t.fail(err)
  }

  await cleanup(fuse)
  await fs.promises.unlink('./test.txt')
  process.removeListener('SIGINT', onint)
  t.end()
})

test('an absolute symlink will not read files outside the sandbox', async (t) => {
  const drive = createDrive()
  await drive.ready()
  await drive.symlink('/test', p.resolve('./mnt', '../test.txt'))
  const fuse = new HyperdriveFuse(drive, './mnt')
  const onint = () => cleanup(fuse, true)
  process.on('SIGINT', onint)

  await fs.promises.writeFile('./test.txt', 'Hello world!')
  await fuse.mount()

  try {
    await new Promise((resolve) => setImmediate(resolve))
    try {
      const contents = await fs.promises.readFile('./mnt/test', { encoding: 'utf8' })
      t.false(contents)
    } catch (err) {
      t.true(err)
      t.is(err.code, 'ENOENT')
    }
  } catch (err) {
    t.fail(err)
  }

  await cleanup(fuse)
  await fs.promises.unlink('./test.txt')
  process.removeListener('SIGINT', onint)
  t.end()
})

test('cannot open a writable file descriptor on a non-writable drive', async (t) => {
  const a = new Corestore(fs.mkdtempSync(p.join(os.tmpdir(), 'hyperdrive-fuse-a-')))
  const b = new Corestore(fs.mkdtempSync(p.join(os.tmpdir(), 'hyperdrive-fuse-b-')))
  const drive = new Hyperdrive(a)
  const clone = new Hyperdrive(b, drive.key)
  await drive.ready()
  await clone.ready()

  await drive.put('/hello', Buffer.from('world'))
  const s1 = a.replicate(true)
  const s2 = b.replicate(false)
  s1.pipe(s2).pipe(s1)
  while (clone.version < drive.version) {
    await new Promise((resolve) => setImmediate(resolve))
  }
  await clone.get('/hello', { wait: true })

  const fuse = new HyperdriveFuse(clone, './mnt')
  const onint = () => cleanup(fuse, true)
  process.on('SIGINT', onint)
  await fuse.mount()

  try {
    await fs.promises.open('./mnt/hello', 'w+')
    t.fail('open did not error')
  } catch (err) {
    t.true(err)
  }

  await cleanup(fuse)
  s1.destroy()
  s2.destroy()
  process.removeListener('SIGINT', onint)
  t.end()
})

test.skip('a hanging get will be aborted after a timeout', async (t) => {
  t.end()
})

async function writeData (numSlices, sliceSize) {
  const content = Buffer.alloc(sliceSize * numSlices).fill('0123456789abcdefghijklmnopqrstuvwxyz')
  const slices = new Array(numSlices).fill(0).map((_, i) => content.slice(sliceSize * i, sliceSize * (i + 1)))
  let fd = await open('./mnt/hello', 'w+')
  for (const slice of slices) {
    await write(fd, slice, 0)
  }
  await close(fd)
  return content
}

async function readData (content, numSlices, sliceSize, readSize) {
  let fd = await open('./mnt/hello', 'r')
  let numReads = 0
  do {
    const pos = numReads * readSize
    const buf = Buffer.alloc(readSize)
    const bytesRead = await read(fd, buf, 0, readSize, pos)
    if (!buf.slice(0, bytesRead).equals(content.slice(pos, pos + readSize))) {
      throw new Error(`Slices do not match at position: ${pos}`)
    }
  } while (++numReads * readSize < numSlices * sliceSize)
  await close(fd)
}

async function cleanup (fuse, exit) {
  await fuse.unmount()
  return new Promise((resolve, reject) => {
    rimraf('./mnt', (err) => {
      if (err) return reject(err)
      if (exit) return process.exit(0)
      return resolve()
    })
  })
}

function read (fd, buf, offset, len, pos) {
  return new Promise((resolve, reject) => {
    fs.read(fd, buf, offset, len, pos, (err, bytesRead) => {
      if (err) return reject(err)
      return resolve(bytesRead)
    })
  })
}

function write (fd, buf, offset, len) {
  return new Promise((resolve, reject) => {
    fs.write(fd, buf, offset, len, (err, bytesWritten) => {
      if (err) return reject(err)
      return resolve(bytesWritten)
    })
  })
}

function open (f, flags) {
  return new Promise((resolve, reject) => {
    fs.open(f, flags, (err, fd) => {
      if (err) return reject(err)
      return resolve(fd)
    })
  })
}

function close (fd) {
  return new Promise((resolve, reject) => {
    fs.close(fd, (err) => {
      if (err) return reject(err)
      return resolve()
    })
  })
}
