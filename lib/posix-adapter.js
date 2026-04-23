'use strict'

const os = require('os')
const fs = require('fs')
const p = require('path')
const unixPathResolve = require('unix-path-resolve')

const S = fs.constants
// `index.js` normalizes FUSE `open` flags to Linux on Darwin (translate → linux). The platform
// `O_CREAT` / `O_TRUNC` / … bit values differ, so we must not mask with `fs.constants` here or
// `O_CREAT` is never seen and creating a new file returns ENOENT (Vim: E514, “file system full?”).
const { O_ACCMODE, O_RDONLY, O_WRONLY, O_RDWR, O_CREAT, O_EXCL, O_TRUNC, O_APPEND } =
  require('filesystem-constants').linux
const errno = os.constants.errno

const DIR_MARKER = '.hdfuse-dir'
// Stable when metadata has no mtime. Do not use new Date() per getattrs — tools like vim
// re-stat before save; changing mtime every call triggers "file changed since reading it".
const EPOCH = new Date(0)
const norm = (s) => unixPathResolve('/', s)

const markerPath = (dir) => (dir === '/' ? `/${DIR_MARKER}` : `${dir.replace(/\/$/, '')}/${DIR_MARKER}`)

function errN (n) {
  const e = new Error('e')
  e.errno = n
  return e
}

function toNumFlags (flags) {
  if (typeof flags === 'number') return flags
  if (flags === 'r') return O_RDONLY
  if (flags === 'w') return O_WRONLY | O_CREAT | O_TRUNC
  if (flags === 'a') return O_WRONLY | O_CREAT | O_APPEND
  if (flags === 'r+') return O_RDWR
  if (flags === 'w+') return O_RDWR | O_CREAT | O_TRUNC
  if (flags === 'a+') return O_RDWR | O_CREAT | O_APPEND
  return O_RDONLY
}

class PosixAdapter {
  constructor (drive) {
    this.drive = drive
    this._nextFd = 1
    this._fds = new Map()
    this._inFlight = new Map()
  }

  get key () {
    return this.drive.key
  }

  get writable () {
    return this.drive.writable
  }

  ready (cb) {
    this.drive.ready().then(() => cb(null), cb)
  }

  replicate (a, b) {
    return this.drive.replicate(a, b)
  }

  _meta (node) {
    if (!node || !node.value) return {}
    const m = node.value.metadata
    return m && typeof m === 'object' ? { ...m } : {}
  }

  _modeFrom (meta, kind) {
    if (kind === 'file') return (meta.mode != null ? meta.mode : 0o644) | S.S_IFREG
    if (kind === 'dir') return (meta.mode != null ? meta.mode : 0o755) | S.S_IFDIR
    return (meta.mode != null ? meta.mode : 0o777) | S.S_IFLNK
  }

  _mkStat (st) {
    st.mtime = st.mtime != null ? st.mtime : (st.atime != null ? st.atime : (st.ctime != null ? st.ctime : EPOCH))
    st.atime = st.atime != null ? st.atime : st.mtime
    st.ctime = st.ctime != null ? st.ctime : st.mtime
    st.blksize = st.blksize || 4096
    st.blocks = st.blocks == null ? Math.ceil((st.size || 0) / 512) : st.blocks
    st.nlink = st.nlink == null ? 1 : st.nlink
    st.dev = st.dev == null ? 0 : st.dev
    st.ino = st.ino == null ? 0 : st.ino
    st.rdev = st.rdev == null ? 0 : st.rdev
    return st
  }

  async _isDirectory (path) {
    if (path === '/') return true
    if (await this.drive.exists(markerPath(path))) return true
    for await (const name of this.drive.readdir(path)) {
      void name
      return true
    }
    return false
  }

  lstat (path, cb) {
    path = norm(path)
    this._lstat(path, false, cb)
  }

  _lstat (path, _follow, cb) {
    ;(async () => {
      if (path === '/') {
        return cb(null, this._mkStat({ size: 4096, mode: 0o755 | S.S_IFDIR, uid: process.getuid(), gid: process.getgid() }))
      }
      if (p.basename(path) === DIR_MARKER) {
        return cb(
          null,
          this._mkStat({ size: 0, mode: 0o644 | S.S_IFREG, uid: process.getuid(), gid: process.getgid() })
        )
      }

      const node = await this.drive.entry(path, { follow: false, wait: true })
      if (node) {
        if (node.value.linkname) {
          const m = this._meta(node)
          return cb(
            null,
            this._mkStat({
              size: node.value.linkname.length,
              mode: this._modeFrom(m, 'link'),
              nlink: 1,
              uid: m.uid != null ? m.uid : process.getuid(),
              gid: m.gid != null ? m.gid : process.getgid(),
              mtime: m.mtime != null ? new Date(m.mtime) : EPOCH,
              atime: m.atime != null ? new Date(m.atime) : EPOCH,
              ctime: m.ctime != null ? new Date(m.ctime) : EPOCH,
              linkname: node.value.linkname,
              metadata: m
            })
          )
        }
        if (node.value.blob) {
          const m = this._meta(node)
          const buf = this._inFlight.get(path)
          const size = buf != null ? buf.length : (node.value.blob && node.value.blob.byteLength) || 0
          return cb(
            null,
            this._mkStat({
              size,
              mode: this._modeFrom(m, 'file'),
              nlink: 1,
              uid: m.uid != null ? m.uid : process.getuid(),
              gid: m.gid != null ? m.gid : process.getgid(),
              mtime: m.mtime != null ? new Date(m.mtime) : EPOCH,
              atime: m.atime != null ? new Date(m.atime) : EPOCH,
              ctime: m.ctime != null ? new Date(m.ctime) : EPOCH,
              metadata: m
            })
          )
        }
      }

      if (await this._isDirectory(path)) {
        return cb(
          null,
          this._mkStat({ size: 4096, mode: 0o755 | S.S_IFDIR, uid: process.getuid(), gid: process.getgid() })
        )
      }

      return cb(errN(errno.ENOENT))
    })().catch(cb)
  }

  stat (path, cb) {
    path = norm(path)
    ;(async () => {
      const node0 = await this.drive.entry(path, { follow: false, wait: true })
      if (node0 && node0.value.linkname) {
        const target = unixPathResolve(p.dirname(path), node0.value.linkname)
        return this._lstat(target, true, cb)
      }
      return this._lstat(path, true, cb)
    })().catch(cb)
  }

  readdir (path, cb) {
    path = norm(path)
    ;(async () => {
      if (!(await this._isDirectory(path))) return cb(errN(errno.ENOTDIR))
      const out = new Set()
      for await (const name of this.drive.readdir(path)) {
        if (name === DIR_MARKER) continue
        out.add(name)
      }
      return cb(null, [...out].sort())
    })().catch(cb)
  }

  _wantsWrite (flags) {
    const f = toNumFlags(flags)
    const m = f & O_ACCMODE
    return m === O_WRONLY || m === O_RDWR
  }

  _allocFd (h) {
    const id = this._nextFd++
    this._fds.set(id, h)
    return id
  }

  open (path, flags, cb) {
    const n = norm(path)
    const f = toNumFlags(flags)
    const w = this._wantsWrite(f)
    const needTrunc = f & O_TRUNC
    const creat = f & O_CREAT
    const ex = () => this.drive.exists(n)

    ;(async () => {
      if (n === '/' && w) {
        return cb(errN(errno.EISDIR))
      }
      if (w && !this.drive.writable) {
        return cb(errN(errno.EROFS))
      }
      // The drive has no "entry" for `/` (it is implied). OPENDIR and O_RDONLY opens of `/`
      // must not fall through to `!exists` → ENOENT, or macFUSE breaks the mount and you
      // get ENXIO for later syscalls.
      if (n === '/' && !w) {
        return cb(null, this._allocFd({ type: 'dir', path: '/', readOnly: true, buf: null }))
      }

      const exists = await ex()
      const entNoFollow = exists ? await this.drive.entry(n, { follow: false, wait: true }) : null
      const isSymlink = entNoFollow && entNoFollow.value && entNoFollow.value.linkname
      const isDir = await this._isDirectory(n)

      if (isDir && !isSymlink) {
        if (w) {
          return cb(errN(errno.EISDIR))
        }
        return cb(null, this._allocFd({ type: 'dir', path: n, readOnly: true, buf: null }))
      }

      if (!exists) {
        if (w && creat) {
          this._inFlight.set(n, needTrunc ? Buffer.alloc(0) : Buffer.alloc(0))
          const b = this._inFlight.get(n)
          return cb(
            null,
            this._allocFd({ path: n, readOnly: false, buf: b, append: !!(f & O_APPEND), meta: {} })
          )
        }
        return cb(errN(errno.ENOENT))
      }
      if (w && creat && (f & O_EXCL)) {
        return cb(errN(errno.EEXIST))
      }

      if (w) {
        let buf
        if (this._inFlight.has(n)) {
          buf = this._inFlight.get(n)
        } else {
          const raw = (await this.drive.get(n, { wait: true })) || Buffer.alloc(0)
          buf = Buffer.from(raw)
        }
        if (needTrunc) {
          buf = Buffer.alloc(0)
        }
        this._inFlight.set(n, buf)
        const node = await this.drive.entry(n, { follow: true, wait: true })
        return cb(
          null,
          this._allocFd({
            path: n,
            readOnly: false,
            buf: this._inFlight.get(n),
            append: !!(f & O_APPEND),
            meta: this._meta(node)
          })
        )
      }

      if (isSymlink) {
        const t = unixPathResolve(p.dirname(n), entNoFollow.value.linkname)
        const b = (await this.drive.get(t, { wait: true })) || Buffer.alloc(0)
        return cb(null, this._allocFd({ path: t, readOnly: true, buf: Buffer.from(b) }))
      }
      const b = (await this.drive.get(n, { wait: true })) || Buffer.alloc(0)
      return cb(null, this._allocFd({ path: n, readOnly: true, buf: Buffer.from(b) }))
    })().catch(cb)
  }

  // (fd, buf, bufOffset, len, fileOffset, cb) — FUSE handlers do not pass path here; match index.js
  read (fd, buf, o, l, off, cb) {
    const h = this._fds.get(fd)
    if (!h) {
      return cb(errN(errno.EBADF))
    }
    if (h.type === 'dir') {
      return cb(errN(errno.EISDIR))
    }
    const src = h.buf
    if (off >= src.length) {
      return cb(null, 0)
    }
    const n = src.copy(buf, o, off, Math.min(src.length, off + l))
    return cb(null, n)
  }

  write (fd, b, o, l, off, cb) {
    const h = this._fds.get(fd)
    if (!h) {
      return cb(errN(errno.EBADF))
    }
    if (h.readOnly) {
      return cb(errN(errno.EBADF))
    }
    let out = h.buf
    let pos = off
    if (h.append) {
      pos = out.length
    }
    const need = pos + l
    if (need > out.length) {
      const n = Buffer.alloc(need, 0)
      out.copy(n, 0, 0, out.length)
      out = n
    }
    b.copy(out, pos, o, o + l)
    h.buf = out
    this._inFlight.set(h.path, out)
    return cb(null, l)
  }

  close (fd, cb) {
    const h = this._fds.get(fd)
    if (!h) {
      return cb(errN(errno.EBADF))
    }
    this._fds.delete(fd)
    if (h.readOnly) {
      return process.nextTick(() => cb(null))
    }
    this.drive
      .put(
        h.path,
        h.buf,
        { metadata: { ...h.meta, mtime: Date.now(), ctime: Date.now() } }
      )
      .then(() => {
        this._inFlight.delete(h.path)
        return cb(null)
      })
      .catch(cb)
  }

  /**
   * fsync(2) — push buffered data to the drive. Vim and other editors may fsync/flush
   * before close; a no-op can surface as E514 or "write failed" on FUSE.
   * Unlike close(), the fd remains open; further writes re-add the path to _inFlight in write().
   */
  fsync (fd, cb) {
    const h = this._fds.get(fd)
    if (!h) {
      return cb(errN(errno.EBADF))
    }
    if (h.type === 'dir' || h.readOnly) {
      return process.nextTick(() => cb(null))
    }
    this.drive
      .put(
        h.path,
        h.buf,
        { metadata: { ...h.meta, mtime: Date.now(), ctime: Date.now() } }
      )
      .then(() => {
        this._inFlight.delete(h.path)
        return cb(null)
      })
      .catch(cb)
  }

  truncate (path, size, cb) {
    path = norm(path)
    ;(async () => {
      if (this._inFlight.has(path)) {
        const b = this._inFlight.get(path)
        this._inFlight.set(path, resize(b, size))
        return cb(null)
      }
      const data = (await this.drive.get(path, { wait: true })) || Buffer.alloc(0)
      const b = Buffer.alloc(size, 0)
      Buffer.from(data).copy(b, 0, 0, Math.min(data.length, size))
      const node = await this.drive.entry(path, { follow: true, wait: true })
      await this.drive.put(path, b, { metadata: { ...this._meta(node) }, executable: node && node.value && node.value.executable })
      return cb(null)
    })().catch(cb)
  }

  ftruncate (fd, size, cb) {
    const h = this._fds.get(fd)
    if (!h) {
      return cb(errN(errno.EBADF))
    }
    h.buf = resize(h.buf, size)
    this._inFlight.set(h.path, h.buf)
    cb(null)
  }

  _bumpRenamePaths (from, to) {
    for (const h of this._fds.values()) {
      if (h && h.path === from) {
        h.path = to
      }
    }
    if (this._inFlight.has(from)) {
      this._inFlight.set(to, this._inFlight.get(from))
      this._inFlight.delete(from)
    }
  }

  /**
   * POSIX link(2) — Vim (backup) hard-links the target to `file~` before writing.
   * We duplicate blob/symlink into `dest` (no shared inode, but enough for save flow).
   */
  link (from, to, cb) {
    from = norm(from)
    to = norm(to)
    if (from === to) {
      return process.nextTick(() => cb(errN(errno.EINVAL)))
    }
    if (from === '/' || to === '/') {
      return process.nextTick(() => cb(errN(errno.EPERM)))
    }
    if (p.basename(from) === DIR_MARKER || p.basename(to) === DIR_MARKER) {
      return process.nextTick(() => cb(errN(errno.EPERM)))
    }
    if (!this.drive.writable) {
      return process.nextTick(() => cb(errN(errno.EROFS)))
    }
    ;(async () => {
      if (this._inFlight.has(to)) {
        return cb(errN(errno.EEXIST))
      }
      if (await this.drive.exists(to)) {
        return cb(errN(errno.EEXIST))
      }
      if (await this._isDirectory(from)) {
        return cb(errN(errno.EPERM))
      }
      const inMem = this._inFlight.get(from)
      const src = await this.drive.entry(from, { follow: false, wait: true })
      if (!inMem && !src) {
        return cb(errN(errno.ENOENT))
      }
      if (src && src.value.linkname) {
        const m = { ...this._meta(src) }
        await this.drive.symlink(to, src.value.linkname, { metadata: m })
        return cb(null)
      }
      if (inMem) {
        const b = inMem
        const m =
          src && src.value && src.value.blob
            ? { ...this._meta(src) }
            : { mode: 0o644 | S.S_IFREG, uid: process.getuid(), gid: process.getgid() }
        const exec = src && src.value && !!src.value.executable
        await this.drive.put(to, b, { metadata: m, executable: exec })
        return cb(null)
      }
      if (src && src.value.blob) {
        const b = (await this.drive.get(from, { wait: true })) || Buffer.alloc(0)
        const m = { ...this._meta(src) }
        await this.drive.put(to, b, { metadata: m, executable: !!src.value.executable })
        return cb(null)
      }
      return cb(errN(errno.EPERM))
    })().catch(cb)
  }

  /** POSIX rename(2) — editors use this for atomic save (write temp, then rename over target). */
  rename (from, to, cb) {
    from = norm(from)
    to = norm(to)
    if (from === to) {
      return process.nextTick(() => cb(null))
    }
    if (from === '/' || to === '/') {
      return process.nextTick(() => cb(errN(errno.EINVAL)))
    }
    if (p.basename(from) === DIR_MARKER || p.basename(to) === DIR_MARKER) {
      return process.nextTick(() => cb(errN(errno.EINVAL)))
    }
    if (!this.drive.writable) {
      return process.nextTick(() => cb(errN(errno.EROFS)))
    }
    ;(async () => {
      if (await this._isDirectory(from)) {
        return cb(errN(errno.EOPNOTSUPP))
      }
      const src = await this.drive.entry(from, { follow: false, wait: true })
      const inMem = this._inFlight.get(from)
      if (!src && inMem == null) {
        return cb(errN(errno.ENOENT))
      }
      if (this._inFlight.has(to)) {
        return cb(errN(errno.EBUSY))
      }
      if (await this.drive.exists(to)) {
        if (await this._isDirectory(to)) {
          return cb(errN(errno.EISDIR))
        }
        await this.drive.del(to)
      }
      if (src && src.value.linkname) {
        const m = { ...this._meta(src) }
        await this.drive.symlink(to, src.value.linkname, { metadata: m })
        if (await this.drive.exists(from)) {
          await this.drive.del(from)
        }
        this._bumpRenamePaths(from, to)
        return cb(null)
      }
      let b
      let meta
      let exec = false
      if (inMem) {
        b = inMem
        if (src && src.value.blob) {
          meta = { ...this._meta(src) }
          exec = !!src.value.executable
        } else {
          meta = { mode: 0o644 | S.S_IFREG, uid: process.getuid(), gid: process.getgid() }
        }
      } else {
        b = (await this.drive.get(from, { wait: true })) || Buffer.alloc(0)
        meta = { ...this._meta(src) }
        exec = !!src.value.executable
      }
      await this.drive.put(to, b, { metadata: { ...meta, mtime: Date.now() }, executable: exec })
      if (await this.drive.exists(from)) {
        await this.drive.del(from)
      }
      this._bumpRenamePaths(from, to)
      return cb(null)
    })().catch(cb)
  }

  unlink (path, cb) {
    path = norm(path)
    if (p.basename(path) === DIR_MARKER) {
      return cb(errN(errno.EPERM))
    }
    this.drive
      .del(path)
      .then(() => {
        this._inFlight.delete(path)
        return cb(null)
      })
      .catch(cb)
  }

  mkdir (path, opts, cb) {
    path = norm(path)
    const m = markerPath(path)
    ;(async () => {
      if (await this.drive.exists(m)) {
        return cb(errN(errno.EEXIST))
      }
      await this.drive.put(m, Buffer.alloc(0), { metadata: { mode: opts && opts.mode, mkdir: true } })
      return cb(null)
    })().catch(cb)
  }

  rmdir (path, cb) {
    path = norm(path)
    ;(async () => {
      const mPath = markerPath(path)
      const children = []
      for await (const name of this.drive.readdir(path)) {
        children.push(name)
      }
      if (children.length > 1) {
        return cb(errN(errno.ENOTEMPTY))
      }
      if (children.length === 1) {
        if (children[0] !== DIR_MARKER) {
          return cb(errN(errno.ENOTEMPTY))
        }
        await this.drive.del(mPath)
        return cb(null)
      }
      if (children.length === 0) {
        if (await this.drive.exists(mPath)) {
          await this.drive.del(mPath)
          return cb(null)
        }
        if (await this._isDirectory(path)) {
          return cb(errN(errno.ENOENT))
        }
        return cb(errN(errno.ENOENT))
      }
    })().catch(cb)
  }

  create (path, opts, cb) {
    this.drive
      .put(norm(path), Buffer.alloc(0), { metadata: this._fileMeta(opts) })
      .then(() => cb(null), cb)
  }

  _fileMeta (opts) {
    return {
      mode: opts && opts.mode != null ? (opts.mode | S.S_IFREG) : 0o644 | S.S_IFREG,
      uid: opts && opts.uid != null ? opts.uid : process.getuid(),
      gid: opts && opts.gid != null ? opts.gid : process.getgid()
    }
  }

  _update (path, fields, cb) {
    path = norm(path)
    ;(async () => {
      const node = await this.drive.entry(path, { follow: false, wait: true })
      if (!node) {
        return cb(errN(errno.ENOENT))
      }
      if (node.value.linkname) {
        const dst = node.value.linkname
        const m = { ...this._meta(node), ...fields, mtime: Date.now() }
        await this.drive.del(path)
        await this.drive.symlink(path, dst, { metadata: m })
        return cb(null)
      }
      if (node.value.blob) {
        const b = (await this.drive.get(path, { wait: true })) || Buffer.alloc(0)
        const m = { ...this._meta(node), ...fields, mtime: Date.now() }
        await this.drive.put(path, b, { metadata: m, executable: !!node.value.executable })
        return cb(null)
      }
      return cb(errN(errno.EPERM))
    })().catch(cb)
  }

  symlink (src, dest, cb) {
    this.drive
      .symlink(norm(dest), src, { metadata: { mode: 0o777 } })
      .then(() => cb(null), cb)
  }

  setMetadata (path, name, buffer, cb) {
    this._xattr(path, (m) => {
      m._fuse = m._fuse || { xattr: {} }
      m._fuse.xattr = m._fuse.xattr || {}
      m._fuse.xattr[name] = Buffer.from(buffer).toString('base64')
      return m
    }, cb)
  }

  removeMetadata (path, name, cb) {
    this._xattr(
      path,
      (m) => {
        if (m._fuse && m._fuse.xattr) {
          delete m._fuse.xattr[name]
        }
        return m
      },
      cb
    )
  }

  _xattr (path, fn, cb) {
    path = norm(path)
    ;(async () => {
      const ent = await this.drive.entry(path, { follow: true, wait: true })
      if (!ent || !ent.value.blob) {
        return cb(errN(errno.ENOENT))
      }
      const b = (await this.drive.get(path, { wait: true })) || Buffer.alloc(0)
      const m = fn(this._meta(ent))
      await this.drive.put(path, b, { metadata: m, executable: !!ent.value.executable })
      return cb(null)
    })().catch(cb)
  }

  getxattr (path, name, _pos, cb) {
    this.stat(path, (err, st) => {
      if (err) {
        return cb(err)
      }
      const m = st && st.metadata
      if (m && m._fuse && m._fuse.xattr && m._fuse.xattr[name] != null) {
        return cb(null, Buffer.from(m._fuse.xattr[name], 'base64'))
      }
      const e = new Error('ENODATA')
      e.code = 'ENODATA'
      e.errno = 61
      return cb(e)
    })
  }

  listxattr (path, cb) {
    this.stat(path, (err, st) => {
      if (err) {
        return cb(err)
      }
      const m = st && st.metadata
      if (!m || !m._fuse || !m._fuse.xattr) {
        return cb(null, [])
      }
      return cb(null, Object.keys(m._fuse.xattr))
    })
  }

  writeFile (path, data, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts
      opts = {}
    }
    const b = typeof data === 'string' ? Buffer.from(data) : data
    this.drive
      .put(norm(path), b, { metadata: this._fileMeta(opts) })
      .then(() => cb(null), cb)
  }
}

function resize (buf, size) {
  if (buf.length === size) {
    return buf
  }
  const n = Buffer.alloc(size, 0)
  buf.copy(n, 0, 0, Math.min(buf.length, size))
  return n
}

module.exports = function createPosixAdapter (drive) {
  return new PosixAdapter(drive)
}
