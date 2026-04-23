const p = require('path')
const fs = require('fs')
const os = require('os')

const W_OK = fs.constants.W_OK != null ? fs.constants.W_OK : 2
const EOPNOTSUPP = os.constants.errno.EOPNOTSUPP

const z32 = require('z32')
const fsConstants = require('filesystem-constants')
const Fuse = require('@zkochan/fuse-native')
const { translate, linux } = fsConstants

const createPosixAdapter = require('./lib/posix-adapter')
const debug = require('debug')('hyperdrive-fuse')

const platform = os.platform()

class HyperdriveFuse {
  constructor (drive, mnt, opts = {}) {
    this.raw = drive
    this.drive = createPosixAdapter(drive)
    this.mnt = p.resolve(mnt)
    this.opts = opts
    this.fuse = null
  }

  getBaseHandlers () {
    const self = this
    const handlers = {}
    const log = this.opts.log || debug

    handlers.getattr = function (path, cb) {
      log('getattr', path)
      self.drive.lstat(path, (err, stat) => {
        if (err) return cb(-err.errno || Fuse.ENOENT)
        stat.uid = process.getuid()
        stat.gid = process.getgid()
        return cb(0, stat)
      })
    }

    handlers.readdir = function (path, cb) {
      log('readdir', path)
      self.drive.readdir(path, (err, files) => {
        if (err) return cb(-err.errno || Fuse.ENOENT)
        return cb(0, files)
      })
    }

    handlers.open = function (path, flags, cb) {
      log('open', path, flags)
      if (platform !== 'linux') {
        flags = translate(fsConstants[platform], linux, flags)
      }
      self.drive.open(path, flags, (err, fd) => {
        if (err) return cb(-err.errno || Fuse.ENOENT)
        return cb(0, fd)
      })
    }

    handlers.opendir = function (path, flags, cb) {
      // open() already translates; double-translating can corrupt FOPEN flags.
      return handlers.open(path, flags, cb)
    }

    handlers.release = function (path, handle, cb) {
      log('release', path, handle)
      self.drive.close(handle, (err) => {
        if (err) return cb(-(err.errno != null ? err.errno : 9) || Fuse.EBADF)
        return cb(0)
      })
    }

    handlers.releasedir = handlers.release

    handlers.read = function (path, handle, buf, len, offset, cb) {
      log('read', path, handle, len, offset)
      const proxy = Buffer.from(buf)
      self.drive.read(handle, proxy, 0, len, offset, (err, bytesRead) => {
        if (err) return cb(-(err.errno != null ? err.errno : 9) || Fuse.EBADF)
        proxy.copy(buf, 0, 0, bytesRead)
        return cb(0, bytesRead)
      })
    }

    handlers.write = function (path, handle, buf, len, offset, cb) {
      log('write', path, handle, len, offset)
      buf = Buffer.from(buf)
      self.drive.write(handle, buf, 0, len, offset, (err, bytesWritten) => {
        if (err) return cb(-(err.errno != null ? err.errno : 9) || Fuse.EBADF)
        return cb(0, bytesWritten)
      })
    }

    // Vim and others call flush/fsync; must actually persist the fd buffer (see PosixAdapter.fsync).
    handlers.flush = function (path, handle, cb) {
      self.drive.fsync(handle, (err) => {
        if (err) return cb(-(err.errno != null ? err.errno : 5) || Fuse.EIO)
        return cb(0)
      })
    }
    handlers.fsync = function (path, datasync, handle, cb) {
      self.drive.fsync(handle, (err) => {
        if (err) return cb(-(err.errno != null ? err.errno : 5) || Fuse.EIO)
        return cb(0)
      })
    }
    handlers.fsyncdir = function (path, datasync, handle, cb) {
      return cb(0)
    }

    handlers.truncate = function (path, size, cb) {
      log('truncate', path, size)
      self.drive.truncate(path, size, (err) => {
        if (err) return cb(-(err.errno != null ? err.errno : 1) || Fuse.EPERM)
        return cb(0)
      })
    }

    handlers.ftruncate = function (path, fd, size, cb) {
      log('ftruncate', path, fd, size)
      self.drive.ftruncate(fd, size, (err) => {
        if (err) return cb(-(err.errno != null ? err.errno : 1) || Fuse.EPERM)
        return cb(0)
      })
    }

    handlers.rename = function (from, to, cb) {
      log('rename', from, to)
      self.drive.rename(from, to, (err) => {
        if (err) {
          const n = err.errno
          if (n != null) {
            if (n === EOPNOTSUPP) {
              return cb(Fuse.EOPNOTSUPP)
            }
            return cb(-n)
          }
          return cb(Fuse.EPERM)
        }
        return cb(0)
      })
    }

    // Hard link(2); Vim with `set backup` uses link(src, "file~") before rewriting src.
    handlers.link = function (from, to, cb) {
      log('link', from, to)
      self.drive.link(from, to, (err) => {
        if (err) {
          return cb(-(err.errno != null ? err.errno : 1) || Fuse.EPERM)
        }
        return cb(0)
      })
    }

    handlers.unlink = function (path, cb) {
      log('unlink', path)
      self.drive.unlink(path, (err) => {
        if (err) return cb(-(err.errno != null ? err.errno : 2) || Fuse.ENOENT)
        return cb(0)
      })
    }

    handlers.mkdir = function (path, mode, cb) {
      log('mkdir', path, mode)
      self.drive.mkdir(
        path,
        { mode, uid: process.getuid(), gid: process.getgid() },
        (err) => {
          if (err) return cb(-(err.errno != null ? err.errno : 1) || Fuse.EPERM)
          return cb(0)
        }
      )
    }

    handlers.rmdir = function (path, cb) {
      log('rmdir', path)
      self.drive.rmdir(path, (err) => {
        if (err) return cb(-(err.errno != null ? err.errno : 2) || Fuse.ENOENT)
        return cb(0)
      })
    }

    handlers.create = function (path, mode, cb) {
      log('create', path, mode)
      const opts = { mode, uid: process.getuid(), gid: process.getgid() }
      self.drive.create(path, opts, (err) => {
        if (err) return cb(-(err.errno != null ? err.errno : 2) || Fuse.ENOENT)
        self.drive.open(path, 'w', (err2, fd) => {
          if (err2) return cb(-(err2.errno != null ? err2.errno : 2) || Fuse.ENOENT)
          return cb(0, fd)
        })
      })
    }

    handlers.chown = function (path, uid, gid, cb) {
      log('chown', path, uid, gid)
      self.drive._update(path, { uid, gid }, (err) => {
        if (err) return cb(-(err.errno != null ? err.errno : 1) || Fuse.EPERM)
        return cb(0)
      })
    }

    handlers.chmod = function (path, mode, cb) {
      log('chmod', path, mode)
      self.drive._update(path, { mode }, (err) => {
        if (err) return cb(-(err.errno != null ? err.errno : 1) || Fuse.EPERM)
        return cb(0)
      })
    }

    handlers.utimens = function (path, atime, mtime, cb) {
      log('utimens', path, atime, mtime)
      self.drive._update(path, { atime, mtime }, (err) => {
        if (err) return cb(-(err.errno != null ? err.errno : 1) || Fuse.EPERM)
        return cb(0)
      })
    }

    handlers.symlink = function (target, linkname, cb) {
      log('symlink', target, linkname)
      self.drive.symlink(target, linkname, (err) => {
        if (err) return cb(-(err.errno != null ? err.errno : 1) || Fuse.EPERM)
        return cb(0)
      })
    }

    handlers.readlink = function (path, cb) {
      log('readlink', path)
      self.drive.lstat(path, (err, st) => {
        if (err) return cb(-(err.errno != null ? err.errno : 2) || Fuse.ENOENT)
        const linkname =
          !p.isAbsolute(st.linkname) && !st.linkname.startsWith('.') ? '/' + st.linkname : st.linkname
        const resolved = p.isAbsolute(st.linkname)
          ? p.join(self.mnt, linkname)
          : p.join(self.mnt, p.resolve(path, linkname))
        return cb(0, resolved)
      })
    }

    handlers.statfs = function (path, cb) {
      // flag must not set ST_RDONLY; avoid arbitrary large f_flag (some clients treat
      // the volume as read-only or mis-handle unknown bits).
      cb(0, {
        bsize: 4096,
        frsize: 4096,
        blocks: 1000000,
        bfree: 1000000,
        bavail: 1000000,
        files: 1000000,
        ffree: 1000000,
        favail: 1000000,
        fsid: 0,
        flag: 0,
        namemax: 255
      })
    }

    // macOS and some apps use access(2) before open. Implement it so a writable drive
    // is not spuriously treated as read-only, while still matching existence checks.
    handlers.access = function (path, mode, cb) {
      self.drive.lstat(path, (err) => {
        if (err) {
          return cb(-(err.errno != null ? err.errno : 2) || Fuse.ENOENT)
        }
        if (!self.raw.writable && (mode & W_OK)) {
          return cb(-Fuse.EROFS)
        }
        return cb(0)
      })
    }

    handlers.setxattr = function (path, name, buffer, position, flags, cb) {
      log('setxattr', path, name)
      if (platform === 'darwin' && name && name.startsWith('com.apple')) {
        return cb(0)
      }
      self.drive.setMetadata(path, name, Buffer.from(buffer), (err) => {
        if (err) return cb(-(err.errno != null ? err.errno : 1) || Fuse.EPERM)
        return cb(0)
      })
    }

    handlers.getxattr = function (path, name, position, cb) {
      log('getxattr', path, name)
      self.drive.getxattr(path, name, position, (err, value) => {
        if (err) {
          if (err.code === 'ENODATA') {
            return cb(platform === 'darwin' ? -93 : Fuse.ENODATA, null)
          }
          if (err.errno != null) {
            return cb(-err.errno, null)
          }
          return cb(Fuse.EPERM, null)
        }
        return cb(0, value)
      })
    }

    handlers.listxattr = function (path, cb) {
      log('listxattr', path)
      self.drive.listxattr(path, (err, list) => {
        if (err) {
          if (err.errno != null) {
            return cb(-err.errno, null)
          }
          return cb(Fuse.EPERM, null)
        }
        return cb(0, list)
      })
    }

    handlers.removexattr = function (path, name, cb) {
      log('removexattr', path, name)
      self.drive.removeMetadata(path, name, (err) => {
        if (err) return cb(-(err.errno != null ? err.errno : 1) || Fuse.EPERM)
        return cb(0)
      })
    }

    return handlers
  }

  async mount (handlers) {
    if (this.fuse) {
      throw new Error('Cannot remount the same HyperdriveFuse instance.')
    }
    const self = this
    handlers = handlers ? { ...handlers } : this.getBaseHandlers()
    const mountOpts = {
      uid: process.getuid(),
      gid: process.getgid(),
      displayFolder: true,
      autoCache: true,
      force: true,
      mkdir: true,
      debug: debug.enabled,
      // release must not be capped while put() flushes; read/readdir for large views.
      timeout: {
        write: false,
        read: false,
        release: false,
        releasedir: false,
        readdir: false,
        open: false,
        create: false,
        default: 60 * 1000
      }
    }
    const fuse = new Fuse(this.mnt, handlers, mountOpts)
    return new Promise((resolve, reject) => {
      return self.drive.ready((err) => {
        if (err) return reject(err)
        return fuse.mount((e) => {
          if (e) return reject(e)
          const keyString = z32.encode(self.raw.key)
          self.fuse = fuse
          return resolve({
            handlers,
            mnt: self.mnt,
            key: keyString,
            drive: self.raw
          })
        })
      })
    })
  }

  unmount () {
    if (!this.fuse) return null
    return new Promise((resolve, reject) => {
      return this.fuse.unmount((err) => {
        if (err) return reject(err)
        return resolve()
      })
    })
  }
}

module.exports = {
  HyperdriveFuse,
  createPosixAdapter,
  configure: Fuse.configure,
  unconfigure: Fuse.unconfigure,
  isConfigured: Fuse.isConfigured,
  beforeMount: Fuse.beforeMount,
  beforeUnmount: Fuse.beforeUnmount,
  unmount: Fuse.unmount
}
