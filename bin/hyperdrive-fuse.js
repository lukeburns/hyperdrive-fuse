#!/usr/bin/env node
'use strict'

// Before loading index / fuse, so `debug('hyperdrive-fuse')` and mountOpts see DEBUG.
;(() => {
  const a = process.argv
  for (let i = 0; i < a.length; i++) {
    if (a[i] === '--debug' || a[i] === '-d') {
      const m = 'hyperdrive-fuse'
      process.env.DEBUG = process.env.DEBUG ? process.env.DEBUG + ',' + m : m
      break
    }
  }
})()

const p = require('path')
const fs = require('fs')
const os = require('os')

const Corestore = require('corestore')
const Hyperdrive = require('hyperdrive')
const z32 = require('z32')

const { HyperdriveFuse, isConfigured, unmount: fuseUnmount } = require('..')

const name = 'hyperdrive-fuse'
const version = readPkgVersion()
const isDarwin = process.platform === 'darwin'

function readPkgVersion () {
  try {
    const j = JSON.parse(
      fs.readFileSync(p.join(__dirname, '..', 'package.json'), 'utf8')
    )
    return j.version || '0.0.0'
  } catch {
    return '0.0.0'
  }
}

function die (code, msg) {
  if (msg) {
    process.stderr.write(msg + (msg.endsWith('\n') ? '' : '\n'))
  }
  process.exit(typeof code === 'number' ? code : 1)
}

function help () {
  process.stdout.write(`\
${name} v${version}

Usage:
  ${name} mount <mountpoint> [options]   Start a FUSE mount backed by a Hyperdrive
  ${name} unmount <mountpoint>            Unmount a FUSE path (or use Ctrl+C on mount)
  ${name} help                            Show this help
  ${name} version                         Print version

Mount options:
  -d, --debug            Verbose FUSE op logging (getattr, readdir, open, etc.)
  -s, --storage <path>   Corestore directory (default: ~/.hyperdrive-fuse)
  -k, --key <z32-key>    Open an existing drive by public key (z32, 52 chars)
  --no-swarm             Do not join Hyperswarm / DHT (local corestore only)

By default, mount replicates the drive over the network with Hyperswarm (DHT P2P).
See: https://github.com/holepunchto/hyperswarm

Examples:
  ${name} mount ~/mnt
  ${name} mount ~/mnt -s ~/.cache/my-hyperdrive
  ${name} mount ~/mnt -k <your-drive-key>
  ${name} mount -d ~/mnt                    # FUSE + handler debug
  ${name} mount ~/mnt --no-swarm            # offline / no replication
`)
}

function parseMountArgs (raw) {
  let storage = p.join(os.homedir(), '.hyperdrive-fuse')
  let keyStr = null
  let noSwarm = false
  const pos = []
  for (let i = 0; i < raw.length; i++) {
    const a = raw[i]
    if (a === '-s' || a === '--storage') {
      const v = raw[++i]
      if (v == null) die(1, 'Missing value for ' + a)
      storage = p.resolve(v)
    } else if (a === '-k' || a === '--key') {
      const v = raw[++i]
      if (v == null) die(1, 'Missing value for ' + a)
      keyStr = v
    } else if (a === '--no-swarm') {
      noSwarm = true
    } else if (a === '-d' || a === '--debug') {
      // DEBUG is set in the bootstrap above; nothing to parse here
    } else if (a === '-h' || a === '--help') {
      help()
      process.exit(0)
    } else if (a.startsWith('-')) {
      die(1, `Unknown option: ${a}\nRun "${name} help" for usage.`)
    } else {
      pos.push(a)
    }
  }
  if (pos.length < 1) {
    die(1, 'mount: missing <mountpoint>\nRun "' + name + ' help" for usage.')
  }
  if (pos.length > 1) {
    die(1, 'mount: only one <mountpoint> is allowed.')
  }
  return { storage, keyStr, mountPath: p.resolve(pos[0]), noSwarm }
}

async function cmdMount (rest) {
  const { storage, keyStr, mountPath, noSwarm } = parseMountArgs(rest)

  let key = null
  if (keyStr) {
    try {
      if (keyStr.length === 64 && /^[0-9a-f]+$/i.test(keyStr)) {
        key = Buffer.from(keyStr, 'hex')
      } else {
        key = z32.decode(keyStr)
      }
      if (!key || key.length !== 32) {
        die(1, 'Invalid --key: expected 32-byte public key after decode.')
      }
    } catch (e) {
      die(1, 'Invalid --key: use z32 (52 chars) or 64 hex.\n' + (e && e.message))
    }
  }

  fs.mkdirSync(storage, { recursive: true })

  if (isDarwin) {
    await new Promise((resolve) => {
      isConfigured((err, ok) => {
        if (!err && !ok) {
          process.stderr.write(
            'Warning: FUSE may not be configured. On macOS see @zkochan/fuse-native (e.g. fuse-native configure)\n'
          )
        }
        resolve()
      })
    })
  }

  const store = new Corestore(storage)
  const drive = key ? new Hyperdrive(store, key) : new Hyperdrive(store)
  await drive.ready()

  let swarm = null
  let doneFinding = null
  if (!noSwarm) {
    const Hyperswarm = require('hyperswarm')
    swarm = new Hyperswarm()
    swarm.on('connection', (conn) => {
      store.replicate(conn)
    })
    try {
      swarm.join(drive.discoveryKey, { server: true, client: true })
    } catch (e) {
      process.stderr.write('Warning: Hyperswarm join failed: ' + (e && e.message ? e.message : e) + '\n')
    }
    doneFinding = drive.findingPeers()
    // Avoid blocking the shell on first DHT round-trip; connections arrive asynchronously.
    void swarm.flush()
  }

  const fuse = new HyperdriveFuse(drive, mountPath)
  let result
  try {
    result = await fuse.mount()
  } catch (e) {
    if (doneFinding) {
      try {
        doneFinding()
      } catch {
        // ignore
      }
      doneFinding = null
    }
    if (swarm) {
      try {
        await swarm.destroy()
      } catch {
        // ignore
      }
    }
    try {
      await drive.close()
    } catch {
      // ignore
    }
    try {
      await store.close()
    } catch {
      // ignore
    }
    die(1, 'Mount failed: ' + (e && e.message ? e.message : e))
  }

  const keyZ32 = result.key
  const discZ32 = z32.encode(drive.discoveryKey)
  const w = result.drive && result.drive.writable
  const ro = w ? '' : `  (read-only: open without --key for a new writable drive, or use storage that has the write key for this key.)\n`
  const swarmLine = noSwarm
    ? '  Hyperswarm: disabled (--no-swarm)\n'
    : '  Hyperswarm: DHT P2P replication enabled (client + server on discovery key)\n' +
    `  Discovery (z32):  ${discZ32}  (topic for swarm.join)\n`
  process.stderr.write(
    `Hyperdrive FUSE mounted\n  Mount:  ${result.mnt}\n  Storage: ${storage}\n  Public key (z32): ${keyZ32}\n` +
    swarmLine +
    (w ? '  Mode:  read/write\n' : '  Mode:  read-only\n') +
    ro +
    `  Node PID: ${process.pid} (keep this process running — if it stops, the mount returns ENXIO / "Device not configured".)\n` +
    `  Press Ctrl+C to unmount.\n`
  )

  const shutdown = async (signal) => {
    if (signal) {
      process.stderr.write(`\n${signal} received, unmounting…\n`)
    }
    if (doneFinding) {
      try {
        doneFinding()
      } catch {
        // ignore
      }
    }
    if (swarm) {
      try {
        await swarm.destroy()
      } catch {
        // ignore
      }
    }
    try {
      await fuse.unmount()
    } catch (e) {
      process.stderr.write('Unmount: ' + (e && e.message) + '\n')
    }
    try {
      await drive.close()
    } catch {
      // ignore
    }
    process.exit(0)
  }
  process.once('SIGINT', () => {
    shutdown('SIGINT')
  })
  process.once('SIGTERM', () => {
    shutdown('SIGTERM')
  })
}

function cmdUnmount (rest) {
  if (rest.length < 1) {
    die(1, 'unmount: missing <mountpoint>\nRun "' + name + ' help" for usage.')
  }
  if (rest.length > 1) {
    die(1, 'unmount: only one <mountpoint> is allowed.')
  }
  const mnt = p.resolve(rest[0])
  fuseUnmount(mnt, (err) => {
    if (err) {
      die(1, 'Unmount failed: ' + (err.message || err))
    }
    process.stderr.write('Unmounted ' + mnt + '\n')
  })
}

function main () {
  const args = process.argv.slice(2)
  if (args.length === 0) {
    help()
    process.exit(0)
  }
  const cmd = args[0]
  if (cmd === 'help' || cmd === '-h' || cmd === '--help') {
    help()
    process.exit(0)
  }
  if (cmd === 'version' || cmd === '-V' || cmd === '--version') {
    process.stdout.write(name + ' v' + version + '\n')
    process.exit(0)
  }
  if (cmd === 'mount' || cmd === 'm') {
    cmdMount(args.slice(1)).catch((e) => {
      process.stderr.write((e && e.stack) || String(e) + '\n')
      process.exit(1)
    })
    return
  }
  if (cmd === 'unmount' || cmd === 'u' || cmd === 'umount') {
    cmdUnmount(args.slice(1))
    return
  }
  if (cmd.startsWith('-')) {
    die(1, `Unknown option: ${cmd}\nRun "${name} help" for usage.`)
  }
  process.stderr.write(
    `Unknown command: ${cmd}\nRun "${name} help" for usage.\n`
  )
  process.exit(1)
}

main()
