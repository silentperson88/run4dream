import { spawn } from 'child_process'
import path from 'path'
import process from 'process'
import url from 'url'

const __dirname = path.dirname(url.fileURLToPath(import.meta.url))
const rootDir = path.resolve(__dirname, '..')

const mode = String(process.argv[2] || 'dev').toLowerCase()

const serviceConfigs = [
  {
    name: 'market-backend',
    cwd: path.join(rootDir, 'services', 'market-backend'),
    command: 'npm',
    args: ['run', 'dev']
  },
  {
    name: 'user-backend',
    cwd: path.join(rootDir, 'services', 'user-backend'),
    command: 'npm',
    args: ['run', 'dev']
  },
  {
    name: 'content-backend',
    cwd: path.join(rootDir, 'services', 'content-backend'),
    command: 'npm',
    args: mode === 'mixed' ? ['run', 'start'] : ['run', 'dev']
  },
  {
    name: 'gateway',
    cwd: path.join(rootDir, 'gateway'),
    command: 'npm',
    args: ['run', 'dev']
  }
]

const children = []
let shuttingDown = false

function stopAll(exitCode = 0) {
  if (shuttingDown) return
  shuttingDown = true

  for (const child of children) {
    if (child?.kill) {
      child.kill()
    }
  }

  setTimeout(() => process.exit(exitCode), 500)
}

process.on('SIGINT', () => stopAll(0))
process.on('SIGTERM', () => stopAll(0))

for (const service of serviceConfigs) {
  const child = spawn(service.command, service.args, {
    cwd: service.cwd,
    stdio: 'inherit',
    shell: true,
    env: process.env
  })

  children.push(child)

  child.on('exit', code => {
    if (shuttingDown) return
    if (typeof code === 'number' && code !== 0) {
      console.error(`[${service.name}] exited with code ${code}`)
      stopAll(code)
    }
  })
}

