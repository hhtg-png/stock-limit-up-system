import test from 'node:test'
import assert from 'node:assert/strict'
import {
  existsSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync
} from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const frontendRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const runnerPath = resolve(frontendRoot, 'tests', 'run-all-tests.mjs')

test('npm test uses a cross-platform runner that discovers each direct Node test once', async () => {
  assert.equal(existsSync(runnerPath), true, 'tests/run-all-tests.mjs must exist')

  const packageJson = JSON.parse(
    readFileSync(resolve(frontendRoot, 'package.json'), 'utf8')
  )
  assert.equal(packageJson.scripts.test, 'node tests/run-all-tests.mjs')

  const { discoverDirectTests } = await import(pathToFileURL(runnerPath).href)
  const discovered = discoverDirectTests(frontendRoot)
  const expected = readdirSync(resolve(frontendRoot, 'tests'))
    .filter((name) => name.endsWith('.test.mjs'))
    .filter((name) => !['chinaDate.test.mjs', 'reviewRange.test.mjs'].includes(name))
    .map((name) => `tests/${name}`)
    .sort()

  assert.deepEqual(discovered, expected)
  assert.equal(new Set(discovered).size, discovered.length)
})

test('runner fixes every child cwd to the frontend root', async () => {
  assert.equal(existsSync(runnerPath), true, 'tests/run-all-tests.mjs must exist')

  const { main, runSteps } = await import(pathToFileURL(runnerPath).href)
  const probeRoot = mkdtempSync(join(tmpdir(), 'frontend-runner-cwd-'))
  writeFileSync(resolve(probeRoot, 'relative-probe.mjs'), 'process.exit(0)\n')

  try {
    const exitCode = runSteps([
      {
        label: 'relative cwd probe',
        command: process.execPath,
        args: ['relative-probe.mjs']
      }
    ], { cwd: probeRoot, stdio: 'pipe', report: false })

    assert.equal(exitCode, 0)

    let observed
    const previousExitCode = process.exitCode
    try {
      main((steps, options) => {
        observed = { steps, options }
        return 0
      })
    } finally {
      process.exitCode = previousExitCode
    }

    assert.equal(observed.options.cwd, frontendRoot)
    assert.equal(observed.steps.length, 3)
  } finally {
    rmSync(probeRoot, { recursive: true, force: true })
  }
})

test('runner propagates child exit failures, spawn errors, and signals', async () => {
  assert.equal(existsSync(runnerPath), true, 'tests/run-all-tests.mjs must exist')

  const { normalizeSpawnExitCode, runSteps } = await import(
    pathToFileURL(runnerPath).href
  )
  const options = { stdio: 'pipe', report: false }
  const runOne = (step) => runSteps([step], options)

  assert.equal(normalizeSpawnExitCode({ status: 7 }), 7)
  assert.equal(normalizeSpawnExitCode({ status: null, error: { code: 'ENOENT' } }), 1)
  assert.equal(normalizeSpawnExitCode({ status: null, signal: 'SIGTERM' }), 1)

  assert.equal(runOne({
    label: 'intentional nonzero exit',
    command: process.execPath,
    args: ['-e', 'process.exit(7)']
  }), 7)

  assert.notEqual(runOne({
    label: 'intentional spawn error',
    command: resolve(frontendRoot, 'missing-command-for-contract-test'),
    args: []
  }), 0)

  assert.notEqual(runOne({
    label: 'intentional signal termination',
    command: process.execPath,
    args: ['-e', "process.kill(process.pid, 'SIGTERM')"]
  }), 0)
})
