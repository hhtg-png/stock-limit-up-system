import test from 'node:test'
import assert from 'node:assert/strict'
import { existsSync, readFileSync, readdirSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
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

test('runner propagates a failing child process status', async () => {
  assert.equal(existsSync(runnerPath), true, 'tests/run-all-tests.mjs must exist')

  const { runSteps } = await import(pathToFileURL(runnerPath).href)
  const exitCode = runSteps([
    {
      label: 'intentional contract failure',
      command: process.execPath,
      args: ['-e', 'process.exit(7)']
    }
  ], { stdio: 'pipe', report: false })

  assert.equal(exitCode, 7)
})
