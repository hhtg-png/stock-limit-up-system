import { spawnSync } from 'node:child_process'
import { readdirSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const WRAPPED_TESTS = new Set([
  'chinaDate.test.mjs',
  'reviewRange.test.mjs'
])

export function discoverDirectTests(frontendRoot) {
  return readdirSync(resolve(frontendRoot, 'tests'), { withFileTypes: true })
    .filter((entry) => entry.isFile())
    .map((entry) => entry.name)
    .filter((name) => name.endsWith('.test.mjs'))
    .filter((name) => !WRAPPED_TESTS.has(name))
    .map((name) => `tests/${name}`)
    .sort()
}

export function runSteps(steps, options = {}) {
  const stdio = options.stdio ?? 'inherit'
  const report = options.report ?? true
  let exitCode = 0

  for (const step of steps) {
    if (report) console.log(`\n[frontend:test] ${step.label}`)
    const result = spawnSync(step.command, step.args, { stdio })
    const stepExitCode = result.status ?? 1

    if (report && result.error) {
      console.error(`[frontend:test] 无法启动 ${step.label}: ${result.error.message}`)
    } else if (report && stepExitCode !== 0) {
      console.error(`[frontend:test] ${step.label} 失败，退出码 ${stepExitCode}`)
    }

    if (exitCode === 0 && stepExitCode !== 0) {
      exitCode = stepExitCode
    }
  }

  return exitCode
}

function main() {
  const frontendRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')
  const directTests = discoverDirectTests(frontendRoot)
  const steps = [
    {
      label: `直接 Node 测试（${directTests.length} 个文件）`,
      command: process.execPath,
      args: ['--test', ...directTests]
    },
    {
      label: '中国日期 TypeScript 包装测试',
      command: process.execPath,
      args: ['tests/run-china-date-test.mjs']
    },
    {
      label: '复盘区间 TypeScript 包装测试',
      command: process.execPath,
      args: ['tests/run-review-range-test.mjs']
    }
  ]

  process.exitCode = runSteps(steps)
}

const invokedPath = process.argv[1] ? resolve(process.argv[1]) : ''
if (invokedPath === fileURLToPath(import.meta.url)) {
  main()
}
