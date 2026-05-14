import { execFileSync } from 'node:child_process'
import { rmSync } from 'node:fs'

const tempDir = '.tmp-china-date'
const tscEntry = 'node_modules/typescript/bin/tsc'

try {
  rmSync(tempDir, { recursive: true, force: true })
  execFileSync(process.execPath, [
    tscEntry,
    'src/utils/chinaDate.ts',
    '--target',
    'ES2020',
    '--module',
    'ESNext',
    '--moduleResolution',
    'bundler',
    '--outDir',
    tempDir,
    '--noEmit',
    'false',
    '--declaration',
    'false',
    '--skipLibCheck',
    'true',
    '--strict',
    'true'
  ], { stdio: 'inherit' })
  execFileSync(process.execPath, ['--test', 'tests/chinaDate.test.mjs'], { stdio: 'inherit' })
} finally {
  rmSync(tempDir, { recursive: true, force: true })
}
