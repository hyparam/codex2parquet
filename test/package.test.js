import { describe, expect, it } from 'vitest'
import packageJson from '../package.json' with { type: 'json' }

describe('package.json', () => {
  it('has the correct name', () => {
    expect(packageJson.name).toBe('codex2parquet')
  })

  it('has a valid version', () => {
    expect(packageJson.version).toMatch(/^\d+\.\d+\.\d+$/)
  })

  it('has MIT license', () => {
    expect(packageJson.license).toBe('MIT')
  })

  it('uses precise dependency versions', () => {
    const { dependencies, devDependencies } = packageJson
    for (const deps of [dependencies, devDependencies]) {
      Object.values(deps).forEach(version => {
        expect(version).toMatch(/^\d+\.\d+\.\d+$/)
      })
    }
  })
})
