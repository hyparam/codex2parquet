import { mkdirSync, writeFileSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import { describe, expect, it } from 'vitest'
import { readCodexLogs } from '../src/index.js'

function fixtureDir() {
  const root = join(tmpdir(), `codex2parquet-${process.pid}-${Math.random().toString(16).slice(2)}`)
  mkdirSync(join(root, 'sessions', '2026', '04', '13'), { recursive: true })
  return root
}

describe('readCodexLogs', () => {
  it('reads current jsonl rollout rows and history rows', () => {
    const codexDir = fixtureDir()
    const rollout = join(codexDir, 'sessions', '2026', '04', '13', 'rollout-2026-04-13T21-00-02-019d8a25-6f8f-7750-aa00-15e23ea2bf64.jsonl')
    writeFileSync(rollout, [
      JSON.stringify({
        timestamp: '2026-04-14T04:00:02.000Z',
        type: 'session_meta',
        payload: {
          id: '019d8a25-6f8f-7750-aa00-15e23ea2bf64',
          cwd: '/tmp/my-project',
          cli_version: '0.120.0',
          source: 'cli',
          model_provider: 'openai',
        },
      }),
      JSON.stringify({
        timestamp: '2026-04-14T04:00:03.000Z',
        type: 'response_item',
        payload: {
          type: 'message',
          role: 'user',
          content: [{ type: 'input_text', text: 'hello codex' }],
        },
      }),
    ].join('\n'))
    writeFileSync(join(codexDir, 'history.jsonl'), JSON.stringify({
      session_id: '019d8a25-6f8f-7750-aa00-15e23ea2bf64',
      ts: 1776139203,
      text: 'hello codex',
    }) + '\n')

    const rows = readCodexLogs({
      codexDir,
      project: '/tmp/my-project',
      includeDiagnostics: false,
    })

    expect(rows).toHaveLength(3)
    expect(rows.map(row => row.source_kind).sort()).toEqual(['history', 'rollout', 'rollout'])
    const messageRow = rows.find(row => row.top_level_type === 'response_item')
    expect(messageRow).toMatchObject({
      session_id: '019d8a25-6f8f-7750-aa00-15e23ea2bf64',
      top_level_type: 'response_item',
      item_type: 'message',
      role: 'user',
      text: 'hello codex',
      cwd: '/tmp/my-project',
      project: 'my-project',
      cli_version: '0.120.0',
    })
  })

  it('reads legacy json rollout rows when exporting all logs', () => {
    const codexDir = fixtureDir()
    const rollout = join(codexDir, 'sessions', 'rollout-2025-04-25-d3cea4c3-86b6-4131-8872-65a69df59730.json')
    writeFileSync(rollout, JSON.stringify({
      session: {
        timestamp: '2025-04-25T05:18:20.977Z',
        id: 'd3cea4c3-86b6-4131-8872-65a69df59730',
        instructions: '',
      },
      items: [{
        type: 'function_call',
        name: 'shell',
        call_id: 'call_123',
        arguments: '{"command":["npm","test"]}',
      }],
    }))

    const rows = readCodexLogs({
      codexDir,
      includeHistory: false,
      includeDiagnostics: false,
    })

    expect(rows).toHaveLength(1)
    expect(rows[0]).toMatchObject({
      source_kind: 'rollout',
      session_id: 'd3cea4c3-86b6-4131-8872-65a69df59730',
      item_type: 'function_call',
      name: 'shell',
      call_id: 'call_123',
    })
  })
})
