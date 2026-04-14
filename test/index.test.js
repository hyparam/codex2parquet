import { mkdirSync, writeFileSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import { DatabaseSync } from 'node:sqlite'
import { describe, expect, it } from 'vitest'
import { readCodexLogs } from '../src/index.js'

function fixtureDir() {
  const root = join(tmpdir(), `codex2parquet-${process.pid}-${Math.random().toString(16).slice(2)}`)
  mkdirSync(join(root, 'sessions', '2026', '04', '13'), { recursive: true })
  return root
}

/**
 * @param {string} filename
 * @param {(db: DatabaseSync) => void} write
 */
function writeSqliteFixture(filename, write) {
  const db = new DatabaseSync(filename)
  try {
    write(db)
  } finally {
    db.close()
  }
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

  it('reads Codex sqlite metadata with the native Node sqlite module', () => {
    const codexDir = fixtureDir()
    const rollout = join(codexDir, 'sessions', '2026', '04', '13', 'rollout-2026-04-13T21-00-02-019d8a25-6f8f-7750-aa00-15e23ea2bf64.jsonl')
    writeFileSync(rollout, JSON.stringify({
      timestamp: '2026-04-14T04:00:02.000Z',
      type: 'session_meta',
      payload: {
        id: '019d8a25-6f8f-7750-aa00-15e23ea2bf64',
      },
    }) + '\n')

    writeSqliteFixture(join(codexDir, 'state_5.sqlite'), db => {
      db.exec(`
        create table threads (
          id text, rollout_path text, created_at text, updated_at text, source text,
          model_provider text, cwd text, title text, sandbox_policy text,
          approval_mode text, tokens_used integer, has_user_event integer,
          archived integer, archived_at text, git_sha text, git_branch text,
          git_origin_url text, cli_version text, first_user_message text,
          agent_nickname text, agent_role text, memory_mode text, model text,
          reasoning_effort text, agent_path text
        );
      `)
      db.prepare(`
        insert into threads (
          id, rollout_path, source, model_provider, cwd, title, sandbox_policy,
          approval_mode, tokens_used, has_user_event, archived, git_sha,
          git_branch, git_origin_url, cli_version, model, reasoning_effort
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        '019d8a25-6f8f-7750-aa00-15e23ea2bf64',
        rollout,
        'cli',
        'openai',
        '/tmp/sqlite-project',
        'SQLite metadata',
        'workspace-write',
        'on-request',
        123,
        1,
        0,
        'abc123',
        'main',
        'git@example.com:repo/project.git',
        '0.120.0',
        'gpt-5.4',
        'medium'
      )
    })

    const rows = readCodexLogs({
      codexDir,
      project: '/tmp/sqlite-project',
      includeHistory: false,
      includeDiagnostics: false,
    })

    expect(rows).toHaveLength(1)
    expect(rows[0]).toMatchObject({
      source_kind: 'rollout',
      session_id: '019d8a25-6f8f-7750-aa00-15e23ea2bf64',
      cwd: '/tmp/sqlite-project',
      project: 'sqlite-project',
      title: 'SQLite metadata',
      approval_mode: 'on-request',
      sandbox_policy: 'workspace-write',
      tokens_used: '123',
      model: 'gpt-5.4',
    })
  })
})
