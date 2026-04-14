import { existsSync, readFileSync, readdirSync } from 'fs'
import { execFileSync } from 'child_process'
import { basename, extname, join, resolve } from 'path'
import { homedir } from 'os'
import { parquetWriteFile } from 'hyparquet-writer'

const defaultFilename = 'codex_logs.parquet'
const rolloutIdPattern = /rollout-.+?-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl?$/i

const columns = [
  'source_kind',
  'project',
  'session_id',
  'item_index',
  'timestamp',
  'timestamp_unix',
  'rollout_path',
  'top_level_type',
  'event_type',
  'item_type',
  'role',
  'name',
  'status',
  'call_id',
  'item_id',
  'turn_id',
  'text',
  'tool_input_json',
  'tool_output',
  'duration_ms',
  'model',
  'model_provider',
  'reasoning_effort',
  'cwd',
  'title',
  'source',
  'cli_version',
  'originator',
  'approval_mode',
  'sandbox_policy',
  'tokens_used',
  'git_sha',
  'git_branch',
  'git_origin_url',
  'archived',
  'has_user_event',
  'input_tokens',
  'cached_input_tokens',
  'output_tokens',
  'reasoning_output_tokens',
  'total_tokens',
  'rate_limits_json',
  'metadata_json',
  'content_json',
  'payload_json',
  'raw_json',
]

/**
 * Convert unknown values to stable string cells.
 * @param {any} value
 * @returns {string}
 */
function cell(value) {
  if (value === null || value === undefined) return ''
  if (typeof value === 'string') return value
  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') return String(value)
  return JSON.stringify(value)
}

/**
 * Parse JSON without throwing.
 * @param {string} text
 * @returns {any}
 */
function parseJson(text) {
  try {
    return JSON.parse(text)
  } catch {
    return undefined
  }
}

/**
 * @param {number | string | undefined | null} ts
 * @returns {string}
 */
function timestampFromUnix(ts) {
  if (ts === null || ts === undefined || ts === '') return ''
  const n = Number(ts)
  if (!Number.isFinite(n)) return ''
  return new Date(n * 1000).toISOString()
}

/**
 * Recursively find Codex rollout files under a directory.
 * @param {string} dir
 * @returns {string[]}
 */
function findRolloutFiles(dir) {
  const files = []
  if (!existsSync(dir)) return files

  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name)
    if (entry.isDirectory()) {
      files.push(...findRolloutFiles(full))
    } else if (
      entry.name.startsWith('rollout-') &&
      (entry.name.endsWith('.jsonl') || entry.name.endsWith('.json'))
    ) {
      files.push(full)
    }
  }
  return files
}

/**
 * @param {string} file
 * @returns {string}
 */
function sessionIdFromPath(file) {
  const match = basename(file).match(rolloutIdPattern)
  return match?.[1] ?? ''
}

/**
 * @param {string} cwd
 * @returns {string}
 */
function projectName(cwd) {
  return cwd ? basename(cwd) : ''
}

/**
 * @param {string | undefined} maybePath
 * @returns {string}
 */
function expandHome(maybePath) {
  if (!maybePath) return maybePath ?? ''
  if (maybePath === '~') return homedir()
  if (maybePath.startsWith('~/')) return join(homedir(), maybePath.slice(2))
  return maybePath
}

/**
 * @param {string} a
 * @param {string} b
 * @returns {boolean}
 */
function samePath(a, b) {
  return resolve(expandHome(a)) === resolve(expandHome(b))
}

/**
 * @param {string} codexDir
 * @returns {Record<string, any>[]}
 */
function sqliteJson(codexDir, dbName, sql) {
  const dbPath = join(codexDir, dbName)
  if (!existsSync(dbPath)) return []

  try {
    const output = execFileSync('sqlite3', ['-json', dbPath, sql], {
      encoding: 'utf8',
      maxBuffer: 256 * 1024 * 1024,
      stdio: ['ignore', 'pipe', 'ignore'],
    }).trim()
    return output ? JSON.parse(output) : []
  } catch {
    return []
  }
}

/**
 * Read Codex thread metadata from ~/.codex/state_5.sqlite when sqlite3 is available.
 * @param {string} codexDir
 * @returns {{byId: Map<string, Record<string, any>>, byPath: Map<string, Record<string, any>>}}
 */
function readThreadMetadata(codexDir) {
  const threadRows = sqliteJson(codexDir, 'state_5.sqlite', `
    select
      id, rollout_path, created_at, updated_at, source, model_provider, cwd, title,
      sandbox_policy, approval_mode, tokens_used, has_user_event, archived,
      archived_at, git_sha, git_branch, git_origin_url, cli_version,
      first_user_message, agent_nickname, agent_role, memory_mode, model,
      reasoning_effort, agent_path
    from threads
  `)
  const toolRows = sqliteJson(codexDir, 'state_5.sqlite', `
    select thread_id, position, name, description, input_schema, defer_loading
    from thread_dynamic_tools
    order by thread_id, position
  `)

  /** @type {Map<string, any[]>} */
  const toolsByThread = new Map()
  for (const tool of toolRows) {
    const list = toolsByThread.get(tool.thread_id) ?? []
    list.push(tool)
    toolsByThread.set(tool.thread_id, list)
  }

  const byId = new Map()
  const byPath = new Map()
  for (const row of threadRows) {
    row.dynamic_tools_json = JSON.stringify(toolsByThread.get(row.id) ?? [])
    byId.set(row.id, row)
    byPath.set(row.rollout_path, row)
  }
  return { byId, byPath }
}

/**
 * @param {string} codexDir
 * @returns {Map<string, string>}
 */
function readParentThreadMap(codexDir) {
  const rows = sqliteJson(codexDir, 'state_5.sqlite', `
    select parent_thread_id, child_thread_id, status
    from thread_spawn_edges
  `)
  const parents = new Map()
  for (const row of rows) {
    parents.set(row.child_thread_id, row.parent_thread_id)
  }
  return parents
}

/**
 * @param {any} content
 * @returns {string}
 */
function flattenContent(content) {
  if (typeof content === 'string') return content
  if (!Array.isArray(content)) return cell(content)

  return content.map(block => {
    if (!block || typeof block !== 'object') return cell(block)
    if (typeof block.text === 'string') return block.text
    if (typeof block.output_text === 'string') return block.output_text
    if (typeof block.content === 'string') return block.content
    if (block.type === 'image_url') return '[image_url]'
    if (block.type === 'local_image') return `[local_image: ${block.path ?? ''}]`
    if (block.type === 'tool_use' || block.type === 'function_call') return `[tool: ${block.name ?? ''}]`
    if (block.type === 'tool_result' || block.type === 'function_call_output') return cell(block.content ?? block.output)
    return ''
  }).filter(Boolean).join('\n')
}

/**
 * @param {any} item
 * @returns {string}
 */
function extractText(item) {
  if (!item || typeof item !== 'object') return ''
  if (typeof item.text === 'string') return item.text
  if (typeof item.message === 'string') return item.message
  if (typeof item.output === 'string') {
    const parsed = parseJson(item.output)
    return typeof parsed?.output === 'string' ? parsed.output : item.output
  }
  return flattenContent(item.content ?? item.message?.content)
}

/**
 * Decode function/tool output bodies to the plain output text when possible.
 * @param {any} output
 * @returns {string}
 */
function extractToolOutput(output) {
  if (output === null || output === undefined) return ''
  if (typeof output !== 'string') return cell(output)
  const parsed = parseJson(output)
  if (typeof parsed?.output === 'string') return parsed.output
  if (typeof parsed?.content === 'string') return parsed.content
  return output
}

/**
 * Drop empty values from an object before preserving it as JSON.
 * @param {Record<string, any>} obj
 * @returns {Record<string, any>}
 */
function compactObject(obj) {
  return Object.fromEntries(Object.entries(obj).filter(([, value]) => {
    if (value === null || value === undefined || value === '') return false
    if (Array.isArray(value) && value.length === 0) return false
    return true
  }))
}

/**
 * Serialize an object, returning an empty cell when it has no useful keys.
 * @param {Record<string, any>} obj
 * @returns {string}
 */
function jsonCell(obj) {
  const compacted = compactObject(obj)
  return Object.keys(compacted).length ? JSON.stringify(compacted) : ''
}

/**
 * @param {any} obj
 * @returns {{input_tokens:string, cached_input_tokens:string, output_tokens:string, reasoning_output_tokens:string, total_tokens:string}}
 */
function tokenColumns(obj) {
  const usage = obj?.info?.total_token_usage ?? obj?.usage ?? obj?.token_usage ?? obj
  return {
    input_tokens: cell(usage?.input_tokens),
    cached_input_tokens: cell(usage?.cached_input_tokens),
    output_tokens: cell(usage?.output_tokens),
    reasoning_output_tokens: cell(usage?.reasoning_output_tokens),
    total_tokens: cell(usage?.total_tokens),
  }
}

/**
 * @param {Record<string, any>} metadata
 * @param {Record<string, string>} extra
 * @returns {Record<string, string>}
 */
function makeRow(metadata, extra) {
  const { metadata_json: extraMetadataJson, ...restExtra } = extra
  const cwd = cell(extra.cwd || metadata.cwd)
  const metadataJson = jsonCell({
    created_at: metadata.created_at,
    updated_at: metadata.updated_at,
    archived_at: metadata.archived_at,
    first_user_message: metadata.first_user_message,
    agent_nickname: metadata.agent_nickname,
    agent_role: metadata.agent_role,
    agent_path: metadata.agent_path,
    memory_mode: metadata.memory_mode,
    dynamic_tools: parseJson(metadata.dynamic_tools_json || ''),
    ...(extraMetadataJson ? parseJson(extraMetadataJson) ?? {} : {}),
  })
  return {
    source_kind: '',
    project: projectName(cwd),
    session_id: cell(extra.session_id || metadata.id),
    item_index: '',
    timestamp: '',
    timestamp_unix: '',
    rollout_path: cell(metadata.rollout_path),
    top_level_type: '',
    event_type: '',
    item_type: '',
    role: '',
    name: '',
    status: '',
    call_id: '',
    item_id: '',
    turn_id: '',
    text: '',
    tool_input_json: '',
    tool_output: '',
    duration_ms: '',
    model: cell(extra.model || metadata.model),
    model_provider: cell(metadata.model_provider),
    reasoning_effort: cell(extra.reasoning_effort || metadata.reasoning_effort),
    cwd,
    title: cell(metadata.title),
    source: cell(metadata.source),
    cli_version: cell(extra.cli_version || metadata.cli_version),
    originator: cell(extra.originator),
    approval_mode: cell(metadata.approval_mode),
    sandbox_policy: cell(metadata.sandbox_policy),
    tokens_used: cell(metadata.tokens_used),
    git_sha: cell(metadata.git_sha),
    git_branch: cell(metadata.git_branch),
    git_origin_url: cell(metadata.git_origin_url),
    archived: cell(metadata.archived),
    has_user_event: cell(metadata.has_user_event),
    input_tokens: '',
    cached_input_tokens: '',
    output_tokens: '',
    reasoning_output_tokens: '',
    total_tokens: '',
    rate_limits_json: '',
    metadata_json: metadataJson,
    content_json: '',
    payload_json: '',
    raw_json: '',
    ...restExtra,
  }
}

/**
 * @param {Record<string, string>} row
 * @param {string | undefined} project
 * @returns {boolean}
 */
function includeForProject(row, project) {
  if (!project) return true
  return row.cwd ? samePath(row.cwd, project) : false
}

/**
 * @param {string} file
 * @param {{byId: Map<string, Record<string, any>>, byPath: Map<string, Record<string, any>>}} threads
 * @param {Map<string, string>} parents
 * @returns {Record<string, string>[]}
 */
function readJsonlRollout(file, threads, parents) {
  const lines = readFileSync(file, 'utf8').split('\n').filter(Boolean)
  const pathSessionId = sessionIdFromPath(file)
  const metadata = { ...(threads.byId.get(pathSessionId) ?? threads.byPath.get(file) ?? {}) }
  metadata.id ??= pathSessionId
  metadata.rollout_path ??= file

  /** @type {Record<string, string>[]} */
  const rows = []
  for (let i = 0; i < lines.length; i++) {
    const obj = parseJson(lines[i])
    if (!obj) continue

    if (obj.type === 'session_meta') {
      metadata.id = obj.payload?.id ?? metadata.id
      metadata.cwd = obj.payload?.cwd ?? metadata.cwd
      metadata.source = obj.payload?.source ?? metadata.source
      metadata.model_provider = obj.payload?.model_provider ?? metadata.model_provider
      metadata.cli_version = obj.payload?.cli_version ?? metadata.cli_version
      metadata.rollout_path = file
    }

    const payload = obj.payload ?? {}
    const item = obj.type === 'response_item' ? payload : payload
    const tokens = obj.type === 'event_msg' && payload.type === 'token_count' ? tokenColumns(payload) : {}
    const content = item.content ?? item.message?.content
    const parentThreadId = parents.get(metadata.id) ?? ''

    rows.push(makeRow(metadata, {
      source_kind: 'rollout',
      session_id: cell(metadata.id),
      item_index: cell(i),
      timestamp: cell(obj.timestamp ?? payload.timestamp),
      timestamp_unix: '',
      rollout_path: file,
      top_level_type: cell(obj.type),
      event_type: cell(payload.type),
      item_type: cell(item.type),
      role: cell(item.role),
      name: cell(item.name),
      status: cell(item.status),
      call_id: cell(item.call_id),
      item_id: cell(item.id),
      turn_id: cell(payload.turn_id),
      text: extractText(item),
      tool_input_json: cell(item.arguments),
      tool_output: extractToolOutput(item.output),
      duration_ms: cell(item.duration_ms),
      model: cell(item.model || metadata.model),
      cwd: cell(payload.cwd || metadata.cwd),
      title: cell(metadata.title || payload.title),
      source: cell(payload.source || metadata.source),
      cli_version: cell(payload.cli_version || metadata.cli_version),
      originator: cell(payload.originator),
      rate_limits_json: cell(payload.rate_limits),
      metadata_json: cell({
        parent_thread_id: parentThreadId,
        child_thread_id: parentThreadId ? metadata.id : '',
      }),
      content_json: cell(content),
      payload_json: cell(payload),
      raw_json: lines[i],
      ...tokens,
    }))
  }
  return rows
}

/**
 * @param {string} file
 * @param {{byId: Map<string, Record<string, any>>, byPath: Map<string, Record<string, any>>}} threads
 * @param {Map<string, string>} parents
 * @returns {Record<string, string>[]}
 */
function readJsonRollout(file, threads, parents) {
  const obj = parseJson(readFileSync(file, 'utf8'))
  if (!obj) return []

  const session = obj.session ?? {}
  const pathSessionId = sessionIdFromPath(file)
  const sessionId = session.id ?? pathSessionId
  const metadata = { ...(threads.byId.get(sessionId) ?? threads.byPath.get(file) ?? {}) }
  metadata.id ??= sessionId
  metadata.rollout_path ??= file
  metadata.title ??= session.instructions

  const parentThreadId = parents.get(metadata.id) ?? ''
  const items = Array.isArray(obj.items) ? obj.items : []
  return items.map((item, i) => {
    const content = item.content ?? item.message?.content
    const parentThreadId = parents.get(metadata.id) ?? ''
    return makeRow(metadata, {
      source_kind: 'rollout',
      session_id: cell(metadata.id),
      item_index: cell(i),
      timestamp: cell(session.timestamp),
      timestamp_unix: '',
      rollout_path: file,
      top_level_type: 'item',
      event_type: '',
      item_type: cell(item.type),
      role: cell(item.role),
      name: cell(item.name),
      status: cell(item.status),
      call_id: cell(item.call_id),
      item_id: cell(item.id),
      text: extractText(item),
      tool_input_json: cell(item.arguments),
      tool_output: extractToolOutput(item.output),
      duration_ms: cell(item.duration_ms),
      metadata_json: cell({
        parent_thread_id: parentThreadId,
        child_thread_id: parentThreadId ? metadata.id : '',
      }),
      content_json: cell(content),
      payload_json: cell(item),
      raw_json: cell(item),
    })
  })
}

/**
 * @param {string} codexDir
 * @param {{byId: Map<string, Record<string, any>>, byPath: Map<string, Record<string, any>>}} threads
 * @returns {Record<string, string>[]}
 */
function readHistoryRows(codexDir, threads) {
  const historyPath = join(codexDir, 'history.jsonl')
  if (!existsSync(historyPath)) return []

  return readFileSync(historyPath, 'utf8').split('\n').filter(Boolean).flatMap((line, i) => {
    const obj = parseJson(line)
    if (!obj) return []
    const metadata = { ...(threads.byId.get(obj.session_id) ?? {}) }
    metadata.id ??= obj.session_id
    return makeRow(metadata, {
      source_kind: 'history',
      session_id: cell(obj.session_id),
      item_index: cell(i),
      timestamp: timestampFromUnix(obj.ts),
      timestamp_unix: cell(obj.ts),
      top_level_type: 'history',
      item_type: 'user_prompt',
      role: 'user',
      text: cell(obj.text),
      payload_json: cell(obj),
      raw_json: line,
    })
  })
}

/**
 * @param {string} codexDir
 * @param {{byId: Map<string, Record<string, any>>, byPath: Map<string, Record<string, any>>}} threads
 * @returns {Record<string, string>[]}
 */
function readDiagnosticRows(codexDir, threads) {
  const logs = sqliteJson(codexDir, 'logs_2.sqlite', `
    select id, ts, ts_nanos, level, target, feedback_log_body, module_path,
      file, line, thread_id, process_uuid, estimated_bytes
    from logs
    order by ts asc, ts_nanos asc, id asc
  `)

  return logs.map((log, i) => {
    const metadata = { ...(threads.byId.get(log.thread_id) ?? {}) }
    metadata.id ??= log.thread_id
    const ts = Number(log.ts)
    const nanos = Number(log.ts_nanos)
    const timestamp = Number.isFinite(ts)
      ? new Date((ts * 1000) + (Number.isFinite(nanos) ? Math.floor(nanos / 1e6) : 0)).toISOString()
      : ''

    return makeRow(metadata, {
      source_kind: 'diagnostic_log',
      session_id: cell(log.thread_id),
      item_index: cell(i),
      timestamp,
      timestamp_unix: cell(log.ts),
      top_level_type: 'diagnostic_log',
      item_type: 'log',
      name: cell(log.target),
      status: cell(log.level),
      item_id: cell(log.id),
      text: cell(log.feedback_log_body),
      metadata_json: cell({
        ts_nanos: log.ts_nanos,
        module_path: log.module_path,
        file: log.file,
        line: log.line,
        thread_id: log.thread_id,
        process_uuid: log.process_uuid,
        estimated_bytes: log.estimated_bytes,
      }),
      payload_json: cell(log),
      raw_json: cell(log),
    })
  })
}

/**
 * Use parsed rollout rows as a fallback metadata source when state_5.sqlite is
 * unavailable or does not contain older sessions.
 * @param {Record<string, string>[]} rows
 * @param {Map<string, Record<string, any>>} threadsById
 */
function addFallbackThreadMetadata(rows, threadsById) {
  for (const row of rows) {
    if (!row.session_id || threadsById.has(row.session_id)) continue
    if (!row.cwd && !row.rollout_path) continue
    threadsById.set(row.session_id, {
      id: row.session_id,
      rollout_path: row.rollout_path,
      cwd: row.cwd,
      title: row.title,
      source: row.source,
      model_provider: row.model_provider,
      cli_version: row.cli_version,
      model: row.model,
      reasoning_effort: row.reasoning_effort,
      git_sha: row.git_sha,
      git_branch: row.git_branch,
      git_origin_url: row.git_origin_url,
    })
  }
}

/**
 * Read and parse Codex logs into flat rows.
 * @param {{codexDir?: string, project?: string, includeHistory?: boolean, includeDiagnostics?: boolean}} [opts]
 * @returns {Record<string, string>[]}
 */
export function readCodexLogs(opts = {}) {
  const codexDir = resolve(expandHome(opts.codexDir ?? join(homedir(), '.codex')))
  const sessionsDir = join(codexDir, 'sessions')
  const threads = readThreadMetadata(codexDir)
  const parents = readParentThreadMap(codexDir)
  const rows = []

  for (const file of findRolloutFiles(sessionsDir)) {
    const ext = extname(file)
    if (ext === '.jsonl') rows.push(...readJsonlRollout(file, threads, parents))
    else if (ext === '.json') rows.push(...readJsonRollout(file, threads, parents))
  }
  addFallbackThreadMetadata(rows, threads.byId)

  if (opts.includeHistory !== false) {
    rows.push(...readHistoryRows(codexDir, threads))
  }

  if (opts.includeDiagnostics !== false) {
    rows.push(...readDiagnosticRows(codexDir, threads))
  }

  return rows
    .filter(row => includeForProject(row, opts.project))
    .sort((a, b) => {
      const byTimestamp = a.timestamp.localeCompare(b.timestamp)
      if (byTimestamp) return byTimestamp
      const bySession = a.session_id.localeCompare(b.session_id)
      if (bySession) return bySession
      return Number(a.item_index) - Number(b.item_index)
    })
}

/**
 * Convert rows into column-oriented data for hyparquet-writer.
 * @param {Record<string, string>[]} rows
 * @returns {{name: string, type: 'STRING', data: string[]}[]}
 */
function toColumnData(rows) {
  return columns.map(name => ({
    name,
    type: 'STRING',
    data: rows.map(row => cell(row[name])),
  }))
}

/**
 * Write Codex session logs to a Parquet file.
 * @param {{filename?: string, project?: string, all?: boolean, codexDir?: string, includeHistory?: boolean, includeDiagnostics?: boolean}} [opts]
 * @returns {Promise<{eventCount:number, sessionCount:number, filename:string}>}
 */
export async function writeCodexLogsParquet(opts = {}) {
  if (opts && typeof opts !== 'object') {
    throw new Error('Options must be an object')
  }

  if (opts.filename && typeof opts.filename !== 'string') {
    throw new Error('Filename must be a string')
  }

  const project = opts.all ? undefined : opts.project
  const rows = readCodexLogs({ ...opts, project })
  const codexDir = resolve(expandHome(opts.codexDir ?? join(homedir(), '.codex')))
  if (!rows.length) {
    if (project) {
      const resolvedPath = resolve(expandHome(project))
      throw new Error(
        `No Codex logs found for project: ${resolvedPath}\n` +
        'Run from a directory where Codex has been used, or specify:\n' +
        '  --project ~/path/to/project\n' +
        '  --all'
      )
    }
    throw new Error(`No Codex logs found in ${codexDir}`)
  }

  const filename = resolve(opts.filename ?? defaultFilename)

  try {
    await parquetWriteFile({
      filename,
      columnData: toColumnData(rows),
    })
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(`Failed to write parquet file: ${message}`)
  }

  const sessionCount = new Set(rows.map(r => r.session_id).filter(Boolean)).size
  return { eventCount: rows.length, sessionCount, filename }
}
