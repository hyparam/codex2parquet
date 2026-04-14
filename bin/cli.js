#!/usr/bin/env node

import { writeCodexLogsParquet } from '../src/index.js'

/**
 * Parse command line arguments.
 * @returns {Record<string, string | boolean>}
 */
function parseCliArgs() {
  const args = process.argv.slice(2)
  const options = {}

  for (let i = 0; i < args.length; i++) {
    const arg = args[i]

    if (arg === '--help' || arg === '-h') {
      console.log(`codex2parquet

Usage: codex2parquet [options]

Options:
  --output <file>      Output parquet filename (default: codex_logs.parquet)
  --project <path>     Filter logs to a specific project directory
  --all                Export logs from all Codex projects
  --codex-dir <path>   Codex data directory (default: ~/.codex)
  --no-history         Do not include ~/.codex/history.jsonl prompt history rows
  --no-diagnostics     Do not include ~/.codex/logs_2.sqlite diagnostic log rows
  -h, --help           Show this help message

By default, exports logs for the current directory.

Examples:
  codex2parquet
  codex2parquet --all
  codex2parquet --output logs.parquet
  codex2parquet --project ~/code/myapp
  codex2parquet --codex-dir ~/.codex`)
      process.exit(0)
    }

    if (arg === '--output' || arg === '-o') {
      if (i + 1 >= args.length) {
        console.error('Error: --output requires a filename argument')
        process.exit(1)
      }
      options.filename = args[++i]
      continue
    }

    if (arg === '--project') {
      if (i + 1 >= args.length) {
        console.error('Error: --project requires a path argument')
        process.exit(1)
      }
      options.project = args[++i]
      continue
    }

    if (arg === '--codex-dir') {
      if (i + 1 >= args.length) {
        console.error('Error: --codex-dir requires a path argument')
        process.exit(1)
      }
      options.codexDir = args[++i]
      continue
    }

    if (arg === '--all') {
      options.all = true
      continue
    }

    if (arg === '--no-history') {
      options.includeHistory = false
      continue
    }

    if (arg === '--no-diagnostics') {
      options.includeDiagnostics = false
      continue
    }

    console.error(`Error: Unknown option '${arg}'`)
    console.error('Use --help for usage information')
    process.exit(1)
  }

  if (!options.project && !options.all) {
    options.project = '.'
  }

  return options
}

const options = parseCliArgs()

writeCodexLogsParquet(options).then(result => {
  const cwd = process.cwd()
  const localPath = result.filename.startsWith(cwd)
    ? result.filename.slice(cwd.length + 1)
    : result.filename
  const filename = result.filename.split('/').pop()

  console.log(`Exported ${result.eventCount} events from ${result.sessionCount} sessions to ${filename}`)

  const line1 = 'Analyze logs with Hyperparam:'
  const line2 = `npx hyperparam scope ${localPath}`
  const width = Math.max(line1.length, line2.length) + 2
  const top = '+' + '-'.repeat(width) + '+'
  const bottom = '+' + '-'.repeat(width) + '+'
  const pad1 = ' '.repeat(width - 1 - line1.length)
  const pad2 = ' '.repeat(width - 1 - line2.length)
  console.log(`\n${top}`)
  console.log(`| ${line1}${pad1}|`)
  console.log(`| \x1b[36m${line2}\x1b[0m${pad2}|`)
  console.log(`${bottom}\n`)
}).catch(err => {
  console.error(`Error: ${err.message}`)
  process.exit(1)
})
