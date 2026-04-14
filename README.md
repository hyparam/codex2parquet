# codex2parquet

[![mit license](https://img.shields.io/badge/License-MIT-orange.svg)](https://opensource.org/licenses/MIT)
[![dependencies](https://img.shields.io/badge/Dependencies-1-blueviolet)](https://www.npmjs.com/package/codex2parquet?activeTab=dependencies)

A command-line tool to convert Codex session logs to Parquet format for data analysis and AI applications.

## Installation

```bash
npm install -g codex2parquet
```

## Usage

```bash
# Export Codex logs for current directory to codex_logs.parquet
codex2parquet

# Export logs from all projects
codex2parquet --all

# Export to custom filename
codex2parquet --output logs.parquet

# Export logs for a specific project directory
codex2parquet --project ~/code/myapp

# Read from a non-default Codex data directory
codex2parquet --codex-dir ~/.codex
```

## What Gets Exported

Codex stores local data under `~/.codex` by default. This tool reads:

- `~/.codex/sessions/**/*.jsonl`: current Codex rollout logs. Each line is a JSON object with `timestamp`, `type`, and `payload`.
- `~/.codex/sessions/rollout-*.json`: legacy rollout logs. Each file contains a `session` object and an `items` array.
- `~/.codex/state_5.sqlite`: thread metadata, including cwd, title, model, model provider, CLI version, sandbox policy, approval mode, token totals, git metadata, dynamic tools, and subagent parent/child edges.
- `~/.codex/history.jsonl`: prompt history rows with `session_id`, Unix timestamp, and text.
- `~/.codex/logs_2.sqlite`: diagnostic/runtime log rows when the `sqlite3` CLI is available.

The SQLite sources are optional. If `sqlite3` is not installed, the exporter still writes rollout and history rows.

## Output Schema

The generated Parquet file is an event table. It includes one row per rollout event, legacy item, history prompt, or diagnostic log entry.

Important columns:

- `source_kind`: `rollout`, `history`, or `diagnostic_log`
- `project`: Project name derived from `cwd`
- `session_id`: Codex thread/session identifier
- `item_index`: Event index within its source
- `timestamp`: ISO timestamp when available
- `rollout_path`: Source rollout file path
- `top_level_type`: Current JSONL top-level type, such as `session_meta`, `event_msg`, `response_item`, or `turn_context`
- `event_type`: Nested event type for `event_msg` payloads
- `item_type`: Response item type, such as `message`, `reasoning`, `function_call`, or `function_call_output`
- `role`, `name`, `status`, `call_id`, `item_id`, `turn_id`: Common message and tool-call identifiers
- `content`, `text`, `arguments`, `output`: Flattened text and tool-call data
- `model`, `model_provider`, `reasoning_effort`, `cwd`, `title`, `source`, `cli_version`: Thread/session metadata
- `approval_mode`, `sandbox_policy`, `tokens_used`, `git_sha`, `git_branch`, `git_origin_url`: Execution metadata from `state_5.sqlite`
- `parent_thread_id`, `child_thread_id`: Subagent relationships from `thread_spawn_edges`
- `log_*`: Diagnostic log columns from `logs_2.sqlite`
- `input_tokens`, `cached_input_tokens`, `output_tokens`, `reasoning_output_tokens`, `total_tokens`: Token usage when present in event payloads
- `rate_limits_json`, `dynamic_tools_json`, `content_json`, `payload_json`, `raw_json`: Raw JSON preservation columns

All Parquet columns are written as strings to keep the schema stable across Codex log format changes.

## Options

- `--output <file>`, `-o <file>`: Output parquet filename (default: `codex_logs.parquet`)
- `--project <path>`: Filter logs to a specific project directory
- `--all`: Export logs from all Codex projects
- `--codex-dir <path>`: Codex data directory (default: `~/.codex`)
- `--no-history`: Skip prompt history rows
- `--no-diagnostics`: Skip diagnostic log rows
- `--help`, `-h`: Show help message

## Requirements

- Node.js
- Codex local data in `~/.codex`
- Optional: `sqlite3` CLI for metadata and diagnostic-log enrichment

## Use Cases

- Analyzing Codex usage patterns across projects
- Building datasets from human-agent coding sessions
- Auditing tool calls, command outputs, and runtime diagnostics
- Creating dashboards over models, projects, token usage, and git branches

## Hyperparam

[Hyperparam](https://hyperparam.app) is a tool for exploring and curating AI datasets, such as those produced by codex2parquet.
