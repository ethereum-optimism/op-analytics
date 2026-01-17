# Optimism Knowledge MCP Server

An MCP (Model Context Protocol) server that gives Claude Code access to Optimism documentation, specs, and GitHub resources.

## Quick Start (5 minutes)

### Prerequisites

- Node.js 18+ installed
- Claude Code CLI installed (`claude` command works)

### Step 1: Install dependencies

```bash
cd packages/op-knowledge-mcp
npm install
```

### Step 2: Add to Claude Code settings

Open your Claude Code settings file:

```bash
# macOS/Linux
code ~/.claude/settings.json
# Or use any editor
nano ~/.claude/settings.json
```

Add the `mcpServers` section (merge with existing settings if any):

```json
{
  "mcpServers": {
    "op-knowledge": {
      "command": "npx",
      "args": ["tsx", "/FULL/PATH/TO/op-analytics/packages/op-knowledge-mcp/src/index.ts"]
    }
  }
}
```

**Important:** Replace `/FULL/PATH/TO/` with your actual path. Find it with:
```bash
cd packages/op-knowledge-mcp && pwd
```

### Step 3: Restart Claude Code

Exit and reopen Claude Code. You should see the MCP tools available.

### Step 4: Test it works

In Claude Code, ask:
> "Use get_resource_index to show me what Optimism resources are available"

---

## What This Does

When working with Claude Code, you can now ask questions like:
- "Search the optimism specs for information about derivation"
- "Find the op-node source code for batch submission"
- "Get the README from the superchain-registry repo"

Claude will use these tools automatically when relevant, or you can ask it to use specific tools.

## Optional: GitHub Token

For higher API rate limits (recommended for heavy use), add a GitHub personal access token:

```json
{
  "mcpServers": {
    "op-knowledge": {
      "command": "npx",
      "args": ["tsx", "/FULL/PATH/TO/op-analytics/packages/op-knowledge-mcp/src/index.ts"],
      "env": {
        "GITHUB_TOKEN": "ghp_your_token_here"
      }
    }
  }
}
```

Create a token at: https://github.com/settings/tokens (no special scopes needed for public repos)

---

## Troubleshooting

**MCP not showing up in Claude Code?**
- Ensure the path in settings.json is absolute (starts with `/`)
- Check Node.js is installed: `node --version` (need 18+)
- Restart Claude Code completely (not just the conversation)

**"Cannot find module" errors?**
- Run `npm install` in the `packages/op-knowledge-mcp` directory

**GitHub API rate limited?**
- Add a `GITHUB_TOKEN` (see above)

---

## Design Principles

This MCP is optimized for **context efficiency**:

- **Minimal default responses**: Max 5 results per search, hard limit of 2000 chars per response
- **Two-phase pattern**: Search returns IDs/titles only; separate fetch for content
- **Summaries first**: 1-2 sentence summaries, never full content in search results
- **No auto-expansion**: Never auto-includes related content or "helpful extras"

---

## Available Tools

### Phase 1: Search Tools (Lightweight)

These return IDs, titles, and brief summaries only. Use them first to find what you need.

#### `get_resource_index`

Get a compact overview of available Optimism resources.

```typescript
// Input
{ category?: "docs" | "specs" | "github" }

// Output
{ summary: string }
```

#### `list_github_repos`

List repositories in the ethereum-optimism organization.

```typescript
// Input
{ limit?: number }  // default: 10, max: 20

// Output
{ repos: [{ name, description_short, url }] }
```

#### `search_optimism_github`

Search code in ethereum-optimism GitHub repositories.

```typescript
// Input
{
  query: string,        // e.g., "derivation pipeline"
  repo?: string,        // e.g., "optimism"
  file_type?: string,   // e.g., "go", "sol"
  limit?: number        // default: 5, max: 5
}

// Output
{ results: [{ repo, path, score, url }], total: number }
```

#### `search_optimism_docs`

Search Optimism documentation (docs.optimism.io).

```typescript
// Input
{
  query: string,
  section?: string,  // chain-operators, node-operators, app-developers, etc.
  limit?: number
}

// Output
{ results: [{ id, title, section, score }], total: number }
```

#### `search_optimism_specs`

Search OP Stack technical specifications (specs.optimism.io).

```typescript
// Input
{ query: string, limit?: number }

// Output
{ results: [{ id, title, score }], total: number }
```

### Phase 2: Fetch Tools (On-Demand Content)

Use these after searching to get actual content with explicit size limits.

#### `get_optimism_doc`

Fetch content from a specific documentation page.

```typescript
// Input
{
  path: string,        // e.g., "chain-operators", "/app-developers/bridging"
  max_chars?: number   // default: 1500, max: 2000
}

// Output
{ title, content, url, truncated: boolean }
```

#### `get_optimism_spec`

Fetch content from a specific specification section.

```typescript
// Input
{
  id: string,          // e.g., "protocol", "fjord"
  max_chars?: number
}

// Output
{ title, content, url, truncated: boolean }
```

#### `get_github_file`

Fetch content of a specific file from GitHub.

```typescript
// Input
{
  repo: string,        // e.g., "optimism"
  path: string,        // e.g., "README.md", "op-node/rollup/derive/pipeline.go"
  max_chars?: number,
  line_start?: number, // 1-indexed
  line_end?: number
}

// Output
{ content, url, lines_shown, total_lines, truncated: boolean }
```

### Maintenance Tools

#### `check_index_freshness`

Check if the knowledge index is up-to-date.

```typescript
// Input
{}

// Output
{ is_fresh: boolean, last_synced: string, age_days: number, message: string }
```

#### `sync_resource_index`

Sync the knowledge index from upstream sources (GitHub, docs, specs).

```typescript
// Input
{}

// Output
{ success: boolean, last_synced: string, sections_count: { docs, specs, github_repos }, message: string }
```

## Usage Examples

### Find documentation about bridging

```
1. search_optimism_docs({ query: "bridging tokens" })
   → Returns: [{ id: "app-developers", title: "Build blockchain apps", ... }]

2. get_optimism_doc({ path: "app-developers", max_chars: 1500 })
   → Returns: { title: "...", content: "...", url: "..." }
```

### Find op-node source code

```
1. search_optimism_github({ query: "derivation", repo: "optimism", file_type: "go" })
   → Returns: [{ repo: "optimism", path: "op-node/rollup/derive/pipeline.go", ... }]

2. get_github_file({ repo: "optimism", path: "op-node/rollup/derive/pipeline.go" })
   → Returns: { content: "...", url: "...", lines_shown: "1-50", ... }
```

### Get specification for fault proofs

```
1. search_optimism_specs({ query: "fault proofs" })
   → Returns: [{ id: "protocol", title: "Core protocol specifications", ... }]

2. get_optimism_spec({ id: "protocol" })
   → Returns: { title: "...", content: "...", url: "..." }
```

## Resources Covered

### Documentation (docs.optimism.io)

- **chain-operators**: Deploy and manage L2 rollups
- **node-operators**: Run network nodes
- **app-developers**: Build blockchain apps
- **op-stack**: Protocol fundamentals
- **superchain**: Multi-chain coordination
- **governance**: How Optimism evolves

### Specifications (specs.optimism.io)

- **protocol**: Core protocol specs (derivation, deposits, withdrawals, fault proofs)
- **experimental**: Experimental features (interop, custom gas token, plasma)
- **fjord**: Fjord network upgrade
- **granite**: Granite network upgrade

### GitHub (ethereum-optimism)

- **optimism**: Main monorepo (op-node, op-batcher, contracts-bedrock, etc.)
- **op-geth**: Execution client fork
- **supersim**: Local multi-chain dev environment
- **superchain-registry**: Canonical chain registry
- **specs**: Specification documents source

## Keeping the Index Up-to-Date

The MCP server uses a local resource index (`data/resource-index.json`) that caches information about Optimism docs, specs, and GitHub repos. This index can become stale as Optimism's documentation evolves.

### Manual Sync

Run the sync script to update the index from upstream sources:

```bash
npm run sync
```

This fetches:
- Specs structure from `ethereum-optimism/specs` SUMMARY.md
- Docs sections from `ethereum-optimism/docs`
- GitHub repos from the ethereum-optimism org API

### Checking Freshness

Use the `check_index_freshness` MCP tool to see when the index was last synced:

```
check_index_freshness({})
→ { is_fresh: true, last_synced: "2026-01-16T...", age_days: 0 }
```

The index is considered stale after 7 days.

### Auto-Sync via MCP

Use the `sync_resource_index` MCP tool to trigger a sync from within Claude:

```
sync_resource_index({})
→ { success: true, sections_count: { docs: 7, specs: 6, github_repos: 9 } }
```

### Recommended: Weekly Sync

For production use, consider setting up a cron job or CI workflow to run `npm run sync` weekly.

---

## Development

```bash
# Run in development mode
npm run dev

# Build for production
npm run build

# Run built server
npm start

# Sync resource index from upstream
npm run sync
```

## Caching

All responses are cached for 1 hour by default to minimize API calls. GitHub search results are cached for 15 minutes.

## Architecture

```
src/
├── index.ts                # MCP server entry point
├── tools/
│   ├── get-index.ts        # Resource index tool
│   ├── search-docs.ts      # Docs search tool
│   ├── search-specs.ts     # Specs search tool
│   ├── search-github.ts    # GitHub search tools
│   ├── get-content.ts      # Content fetch tools
│   └── sync-index.ts       # Index freshness/sync tools
├── sync/
│   └── sync-resources.ts   # Upstream sync script
├── resources/
│   └── knowledge-base.ts   # Curated knowledge index
├── cache/
│   └── cache-manager.ts    # TTL-based caching
└── utils/
    ├── web-fetcher.ts      # Web scraping utilities
    └── github-api.ts       # GitHub API client

data/
└── resource-index.json     # Synced knowledge index (auto-updated)
```

## License

MIT
