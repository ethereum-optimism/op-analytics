# Optimism Knowledge MCP - Conversation Audit Log

**Date**: 2026-01-16
**Purpose**: Audit and traceability for MCP implementation decisions

---

## User Request

> Can you make me an MCP that I can use for the goal of having claude/agents know exactly what Optimism docs/specs/github links and files to look at? The user experience I want is for claude to just know what all of the optimism docs and resources (e.g., code in github) are. I'm not sure if this is best as an MCP, or a plugin, or a skill or something else? Please help me decide.

### Resources Specified:
- **Docs**: https://docs.optimism.io/
- **Specs**: https://specs.optimism.io/
- **GitHub**: https://github.com/ethereum-optimism (optimism repo is main)
- **Block Explorer**: https://explorer.optimism.io/

### Additional Requirements:
- Save prompts as md file for audit/traceability
- Build under unique folder in repo

---

## Research Findings

### Optimism Documentation Structure (docs.optimism.io)
6 main sections:
1. **Chain Operators** - Deploying and managing L2 rollups
2. **Node Operators** - Running network nodes
3. **App Developers** - Building blockchain applications
4. **OP Stack** - Protocol fundamentals
5. **OP Mainnet & Superchain** - Network-specific info
6. **Governance** - How Optimism evolves

### Optimism Specs Structure (specs.optimism.io)
- Technical specifications in chapter format
- Covers: About Optimism, About OP Stack, Protocol specs
- Sequential chapter organization

### GitHub Repository (ethereum-optimism/optimism)
**Languages**: Go (58.4%), Solidity (21.5%), Rust (17.1%)

**Key Components**:
- `op-node` - Rollup consensus-layer client
- `op-batcher` - Batch submission service
- `op-proposer` - Output submission service
- `op-challenger` - Dispute game challenge agent
- `op-program` - Fault proof program
- `packages/contracts-bedrock` - Smart contracts
- `op-deployer` - Deployment CLI
- `op-conductor` - HA sequencer service
- `op-supervisor` - Cross-chain message safety

---

## Decision Analysis

### Option 1: MCP Server (Recommended)
**Pros**:
- Dynamic search across all resources
- Live content fetching
- Reusable across sessions
- Standard protocol works with Claude Code and other clients
- Richest user experience

**Cons**:
- Requires running a server
- More complex to implement

### Option 2: Claude Code Plugin
- Essentially wraps an MCP server
- Same capabilities as MCP

### Option 3: Skill
- Pre-defined prompts/workflows
- Too limited for dynamic knowledge access

### Option 4: Static Context File
- Simple but no dynamic capabilities
- Good as supplement to MCP

---

## Recommendation

**Implement an MCP Server** because:
1. Provides dynamic search across all Optimism resources
2. Can fetch and summarize documentation on demand
3. Can query GitHub API for relevant code
4. Works with Claude Code and any MCP-compatible client
5. Best matches the goal of Claude "just knowing" about resources

---

## Implementation Plan Summary

See full plan at: `/Users/michael/.claude/plans/luminous-cuddling-moore.md`

**Location**: `packages/op-knowledge-mcp/`

**Key Tools**:
- `search_optimism_docs` - Search documentation
- `search_optimism_specs` - Search specifications
- `search_optimism_github` - Search GitHub repo
- `get_optimism_doc` - Fetch specific doc
- `get_github_file` - Fetch specific file
- `get_resource_index` - Get curated resource index

---

---

## User Decisions (from Q&A)

| Question | Answer |
|----------|--------|
| Language preference | TypeScript (Recommended) |
| GitHub API strategy | Both options (works without token, enhanced with token) |
| Caching | Yes, with TTL (1-hour refresh) |
| Repo scope | All ethereum-optimism organization repos |
| Approach (after review) | Context-efficient MCP |

## Context-Efficiency Requirements (User Priority)

User emphasized that MCPs often take up too much context, so this MCP must be designed with strict context-efficiency principles:

1. **Minimal default responses**: Max 5 results per search
2. **Two-phase tools**: Search returns IDs only, fetch gets content
3. **Response size caps**: Hard limit 2000 chars per response
4. **Summaries first**: Never full content in search results
5. **No auto-expansion**: No "helpful extras"
6. **Structured output**: Compact JSON
7. **Relevance threshold**: Only results > 0.5 score

---

## Session Metadata

- **Claude Model**: claude-opus-4-5-20251101
- **Working Directory**: /Users/michael/Documents/GitHub/op-analytics
- **Branch**: fix-coll-dupes
- **Plan File**: /Users/michael/.claude/plans/luminous-cuddling-moore.md
