/**
 * Tool: search_optimism_github
 * Search code in ethereum-optimism GitHub repositories.
 * Phase 1: Returns IDs/paths only, no content.
 */

import { searchCode, listRepos, type RepoInfo, type SearchResult } from "../utils/github-api.js";

export interface SearchGitHubInput {
  query: string;
  repo?: string;
  file_type?: string;
  limit?: number;
}

export interface SearchGitHubOutput {
  results: Array<{
    repo: string;
    path: string;
    score: number;
    url: string;
  }>;
  total: number;
}

const MAX_RESULTS = 5;
const MAX_RESPONSE_CHARS = 400;

export async function searchOptimismGitHub(
  input: SearchGitHubInput
): Promise<SearchGitHubOutput> {
  const limit = Math.min(input.limit ?? MAX_RESULTS, MAX_RESULTS);

  const results = await searchCode(
    input.query,
    input.repo,
    input.file_type,
    limit
  );

  // Filter by relevance threshold
  const filtered = results.filter((r) => r.score > 0);

  return {
    results: filtered.map((r) => ({
      repo: r.repo,
      path: r.path,
      score: Math.round(r.score * 100) / 100,
      url: r.url,
    })),
    total: filtered.length,
  };
}

export interface ListReposInput {
  limit?: number;
}

export interface ListReposOutput {
  repos: Array<{
    name: string;
    description_short: string;
    url: string;
  }>;
}

export async function listGitHubRepos(
  input: ListReposInput
): Promise<ListReposOutput> {
  const limit = Math.min(input.limit ?? 10, 20);
  const repos = await listRepos(limit);

  return {
    repos: repos.map((r) => ({
      name: r.name,
      description_short: r.description
        ? r.description.slice(0, 60) + (r.description.length > 60 ? "..." : "")
        : "No description",
      url: r.url,
    })),
  };
}

export const searchGitHubSchema = {
  name: "search_optimism_github",
  description:
    "Search code in ethereum-optimism GitHub repositories. Returns file paths only (no content). Use get_github_file to fetch content.",
  inputSchema: {
    type: "object" as const,
    properties: {
      query: {
        type: "string",
        description: "Search query (e.g., 'derivation pipeline', 'L2OutputOracle')",
      },
      repo: {
        type: "string",
        description: "Specific repo to search (e.g., 'optimism', 'op-geth')",
      },
      file_type: {
        type: "string",
        description: "File extension filter (e.g., 'go', 'sol', 'ts')",
      },
      limit: {
        type: "number",
        description: "Max results (default: 5, max: 5)",
      },
    },
    required: ["query"],
  },
};

export const listReposSchema = {
  name: "list_github_repos",
  description:
    "List repositories in the ethereum-optimism GitHub organization.",
  inputSchema: {
    type: "object" as const,
    properties: {
      limit: {
        type: "number",
        description: "Max repos to return (default: 10, max: 20)",
      },
    },
  },
};
