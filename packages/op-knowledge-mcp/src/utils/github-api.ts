/**
 * GitHub API client with optional authentication.
 * Works without a token but has lower rate limits.
 */

import { Octokit } from "@octokit/rest";
import { cache } from "../cache/cache-manager.js";

const ORG = "ethereum-optimism";

// Initialize Octokit with optional token
const octokit = new Octokit({
  auth: process.env.GITHUB_TOKEN,
});

export interface RepoInfo {
  name: string;
  description: string | null;
  url: string;
  stars: number;
  language: string | null;
}

export interface SearchResult {
  repo: string;
  path: string;
  score: number;
  url: string;
}

export interface FileContent {
  content: string;
  url: string;
  size: number;
  sha: string;
}

/**
 * List repositories in the ethereum-optimism organization.
 */
export async function listRepos(limit: number = 10): Promise<RepoInfo[]> {
  const cacheKey = `github:repos:${limit}`;

  return cache.getOrSet(cacheKey, async () => {
    const { data } = await octokit.repos.listForOrg({
      org: ORG,
      sort: "updated",
      per_page: Math.min(limit, 100),
    });

    return data.map((repo) => ({
      name: repo.name,
      description: repo.description,
      url: repo.html_url,
      stars: repo.stargazers_count ?? 0,
      language: repo.language ?? null,
    }));
  });
}

/**
 * Search code in the ethereum-optimism organization.
 */
export async function searchCode(
  query: string,
  repo?: string,
  fileType?: string,
  limit: number = 5
): Promise<SearchResult[]> {
  // Build search query
  let searchQuery = `${query} org:${ORG}`;
  if (repo) {
    searchQuery = `${query} repo:${ORG}/${repo}`;
  }
  if (fileType) {
    searchQuery += ` extension:${fileType}`;
  }

  const cacheKey = `github:search:${searchQuery}:${limit}`;

  return cache.getOrSet(
    cacheKey,
    async () => {
      const { data } = await octokit.search.code({
        q: searchQuery,
        per_page: Math.min(limit, 100),
      });

      return data.items.map((item) => ({
        repo: item.repository.name,
        path: item.path,
        score: item.score,
        url: item.html_url,
      }));
    },
    15 // Cache search results for 15 minutes
  );
}

/**
 * Get file content from a repository.
 */
export async function getFileContent(
  repo: string,
  path: string
): Promise<FileContent> {
  const cacheKey = `github:file:${repo}:${path}`;

  return cache.getOrSet(cacheKey, async () => {
    const { data } = await octokit.repos.getContent({
      owner: ORG,
      repo,
      path,
    });

    // Handle file content (not directory)
    if (Array.isArray(data) || data.type !== "file") {
      throw new Error(`Path is not a file: ${path}`);
    }

    const content = Buffer.from(data.content, "base64").toString("utf-8");

    return {
      content,
      url: data.html_url ?? `https://github.com/${ORG}/${repo}/blob/main/${path}`,
      size: data.size,
      sha: data.sha,
    };
  });
}

/**
 * Get repository README content.
 */
export async function getReadme(repo: string): Promise<FileContent | null> {
  const cacheKey = `github:readme:${repo}`;

  return cache.getOrSet(cacheKey, async () => {
    try {
      const { data } = await octokit.repos.getReadme({
        owner: ORG,
        repo,
      });

      const content = Buffer.from(data.content, "base64").toString("utf-8");

      return {
        content,
        url: data.html_url ?? `https://github.com/${ORG}/${repo}#readme`,
        size: data.size,
        sha: data.sha,
      };
    } catch {
      return null;
    }
  });
}

/**
 * Search repositories in the organization.
 */
export async function searchRepos(
  query: string,
  limit: number = 5
): Promise<RepoInfo[]> {
  const cacheKey = `github:repo-search:${query}:${limit}`;

  return cache.getOrSet(
    cacheKey,
    async () => {
      const { data } = await octokit.search.repos({
        q: `${query} org:${ORG}`,
        per_page: Math.min(limit, 100),
      });

      return data.items.map((repo) => ({
        name: repo.name,
        description: repo.description,
        url: repo.html_url,
        stars: repo.stargazers_count,
        language: repo.language,
      }));
    },
    15
  );
}
