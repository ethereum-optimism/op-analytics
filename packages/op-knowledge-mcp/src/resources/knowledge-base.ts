/**
 * Knowledge base that provides access to the curated resource index.
 */

import { readFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const INDEX_PATH = join(__dirname, "../../data/resource-index.json");

export interface DocSection {
  path: string;
  description: string;
  topics: string[];
}

export interface SpecSection {
  path: string;
  description: string;
  topics: string[];
}

export interface GitHubRepo {
  description: string;
  url: string;
  components?: string[];
}

export interface ResourceIndex {
  docs: {
    base_url: string;
    description: string;
    sections: Record<string, DocSection>;
  };
  specs: {
    base_url: string;
    description: string;
    sections: Record<string, SpecSection>;
  };
  github: {
    org: string;
    org_url: string;
    key_repos: Record<string, GitHubRepo>;
  };
  explorer: Record<string, { url: string; description: string }>;
  community: Record<string, { url: string; description: string }>;
}

let cachedIndex: ResourceIndex | null = null;

/**
 * Load the resource index from disk.
 */
export function loadResourceIndex(): ResourceIndex {
  if (cachedIndex) {
    return cachedIndex;
  }

  const content = readFileSync(INDEX_PATH, "utf-8");
  cachedIndex = JSON.parse(content) as ResourceIndex;
  return cachedIndex;
}

/**
 * Get a compact summary of the resource index.
 */
export function getIndexSummary(category?: "docs" | "specs" | "github"): string {
  const index = loadResourceIndex();
  const parts: string[] = [];

  if (!category || category === "docs") {
    parts.push(`## Docs (${index.docs.base_url})`);
    for (const [name, section] of Object.entries(index.docs.sections)) {
      parts.push(`- ${name}: ${section.description}`);
    }
  }

  if (!category || category === "specs") {
    parts.push(`## Specs (${index.specs.base_url})`);
    for (const [name, section] of Object.entries(index.specs.sections)) {
      parts.push(`- ${name}: ${section.description}`);
    }
  }

  if (!category || category === "github") {
    parts.push(`## GitHub (${index.github.org_url})`);
    for (const [name, repo] of Object.entries(index.github.key_repos)) {
      parts.push(`- ${name}: ${repo.description}`);
    }
  }

  return parts.join("\n");
}

/**
 * Get the full URL for a docs section.
 */
export function getDocsUrl(sectionOrPath: string): string {
  const index = loadResourceIndex();
  const section = index.docs.sections[sectionOrPath];
  if (section) {
    return `${index.docs.base_url}${section.path}`;
  }
  // Assume it's a path
  if (sectionOrPath.startsWith("/")) {
    return `${index.docs.base_url}${sectionOrPath}`;
  }
  return `${index.docs.base_url}/${sectionOrPath}`;
}

/**
 * Get the full URL for a specs section.
 */
export function getSpecsUrl(sectionOrPath: string): string {
  const index = loadResourceIndex();
  const section = index.specs.sections[sectionOrPath];
  if (section) {
    return `${index.specs.base_url}${section.path}`;
  }
  if (sectionOrPath.startsWith("/")) {
    return `${index.specs.base_url}${sectionOrPath}`;
  }
  return `${index.specs.base_url}/${sectionOrPath}`;
}

/**
 * Search the knowledge base for relevant sections.
 */
export function searchKnowledgeBase(
  query: string,
  limit: number = 5
): Array<{ type: "docs" | "specs" | "github"; name: string; description: string; score: number }> {
  const index = loadResourceIndex();
  const queryLower = query.toLowerCase();
  const results: Array<{
    type: "docs" | "specs" | "github";
    name: string;
    description: string;
    score: number;
  }> = [];

  // Search docs sections
  for (const [name, section] of Object.entries(index.docs.sections)) {
    let score = 0;
    if (name.toLowerCase().includes(queryLower)) score += 5;
    if (section.description.toLowerCase().includes(queryLower)) score += 3;
    for (const topic of section.topics) {
      if (topic.toLowerCase().includes(queryLower)) score += 2;
    }
    if (score > 0) {
      results.push({ type: "docs", name, description: section.description, score });
    }
  }

  // Search specs sections
  for (const [name, section] of Object.entries(index.specs.sections)) {
    let score = 0;
    if (name.toLowerCase().includes(queryLower)) score += 5;
    if (section.description.toLowerCase().includes(queryLower)) score += 3;
    for (const topic of section.topics) {
      if (topic.toLowerCase().includes(queryLower)) score += 2;
    }
    if (score > 0) {
      results.push({ type: "specs", name, description: section.description, score });
    }
  }

  // Search GitHub repos
  for (const [name, repo] of Object.entries(index.github.key_repos)) {
    let score = 0;
    if (name.toLowerCase().includes(queryLower)) score += 5;
    if (repo.description.toLowerCase().includes(queryLower)) score += 3;
    if (repo.components) {
      for (const component of repo.components) {
        if (component.toLowerCase().includes(queryLower)) score += 2;
      }
    }
    if (score > 0) {
      results.push({ type: "github", name, description: repo.description, score });
    }
  }

  // Sort by score and limit
  results.sort((a, b) => b.score - a.score);
  return results.slice(0, limit);
}
