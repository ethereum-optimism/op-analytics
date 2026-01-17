/**
 * Tool: search_optimism_docs
 * Search docs.optimism.io for documentation.
 * Phase 1: Returns titles/IDs only, no content.
 */

import { fetchPage, searchInContent } from "../utils/web-fetcher.js";
import {
  getDocsUrl,
  loadResourceIndex,
  searchKnowledgeBase,
} from "../resources/knowledge-base.js";
import { cache } from "../cache/cache-manager.js";

export interface SearchDocsInput {
  query: string;
  section?: string;
  limit?: number;
}

export interface SearchDocsOutput {
  results: Array<{
    id: string;
    title: string;
    section: string;
    score: number;
  }>;
  total: number;
}

const MAX_RESULTS = 5;
const MAX_RESPONSE_CHARS = 300;

export async function searchOptimismDocs(
  input: SearchDocsInput
): Promise<SearchDocsOutput> {
  const limit = Math.min(input.limit ?? MAX_RESULTS, MAX_RESULTS);
  const index = loadResourceIndex();
  const results: Array<{
    id: string;
    title: string;
    section: string;
    score: number;
  }> = [];

  // First, check knowledge base for relevant sections
  const kbResults = searchKnowledgeBase(input.query, limit);
  for (const kb of kbResults) {
    if (kb.type === "docs") {
      results.push({
        id: kb.name,
        title: kb.description,
        section: kb.name,
        score: kb.score,
      });
    }
  }

  // If a specific section is requested, search within it
  if (input.section) {
    const sectionInfo = index.docs.sections[input.section];
    if (sectionInfo) {
      const url = getDocsUrl(input.section);
      try {
        const page = await fetchPage(url);
        const matches = searchInContent(page.content, input.query, limit);

        for (const match of matches) {
          // Extract a title from the excerpt (first line or truncated)
          const firstLine = match.excerpt.split("\n")[0];
          const title =
            firstLine.length > 50 ? firstLine.slice(0, 47) + "..." : firstLine;

          results.push({
            id: `${input.section}:${results.length}`,
            title,
            section: input.section,
            score: match.score,
          });
        }
      } catch (error) {
        // Section not accessible, continue with knowledge base results
      }
    }
  }

  // Sort by score and deduplicate
  results.sort((a, b) => b.score - a.score);
  const seen = new Set<string>();
  const deduped = results.filter((r) => {
    const key = `${r.section}:${r.title}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  return {
    results: deduped.slice(0, limit),
    total: deduped.length,
  };
}

export const searchDocsSchema = {
  name: "search_optimism_docs",
  description:
    "Search Optimism documentation (docs.optimism.io). Returns titles only. Use get_optimism_doc to fetch content.",
  inputSchema: {
    type: "object" as const,
    properties: {
      query: {
        type: "string",
        description: "Search query (e.g., 'bridging tokens', 'deploy rollup')",
      },
      section: {
        type: "string",
        description:
          "Section to search: chain-operators, node-operators, app-developers, op-stack, superchain, governance",
      },
      limit: {
        type: "number",
        description: "Max results (default: 5, max: 5)",
      },
    },
    required: ["query"],
  },
};
