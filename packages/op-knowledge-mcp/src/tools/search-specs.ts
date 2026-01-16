/**
 * Tool: search_optimism_specs
 * Search specs.optimism.io for technical specifications.
 * Phase 1: Returns titles/IDs only, no content.
 */

import { fetchPage, searchInContent } from "../utils/web-fetcher.js";
import {
  getSpecsUrl,
  loadResourceIndex,
  searchKnowledgeBase,
} from "../resources/knowledge-base.js";

export interface SearchSpecsInput {
  query: string;
  limit?: number;
}

export interface SearchSpecsOutput {
  results: Array<{
    id: string;
    title: string;
    score: number;
  }>;
  total: number;
}

const MAX_RESULTS = 5;
const MAX_RESPONSE_CHARS = 300;

export async function searchOptimismSpecs(
  input: SearchSpecsInput
): Promise<SearchSpecsOutput> {
  const limit = Math.min(input.limit ?? MAX_RESULTS, MAX_RESULTS);
  const index = loadResourceIndex();
  const results: Array<{
    id: string;
    title: string;
    score: number;
  }> = [];

  // Check knowledge base for relevant spec sections
  const kbResults = searchKnowledgeBase(input.query, limit);
  for (const kb of kbResults) {
    if (kb.type === "specs") {
      results.push({
        id: kb.name,
        title: kb.description,
        score: kb.score,
      });
    }
  }

  // Search in spec sections
  for (const [sectionName, sectionInfo] of Object.entries(
    index.specs.sections
  )) {
    // Check if query matches section topics
    const queryLower = input.query.toLowerCase();
    let topicScore = 0;
    for (const topic of sectionInfo.topics) {
      if (topic.toLowerCase().includes(queryLower)) {
        topicScore += 3;
      }
      if (queryLower.includes(topic.toLowerCase())) {
        topicScore += 2;
      }
    }

    if (topicScore > 0) {
      // Avoid duplicates from knowledge base
      const exists = results.some((r) => r.id === sectionName);
      if (!exists) {
        results.push({
          id: sectionName,
          title: sectionInfo.description,
          score: topicScore,
        });
      }
    }
  }

  // Sort by score
  results.sort((a, b) => b.score - a.score);

  return {
    results: results.slice(0, limit),
    total: results.length,
  };
}

export const searchSpecsSchema = {
  name: "search_optimism_specs",
  description:
    "Search OP Stack technical specifications (specs.optimism.io). Returns titles only. Use get_optimism_spec to fetch content.",
  inputSchema: {
    type: "object" as const,
    properties: {
      query: {
        type: "string",
        description:
          "Search query (e.g., 'derivation', 'fault proofs', 'deposits')",
      },
      limit: {
        type: "number",
        description: "Max results (default: 5, max: 5)",
      },
    },
    required: ["query"],
  },
};
