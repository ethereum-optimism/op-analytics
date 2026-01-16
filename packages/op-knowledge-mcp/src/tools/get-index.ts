/**
 * Tool: get_resource_index
 * Returns a compact overview of available Optimism resources.
 */

import { getIndexSummary, loadResourceIndex } from "../resources/knowledge-base.js";

export interface GetIndexInput {
  category?: "docs" | "specs" | "github";
}

export interface GetIndexOutput {
  summary: string;
}

const MAX_RESPONSE_CHARS = 500;

export function getResourceIndex(input: GetIndexInput): GetIndexOutput {
  const summary = getIndexSummary(input.category);

  // Truncate if needed
  if (summary.length > MAX_RESPONSE_CHARS) {
    return {
      summary: summary.slice(0, MAX_RESPONSE_CHARS - 3) + "...",
    };
  }

  return { summary };
}

export const getIndexSchema = {
  name: "get_resource_index",
  description:
    "Get a compact overview of available Optimism documentation, specs, and GitHub resources. Use this first to understand what's available.",
  inputSchema: {
    type: "object" as const,
    properties: {
      category: {
        type: "string",
        enum: ["docs", "specs", "github"],
        description: "Filter to a specific category (optional)",
      },
    },
  },
};
