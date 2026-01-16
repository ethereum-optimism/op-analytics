#!/usr/bin/env node
/**
 * Optimism Knowledge MCP Server
 *
 * Provides Claude/agents with comprehensive knowledge about Optimism
 * documentation, specifications, and GitHub resources.
 *
 * Design principle: Context efficiency through strict response limits,
 * two-phase search patterns, and summary-first design.
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";

// Import tools and types
import { getResourceIndex, getIndexSchema } from "./tools/get-index.js";
import type { GetIndexInput } from "./tools/get-index.js";
import {
  searchOptimismGitHub,
  searchGitHubSchema,
  listGitHubRepos,
  listReposSchema,
} from "./tools/search-github.js";
import type { SearchGitHubInput, ListReposInput } from "./tools/search-github.js";
import { searchOptimismDocs, searchDocsSchema } from "./tools/search-docs.js";
import type { SearchDocsInput } from "./tools/search-docs.js";
import { searchOptimismSpecs, searchSpecsSchema } from "./tools/search-specs.js";
import type { SearchSpecsInput } from "./tools/search-specs.js";
import {
  getOptimismDoc,
  getDocSchema,
  getOptimismSpec,
  getSpecSchema,
  getGitHubFile,
  getFileSchema,
} from "./tools/get-content.js";
import type { GetDocInput, GetSpecInput, GetFileInput } from "./tools/get-content.js";

// Create the MCP server
const server = new Server(
  {
    name: "op-knowledge",
    version: "0.1.0",
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

// Register tool listing handler
server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      getIndexSchema,
      listReposSchema,
      searchGitHubSchema,
      searchDocsSchema,
      searchSpecsSchema,
      getDocSchema,
      getSpecSchema,
      getFileSchema,
    ],
  };
});

// Register tool execution handler
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  try {
    let result: unknown;

    switch (name) {
      case "get_resource_index":
        result = getResourceIndex(args as unknown as GetIndexInput);
        break;

      case "list_github_repos":
        result = await listGitHubRepos(args as unknown as ListReposInput);
        break;

      case "search_optimism_github":
        result = await searchOptimismGitHub(args as unknown as SearchGitHubInput);
        break;

      case "search_optimism_docs":
        result = await searchOptimismDocs(args as unknown as SearchDocsInput);
        break;

      case "search_optimism_specs":
        result = await searchOptimismSpecs(args as unknown as SearchSpecsInput);
        break;

      case "get_optimism_doc":
        result = await getOptimismDoc(args as unknown as GetDocInput);
        break;

      case "get_optimism_spec":
        result = await getOptimismSpec(args as unknown as GetSpecInput);
        break;

      case "get_github_file":
        result = await getGitHubFile(args as unknown as GetFileInput);
        break;

      default:
        return {
          content: [
            {
              type: "text",
              text: `Unknown tool: ${name}`,
            },
          ],
          isError: true,
        };
    }

    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(result, null, 2),
        },
      ],
    };
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : "Unknown error occurred";
    return {
      content: [
        {
          type: "text",
          text: `Error: ${errorMessage}`,
        },
      ],
      isError: true,
    };
  }
});

// Start the server
async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("Optimism Knowledge MCP Server running on stdio");
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
