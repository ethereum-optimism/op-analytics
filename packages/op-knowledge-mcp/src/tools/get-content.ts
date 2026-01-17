/**
 * Phase 2 Tools: Fetch actual content with size limits.
 * - get_optimism_doc: Fetch specific documentation page
 * - get_optimism_spec: Fetch specific spec section
 * - get_github_file: Fetch specific file from GitHub
 */

import { fetchPage, truncateContent } from "../utils/web-fetcher.js";
import { getDocsUrl, getSpecsUrl, loadResourceIndex } from "../resources/knowledge-base.js";
import { getFileContent, getReadme } from "../utils/github-api.js";

const MAX_RESPONSE_CHARS = 2000;
const DEFAULT_MAX_CHARS = 1500;

// ============== get_optimism_doc ==============

export interface GetDocInput {
  path: string;
  max_chars?: number;
}

export interface GetDocOutput {
  title: string;
  content: string;
  url: string;
  truncated: boolean;
}

export async function getOptimismDoc(input: GetDocInput): Promise<GetDocOutput> {
  const maxChars = Math.min(input.max_chars ?? DEFAULT_MAX_CHARS, MAX_RESPONSE_CHARS);
  const url = getDocsUrl(input.path);

  try {
    const page = await fetchPage(url);
    const content = truncateContent(page.content, maxChars);

    return {
      title: page.title,
      content,
      url,
      truncated: page.content.length > maxChars,
    };
  } catch (error) {
    return {
      title: "Error",
      content: `Failed to fetch documentation: ${error instanceof Error ? error.message : "Unknown error"}`,
      url,
      truncated: false,
    };
  }
}

export const getDocSchema = {
  name: "get_optimism_doc",
  description:
    "Fetch content from a specific Optimism documentation page. Use after search_optimism_docs.",
  inputSchema: {
    type: "object" as const,
    properties: {
      path: {
        type: "string",
        description:
          "Doc path or section name (e.g., 'chain-operators', '/app-developers/bridging')",
      },
      max_chars: {
        type: "number",
        description: "Max characters to return (default: 1500, max: 2000)",
      },
    },
    required: ["path"],
  },
};

// ============== get_optimism_spec ==============

export interface GetSpecInput {
  id: string;
  max_chars?: number;
}

export interface GetSpecOutput {
  title: string;
  content: string;
  url: string;
  truncated: boolean;
}

export async function getOptimismSpec(input: GetSpecInput): Promise<GetSpecOutput> {
  const maxChars = Math.min(input.max_chars ?? DEFAULT_MAX_CHARS, MAX_RESPONSE_CHARS);
  const url = getSpecsUrl(input.id);

  try {
    const page = await fetchPage(url);
    const content = truncateContent(page.content, maxChars);

    return {
      title: page.title,
      content,
      url,
      truncated: page.content.length > maxChars,
    };
  } catch (error) {
    return {
      title: "Error",
      content: `Failed to fetch specification: ${error instanceof Error ? error.message : "Unknown error"}`,
      url,
      truncated: false,
    };
  }
}

export const getSpecSchema = {
  name: "get_optimism_spec",
  description:
    "Fetch content from a specific OP Stack specification section. Use after search_optimism_specs.",
  inputSchema: {
    type: "object" as const,
    properties: {
      id: {
        type: "string",
        description: "Spec section ID (e.g., 'protocol', 'fjord', 'experimental')",
      },
      max_chars: {
        type: "number",
        description: "Max characters to return (default: 1500, max: 2000)",
      },
    },
    required: ["id"],
  },
};

// ============== get_github_file ==============

export interface GetFileInput {
  repo: string;
  path: string;
  max_chars?: number;
  line_start?: number;
  line_end?: number;
}

export interface GetFileOutput {
  content: string;
  url: string;
  lines_shown: string;
  total_lines: number;
  truncated: boolean;
}

export async function getGitHubFile(input: GetFileInput): Promise<GetFileOutput> {
  const maxChars = Math.min(input.max_chars ?? DEFAULT_MAX_CHARS, MAX_RESPONSE_CHARS);

  try {
    // Special case: if path is empty or "README", get the README
    if (!input.path || input.path.toLowerCase() === "readme" || input.path.toLowerCase() === "readme.md") {
      const readme = await getReadme(input.repo);
      if (!readme) {
        return {
          content: "README not found",
          url: `https://github.com/ethereum-optimism/${input.repo}`,
          lines_shown: "0",
          total_lines: 0,
          truncated: false,
        };
      }

      const lines = readme.content.split("\n");
      let content = readme.content;
      let linesShown = `1-${lines.length}`;

      if (input.line_start || input.line_end) {
        const start = (input.line_start ?? 1) - 1;
        const end = input.line_end ?? lines.length;
        content = lines.slice(start, end).join("\n");
        linesShown = `${start + 1}-${Math.min(end, lines.length)}`;
      }

      const truncated = content.length > maxChars;
      content = truncateContent(content, maxChars);

      return {
        content,
        url: readme.url,
        lines_shown: linesShown,
        total_lines: lines.length,
        truncated,
      };
    }

    const file = await getFileContent(input.repo, input.path);
    const lines = file.content.split("\n");
    let content = file.content;
    let linesShown = `1-${lines.length}`;

    // Apply line range if specified
    if (input.line_start || input.line_end) {
      const start = (input.line_start ?? 1) - 1;
      const end = input.line_end ?? lines.length;
      content = lines.slice(start, end).join("\n");
      linesShown = `${start + 1}-${Math.min(end, lines.length)}`;
    }

    const truncated = content.length > maxChars;
    content = truncateContent(content, maxChars);

    return {
      content,
      url: file.url,
      lines_shown: linesShown,
      total_lines: lines.length,
      truncated,
    };
  } catch (error) {
    return {
      content: `Failed to fetch file: ${error instanceof Error ? error.message : "Unknown error"}`,
      url: `https://github.com/ethereum-optimism/${input.repo}/blob/main/${input.path}`,
      lines_shown: "0",
      total_lines: 0,
      truncated: false,
    };
  }
}

export const getFileSchema = {
  name: "get_github_file",
  description:
    "Fetch content of a specific file from ethereum-optimism GitHub. Use after search_optimism_github.",
  inputSchema: {
    type: "object" as const,
    properties: {
      repo: {
        type: "string",
        description: "Repository name (e.g., 'optimism', 'op-geth')",
      },
      path: {
        type: "string",
        description: "File path (e.g., 'README.md', 'op-node/rollup/derive/pipeline.go')",
      },
      max_chars: {
        type: "number",
        description: "Max characters to return (default: 1500, max: 2000)",
      },
      line_start: {
        type: "number",
        description: "Starting line number (1-indexed)",
      },
      line_end: {
        type: "number",
        description: "Ending line number (inclusive)",
      },
    },
    required: ["repo", "path"],
  },
};
