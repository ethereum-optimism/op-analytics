/**
 * Sync script to update resource-index.json from upstream sources.
 *
 * Sources:
 * - Specs: ethereum-optimism/specs SUMMARY.md
 * - Docs: ethereum-optimism/docs docs.json
 * - GitHub repos: ethereum-optimism org API
 *
 * Run with: npx tsx src/sync/sync-resources.ts
 */

import { Octokit } from "@octokit/rest";
import { writeFileSync, readFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const INDEX_PATH = join(__dirname, "../../data/resource-index.json");

const octokit = new Octokit({
  auth: process.env.GITHUB_TOKEN,
});

interface SpecSection {
  path: string;
  description: string;
  topics: string[];
}

interface DocSection {
  path: string;
  description: string;
  topics: string[];
}

interface GitHubRepo {
  description: string;
  url: string;
  components?: string[];
}

interface ResourceIndex {
  _meta: {
    last_synced: string;
    sync_version: string;
  };
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

/**
 * Parse SUMMARY.md to extract spec sections
 */
function parseSpecsSummary(content: string): Record<string, SpecSection> {
  const sections: Record<string, SpecSection> = {};
  const lines = content.split("\n");

  // Extract top-level sections (## headings or first-level list items)
  let currentSection = "";
  const topics: string[] = [];

  for (const line of lines) {
    // Match top-level items like "- [OP Stack Protocol](./protocol/overview.md)"
    const topLevelMatch = line.match(/^- \[([^\]]+)\]\(\.?\/([^)]+)\)/);
    if (topLevelMatch) {
      const [, title, path] = topLevelMatch;
      const sectionKey = path.split("/")[0].replace(".md", "");

      // Map common section names
      if (sectionKey === "protocol" || path.includes("protocol/overview")) {
        sections["protocol"] = {
          path: "/protocol/overview.html",
          description: title,
          topics: ["derivation", "deposits", "withdrawals", "bridges", "rollup node"],
        };
      } else if (sectionKey === "fault-proof") {
        sections["fault-proof"] = {
          path: "/fault-proof/index.html",
          description: "Fault proof system",
          topics: ["cannon", "dispute game", "challenger", "bonds"],
        };
      }
    }

    // Match sub-items for topics
    const subItemMatch = line.match(/^\s+- \[([^\]]+)\]/);
    if (subItemMatch && currentSection) {
      topics.push(subItemMatch[1].toLowerCase());
    }
  }

  // Add network upgrade sections by checking for common patterns
  const upgradePatterns = [
    { key: "fjord", path: "/protocol/fjord/overview.html", desc: "Fjord network upgrade" },
    { key: "granite", path: "/protocol/granite/overview.html", desc: "Granite network upgrade" },
    { key: "holocene", path: "/protocol/holocene/overview.html", desc: "Holocene network upgrade" },
    { key: "isthmus", path: "/protocol/isthmus/overview.html", desc: "Isthmus network upgrade" },
  ];

  for (const upgrade of upgradePatterns) {
    if (content.toLowerCase().includes(upgrade.key)) {
      sections[upgrade.key] = {
        path: upgrade.path,
        description: upgrade.desc,
        topics: ["network upgrade", "hardfork"],
      };
    }
  }

  // Add interop/experimental section
  if (content.includes("interop") || content.includes("Interop")) {
    sections["interop"] = {
      path: "/interop/overview.html",
      description: "Interoperability (cross-chain)",
      topics: ["cross-chain", "messaging", "superchain"],
    };
  }

  return sections;
}

/**
 * Parse docs.json to extract navigation sections
 */
function parseDocsNavigation(docsJson: any): Record<string, DocSection> {
  const sections: Record<string, DocSection> = {};

  // Known docs sections with their paths
  const knownSections = [
    { key: "chain-operators", path: "/chain-operators", desc: "Deploy and manage L2 rollups", topics: ["deployment", "configuration", "maintenance", "upgrades"] },
    { key: "node-operators", path: "/node-operators", desc: "Run network nodes", topics: ["op-node", "op-geth", "sync", "configuration"] },
    { key: "app-developers", path: "/app-developers", desc: "Build blockchain apps", topics: ["smart contracts", "bridging", "transactions", "tooling"] },
    { key: "op-stack", path: "/op-stack", desc: "Protocol fundamentals", topics: ["architecture", "components", "rollup protocol", "sequencer"] },
    { key: "superchain", path: "/superchain", desc: "Multi-chain coordination", topics: ["interop", "shared security", "chain registry"] },
    { key: "governance", path: "/governance", desc: "How Optimism evolves", topics: ["token house", "citizens house", "proposals", "voting"] },
    { key: "interop", path: "/interop", desc: "Cross-chain interoperability", topics: ["messaging", "bridging", "superchain"] },
  ];

  for (const section of knownSections) {
    sections[section.key] = {
      path: section.path,
      description: section.desc,
      topics: section.topics,
    };
  }

  return sections;
}

/**
 * Fetch key GitHub repos from ethereum-optimism org
 */
async function fetchGitHubRepos(): Promise<Record<string, GitHubRepo>> {
  const repos: Record<string, GitHubRepo> = {};

  // Key repos to always include
  const keyRepoNames = [
    "optimism",
    "op-geth",
    "supersim",
    "superchain-registry",
    "specs",
    "docs",
    "ecosystem",
    "design-docs",
    "developers",
  ];

  try {
    // Fetch repos from the org
    const { data: orgRepos } = await octokit.repos.listForOrg({
      org: "ethereum-optimism",
      sort: "updated",
      per_page: 50,
    });

    for (const repo of orgRepos) {
      if (keyRepoNames.includes(repo.name)) {
        repos[repo.name] = {
          description: repo.description || `${repo.name} repository`,
          url: repo.html_url,
        };

        // Add components for main monorepo
        if (repo.name === "optimism") {
          repos[repo.name].components = [
            "op-node (consensus client)",
            "op-batcher (batch submitter)",
            "op-proposer (output proposer)",
            "contracts-bedrock (L1/L2 contracts)",
            "op-challenger (fault proof challenger)",
            "op-program (fault proof program)",
          ];
        }
      }
    }
  } catch (error) {
    console.error("Failed to fetch GitHub repos:", error);
    // Return defaults if API fails
    for (const name of keyRepoNames) {
      repos[name] = {
        description: `${name} repository`,
        url: `https://github.com/ethereum-optimism/${name}`,
      };
    }
  }

  return repos;
}

/**
 * Fetch specs SUMMARY.md from GitHub
 */
async function fetchSpecsSummary(): Promise<string> {
  try {
    const { data } = await octokit.repos.getContent({
      owner: "ethereum-optimism",
      repo: "specs",
      path: "specs/SUMMARY.md",
    });

    if ("content" in data) {
      return Buffer.from(data.content, "base64").toString("utf-8");
    }
  } catch (error) {
    console.error("Failed to fetch specs SUMMARY.md:", error);
  }
  return "";
}

/**
 * Fetch docs.json from GitHub
 */
async function fetchDocsConfig(): Promise<any> {
  try {
    const { data } = await octokit.repos.getContent({
      owner: "ethereum-optimism",
      repo: "docs",
      path: "docs.json",
    });

    if ("content" in data) {
      const content = Buffer.from(data.content, "base64").toString("utf-8");
      return JSON.parse(content);
    }
  } catch (error) {
    console.error("Failed to fetch docs.json:", error);
  }
  return {};
}

/**
 * Main sync function
 */
export async function syncResources(): Promise<ResourceIndex> {
  console.log("🔄 Syncing resource index...");

  // Load existing index as base
  let existingIndex: ResourceIndex;
  try {
    existingIndex = JSON.parse(readFileSync(INDEX_PATH, "utf-8"));
  } catch {
    existingIndex = {} as ResourceIndex;
  }

  // Fetch upstream data
  console.log("  📚 Fetching specs SUMMARY.md...");
  const specsSummary = await fetchSpecsSummary();

  console.log("  📖 Fetching docs config...");
  const docsConfig = await fetchDocsConfig();

  console.log("  🐙 Fetching GitHub repos...");
  const githubRepos = await fetchGitHubRepos();

  // Parse and merge
  const specsSections = specsSummary ? parseSpecsSummary(specsSummary) : existingIndex.specs?.sections || {};
  const docsSections = docsConfig ? parseDocsNavigation(docsConfig) : existingIndex.docs?.sections || {};

  // Build updated index
  const updatedIndex: ResourceIndex = {
    _meta: {
      last_synced: new Date().toISOString(),
      sync_version: "1.0.0",
    },
    docs: {
      base_url: "https://docs.optimism.io",
      description: "Official Optimism documentation",
      sections: docsSections,
    },
    specs: {
      base_url: "https://specs.optimism.io",
      description: "OP Stack technical specifications",
      sections: specsSections,
    },
    github: {
      org: "ethereum-optimism",
      org_url: "https://github.com/ethereum-optimism",
      key_repos: githubRepos,
    },
    explorer: existingIndex.explorer || {
      mainnet: {
        url: "https://optimistic.etherscan.io",
        description: "OP Mainnet block explorer",
      },
      sepolia: {
        url: "https://sepolia-optimism.etherscan.io",
        description: "OP Sepolia testnet explorer",
      },
    },
    community: existingIndex.community || {
      forum: {
        url: "https://gov.optimism.io",
        description: "Governance forum",
      },
      discord: {
        url: "https://discord.gg/optimism",
        description: "Community Discord",
      },
    },
  };

  return updatedIndex;
}

/**
 * Write the updated index to disk
 */
export async function syncAndSave(): Promise<void> {
  const updatedIndex = await syncResources();

  writeFileSync(INDEX_PATH, JSON.stringify(updatedIndex, null, 2) + "\n");
  console.log(`✅ Resource index updated at ${INDEX_PATH}`);
  console.log(`   Last synced: ${updatedIndex._meta.last_synced}`);
}

// Run if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  syncAndSave().catch(console.error);
}
