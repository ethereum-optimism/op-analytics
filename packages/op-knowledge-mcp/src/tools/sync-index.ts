/**
 * Tool: check_index_freshness / sync_resource_index
 * Check if the resource index is up-to-date and optionally sync it.
 */

import { readFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { syncResources, syncAndSave } from "../sync/sync-resources.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const INDEX_PATH = join(__dirname, "../../data/resource-index.json");

// Consider index stale if older than 7 days
const STALE_THRESHOLD_MS = 7 * 24 * 60 * 60 * 1000;

export interface CheckFreshnessOutput {
  is_fresh: boolean;
  last_synced: string | null;
  age_days: number | null;
  message: string;
}

/**
 * Check if the resource index is fresh or stale
 */
export async function checkIndexFreshness(): Promise<CheckFreshnessOutput> {
  try {
    const content = readFileSync(INDEX_PATH, "utf-8");
    const index = JSON.parse(content);

    if (!index._meta?.last_synced) {
      return {
        is_fresh: false,
        last_synced: null,
        age_days: null,
        message: "Index has never been synced (no _meta.last_synced field)",
      };
    }

    const lastSynced = new Date(index._meta.last_synced);
    const now = new Date();
    const ageMs = now.getTime() - lastSynced.getTime();
    const ageDays = Math.floor(ageMs / (24 * 60 * 60 * 1000));

    const isFresh = ageMs < STALE_THRESHOLD_MS;

    return {
      is_fresh: isFresh,
      last_synced: index._meta.last_synced,
      age_days: ageDays,
      message: isFresh
        ? `Index is fresh (synced ${ageDays} days ago)`
        : `Index is stale (synced ${ageDays} days ago, threshold is 7 days)`,
    };
  } catch (error) {
    return {
      is_fresh: false,
      last_synced: null,
      age_days: null,
      message: `Failed to check index: ${error instanceof Error ? error.message : "Unknown error"}`,
    };
  }
}

export interface SyncIndexOutput {
  success: boolean;
  last_synced: string;
  sections_count: {
    docs: number;
    specs: number;
    github_repos: number;
  };
  message: string;
}

/**
 * Sync the resource index from upstream sources
 */
export async function syncResourceIndex(): Promise<SyncIndexOutput> {
  try {
    await syncAndSave();

    // Read the updated index
    const content = readFileSync(INDEX_PATH, "utf-8");
    const index = JSON.parse(content);

    return {
      success: true,
      last_synced: index._meta.last_synced,
      sections_count: {
        docs: Object.keys(index.docs.sections).length,
        specs: Object.keys(index.specs.sections).length,
        github_repos: Object.keys(index.github.key_repos).length,
      },
      message: "Resource index synced successfully",
    };
  } catch (error) {
    return {
      success: false,
      last_synced: "",
      sections_count: { docs: 0, specs: 0, github_repos: 0 },
      message: `Sync failed: ${error instanceof Error ? error.message : "Unknown error"}`,
    };
  }
}

export const checkFreshnessSchema = {
  name: "check_index_freshness",
  description:
    "Check if the Optimism knowledge index is up-to-date. Returns freshness status and when it was last synced.",
  inputSchema: {
    type: "object" as const,
    properties: {},
  },
};

export const syncIndexSchema = {
  name: "sync_resource_index",
  description:
    "Sync the Optimism knowledge index from upstream sources (GitHub repos, docs, specs). Use this if the index is stale.",
  inputSchema: {
    type: "object" as const,
    properties: {},
  },
};
