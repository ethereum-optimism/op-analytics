/**
 * TTL-based cache manager for reducing API calls and improving response times.
 */

interface CacheEntry<T> {
  value: T;
  expiresAt: number;
}

export class CacheManager {
  private cache: Map<string, CacheEntry<unknown>> = new Map();
  private defaultTtlMs: number;

  constructor(defaultTtlMinutes: number = 60) {
    this.defaultTtlMs = defaultTtlMinutes * 60 * 1000;
  }

  /**
   * Get a value from cache if it exists and hasn't expired.
   */
  get<T>(key: string): T | undefined {
    const entry = this.cache.get(key) as CacheEntry<T> | undefined;
    if (!entry) return undefined;

    if (Date.now() > entry.expiresAt) {
      this.cache.delete(key);
      return undefined;
    }

    return entry.value;
  }

  /**
   * Set a value in the cache with optional custom TTL.
   */
  set<T>(key: string, value: T, ttlMinutes?: number): void {
    const ttlMs = ttlMinutes ? ttlMinutes * 60 * 1000 : this.defaultTtlMs;
    this.cache.set(key, {
      value,
      expiresAt: Date.now() + ttlMs,
    });
  }

  /**
   * Get a value from cache, or compute and cache it if missing.
   */
  async getOrSet<T>(
    key: string,
    compute: () => Promise<T>,
    ttlMinutes?: number
  ): Promise<T> {
    const cached = this.get<T>(key);
    if (cached !== undefined) {
      return cached;
    }

    const value = await compute();
    this.set(key, value, ttlMinutes);
    return value;
  }

  /**
   * Check if a key exists and is not expired.
   */
  has(key: string): boolean {
    return this.get(key) !== undefined;
  }

  /**
   * Delete a specific key from the cache.
   */
  delete(key: string): boolean {
    return this.cache.delete(key);
  }

  /**
   * Clear all entries from the cache.
   */
  clear(): void {
    this.cache.clear();
  }

  /**
   * Remove all expired entries from the cache.
   */
  prune(): number {
    const now = Date.now();
    let pruned = 0;
    for (const [key, entry] of this.cache.entries()) {
      if (now > entry.expiresAt) {
        this.cache.delete(key);
        pruned++;
      }
    }
    return pruned;
  }

  /**
   * Get the number of entries in the cache (including expired).
   */
  get size(): number {
    return this.cache.size;
  }
}

// Singleton instance for the MCP server
export const cache = new CacheManager(60);
