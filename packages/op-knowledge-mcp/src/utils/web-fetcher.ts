/**
 * Web fetching utilities with HTML-to-text conversion.
 */

import { cache } from "../cache/cache-manager.js";

export interface FetchedPage {
  title: string;
  content: string;
  url: string;
}

/**
 * Simple HTML to plain text conversion.
 * Strips tags and extracts readable content.
 */
function htmlToText(html: string): string {
  // Remove script and style elements
  let text = html.replace(/<script[^>]*>[\s\S]*?<\/script>/gi, "");
  text = text.replace(/<style[^>]*>[\s\S]*?<\/style>/gi, "");

  // Remove nav, header, footer elements (usually boilerplate)
  text = text.replace(/<nav[^>]*>[\s\S]*?<\/nav>/gi, "");
  text = text.replace(/<header[^>]*>[\s\S]*?<\/header>/gi, "");
  text = text.replace(/<footer[^>]*>[\s\S]*?<\/footer>/gi, "");

  // Convert common block elements to newlines
  text = text.replace(/<\/(p|div|h[1-6]|li|tr|br)>/gi, "\n");
  text = text.replace(/<(br|hr)\s*\/?>/gi, "\n");

  // Convert list items
  text = text.replace(/<li[^>]*>/gi, "- ");

  // Convert links - keep the text, add URL in parentheses for important ones
  text = text.replace(/<a[^>]*href="([^"]*)"[^>]*>([^<]*)<\/a>/gi, "$2");

  // Convert code blocks
  text = text.replace(/<code[^>]*>([\s\S]*?)<\/code>/gi, "`$1`");
  text = text.replace(/<pre[^>]*>([\s\S]*?)<\/pre>/gi, "\n```\n$1\n```\n");

  // Strip remaining HTML tags
  text = text.replace(/<[^>]+>/g, "");

  // Decode common HTML entities
  text = text.replace(/&nbsp;/g, " ");
  text = text.replace(/&amp;/g, "&");
  text = text.replace(/&lt;/g, "<");
  text = text.replace(/&gt;/g, ">");
  text = text.replace(/&quot;/g, '"');
  text = text.replace(/&#39;/g, "'");
  text = text.replace(/&mdash;/g, "—");
  text = text.replace(/&ndash;/g, "–");

  // Clean up whitespace
  text = text.replace(/\n\s*\n\s*\n/g, "\n\n");
  text = text.replace(/[ \t]+/g, " ");
  text = text.trim();

  return text;
}

/**
 * Extract title from HTML.
 */
function extractTitle(html: string): string {
  const titleMatch = html.match(/<title[^>]*>([^<]*)<\/title>/i);
  if (titleMatch) {
    return titleMatch[1].trim();
  }

  // Try h1
  const h1Match = html.match(/<h1[^>]*>([^<]*)<\/h1>/i);
  if (h1Match) {
    return h1Match[1].trim();
  }

  return "Untitled";
}

/**
 * Fetch a web page and convert to plain text.
 */
export async function fetchPage(url: string): Promise<FetchedPage> {
  const cacheKey = `web:${url}`;

  return cache.getOrSet(cacheKey, async () => {
    const response = await fetch(url, {
      headers: {
        "User-Agent": "op-knowledge-mcp/0.1.0",
        Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to fetch ${url}: ${response.status}`);
    }

    const html = await response.text();
    const title = extractTitle(html);
    const content = htmlToText(html);

    return { title, content, url };
  });
}

/**
 * Search within fetched page content.
 * Returns matching sections with context.
 */
export function searchInContent(
  content: string,
  query: string,
  maxResults: number = 5
): Array<{ excerpt: string; score: number }> {
  const queryLower = query.toLowerCase();
  const queryTerms = queryLower.split(/\s+/).filter((t) => t.length > 2);
  const lines = content.split("\n");
  const results: Array<{ excerpt: string; score: number; lineIndex: number }> = [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const lineLower = line.toLowerCase();

    // Calculate relevance score
    let score = 0;

    // Exact phrase match
    if (lineLower.includes(queryLower)) {
      score += 10;
    }

    // Individual term matches
    for (const term of queryTerms) {
      if (lineLower.includes(term)) {
        score += 2;
      }
    }

    // Skip if no matches
    if (score === 0) continue;

    // Get context (line + surrounding lines)
    const contextStart = Math.max(0, i - 1);
    const contextEnd = Math.min(lines.length - 1, i + 1);
    const excerpt = lines
      .slice(contextStart, contextEnd + 1)
      .join("\n")
      .trim();

    results.push({ excerpt, score, lineIndex: i });
  }

  // Sort by score and deduplicate overlapping results
  results.sort((a, b) => b.score - a.score);

  const dedupedResults: Array<{ excerpt: string; score: number }> = [];
  const usedLines = new Set<number>();

  for (const result of results) {
    if (!usedLines.has(result.lineIndex)) {
      dedupedResults.push({ excerpt: result.excerpt, score: result.score });
      // Mark nearby lines as used to avoid overlap
      for (let j = result.lineIndex - 1; j <= result.lineIndex + 1; j++) {
        usedLines.add(j);
      }
    }
    if (dedupedResults.length >= maxResults) break;
  }

  return dedupedResults;
}

/**
 * Truncate content to a maximum character length.
 * Tries to break at a natural point (sentence or paragraph).
 */
export function truncateContent(content: string, maxChars: number): string {
  if (content.length <= maxChars) {
    return content;
  }

  // Try to break at a paragraph
  let truncated = content.slice(0, maxChars);
  const lastParagraph = truncated.lastIndexOf("\n\n");
  if (lastParagraph > maxChars * 0.7) {
    return truncated.slice(0, lastParagraph) + "\n\n...";
  }

  // Try to break at a sentence
  const lastSentence = truncated.lastIndexOf(". ");
  if (lastSentence > maxChars * 0.7) {
    return truncated.slice(0, lastSentence + 1) + "\n\n...";
  }

  // Break at word boundary
  const lastSpace = truncated.lastIndexOf(" ");
  if (lastSpace > maxChars * 0.8) {
    return truncated.slice(0, lastSpace) + "...";
  }

  return truncated + "...";
}
