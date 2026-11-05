/**
 * Markdown and lightweight HTML text helpers for Operations Summarizer.
 */

import rehypeRaw from "rehype-raw";
import rehypeSanitize, { defaultSchema } from "rehype-sanitize";
import rehypeStringify from "rehype-stringify";
import remarkGfm from "remark-gfm";
import remarkParse from "remark-parse";
import remarkRehype from "remark-rehype";
import { unified } from "unified";

/**
 * Remove HTML tags from text while preserving markdown formatting.
 * Only strips sequences that look like HTML tags (name starts with a letter)
 * so expressions such as `a < b && c > d` are not corrupted.
 */
export function stripHtmlTags(text: string): string {
  if (!text) return text;

  let cleanText = text.replace(/<\/?[a-zA-Z][\w:-]*[^>]*>/gi, "");

  cleanText = cleanText
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");

  cleanText = cleanText
    .replace(/[ \t]+/g, " ")
    .replace(/\n\s*\n/g, "\n\n")
    .trim();

  return cleanText;
}

// Allow style on table cells so remark-gfm column alignment (text-align) survives sanitization.
const sanitizeSchema = {
  ...defaultSchema,
  attributes: {
    ...defaultSchema.attributes,
    td: [...(defaultSchema.attributes?.td ?? []), "style"],
    th: [...(defaultSchema.attributes?.th ?? []), "style"],
  },
};

const pipeline = unified()
  .use(remarkParse)
  .use(remarkGfm)
  .use(remarkRehype, { allowDangerousHtml: true })
  .use(rehypeRaw)
  .use(rehypeSanitize, sanitizeSchema)
  .use(rehypeStringify)
  .freeze();

/**
 * Options for markdown-to-HTML conversion.
 * - printFriendly: reserved for future PDF-specific styling; currently produces the same output.
 */
export interface MarkdownToHtmlOptions {
  printFriendly?: boolean;
}

/**
 * Convert markdown text to HTML for rendering.
 * Uses remark/rehype for robust GFM parsing (tables, task lists, strikethrough, etc.).
 * Styling is the container's responsibility — wrap output in a prose CSS class.
 */
export function markdownToHtml(
  markdown: string,
  _options?: MarkdownToHtmlOptions,
): string {
  if (!markdown) return "";
  return String(pipeline.processSync(markdown));
}
