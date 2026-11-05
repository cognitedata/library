/**
 * HTML sanitization for rendered summary content.
 */

import DOMPurify from "dompurify";

/**
 * Sanitize HTML to prevent XSS. Must be called on any string passed to
 * dangerouslySetInnerHTML. Keeps Tailwind/Prose class attributes and safe
 * link attributes (rel, target) while stripping javascript: URIs and event
 * handlers.
 */
export function sanitizeHtml(html: string): string {
  return DOMPurify.sanitize(html, {
    ADD_ATTR: ["target"],
    ALLOWED_TAGS: [
      "h1",
      "h2",
      "h3",
      "h4",
      "h5",
      "h6",
      "p",
      "br",
      "hr",
      "div",
      "span",
      "ul",
      "ol",
      "li",
      "strong",
      "b",
      "em",
      "i",
      "code",
      "pre",
      "a",
      "table",
      "thead",
      "tbody",
      "tr",
      "th",
      "td",
    ],
    ALLOWED_ATTR: ["class", "href", "rel", "target", "id"],
    FORCE_BODY: true,
  });
}
