/**
 * @vitest-environment jsdom
 *
 * DOMPurify 3.4.8+ intentionally does not support happy-dom (XSS risk in that
 * environment). These tests must run under jsdom, which DOMPurify supports.
 * @see https://github.com/cure53/DOMPurify/issues/1457
 */
import { describe, expect, it } from "vitest";
import { sanitizeHtml } from "./sanitize";

describe("sanitizeHtml", () => {
  it("strips script tags", () => {
    const out = sanitizeHtml("<p>hi</p><script>alert(1)</script>");
    expect(out).not.toContain("script");
    expect(out).not.toContain("alert");
  });

  it("strips inline event handlers", () => {
    const out = sanitizeHtml('<img src="x" onerror="alert(1)" />');
    expect(out).not.toContain("onerror");
  });

  it("allows safe links with target and rel", () => {
    const out = sanitizeHtml(
      '<a href="https://example.com" target="_blank" rel="noopener">x</a>',
    );
    expect(out).toContain("https://example.com");
    expect(out).toContain("target");
  });

  it("removes javascript: URLs from anchors", () => {
    const out = sanitizeHtml('<a href="javascript:alert(1)">click</a>');
    expect(out).not.toContain("javascript:");
  });
});
