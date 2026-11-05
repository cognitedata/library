import { describe, expect, it } from "vitest";
import { markdownToHtml, stripHtmlTags } from "./markdown-format";

describe("stripHtmlTags", () => {
  it("removes HTML tags", () => {
    expect(stripHtmlTags("<p>hello</p>")).toBe("hello");
    expect(stripHtmlTags("<div><span>nested</span></div>")).toBe("nested");
  });

  it("does not treat comparison operators as HTML tags", () => {
    expect(stripHtmlTags("a < b && c > d")).toBe("a < b && c > d");
    expect(stripHtmlTags("x <= 3 && y >= 2")).toBe("x <= 3 && y >= 2");
  });
  it("decodes common entities", () => {
    expect(stripHtmlTags("&amp;")).toBe("&");
    expect(stripHtmlTags("&lt;")).toBe("<");
  });
  it("returns empty string for empty input", () => {
    expect(stripHtmlTags("")).toBe("");
  });

  it("decodes nbsp, quotes, apostrophe and normalizes whitespace", () => {
    expect(stripHtmlTags("a&nbsp;b")).toBe("a b");
    expect(stripHtmlTags("&quot;x&quot;")).toBe('"x"');
    expect(stripHtmlTags("&#39;y&#39;")).toBe("'y'");
    expect(stripHtmlTags("hello    world")).toBe("hello world");
  });
});

describe("markdownToHtml", () => {
  it("returns empty string for empty input", () => {
    expect(markdownToHtml("")).toBe("");
  });

  it("converts headings and bold without scripts", () => {
    const html = markdownToHtml("## Title\n\n**bold** text");
    expect(html).toContain("Title");
    expect(html).toContain("bold");
    expect(html).not.toContain("<script");
  });

  it("printFriendly omits Tailwind prose classes on headings", () => {
    const html = markdownToHtml("# H1\n\npara", { printFriendly: true });
    expect(html).toContain("<h1>");
    expect(html).not.toContain("text-2xl");
  });

  it("converts markdown pipe tables to HTML with alignment hints", () => {
    const md = ["| A | B |", "| :---: | --: |", "| 1 | 2 |"].join("\n");
    const html = markdownToHtml(md);
    expect(html).toContain("<table");
    expect(html).toContain('align="center"');
    expect(html).toContain('align="right"');
  });

  it("renders raw HTML passthrough and markdown lists together", () => {
    const md = "<table><tr><td>x</td></tr></table>\n\n- item one";
    const html = markdownToHtml(md);
    expect(html).toContain("<table");
    expect(html).toContain("item one");
  });

  it("passes raw HTML through and renders markdown alongside it", () => {
    const md = '<div><div class="inner">nested</div></div>\n\n**bold**';
    const html = markdownToHtml(md);
    expect(html).toContain("nested");
    expect(html).toContain("<strong");
  });

  it("converts horizontal rule line", () => {
    const html = markdownToHtml("before\n\n---\n\nafter");
    expect(html).toContain("<hr");
  });

  it("printFriendly handles fenced code with language and ordered list", () => {
    const md = "```ts\nconst x = 1\n```\n\n1. First\n2. Second";
    const html = markdownToHtml(md, { printFriendly: true });
    expect(html).toContain("<pre");
    expect(html).toContain("<li>");
  });

  it("printFriendly does not wrap block tags in p or insert br inside tables", () => {
    const md = ["# Title", "", "| A | B |", "|:-:|--:|", "| 1 | 2 |"].join(
      "\n",
    );
    const html = markdownToHtml(md, { printFriendly: true });
    expect(html).toContain("<h1>");
    expect(html).toContain("<table");
    expect(html).not.toMatch(/<p[^>]*>\s*<h1\b/i);
    expect(html).not.toMatch(/<p[^>]*>\s*<table\b/i);
    expect(html).not.toMatch(/<table[^>]*>\s*<br/i);
  });

  it("printFriendly preserves fenced code with blank lines inside pre", () => {
    const md = "```\nline1\n\nline2\n```";
    const html = markdownToHtml(md, { printFriendly: true });
    expect(html).toContain("<pre");
    expect(html).not.toContain("</p><p>");
    expect(html).toMatch(/<code>[\s\S]*line1[\s\S]*line2/);
  });
});
