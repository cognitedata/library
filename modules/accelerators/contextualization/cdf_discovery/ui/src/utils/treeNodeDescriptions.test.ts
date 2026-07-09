import { describe, expect, it } from "vitest";
import type { MessageKey } from "../i18n";
import { treeNodeDescription } from "./treeNodeDescriptions";

const identity = (key: MessageKey) => key;

describe("treeNodeDescription", () => {
  it("maps indexing subtree nodes to invertedIndex.nav.desc keys", () => {
    expect(
      treeNodeDescription(
        { id: "index", kind: "folder", has_children: true },
        identity
      )
    ).toBe("invertedIndex.nav.desc.indexing");
    expect(
      treeNodeDescription(
        { id: "index:dashboard", kind: "inverted_index_dashboard", has_children: false },
        identity
      )
    ).toBe("invertedIndex.nav.desc.dashboard");
    expect(
      treeNodeDescription(
        { id: "index:ops:build-metadata", kind: "inverted_index_build_metadata", has_children: false },
        identity
      )
    ).toBe("invertedIndex.nav.desc.buildMetadata");
  });

  it("falls back to inverted index kind descriptions", () => {
    expect(
      treeNodeDescription(
        { id: "index:query", kind: "inverted_index_query", has_children: false },
        identity
      )
    ).toBe("invertedIndex.nav.desc.query");
  });
});
