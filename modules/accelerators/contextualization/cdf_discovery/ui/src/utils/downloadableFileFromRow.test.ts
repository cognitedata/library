/** @vitest-environment node */
import { describe, expect, it } from "vitest";
import {
  downloadableFileRefFromRow,
  fileSizeFromRow,
  isDownloadableFileRow,
} from "./downloadableFileFromRow";

describe("downloadableFileFromRow", () => {
  it("accepts any uploaded file with id or externalId", () => {
    expect(isDownloadableFileRow({ id: 1, isUploaded: true })).toBe(true);
    expect(isDownloadableFileRow({ externalId: "doc.pdf", uploaded: true })).toBe(true);
    expect(isDownloadableFileRow({ id: 2, name: "report.pdf", mimeType: "application/pdf" })).toBe(true);
  });

  it("rejects non-uploaded files", () => {
    expect(isDownloadableFileRow({ id: 1, isUploaded: false })).toBe(false);
  });

  it("rejects rows without file identity", () => {
    expect(isDownloadableFileRow({ name: "orphan.pdf" })).toBe(false);
  });

  it("extracts download ref and size from row", () => {
    const row = {
      id: 99,
      externalId: "my-pdf",
      name: "drawing.pdf",
      size: 12_345_678,
    };
    expect(downloadableFileRefFromRow(row)).toEqual({
      file_id: 99,
      external_id: "my-pdf",
      name: "drawing.pdf",
    });
    expect(fileSizeFromRow(row)).toBe(12_345_678);
  });

  it("prefers instance space + externalId for cdf_nodes file rows", () => {
    const row = {
      space: "cdf_cdm",
      externalId: "my-drawing",
      name: "drawing.pdf",
      isUploaded: true,
      mimeType: "application/pdf",
    };
    expect(downloadableFileRefFromRow(row)).toEqual({
      instance_space: "cdf_cdm",
      external_id: "my-drawing",
      name: "drawing.pdf",
    });
  });
});
