/** @vitest-environment node */
import { describe, expect, it } from "vitest";
import {
  detectFileContentFormat,
  fileContentRefFromRow,
  isQueryableFileRow,
} from "./queryableFileFromRow";

describe("queryableFileFromRow", () => {
  it("detects parquet by extension and mime", () => {
    expect(detectFileContentFormat({ name: "data.parquet" })).toBe("parquet");
    expect(detectFileContentFormat({ mimeType: "application/vnd.apache.parquet" })).toBe("parquet");
  });

  it("detects csv and json", () => {
    expect(detectFileContentFormat({ name: "export.csv" })).toBe("csv");
    expect(detectFileContentFormat({ name: "rows.ndjson" })).toBe("json");
  });

  it("extracts file refs from classic and DM-shaped rows", () => {
    const row = {
      id: 123,
      externalId: "my-file",
      name: "metrics.parquet",
      mimeType: "application/x-parquet",
      isUploaded: true,
    };
    expect(isQueryableFileRow(row)).toBe(true);
    expect(fileContentRefFromRow(row)).toEqual({
      file_id: 123,
      external_id: "my-file",
      name: "metrics.parquet",
      format: "parquet",
    });
  });

  it("rejects non-uploaded files", () => {
    expect(
      isQueryableFileRow({
        id: 1,
        name: "x.parquet",
        isUploaded: false,
      })
    ).toBe(false);
  });
});
