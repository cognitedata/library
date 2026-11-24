import { describe, expect, it } from "vitest";

import {
	doesDocumentPreviewApiSupportFile,
	getCanonicalMimeType,
	getComputedMimeTypeFromFileInfo,
	getFileExtension,
	getNodeTypeFromFileInfo,
	getSupportedMimeTypeFromUrl,
	ImageMimeType,
	isNativelySupportedMimeType,
	mapMimeTypeToNodeType,
	TextMimeType,
} from "./mimeTypes";

describe("getFileExtension", () => {
	it("reads the extension and ignores the query string", () => {
		expect(
			getFileExtension("https://files.example.test/a/Photo.PNG?x=1#y")
		).toBe("png");
	});

	it("returns an empty string when there is no extension", () => {
		expect(getFileExtension("readme")).toBe("");
		expect(getFileExtension(".hidden")).toBe("");
		expect(getFileExtension("file.")).toBe("");
	});
});

describe("isNativelySupportedMimeType", () => {
	it("accepts known types and rejects the rest", () => {
		expect(isNativelySupportedMimeType(ImageMimeType.PNG)).toBe(true);
		expect(isNativelySupportedMimeType("application/zip")).toBe(false);
		expect(isNativelySupportedMimeType(null)).toBe(false);
		expect(isNativelySupportedMimeType(undefined)).toBe(false);
	});
});

describe("doesDocumentPreviewApiSupportFile", () => {
	it("matches office mime types and extensions", () => {
		expect(
			doesDocumentPreviewApiSupportFile({
				mimeType:
					"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
			})
		).toBe(true);
		expect(doesDocumentPreviewApiSupportFile({ name: "deck.PPTX" })).toBe(true);
		expect(doesDocumentPreviewApiSupportFile({ name: "sheet.ods" })).toBe(true);
		expect(doesDocumentPreviewApiSupportFile({ mimeType: "image/png" })).toBe(
			false
		);
		expect(doesDocumentPreviewApiSupportFile({})).toBe(false);
	});
});

describe("getCanonicalMimeType", () => {
	it("normalizes aliases", () => {
		expect(getCanonicalMimeType("image/jpg")).toBe(ImageMimeType.JPEG);
		expect(getCanonicalMimeType("image/tif")).toBe(ImageMimeType.TIFF);
		expect(getCanonicalMimeType("image/svg")).toBe(ImageMimeType.SVG);
		expect(getCanonicalMimeType("application/txt")).toBe(TextMimeType.TXT);
		expect(getCanonicalMimeType("application/zip")).toBe("application/zip");
	});
});

describe("getSupportedMimeTypeFromUrl", () => {
	it.each([
		["a.pdf", "application/pdf"],
		["a.jpeg", "image/jpeg"],
		["a.jpg", "image/jpeg"],
		["a.png", "image/png"],
		["a.svg", "image/svg+xml"],
		["a.tiff", "image/tiff"],
		["a.tif", "image/tiff"],
		["a.webp", "image/webp"],
		["a.txt", "text/plain"],
		["a.csv", "text/csv"],
		["a.json", "application/json"],
		["a.mp4", "video/mp4"],
		["a.webm", "video/webm"],
	])("maps %s", (name, mime) => {
		expect(getSupportedMimeTypeFromUrl(name)).toBe(mime);
	});

	it("returns undefined for an unknown extension", () => {
		expect(getSupportedMimeTypeFromUrl("a.xyz")).toBeUndefined();
	});
});

describe("mime type to node type", () => {
	it("prefers an explicit mime type, then the file name", () => {
		expect(
			getComputedMimeTypeFromFileInfo({ mimeType: "image/jpg", name: "a.png" })
		).toBe(ImageMimeType.JPEG);
		expect(getComputedMimeTypeFromFileInfo({ name: "notes.txt" })).toBe(
			TextMimeType.TXT
		);
		expect(getComputedMimeTypeFromFileInfo({})).toBeUndefined();
	});

	it("maps each family and rejects unknown values", () => {
		expect(mapMimeTypeToNodeType("application/pdf")).toBe("pdf");
		expect(mapMimeTypeToNodeType("image/png")).toBe("image");
		expect(mapMimeTypeToNodeType("text/csv")).toBe("text");
		expect(mapMimeTypeToNodeType("video/mp4")).toBe("video");
		expect(mapMimeTypeToNodeType("application/zip")).toBeUndefined();
		expect(mapMimeTypeToNodeType(null)).toBeUndefined();
		expect(getNodeTypeFromFileInfo({ name: "clip.webm" })).toBe("video");
		expect(getNodeTypeFromFileInfo({ name: "mystery.bin" })).toBeUndefined();
	});
});
