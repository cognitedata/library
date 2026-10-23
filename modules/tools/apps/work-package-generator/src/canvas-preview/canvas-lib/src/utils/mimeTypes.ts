/**
 * MIME Type Utilities
 *
 * Provides MIME type constants, detection helpers, and node type mapping.
 * These utilities help determine file types from MIME types or filenames,
 * and map them to the appropriate canvas node types.
 */

// ============================================================================
// MIME Type Constants
// ============================================================================

export const DocumentMimeType = {
	PDF: "application/pdf",
} as const;

export const ImageMimeType = {
	JPEG: "image/jpeg",
	PNG: "image/png",
	SVG: "image/svg+xml",
	TIFF: "image/tiff",
	WEBP: "image/webp",
} as const;

export const TextMimeType = {
	TXT: "text/plain",
	CSV: "text/csv",
	JSON: "application/json",
} as const;

export const VideoMimeType = {
	MP4: "video/mp4",
	WEBM: "video/webm",
} as const;

// ============================================================================
// Combined Types
// ============================================================================

type ValueOf<T> = T[keyof T];

export const NativelySupportedMimeTypesObject: typeof ImageMimeType &
	typeof DocumentMimeType &
	typeof TextMimeType &
	typeof VideoMimeType = {
	...ImageMimeType,
	...DocumentMimeType,
	...TextMimeType,
	...VideoMimeType,
} as const;

export type NativelySupportedMimeType = ValueOf<
	typeof NativelySupportedMimeTypesObject
>;

// ============================================================================
// Document Preview API Support
// ============================================================================

// Source: https://github.com/cognitedata/document-preview/blob/06ea038d23d6434469c7dfe4376a80bf27ccb798/src/main/kotlin/com/cognite/documentpreview/MediaType.kt

const wordDocumentMimeTypes = [
	"application/msword",
	"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	"application/vnd.openxmlformats-officedocument.wordprocessingml.template",
	"application/vnd.ms-word.document.macroEnabled.12",
	"application/vnd.ms-word.template.macroEnabled.12",
	"application/rtf",
	"application/vnd.oasis.opendocument.text",
	"application/vnd.oasis.opendocument.text-template",
];

const wordDocumentExtensions = [
	"doc",
	"dot",
	"docx",
	"dotx",
	"docm",
	"dotm",
	"rtf",
	"odt",
	"ott",
];

const slidesMimeTypes = [
	"application/vnd.ms-powerpoint",
	"application/vnd.openxmlformats-officedocument.presentationml.presentation",
	"application/vnd.openxmlformats-officedocument.presentationml.template",
	"application/vnd.openxmlformats-officedocument.presentationml.slideshow",
	"application/vnd.ms-powerpoint.presentation.macroEnabled.12",
	"application/vnd.ms-powerpoint.template.macroEnabled.12",
	"application/vnd.ms-powerpoint.slideshow.macroEnabled.12",
	"application/vnd.oasis.opendocument.presentation",
	"application/vnd.oasis.opendocument.presentation-template",
];

const slidesExtensions = [
	"ppt",
	"pot",
	"pps",
	"pptx",
	"potx",
	"ppsx",
	"pptm",
	"potm",
	"ppsm",
	"odp",
	"otp",
];

const cellsMimeTypes = [
	"application/vnd.ms-excel",
	"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	"application/vnd.openxmlformats-officedocument.spreadsheetml.template",
	"application/vnd.ms-excel.sheet.macroEnabled.12",
	"application/vnd.ms-excel.template.macroEnabled.12",
	"application/vnd.ms-excel.sheet.binary.macroEnabled.12",
	"application/vnd.apple.numbers",
	"text/tab-separated-values",
	"application/vnd.oasis.opendocument.spreadsheet",
];

const cellsExtensions = [
	"xls",
	"xlt",
	"xlsx",
	"xltx",
	"xlsm",
	"xltm",
	"xlsb",
	"numbers",
	"tsv",
	"ods",
];

export const DocumentApiSupportedMimeTypes: string[] = [
	...wordDocumentMimeTypes,
	...slidesMimeTypes,
	...cellsMimeTypes,
];

const documentApiSupportedExtensions = [
	...wordDocumentExtensions,
	...slidesExtensions,
	...cellsExtensions,
];

// ============================================================================
// Type Definitions
// ============================================================================

export interface FileInfoLike {
	/** File ID (numeric, used by CDF APIs) */
	id?: number;
	mimeType?: string | null;
	name?: string | null;
}

export type FileNodeType = "pdf" | "image" | "text" | "video";

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Extract file extension from a URL or filename.
 * Handles query strings and fragments.
 */
export function getFileExtension(value: string): string {
	const withoutFragment = value.split("#")[0];
	const withoutQuery = withoutFragment.split("?")[0];
	const filename = withoutQuery.split("/").pop() ?? "";
	const lastDot = filename.lastIndexOf(".");
	if (lastDot <= 0 || lastDot === filename.length - 1) {
		return "";
	}
	return filename.slice(lastDot + 1).toLowerCase();
}

/**
 * Check if the MIME type is natively supported (can be rendered without conversion).
 */
export function isNativelySupportedMimeType(
	mimeType: string | null | undefined
): mimeType is NativelySupportedMimeType {
	return (
		Object.values(NativelySupportedMimeTypesObject).find(
			(supportedMimeType) => supportedMimeType === mimeType
		) !== undefined
	);
}

/**
 * Check if the CDF Document Preview API can convert this file to PDF.
 * Supports Word, Excel, PowerPoint documents based on MIME type or file extension.
 */
export function doesDocumentPreviewApiSupportFile({
	mimeType,
	name,
}: FileInfoLike): boolean {
	return (
		(mimeType !== undefined &&
			mimeType !== null &&
			DocumentApiSupportedMimeTypes.includes(mimeType)) ||
		(name
			? documentApiSupportedExtensions.includes(getFileExtension(name))
			: false)
	);
}

/**
 * Normalize MIME type variations to canonical form.
 * E.g., 'image/jpg' → 'image/jpeg', 'image/tif' → 'image/tiff'
 */
export function getCanonicalMimeType(
	mimeType: string
): NativelySupportedMimeType | string {
	switch (mimeType) {
		case "image/jpeg":
		case "image/jpg":
			return ImageMimeType.JPEG;
		case "image/tiff":
		case "image/tif":
			return ImageMimeType.TIFF;
		case "image/svg":
		case "image/svg+xml":
			return ImageMimeType.SVG;
		case "application/txt":
		case "text/plain":
			return TextMimeType.TXT;
		default:
			return mimeType;
	}
}

/**
 * Infer MIME type from URL or filename extension.
 * Returns undefined if the extension is not recognized.
 */
export function getSupportedMimeTypeFromUrl(
	urlOrName: string
): NativelySupportedMimeType | undefined {
	const fileExtension = getFileExtension(urlOrName);

	// Document types
	if (fileExtension === "pdf") {
		return DocumentMimeType.PDF;
	}

	// Image types
	if (fileExtension === "jpg" || fileExtension === "jpeg") {
		return ImageMimeType.JPEG;
	}
	if (fileExtension === "png") {
		return ImageMimeType.PNG;
	}
	if (fileExtension === "svg") {
		return ImageMimeType.SVG;
	}
	if (fileExtension === "tif" || fileExtension === "tiff") {
		return ImageMimeType.TIFF;
	}
	if (fileExtension === "webp") {
		return ImageMimeType.WEBP;
	}

	// Text types
	if (fileExtension === "txt") {
		return TextMimeType.TXT;
	}
	if (fileExtension === "csv") {
		return TextMimeType.CSV;
	}
	if (fileExtension === "json") {
		return TextMimeType.JSON;
	}

	// Video types
	if (fileExtension === "mp4") {
		return VideoMimeType.MP4;
	}
	if (fileExtension === "webm") {
		return VideoMimeType.WEBM;
	}

	return undefined;
}

/**
 * Compute MIME type from file info.
 * Prefers explicit mimeType, falls back to filename extension.
 */
export function getComputedMimeTypeFromFileInfo({
	mimeType,
	name,
}: FileInfoLike): string | undefined {
	if (mimeType) {
		return getCanonicalMimeType(mimeType);
	}
	if (name) {
		return getSupportedMimeTypeFromUrl(name);
	}
	return undefined;
}

// ============================================================================
// Node Type Mapping
// ============================================================================

/**
 * Map a MIME type to a canvas node type.
 * Returns undefined for unknown/unsupported MIME types.
 */
export function mapMimeTypeToNodeType(
	mimeType?: string | null
): FileNodeType | undefined {
	if (!mimeType) return undefined;

	const documentMimeTypes = Object.values(DocumentMimeType) as string[];
	const imageMimeTypes = Object.values(ImageMimeType) as string[];
	const textMimeTypes = Object.values(TextMimeType) as string[];
	const videoMimeTypes = Object.values(VideoMimeType) as string[];

	if (documentMimeTypes.includes(mimeType)) {
		return "pdf";
	}
	if (imageMimeTypes.includes(mimeType)) {
		return "image";
	}
	if (textMimeTypes.includes(mimeType)) {
		return "text";
	}
	if (videoMimeTypes.includes(mimeType)) {
		return "video";
	}

	return undefined;
}

/**
 * Get the canvas node type from file info.
 * Computes MIME type (preferring explicit mimeType, falling back to filename),
 * then maps it to a node type.
 *
 * Returns undefined for unknown types - caller decides how to handle.
 *
 * @example
 * const nodeType = getNodeTypeFromFileInfo({ mimeType: 'image/png', name: 'photo.png' });
 * // nodeType === 'image'
 *
 * const nodeType = getNodeTypeFromFileInfo({ name: 'document.pdf' });
 * // nodeType === 'pdf' (inferred from extension)
 *
 * const nodeType = getNodeTypeFromFileInfo({ name: 'unknown.xyz' });
 * // nodeType === undefined (unknown type)
 */
export function getNodeTypeFromFileInfo({
	mimeType,
	name,
}: FileInfoLike): FileNodeType | undefined {
	const computedMimeType = getComputedMimeTypeFromFileInfo({ mimeType, name });
	return mapMimeTypeToNodeType(computedMimeType);
}
