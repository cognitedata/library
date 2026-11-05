import type { FileViewerType } from './types';

// ============================================================================
// MIME Type Constants
// ============================================================================

export const DocumentMimeType = {
  PDF: 'application/pdf',
} as const;

export const ImageMimeType = {
  JPEG: 'image/jpeg',
  PNG: 'image/png',
  SVG: 'image/svg+xml',
  TIFF: 'image/tiff',
  WEBP: 'image/webp',
} as const;

export const TextMimeType = {
  TXT: 'text/plain',
  CSV: 'text/csv',
  JSON: 'application/json',
} as const;

const NativelySupportedMimeTypes = {
  ...ImageMimeType,
  ...DocumentMimeType,
  ...TextMimeType,
} as const;

type NativelySupportedMimeType =
  (typeof NativelySupportedMimeTypes)[keyof typeof NativelySupportedMimeTypes];

// Pre-computed Sets for O(1) lookups (these objects are `as const`, never mutated)
const nativelySupportedSet = new Set<string>(Object.values(NativelySupportedMimeTypes));
const documentMimeSet = new Set<string>(Object.values(DocumentMimeType));
const imageMimeSet = new Set<string>(Object.values(ImageMimeType));
const textMimeSet = new Set<string>(Object.values(TextMimeType));
// ============================================================================
// Document Preview API Support (Office → PDF conversion)
// Source: https://github.com/cognitedata/document-preview
// ============================================================================

const documentPreviewMimeTypes = [
  // Word
  'application/msword',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.template',
  'application/vnd.ms-word.document.macroEnabled.12',
  'application/vnd.ms-word.template.macroEnabled.12',
  'application/rtf',
  'application/vnd.oasis.opendocument.text',
  'application/vnd.oasis.opendocument.text-template',
  // PowerPoint
  'application/vnd.ms-powerpoint',
  'application/vnd.openxmlformats-officedocument.presentationml.presentation',
  'application/vnd.openxmlformats-officedocument.presentationml.template',
  'application/vnd.openxmlformats-officedocument.presentationml.slideshow',
  'application/vnd.ms-powerpoint.presentation.macroEnabled.12',
  'application/vnd.ms-powerpoint.template.macroEnabled.12',
  'application/vnd.ms-powerpoint.slideshow.macroEnabled.12',
  'application/vnd.oasis.opendocument.presentation',
  'application/vnd.oasis.opendocument.presentation-template',
  // Excel
  'application/vnd.ms-excel',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.template',
  'application/vnd.ms-excel.sheet.macroEnabled.12',
  'application/vnd.ms-excel.template.macroEnabled.12',
  'application/vnd.ms-excel.sheet.binary.macroEnabled.12',
  'application/vnd.apple.numbers',
  'text/tab-separated-values',
  'application/vnd.oasis.opendocument.spreadsheet',
];

const documentPreviewExtensions = new Set([
  'doc', 'dot', 'docx', 'dotx', 'docm', 'dotm', 'rtf', 'odt', 'ott',
  'ppt', 'pot', 'pps', 'pptx', 'potx', 'ppsx', 'pptm', 'potm', 'ppsm', 'odp', 'otp',
  'xls', 'xlt', 'xlsx', 'xltx', 'xlsm', 'xltm', 'xlsb', 'numbers', 'tsv', 'ods',
]);

const documentPreviewMimeSet = new Set(documentPreviewMimeTypes);

// ============================================================================
// Helpers
// ============================================================================

function getFileExtension(value: string): string {
  const clean = value.split('#')[0].split('?')[0];
  const filename = clean.split('/').pop() ?? '';
  const lastDot = filename.lastIndexOf('.');
  if (lastDot <= 0 || lastDot === filename.length - 1) return '';
  return filename.slice(lastDot + 1).toLowerCase();
}

function canonicaliseMimeType(mimeType: string): string {
  switch (mimeType) {
    case 'image/jpg':
      return ImageMimeType.JPEG;
    case 'image/tif':
      return ImageMimeType.TIFF;
    case 'image/svg':
      return ImageMimeType.SVG;
    case 'application/txt':
      return TextMimeType.TXT;
    default:
      return mimeType;
  }
}

// ============================================================================
// Public API
// ============================================================================

export function isNativelySupportedMimeType(
  mimeType: string | null | undefined,
): mimeType is NativelySupportedMimeType {
  if (!mimeType) return false;
  return nativelySupportedSet.has(mimeType);
}

export function doesDocumentPreviewApiSupportFile(file: {
  mimeType?: string | null;
  name?: string | null;
}): boolean {
  if (file.mimeType && documentPreviewMimeSet.has(file.mimeType)) return true;
  if (file.name && documentPreviewExtensions.has(getFileExtension(file.name))) return true;
  return false;
}

const extensionToMimeType: Record<string, string> = {
  pdf: DocumentMimeType.PDF,
  jpg: ImageMimeType.JPEG,
  jpeg: ImageMimeType.JPEG,
  png: ImageMimeType.PNG,
  svg: ImageMimeType.SVG,
  tif: ImageMimeType.TIFF,
  tiff: ImageMimeType.TIFF,
  webp: ImageMimeType.WEBP,
  txt: TextMimeType.TXT,
  csv: TextMimeType.CSV,
  json: TextMimeType.JSON,
};

export function inferMimeTypeFromUrl(urlOrName: string): string | undefined {
  return extensionToMimeType[getFileExtension(urlOrName)];
}

export function getComputedMimeType(file: {
  mimeType?: string | null;
  name?: string | null;
}): string | undefined {
  if (file.mimeType) return canonicaliseMimeType(file.mimeType);
  if (file.name) return inferMimeTypeFromUrl(file.name);
  return undefined;
}

export function getViewerType(mimeType: string | undefined): FileViewerType {
  if (!mimeType) return 'unsupported';

  const canonical = canonicaliseMimeType(mimeType);

  if (documentMimeSet.has(canonical)) return 'pdf';
  if (imageMimeSet.has(canonical)) return 'image';
  if (textMimeSet.has(canonical)) return 'text';

  // Office documents get converted to PDF
  if (documentPreviewMimeSet.has(canonical)) return 'pdf';

  return 'unsupported';
}
