/**
 * Type declarations for PDF.js worker import
 * Vite uses the ?url suffix to import as a URL string
 */

declare module "pdfjs-dist/build/pdf.worker.min.mjs?url" {
	const workerUrl: string;
	export default workerUrl;
}
