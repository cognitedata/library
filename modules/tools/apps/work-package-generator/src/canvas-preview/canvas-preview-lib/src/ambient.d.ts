// Vite-specific URL imports (e.g. import workerUrl from './worker.mjs?url')
declare module "*?url" {
	const url: string;
	export default url;
}

// CSS imports
declare module "*.css" {
	const styles: Record<string, string>;
	export default styles;
}

// Allow import.meta.env used by upstream canvas-app dependency
interface ImportMetaEnv {
	readonly [key: string]: string | undefined;
}

interface ImportMeta {
	readonly env: ImportMetaEnv;
}
