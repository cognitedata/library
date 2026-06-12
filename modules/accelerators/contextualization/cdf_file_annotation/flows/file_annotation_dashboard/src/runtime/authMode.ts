const configuredRuntimeMode = import.meta.env.VITE_RUNTIME_MODE;

function isHostedFlowsPath() {
	if (typeof window === "undefined") return false;
	const { pathname } = window.location;
	return (
		pathname.includes("/custom-apps/") ||
		pathname.includes("/flows-apps/")
	);
}

function isLocalhostRuntime() {
	if (typeof window === "undefined") return false;
	const { hostname } = window.location;
	return hostname === "localhost" || hostname === "127.0.0.1";
}

const isConfiguredRuntimeMode =
	configuredRuntimeMode === "cdf_local" ||
	configuredRuntimeMode === "cdf_host" ||
	configuredRuntimeMode === "mock";

// Hosted routes must always use host auth, even if build-time env was set to local mode.
export const runtimeMode = isHostedFlowsPath()
	? "cdf_host"
	: isConfiguredRuntimeMode
		? configuredRuntimeMode === "cdf_local" && !isLocalhostRuntime()
			? "cdf_host"
			: configuredRuntimeMode
		: isLocalhostRuntime()
			? "mock"
			: "cdf_host";

export const isLocalMode = runtimeMode !== "cdf_host";
export const isLocalMockMode = runtimeMode === "mock";
export const isLocalCdfMode = runtimeMode === "cdf_local";

export const displayProjectName =
	import.meta.env.VITE_PROJECT_LABEL ||
	import.meta.env.VITE_CDF_PROJECT ||
	"local";
