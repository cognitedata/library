const configuredRuntimeMode = import.meta.env.VITE_RUNTIME_MODE;

function isHostedFlowsPath() {
	if (typeof window === "undefined") return false;
	const { pathname } = window.location;
	return (
		pathname.includes("/custom-apps/development/") ||
		pathname.includes("/flows-apps/development/") ||
		pathname.includes("/streamlit-apps/dune/development/")
	);
}

export const runtimeMode =
	configuredRuntimeMode === "cdf_local" ||
	configuredRuntimeMode === "cdf_host" ||
	configuredRuntimeMode === "mock"
		? configuredRuntimeMode
		: isHostedFlowsPath()
			? "cdf_host"
			: "mock";

export const isLocalMode = runtimeMode !== "cdf_host";
export const isLocalMockMode = runtimeMode === "mock";
export const isLocalCdfMode = runtimeMode === "cdf_local";

export const displayProjectName =
	import.meta.env.VITE_PROJECT_LABEL ||
	import.meta.env.VITE_CDF_PROJECT ||
	"local";
