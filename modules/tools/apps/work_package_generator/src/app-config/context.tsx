import { createContext, type ReactNode, useContext } from "react";
import { getAppConfig } from "./load";
import type { AppConfig } from "./types";

const AppConfigContext = createContext<AppConfig | null>(null);

export function AppConfigProvider({
	children,
	value,
}: {
	children: ReactNode;
	value?: AppConfig;
}) {
	return (
		<AppConfigContext.Provider value={value ?? getAppConfig()}>
			{children}
		</AppConfigContext.Provider>
	);
}

/** Config for hooks and components. Provider value wins; otherwise the loaded config node. */
export function useAppConfig(): AppConfig {
	return useContext(AppConfigContext) ?? getAppConfig();
}
