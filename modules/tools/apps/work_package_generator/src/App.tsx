/**
 * Main Application Component
 * Provides the entry point for the Work Package Generator.
 */

import { CogniteSdkProvider } from "@cognite/app-sdk/react";
import { Loader } from "@cognite/aura/components";
import { AppConfigGate } from "@/app-config/AppConfigGate";
import { ErrorBoundary } from "@/components/ErrorBoundary";
import { Navigation } from "@/components/layout";
import { CdfRuntimeProvider } from "@/contexts/CdfRuntimeContext";
import { NavigationProvider } from "@/contexts/NavigationContext";
import { WorkPackageGenerator } from "@/pages/WorkPackageGenerator";

function AppContent() {
	return (
		<div className="mx-auto flex h-dvh min-h-dvh w-full max-w-7xl flex-col">
			<a
				href="#main-content"
				className="sr-only focus:not-sr-only focus:absolute focus:left-4 focus:top-4 focus:z-50 focus:rounded-md focus:bg-background focus:px-4 focus:py-2 focus:text-sm focus:text-foreground focus:shadow-md"
			>
				Skip to main content
			</a>
			<Navigation />
			<main
				id="main-content"
				tabIndex={-1}
				className="flex min-h-0 flex-1 flex-col overflow-hidden outline-none"
			>
				<WorkPackageGenerator />
			</main>
		</div>
	);
}

function App() {
	return (
		<CogniteSdkProvider
			loadingFallback={
				<div
					role="status"
					className="flex h-dvh items-center justify-center"
					aria-live="polite"
					aria-label="Loading"
				>
					<Loader size={20} />
				</div>
			}
			errorFallback={
				<div
					role="alert"
					className="flex h-dvh items-center justify-center p-8 text-center text-sm text-muted-foreground"
				>
					Failed to connect to Fusion
				</div>
			}
		>
			<AppConfigGate>
				<CdfRuntimeProvider>
					<ErrorBoundary>
						<NavigationProvider>
							<AppContent />
						</NavigationProvider>
					</ErrorBoundary>
				</CdfRuntimeProvider>
			</AppConfigGate>
		</CogniteSdkProvider>
	);
}

export default App;
