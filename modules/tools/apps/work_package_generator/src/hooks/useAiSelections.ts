import { useMutation } from "@tanstack/react-query";
import {
	createContext,
	useContext,
	useEffect,
	useRef,
	useSyncExternalStore,
} from "react";
import {
	aiSelectBestCanvasTemplate,
	aiSelectResource,
	getOrCreateAgent,
} from "../api";
import type { AiSelectionResult } from "../api/ai-agent";
import {
	getAiSelectableTemplates,
	getResourceConfigs,
	isBaseTemplateId,
	useAppConfig,
} from "../app-config";
import { useDefaultWorkPackageStore } from "../contexts/WorkPackageStoreContext";
import type {
	AiSelections,
	ResourceInstance,
	Selection,
	WorkOrder,
} from "../types";
import { selectionSlotKey } from "../utils/canvas-item-helpers";
import { getErrorMessage, toUserFacingErrorMessage } from "../utils/errors";
import { useCdfClientInfo } from "./useClientInfo";

export const AI_UNAVAILABLE_MESSAGE = "AI unavailable: pick manually or retry";
export const AI_RECOMMENDATION_FAILED_MESSAGE = "AI recommendation failed";

type SlotSelectorResult = Selection | null | AiSelectionResult;

export type AiSelectionVariables =
	| undefined
	| ((message: string) => void)
	| { slotKey?: string; onProgress?: (message: string) => void };

type SelectionRunResult = {
	discarded: boolean;
	produced: AiSelections;
	slotErrors: Record<string, string>;
	snapshot: AiSelections;
	runKey: string;
	generation: number;
	slotKey?: string;
};

type AiRunUiSnapshot = {
	recommendationFailed: boolean;
	selectionFailed: boolean;
	slotErrors: Record<string, string>;
	rerunAll: (() => void) | null;
	rerunSlot: ((slotKey: string) => void) | null;
	rerunRecommendation: (() => void) | null;
};

const aiRunUiListeners = new Set<() => void>();
let aiRunUiSnapshot: AiRunUiSnapshot = {
	recommendationFailed: false,
	selectionFailed: false,
	slotErrors: {},
	rerunAll: null,
	rerunSlot: null,
	rerunRecommendation: null,
};
let selectionGeneration = 0;
let recommendationGeneration = 0;

function publishAiRunUi(partial: Partial<AiRunUiSnapshot>) {
	aiRunUiSnapshot = { ...aiRunUiSnapshot, ...partial };
	for (const listener of aiRunUiListeners) listener();
}

export function useAiRunUi(): AiRunUiSnapshot {
	return useSyncExternalStore(
		(listener) => {
			aiRunUiListeners.add(listener);
			return () => aiRunUiListeners.delete(listener);
		},
		() => aiRunUiSnapshot,
		() => aiRunUiSnapshot
	);
}

function candidateSetId(
	candidates: Record<string, ResourceInstance[]>
): string {
	return Object.keys(candidates)
		.sort()
		.map((typeKey) => {
			const ids = (candidates[typeKey] ?? [])
				.map((item) => `${item.space}/${item.externalId}`)
				.sort()
				.join(",");
			return `${typeKey}:${ids}`;
		})
		.join("|");
}

function aiRunKey(state: {
	workOrder: WorkOrder | null;
	templateCanvasId: string | null;
	candidatesByType: Record<string, ResourceInstance[]>;
}): string {
	return [
		state.workOrder?.space ?? "",
		state.workOrder?.externalId ?? "",
		state.templateCanvasId ?? "",
		candidateSetId(state.candidatesByType),
	].join("\u001f");
}

function isAiSelectionResult(
	value: SlotSelectorResult
): value is AiSelectionResult {
	return (
		!!value &&
		typeof value === "object" &&
		"kind" in value &&
		(value.kind === "match" || value.kind === "none" || value.kind === "error")
	);
}

function selectionFromResult(
	value: AiSelectionResult,
	instances: ResourceInstance[]
): Selection | null {
	if (value.kind !== "match") return null;
	const match = instances.find((item) => item.externalId === value.id);
	if (!match) return null;
	return {
		externalId: match.externalId,
		space: match.space,
		name: match.name,
		description: match.description,
	};
}

function normalizeSelectionVariables(input: AiSelectionVariables): {
	slotKey?: string;
	onProgress?: (message: string) => void;
} {
	if (typeof input === "function") return { onProgress: input };
	if (input && typeof input === "object") return input;
	return {};
}

const defaultAiSelectionsDeps = {
	aiSelectBestCanvasTemplate: (
		...args: Parameters<typeof aiSelectBestCanvasTemplate>
	) => aiSelectBestCanvasTemplate(...args),
	aiSelectResource: (
		...args: Parameters<typeof aiSelectResource>
	): Promise<SlotSelectorResult> => aiSelectResource(...args),
	getOrCreateAgent: (...args: Parameters<typeof getOrCreateAgent>) =>
		getOrCreateAgent(...args),
	useWorkPackageStore: useDefaultWorkPackageStore,
	useCdfClientInfo,
};

export type UseAiSelectionsContextType = typeof defaultAiSelectionsDeps;
export const UseAiSelectionsContext = createContext<UseAiSelectionsContextType>(
	defaultAiSelectionsDeps
);

export function useAiCanvasRecommendation() {
	const {
		aiSelectBestCanvasTemplate: selectBestTemplate,
		getOrCreateAgent: getOrCreateAgentFn,
		useWorkPackageStore: useStore,
		useCdfClientInfo: useCdfClient,
	} = useContext(UseAiSelectionsContext);
	const { client, project } = useCdfClient();
	const {
		workOrder,
		templateCanvasId,
		setAiSelectedCanvasId,
		setTemplateCanvasId,
		setLoading,
		setError,
	} = useStore();
	const appConfig = useAppConfig();

	// biome-ignore lint/correctness/useExhaustiveDependencies: cancel when the work order or template changes
	useEffect(() => {
		recommendationGeneration += 1;
		return () => {
			recommendationGeneration += 1;
			setLoading(false);
		};
	}, [workOrder?.externalId, workOrder?.space, templateCanvasId, setLoading]);

	const recommendationAttemptRef = useRef({ generation: 0, runKey: "" });
	const mutation = useMutation({
		mutationFn: async () => {
			const generation = recommendationGeneration;
			const state = useStore.getState();
			const runKey = aiRunKey(state);
			recommendationAttemptRef.current = { generation, runKey };
			const aiTemplates = getAiSelectableTemplates(
				state.candidateTemplates,
				appConfig
			);
			if (!aiTemplates.length)
				return { selectedCanvasId: null, generation, runKey };

			if (!client || !project) throw new Error("CDF client not available");
			if (!state.workOrder || !state.asset)
				throw new Error("Work order and asset required");

			const agentId = await getOrCreateAgentFn(client, project, appConfig);
			if (
				generation !== recommendationGeneration ||
				aiRunKey(useStore.getState()) !== runKey
			) {
				return { selectedCanvasId: null, generation, runKey, discarded: true };
			}

			const selectedCanvasId = await selectBestTemplate(
				client,
				project,
				agentId,
				aiTemplates,
				state.workOrder,
				state.asset,
				state.candidatesByType
			);
			return {
				selectedCanvasId,
				generation,
				runKey,
				discarded:
					generation !== recommendationGeneration ||
					aiRunKey(useStore.getState()) !== runKey,
			};
		},
		onMutate: () => {
			publishAiRunUi({ recommendationFailed: false });
			setLoading(true, "Atlas AI is analyzing templates...");
			setError(null);
		},
		onSuccess: (result) => {
			if (
				result.discarded ||
				result.generation !== recommendationGeneration ||
				aiRunKey(useStore.getState()) !== result.runKey
			) {
				return;
			}
			if (result.selectedCanvasId) {
				setAiSelectedCanvasId(result.selectedCanvasId);
				setTemplateCanvasId(result.selectedCanvasId);
			} else {
				setTemplateCanvasId(appConfig.templates.baseTemplate.id);
			}
			setLoading(false);
		},
		onError: (error) => {
			const attempt = recommendationAttemptRef.current;
			if (
				attempt.generation !== recommendationGeneration ||
				aiRunKey(useStore.getState()) !== attempt.runKey
			) {
				return;
			}
			publishAiRunUi({ recommendationFailed: true });
			setError(
				`AI canvas recommendation failed: ${toUserFacingErrorMessage(getErrorMessage(error))}`
			);
			setLoading(false);
		},
	});

	useEffect(() => {
		publishAiRunUi({
			rerunRecommendation: () => {
				mutation.mutate(undefined);
			},
		});
		return () => publishAiRunUi({ rerunRecommendation: null });
	}, [mutation.mutate]);

	return mutation;
}

export function useAiSelection() {
	const {
		aiSelectResource: selectResource,
		getOrCreateAgent: getOrCreateAgentFn,
		useWorkPackageStore: useStore,
		useCdfClientInfo: useCdfClient,
	} = useContext(UseAiSelectionsContext);
	const { client, project } = useCdfClient();
	const { workOrder, templateCanvasId, setAiSelections, setLoading, setError } =
		useStore();
	const appConfig = useAppConfig();

	// biome-ignore lint/correctness/useExhaustiveDependencies: cancel when the work order or template changes
	useEffect(() => {
		selectionGeneration += 1;
		return () => {
			selectionGeneration += 1;
			setLoading(false);
		};
	}, [workOrder?.externalId, workOrder?.space, templateCanvasId, setLoading]);

	const selectionAttemptRef = useRef({ generation: 0, runKey: "" });
	const mutation = useMutation<SelectionRunResult, Error, AiSelectionVariables>(
		{
			mutationFn: async (variables) => {
				const { slotKey, onProgress } = normalizeSelectionVariables(variables);
				const generation = selectionGeneration;
				const state = useStore.getState();
				const runKey = aiRunKey(state);
				selectionAttemptRef.current = { generation, runKey };
				const snapshot = { ...state.aiSelections };
				const discarded = (): SelectionRunResult => ({
					discarded: true,
					produced: {},
					slotErrors: {},
					snapshot,
					runKey,
					generation,
					slotKey,
				});
				if (
					generation !== selectionGeneration ||
					aiRunKey(useStore.getState()) !== runKey
				) {
					return discarded();
				}

				if (!client || !project) throw new Error("CDF client not available");
				if (!state.workOrder || !state.asset)
					throw new Error("Work order and asset required");
				if (isBaseTemplateId(state.templateCanvasId, appConfig)) {
					return {
						discarded: false,
						produced: {},
						slotErrors: {},
						snapshot,
						runKey,
						generation,
						slotKey,
					};
				}
				const fields = slotKey
					? state.templateFields.filter(
							(field) => selectionSlotKey(field) === slotKey
						)
					: state.templateFields;
				if (!fields.length) throw new Error("No template fields to select");

				const agentId = await getOrCreateAgentFn(client, project, appConfig);
				const selectedResources = new Set<string>();
				if (slotKey) {
					for (const [key, selection] of Object.entries(state.aiSelections)) {
						if (key !== slotKey && selection?.externalId) {
							selectedResources.add(selection.externalId);
						}
					}
				}
				const produced: AiSelections = {};
				const slotErrors: Record<string, string> = {};
				const resourceConfigs = getResourceConfigs(appConfig);
				const context = { workOrder: state.workOrder, asset: state.asset };

				for (let i = 0; i < fields.length; i++) {
					if (
						generation !== selectionGeneration ||
						aiRunKey(useStore.getState()) !== runKey
					) {
						return discarded();
					}
					const field = fields[i];
					const config = resourceConfigs[field.type];
					if (!config) continue;

					const instances = state.candidatesByType[field.type] || [];
					const message = `Selecting ${config.displayName.toLowerCase()} for "${field.label}" (${i + 1}/${fields.length})...`;
					onProgress?.(message);
					setLoading(true, message);

					const key = selectionSlotKey(field);
					try {
						const outcome = await selectResource(
							client,
							project,
							agentId,
							instances,
							field.type,
							field.label,
							context,
							selectedResources
						);
						if (
							generation !== selectionGeneration ||
							aiRunKey(useStore.getState()) !== runKey
						) {
							return discarded();
						}
						if (isAiSelectionResult(outcome)) {
							if (outcome.kind === "error") {
								slotErrors[key] = AI_UNAVAILABLE_MESSAGE;
								produced[key] = null;
								continue;
							}
							const selection = selectionFromResult(outcome, instances);
							produced[key] = selection;
							if (selection) selectedResources.add(selection.externalId);
							continue;
						}
						produced[key] = outcome;
						if (outcome) selectedResources.add(outcome.externalId);
					} catch {
						slotErrors[key] = AI_UNAVAILABLE_MESSAGE;
						produced[key] = null;
					}
				}

				return {
					discarded:
						generation !== selectionGeneration ||
						aiRunKey(useStore.getState()) !== runKey,
					produced,
					slotErrors,
					snapshot,
					runKey,
					generation,
					slotKey,
				};
			},
			onMutate: (variables) => {
				const { slotKey } = normalizeSelectionVariables(variables);
				publishAiRunUi({
					selectionFailed: false,
					slotErrors: slotKey
						? { ...aiRunUiSnapshot.slotErrors, [slotKey]: "" }
						: {},
				});
				if (slotKey) {
					publishAiRunUi({
						slotErrors: Object.fromEntries(
							Object.entries(aiRunUiSnapshot.slotErrors).filter(
								([key]) => key !== slotKey
							)
						),
					});
				}
				setLoading(true, "Running AI selection...");
				setError(null);
			},
			onSuccess: (result) => {
				if (
					result.discarded ||
					result.generation !== selectionGeneration ||
					aiRunKey(useStore.getState()) !== result.runKey
				) {
					return;
				}
				const latest = useStore.getState().aiSelections;
				const next: AiSelections = result.slotKey ? { ...latest } : {};
				for (const [key, value] of Object.entries(result.produced)) {
					if (latest[key] !== result.snapshot[key]) {
						next[key] = latest[key];
						continue;
					}
					next[key] = value;
				}
				for (const key of Object.keys(latest)) {
					if (latest[key] !== result.snapshot[key]) next[key] = latest[key];
				}
				setAiSelections(next);
				publishAiRunUi({
					selectionFailed: false,
					slotErrors: result.slotKey
						? { ...aiRunUiSnapshot.slotErrors, ...result.slotErrors }
						: result.slotErrors,
				});
				setLoading(false);
			},
			onError: (error) => {
				const attempt = selectionAttemptRef.current;
				if (
					attempt.generation !== selectionGeneration ||
					aiRunKey(useStore.getState()) !== attempt.runKey
				) {
					return;
				}
				publishAiRunUi({ selectionFailed: true });
				setError(
					`AI selection failed: ${toUserFacingErrorMessage(getErrorMessage(error))}`
				);
				setLoading(false);
			},
		}
	);

	useEffect(() => {
		publishAiRunUi({
			rerunAll: () => {
				mutation.mutate(undefined);
			},
			rerunSlot: (slotKey: string) => {
				mutation.mutate({ slotKey });
			},
		});
		return () =>
			publishAiRunUi({
				rerunAll: null,
				rerunSlot: null,
			});
	}, [mutation.mutate]);

	return mutation;
}
