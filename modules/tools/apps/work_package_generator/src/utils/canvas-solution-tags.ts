/**
 * Industrial Canvas solution tags (Fusion canvas picker labels).
 */

import type { CogniteClient, ViewReference } from "@cognite/sdk";
import {
	type AppConfig,
	dataModelView,
	resolveAppConfig,
	viewIdToSource,
} from "../app-config";
import { cdfTaskRunner } from "./semaphore";

export type SolutionTagInstanceRef = {
	space: string;
	externalId: string;
};

export function workPackageSolutionTagInstanceRef(
	config?: AppConfig
): SolutionTagInstanceRef {
	const tag = resolveAppConfig(config).solutionTag;
	return {
		externalId: tag.externalId,
		space: tag.instanceSpace,
	};
}

export function solutionTagView(config?: AppConfig): ViewReference {
	return dataModelView("solutionTag", config);
}

function isForbidden(error: unknown): boolean {
	if (!error || typeof error !== "object") return false;
	return (error as { status?: number }).status === 403;
}

function isMissingNode(error: unknown): boolean {
	if (!error || typeof error !== "object") return false;
	const candidate = error as { status?: number; missing?: unknown };
	return candidate.status === 400 && Array.isArray(candidate.missing);
}

export async function ensureWorkPackageSolutionTag(
	client: CogniteClient,
	config?: AppConfig
): Promise<void> {
	const tag = resolveAppConfig(config).solutionTag;
	const view = solutionTagView(config);
	try {
		const existing = await cdfTaskRunner.schedule(() =>
			client.instances.retrieve({
				items: [
					{
						instanceType: "node",
						space: tag.instanceSpace,
						externalId: tag.externalId,
					},
				],
				sources: [viewIdToSource(view)],
			} as Parameters<CogniteClient["instances"]["retrieve"]>[0])
		);
		if ((existing.items?.length ?? 0) > 0) return;
	} catch (error) {
		if (isForbidden(error)) return;
		if (!isMissingNode(error)) throw error;
	}

	await cdfTaskRunner.schedule(() =>
		client.instances.upsert({
			items: [
				{
					instanceType: "node",
					space: tag.instanceSpace,
					externalId: tag.externalId,
					sources: [
						{
							...viewIdToSource(view),
							properties: { name: tag.name },
						},
					],
				},
			],
		})
	);
}

export function readSolutionTagsFromRootProperties(
	properties: Record<string, unknown>
): SolutionTagInstanceRef[] | undefined {
	const raw = properties.solutionTags;
	if (!Array.isArray(raw)) return undefined;

	const tags: SolutionTagInstanceRef[] = [];
	for (const item of raw) {
		if (!item || typeof item !== "object") continue;
		const record = item as Record<string, unknown>;
		const space = typeof record.space === "string" ? record.space : "";
		const externalId =
			(typeof record.externalId === "string" ? record.externalId : null) ??
			(typeof record.external_id === "string" ? record.external_id : null);
		if (space && externalId) {
			tags.push({ space, externalId });
		}
	}
	return tags.length > 0 ? tags : undefined;
}

export function withWorkPackageSolutionTag(
	existing?: SolutionTagInstanceRef[],
	config?: AppConfig
): SolutionTagInstanceRef[] {
	const workPackage = workPackageSolutionTagInstanceRef(config);
	if (!existing?.length) return [workPackage];
	const hasWorkPackage = existing.some(
		(tag) =>
			tag.space === workPackage.space &&
			tag.externalId === workPackage.externalId
	);
	return hasWorkPackage ? existing : [...existing, workPackage];
}
