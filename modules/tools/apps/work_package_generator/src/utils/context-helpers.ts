/**
 * Context Helper Utilities
 * Utilities for extracting and formatting context from work orders and assets
 */

import { type AppConfig, getContextConfigs } from "../app-config";
import type { Asset, WorkOrder } from "../types";

/**
 * Convert any value to a string representation
 */
function convertToString(value: unknown): string {
	if (value == null) return "Unknown";
	if (typeof value === "string") return value;
	if (typeof value === "number" || typeof value === "boolean")
		return String(value);

	if (typeof value === "object") {
		const obj = value as Record<string, unknown>;

		if ("name" in obj) return String(obj.name);
		if ("externalId" in obj) return String(obj.externalId);
		if ("value" in obj) return String(obj.value);

		if (Array.isArray(value)) {
			return value.map(convertToString).filter(Boolean).join(", ");
		}

		return Object.entries(obj)
			.filter(([, v]) => v != null)
			.map(([k, v]) => `${k}: ${convertToString(v)}`)
			.join(", ");
	}

	return String(value);
}

/**
 * Extract context from an object using a config
 */
function extractContextFromProps(
	props: Record<string, unknown>,
	configProperties: string[]
): Record<string, string> {
	const context: Record<string, string> = {};
	for (const prop of configProperties) {
		const value = props[prop];
		if (value != null) {
			context[prop] = convertToString(value);
		}
	}
	return context;
}

/**
 * Extract work order context from instance
 */
export function extractWorkOrderContext(
	workOrder: WorkOrder | Record<string, unknown> | undefined,
	config?: AppConfig
): Record<string, string> {
	if (!workOrder) return {};

	const props = workOrder as Record<string, unknown>;
	const contextConfig = getContextConfigs(config).workOrder;
	return extractContextFromProps(props, contextConfig.properties);
}

/**
 * Extract asset context from instance
 */
export function extractAssetContext(
	asset: Asset | Record<string, unknown> | undefined,
	config?: AppConfig
): Record<string, string> {
	if (!asset) return {};

	const props = asset as Record<string, unknown>;
	const context: Record<string, string> = {};

	// Handle both Asset interface and raw properties
	if ("assetName" in props) {
		context.name = String(props.assetName);
	} else if ("name" in props) {
		context.name = String(props.name);
	}

	const contextConfig = getContextConfigs(config).asset;
	const extracted = extractContextFromProps(props, contextConfig.properties);
	return { ...context, ...extracted };
}
