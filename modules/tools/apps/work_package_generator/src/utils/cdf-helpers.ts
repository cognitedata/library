/**
 * CDF Helper Utilities
 * Type-safe utilities for working with CDF instances
 */

import type { NodeDefinition, ViewReference } from "@cognite/sdk";
import { getViewKey } from "../app-config";
import type { ResourceInstance } from "../types";

/**
 * Generate a unique reference key for CDF nodes
 * Combines externalId and space into a consistent format
 */
export function getRefKey(externalId: string, space: string): string {
	return `${externalId}:${space}`;
}

/**
 * Generate a reference key from a CDF node reference object
 */
export function getRefKeyFromNode(ref: {
	externalId: string;
	space: string;
}): string {
	return getRefKey(ref.externalId, ref.space);
}

/**
 * Type guard for CDF node structure
 */
type NodeLike = Pick<NodeDefinition, "externalId" | "space"> & {
	properties?: unknown;
};

/**
 * Type guard for CDF node structure
 */
export function isCdfNode(value: unknown): value is NodeLike {
	return (
		typeof value === "object" &&
		value !== null &&
		"externalId" in value &&
		typeof (value as { externalId: unknown }).externalId === "string" &&
		"space" in value &&
		typeof (value as { space: unknown }).space === "string"
	);
}

/**
 * Safely cast unknown to CDF node
 */
export function toCdfNode(value: unknown): NodeLike | null {
	if (isCdfNode(value)) {
		return value;
	}
	return null;
}

/**
 * Extract properties from a CDF instance node
 * Handles both nested and flat property structures
 */
export function extractInstanceProperties(
	node: NodeLike,
	viewId: ViewReference
): Record<string, unknown> {
	const properties = node.properties;
	if (!properties || typeof properties !== "object") {
		return {};
	}

	const propsObj = properties as Record<string, unknown>;
	const viewKey = getViewKey(viewId);

	// Try primary format: properties[space][externalId/version]
	const spaceProps = propsObj[viewId.space];
	if (spaceProps && typeof spaceProps === "object") {
		const spacePropsObj = spaceProps as Record<string, unknown>;
		const props = spacePropsObj[viewKey];
		if (props && typeof props === "object") {
			return props as Record<string, unknown>;
		}
	}

	// Fallback: properties[externalId/version]
	const props = propsObj[viewKey];
	if (props && typeof props === "object") {
		return props as Record<string, unknown>;
	}

	return {};
}

/**
 * Safely extract string property from props
 */
export function getStringProp(
	props: Record<string, unknown>,
	key: string,
	defaultValue = ""
): string {
	const value = props[key];
	return typeof value === "string" ? value : defaultValue;
}

/**
 * Safely extract optional string property from props
 */
export function getOptionalStringProp(
	props: Record<string, unknown>,
	key: string
): string | undefined {
	const value = props[key];
	return typeof value === "string" ? value : undefined;
}

/**
 * Safely extract number property from props with default
 */
export function getNumberProp(
	props: Record<string, unknown>,
	key: string,
	defaultValue: number
): number {
	const value = props[key];
	return typeof value === "number" && !Number.isNaN(value)
		? value
		: defaultValue;
}

/**
 * Safely extract array property from props
 */
export function getArrayProp<T>(
	props: Record<string, unknown>,
	key: string,
	defaultValue: T[] = []
): T[] {
	const value = props[key];
	return Array.isArray(value) ? (value as T[]) : defaultValue;
}

/**
 * Map CDF node to ResourceInstance
 */
export function mapNodeToResourceInstance(
	node: NodeLike,
	viewId: ViewReference
): ResourceInstance {
	const props = extractInstanceProperties(node, viewId);
	return {
		externalId: node.externalId,
		space: node.space,
		name: getOptionalStringProp(props, "name"),
		description: getOptionalStringProp(props, "description"),
		properties: props,
	};
}
