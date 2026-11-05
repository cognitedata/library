/**
 * Node property extraction utilities.
 * Provides functions for extracting properties from CDF nodes.
 */

import type { InstanceRow, ViewId } from "@/types/operations";

/**
 * Extract properties from a CDF node based on view.
 * Tries multiple patterns to find properties in the node structure.
 *
 * @param node - The CDF node object
 * @param viewId - The view identifier to look for
 * @returns The extracted properties object
 */
export function extractNodeProperties(
  node: { properties?: Record<string, Record<string, unknown>> },
  viewId: ViewId,
): InstanceRow {
  const viewKey = `${viewId.space}/${viewId.externalId}/${viewId.version}`;
  const { properties } = node;

  // Try direct view key match
  if (properties?.[viewKey]) {
    return properties[viewKey] as InstanceRow;
  }

  // Try to find properties in any view (fallback)
  if (properties) {
    for (const key of Object.keys(properties)) {
      if (Object.keys(properties[key]).length > 0) {
        return properties[key] as InstanceRow;
      }
    }
  }

  return {};
}

/**
 * Extract properties from a node with multiple fallback patterns.
 * Used for summary content extraction where the structure may vary.
 *
 * @param node - The node object with properties
 * @param viewId - The view identifier
 * @returns The file properties object or undefined
 */
export function extractFileProperties(
  node: { properties?: Record<string, Record<string, unknown>> },
  viewId: ViewId,
): InstanceRow | undefined {
  if (!node.properties) return undefined;

  // Pattern 1: Flat key "space/externalId/version"
  const flatKey = `${viewId.space}/${viewId.externalId}/${viewId.version}`;
  const direct = node.properties[flatKey];
  if (direct && Object.keys(direct).length > 0) return direct as InstanceRow;

  // Pattern 2: Nested structure properties.space["externalId/version"]
  const spaceProps = node.properties[viewId.space] as
    Record<string, unknown> | undefined;
  if (spaceProps) {
    const nested = spaceProps[`${viewId.externalId}/${viewId.version}`] as
      Record<string, unknown> | undefined;
    if (nested && Object.keys(nested).length > 0) return nested as InstanceRow;
  }

  // Pattern 3: Any non-empty view (reuse extractNodeProperties fallback)
  const fallback = extractNodeProperties(node, viewId);
  return Object.keys(fallback).length > 0 ? fallback : undefined;
}
