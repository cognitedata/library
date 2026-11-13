/**
 * Plugin registry for node types and tools
 */

import type {
	NodeTypePlugin,
	PluginRegistry,
	ToolId,
	ToolPlugin,
} from "../core/types";

// ============================================================================
// Global Registry
// ============================================================================

const registry: PluginRegistry = {
	nodeTypes: new Map(),
	tools: new Map(),
};

// ============================================================================
// Node Type Registration
// ============================================================================

/**
 * Register a node type plugin
 */
export function registerNodeType<TData = unknown>(
	plugin: NodeTypePlugin<TData>
): void {
	if (registry.nodeTypes.has(plugin.type)) {
		console.warn(
			`Node type "${plugin.type}" is already registered. Overwriting.`
		);
	}
	registry.nodeTypes.set(plugin.type, plugin as NodeTypePlugin);
}

/**
 * Get a registered node type plugin
 */
export function getNodeType(type: string): NodeTypePlugin | undefined {
	return registry.nodeTypes.get(type);
}

// ============================================================================
// Tool Lookup
// ============================================================================

/**
 * Get a registered tool plugin. The read-only preview registers none, so
 * this returns undefined; Canvas already handles that.
 */
export function getTool(id: ToolId): ToolPlugin | undefined {
	return registry.tools.get(id);
}
