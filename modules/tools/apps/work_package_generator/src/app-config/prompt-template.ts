/**
 * Fill `{{token}}` placeholders in a prompt loaded from the config node.
 * Required tokens are checked when the file is parsed.
 */
export function renderPromptTemplate(
	template: string,
	values: Record<string, string>
): string {
	return template.replace(/\{\{([A-Za-z0-9_]+)\}\}/g, (match, key: string) =>
		Object.hasOwn(values, key) ? values[key] : match
	);
}
