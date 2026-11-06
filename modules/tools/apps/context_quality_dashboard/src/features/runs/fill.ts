/** Puts values into the {{name}} places of a translated text. */
export function fill(template: string, values: Readonly<Record<string, string>>): string {
  return template.replace(/\{\{(\w+)\}\}/g, (_match: string, name: string) => values[name] ?? '');
}
