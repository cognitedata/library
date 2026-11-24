/** Replaces `{{name}}` in a translated string. A name with no value becomes `fallback`. */
export function interpolate(template: string, vars: Readonly<Record<string, string>>, fallback = ''): string {
  return template.replace(/\{\{(\w+)\}\}/g, (_match: string, name: string) => vars[name] ?? fallback);
}
