/** Thrown when the config node is missing a required key, has a typo, or has the wrong type. */
export class AppConfigError extends Error {
	readonly issues: readonly string[];

	constructor(issues: readonly string[]) {
		super(
			`Invalid WPG config:\n${issues.map((issue) => `- ${issue}`).join("\n")}`
		);
		this.name = "AppConfigError";
		this.issues = issues;
	}
}
