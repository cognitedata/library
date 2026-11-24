/** Split an array into consecutive slices of at most `size` elements. */
export function chunk<T>(arr: T[], size: number): T[][] {
	if (size <= 0) {
		throw new Error("Chunk size must be positive");
	}

	const chunks: T[][] = [];
	for (let i = 0; i < arr.length; i += size) {
		chunks.push(arr.slice(i, i + size));
	}
	return chunks;
}
