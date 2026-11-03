/**
 * Read-side helpers for podcast script/audio CogniteFile instances.
 * External IDs mirror functions/Operations_Summarizer_Function_v2/save_results.py.
 */

import type { CogniteClient } from "@cognite/sdk";
import { getPodcastPrefix, getSummarySpace } from "@/config/dataTypes";
import { withCdfConcurrency } from "@/utils/cdfRequest";

export interface PodcastFileIds {
  scriptExternalId: string;
  audioExternalId: string;
}

export interface SavedPodcast {
  script: string;
  audioUrl: string | null;
  scriptExternalId: string;
  audioExternalId: string;
}

/** Mirrors Python `_sanitize` in save_results.py. */
export function sanitizePodcastBase(name: string): string {
  return name.replace(/[^A-Za-z0-9_\-.]/g, "");
}

/**
 * Deterministic external IDs derived from the source summary external ID.
 * Convention: `{podcastPrefix}{sanitized_source}_script` / `_audio`.
 * The prefix keeps podcast files out of `ops_summary_` and `test_report_` lists.
 */
export function derivePodcastIds(
  sourceSummaryId: string,
  prefix: string = getPodcastPrefix(),
): PodcastFileIds {
  const base = sanitizePodcastBase(sourceSummaryId);
  return {
    scriptExternalId: `${prefix}${base}_script`,
    audioExternalId: `${prefix}${base}_audio`,
  };
}

async function getDownloadUrl(
  client: CogniteClient,
  space: string,
  externalId: string,
): Promise<string | null> {
  try {
    const [result] = await withCdfConcurrency(() =>
      client.files.getDownloadUrls([{ instanceId: { space, externalId } }]),
    );
    return result?.downloadUrl ?? null;
  } catch {
    return null;
  }
}

export async function fetchPodcastScript(
  client: CogniteClient,
  scriptExternalId: string,
  space: string = getSummarySpace(),
): Promise<string | null> {
  const downloadUrl = await getDownloadUrl(client, space, scriptExternalId);
  if (!downloadUrl) return null;

  const response = await fetch(downloadUrl);
  if (!response.ok) {
    throw new Error(
      `Failed to download podcast script (HTTP ${response.status})`,
    );
  }
  const text = await response.text();
  return text.trim() ? text : null;
}

export async function fetchAudioDownloadUrl(
  client: CogniteClient,
  audioExternalId: string,
  space: string = getSummarySpace(),
): Promise<string | null> {
  return getDownloadUrl(client, space, audioExternalId);
}

/**
 * Loads an existing podcast for a summary, or returns null when none is saved.
 */
export async function fetchSavedPodcast(
  client: CogniteClient,
  sourceSummaryId: string,
  space: string = getSummarySpace(),
): Promise<SavedPodcast | null> {
  const { scriptExternalId, audioExternalId } =
    derivePodcastIds(sourceSummaryId);

  const [script, audioUrl] = await Promise.all([
    fetchPodcastScript(client, scriptExternalId, space),
    fetchAudioDownloadUrl(client, audioExternalId, space),
  ]);
  if (!script) return null;

  return {
    script,
    audioUrl,
    scriptExternalId,
    audioExternalId,
  };
}
