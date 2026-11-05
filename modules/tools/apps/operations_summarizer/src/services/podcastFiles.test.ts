import type { CogniteClient } from "@cognite/sdk";
import { afterEach, describe, expect, it, vi } from "vitest";
import { cogniteClientMock } from "@/test-utils/testDoubles";
import {
  derivePodcastIds,
  fetchAudioDownloadUrl,
  fetchPodcastScript,
  fetchSavedPodcast,
  sanitizePodcastBase,
} from "./podcastFiles";

// withCdfConcurrency waits on a shared queue. Tests run the task immediately.
vi.mock("@/utils/cdfRequest", () => ({
  withCdfConcurrency: <T>(fn: () => Promise<T>) => fn(),
}));

afterEach(() => {
  vi.restoreAllMocks();
});

describe(sanitizePodcastBase.name, () => {
  it("keeps alphanumerics, underscore, hyphen, and dot", () => {
    expect(sanitizePodcastBase("ops_summary-2026.01")).toBe(
      "ops_summary-2026.01",
    );
  });

  it("strips other characters", () => {
    expect(sanitizePodcastBase("ops summary/id!")).toBe("opssummaryid");
  });
});

describe(derivePodcastIds.name, () => {
  it("derives script and audio external IDs from source summary id", () => {
    expect(derivePodcastIds("ops_summary_x")).toEqual({
      scriptExternalId: "ops_podcast_ops_summary_x_script",
      audioExternalId: "ops_podcast_ops_summary_x_audio",
    });
  });
});

describe(fetchPodcastScript.name, () => {
  it("returns script text when download succeeds", async () => {
    const getDownloadUrls = vi
      .fn()
      .mockResolvedValue([{ downloadUrl: "https://dl.example/script" }]);
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      text: async () => "Podcast script body",
    });

    const client = filesClient(getDownloadUrls);

    await expect(
      fetchPodcastScript(client, "sum_podcast_script"),
    ).resolves.toBe("Podcast script body");

    expect(getDownloadUrls).toHaveBeenCalledWith([
      {
        instanceId: {
          space: "ops_summaries",
          externalId: "sum_podcast_script",
        },
      },
    ]);
  });

  it("returns null when file is not found", async () => {
    const client = filesClient(
      vi.fn().mockRejectedValue(new Error("not found")),
    );

    await expect(
      fetchPodcastScript(client, "missing_podcast_script"),
    ).resolves.toBeNull();
  });

  it("throws when download HTTP fails", async () => {
    const client = filesClient(
      vi.fn().mockResolvedValue([{ downloadUrl: "https://dl.example/script" }]),
    );
    global.fetch = vi.fn().mockResolvedValue({ ok: false, status: 500 });

    await expect(
      fetchPodcastScript(client, "sum_podcast_script"),
    ).rejects.toThrow("Failed to download podcast script");
  });
});

describe(fetchAudioDownloadUrl.name, () => {
  it("returns the download URL", async () => {
    const client = filesClient(
      vi
        .fn()
        .mockResolvedValue([{ downloadUrl: "https://dl.example/audio.mp3" }]),
    );

    await expect(
      fetchAudioDownloadUrl(client, "sum_podcast_audio"),
    ).resolves.toBe("https://dl.example/audio.mp3");
  });
});

describe(fetchSavedPodcast.name, () => {
  it("returns null when no script exists", async () => {
    const client = filesClient(vi.fn().mockRejectedValue(new Error("missing")));

    await expect(fetchSavedPodcast(client, "sum-1")).resolves.toBeNull();
  });

  it("returns script and audio URL when both exist", async () => {
    const getDownloadUrls = vi
      .fn()
      .mockImplementation((items: { instanceId: { externalId: string } }[]) => {
        const externalId = items[0].instanceId.externalId;
        if (externalId.endsWith("_script")) {
          return Promise.resolve([
            { downloadUrl: "https://dl.example/script" },
          ]);
        }
        if (externalId.endsWith("_audio")) {
          return Promise.resolve([{ downloadUrl: "https://dl.example/audio" }]);
        }
        return Promise.reject(new Error("missing"));
      });
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      text: async () => "Saved script",
    });

    const client = filesClient(getDownloadUrls);

    await expect(fetchSavedPodcast(client, "sum-1")).resolves.toEqual({
      script: "Saved script",
      audioUrl: "https://dl.example/audio",
      scriptExternalId: "ops_podcast_sum-1_script",
      audioExternalId: "ops_podcast_sum-1_audio",
    });
  });
});

function filesClient(getDownloadUrls: unknown): CogniteClient {
  return cogniteClientMock({
    project: "test-project",
    files: { getDownloadUrls },
  });
}
