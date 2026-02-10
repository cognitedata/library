export const toTimestamp = (value: unknown) => {
  if (value instanceof Date) return value.getTime();
  if (typeof value === "number") return value;
  return undefined;
};

export const toTimestampLoose = (value: unknown) => {
  const direct = toTimestamp(value);
  if (direct != null) return direct;
  if (typeof value === "string") {
    const parsed = Date.parse(value);
    if (!Number.isNaN(parsed)) return parsed;
  }
  return undefined;
};

export const normalizeStatus = (value?: string) => value?.toLowerCase() ?? "";

export const formatDuration = (ms: number | null) => {
  if (ms == null) return "Unknown duration";
  const seconds = Math.max(0, Math.round(ms / 1000));
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  return `${minutes}m ${remainder}s`;
};

export const formatIso = (value: number | Date) => {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toISOString().replace(/\.\d{3}Z$/, "Z");
};

export const formatUtcRangeCompact = (start: number, end: number) => {
  const startDate = new Date(start);
  const endDate = new Date(end);
  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) {
    return `${start} → ${end}`;
  }
  const datePart = startDate.toISOString().slice(0, 10);
  const startHour = String(startDate.getUTCHours()).padStart(2, "0");
  const endHour = String(endDate.getUTCHours()).padStart(2, "0");
  const endMinute = String(endDate.getUTCMinutes()).padStart(2, "0");
  const isPartial = endDate.getUTCMinutes() !== 0 || endDate.getUTCSeconds() !== 0;
  return `${datePart}: ${startHour} → ${endHour}${isPartial ? `:${endMinute}` : ""}`;
};

export const formatZoned = (value: number, timeZone: string) =>
  new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(value));

export const formatZonedRangeCompact = (start: number, end: number, timeZone: string) => {
  const startDate = new Date(start);
  const endDate = new Date(end);
  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) {
    return `${start} → ${end}`;
  }
  const dateFormatter = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  const hourFormatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    hour: "2-digit",
    hour12: false,
  });
  const minuteFormatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    minute: "2-digit",
    hour12: false,
  });
  const datePart = dateFormatter.format(startDate);
  const startHour = hourFormatter.format(startDate);
  const endHour = hourFormatter.format(endDate);
  const endMinuteRaw = minuteFormatter.format(endDate);
  const endMinute = endMinuteRaw.padStart(2, "0");
  const hasPartial = endMinute !== "00";
  return `${datePart}: ${startHour} → ${endHour}${hasPartial ? `:${endMinute}` : ""}`;
};

export const getUserTimeZone = () => {
  return Intl.DateTimeFormat().resolvedOptions().timeZone || "local";
};

export const getTimeZoneLabel = (timeZone: string) => {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    timeZoneName: "short",
  }).formatToParts(now);
  const tzName = parts.find((part) => part.type === "timeZoneName")?.value ?? timeZone;
  return `${timeZone} (${tzName})`;
};

export const formatTimeFields = (input: unknown): unknown => {
  if (Array.isArray(input)) {
    return input.map((entry) => formatTimeFields(entry));
  }
  if (input && typeof input === "object") {
    const entries = Object.entries(input as Record<string, unknown>).map(([key, value]) => {
      if (key.endsWith("Time") && (typeof value === "number" || value instanceof Date)) {
        return [key, formatIso(value)];
      }
      return [key, formatTimeFields(value)];
    });
    return Object.fromEntries(entries);
  }
  return input;
};
