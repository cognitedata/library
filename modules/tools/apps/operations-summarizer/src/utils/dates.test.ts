import { describe, expect, it } from "vitest";
import {
  calendarIsoDateSpanDays,
  extractDateRangeFromContent,
  formatDateDisplay,
  formatDateRangeDisplay,
  formatDateShort,
  formatDateTimeShort,
  formatIsoDate,
  formatTimestamp,
  isCalendarIsoDateString,
  parseCalendarIsoToUtcDate,
  toCalendarIsoDate,
} from "./dates";

describe("toCalendarIsoDate", () => {
  it("formats a local calendar day", () => {
    expect(toCalendarIsoDate(new Date(2024, 2, 5))).toBe("2024-03-05");
  });
});

describe("parseCalendarIsoToUtcDate", () => {
  it("parses a real calendar day to UTC midnight", () => {
    expect(parseCalendarIsoToUtcDate("2024-03-05").toISOString()).toBe(
      "2024-03-05T00:00:00.000Z",
    );
  });

  it("rejects strings that are not YYYY-MM-DD", () => {
    expect(Number.isNaN(parseCalendarIsoToUtcDate("2024-3-05").getTime())).toBe(
      true,
    );
    expect(Number.isNaN(parseCalendarIsoToUtcDate("").getTime())).toBe(true);
  });

  it("rejects calendar days that roll over", () => {
    expect(
      Number.isNaN(parseCalendarIsoToUtcDate("2024-02-31").getTime()),
    ).toBe(true);
  });
});

describe("isCalendarIsoDateString", () => {
  it("accepts a real day and rejects an impossible one", () => {
    expect(isCalendarIsoDateString("2024-02-29")).toBe(true);
    expect(isCalendarIsoDateString("2023-02-29")).toBe(false);
  });
});

describe("calendarIsoDateSpanDays", () => {
  it("returns whole days between two dates", () => {
    expect(calendarIsoDateSpanDays("2024-01-01", "2024-01-11")).toBe(10);
  });

  it("returns 0 for an inverted or invalid range", () => {
    expect(calendarIsoDateSpanDays("2024-01-11", "2024-01-01")).toBe(0);
    expect(calendarIsoDateSpanDays("nope", "2024-01-01")).toBe(0);
    expect(calendarIsoDateSpanDays("2024-01-01", "nope")).toBe(0);
  });
});

describe("formatIsoDate", () => {
  it("formats valid ISO date to YYYY-MM-DD", () => {
    expect(formatIsoDate("2024-03-15T12:00:00Z")).toBe("2024-03-15");
  });
  it("returns 'None' for null, undefined, or empty", () => {
    expect(formatIsoDate(null)).toBe("None");
    expect(formatIsoDate(undefined)).toBe("None");
    expect(formatIsoDate("")).toBe("None");
  });
  it("returns 'Invalid Date' for invalid string", () => {
    expect(formatIsoDate("not-a-date")).toBe("Invalid Date");
  });
});

describe("formatDateDisplay", () => {
  it("formats date to locale string", () => {
    const d = new Date("2024-01-15");
    expect(formatDateDisplay(d)).toMatch(/January/);
    expect(formatDateDisplay(d)).toMatch(/2024/);
    expect(formatDateDisplay(d).length).toBeGreaterThan(0);
  });

  it("formats a calendar ISO string and rejects an invalid value", () => {
    expect(formatDateDisplay("2024-01-15")).toBe("January 15, 2024");
    expect(formatDateDisplay("not-a-date")).toBe("Invalid date");
    expect(formatDateDisplay(new Date(Number.NaN))).toBe("Invalid date");
  });
});

describe("formatDateShort", () => {
  it("uses abbreviated month", () => {
    const d = new Date("2024-06-10");
    const s = formatDateShort(d);
    expect(s).toMatch(/Jun/);
    expect(s).toMatch(/2024/);
  });
});

describe("formatDateTimeShort", () => {
  it("includes time fields", () => {
    const d = new Date("2024-06-10T14:30:00");
    const s = formatDateTimeShort(d);
    expect(s).toMatch(/2024/);
    expect(s.length).toBeGreaterThan(10);
  });
});

describe("formatTimestamp", () => {
  it("returns 'Unknown' for invalid date", () => {
    expect(formatTimestamp(new Date("invalid"))).toBe("Unknown");
  });
  it("formats Date for display", () => {
    const d = new Date("2024-06-01T12:00:00Z");
    expect(formatTimestamp(d)).toMatch(/2024/);
  });
  it("formats ISO string for display", () => {
    expect(formatTimestamp("2024-06-01T12:00:00Z")).toMatch(/2024/);
  });
  it("returns Unknown for an invalid ISO string", () => {
    expect(formatTimestamp("not-a-date")).toBe("Unknown");
  });
});

describe("formatDateRangeDisplay", () => {
  it("formats single day", () => {
    const d = new Date("2026-03-17T12:00:00Z");
    expect(formatDateRangeDisplay(d, d)).toMatch(/Mar/);
    expect(formatDateRangeDisplay(d, d)).toMatch(/2026/);
  });

  it("formats range in same month", () => {
    const start = new Date(2026, 2, 1);
    const end = new Date(2026, 2, 17);
    const s = formatDateRangeDisplay(start, end);
    expect(s).toBe("Mar 1–17, 2026");
  });

  it("returns message for invalid range", () => {
    expect(
      formatDateRangeDisplay(new Date("invalid"), new Date("2024-01-01")),
    ).toBe("Invalid date range");
    expect(formatDateRangeDisplay("2024-01-01", "not-a-date")).toBe(
      "Invalid date range",
    );
  });

  it("formats the same year across months and a range that crosses years", () => {
    expect(
      formatDateRangeDisplay(new Date(2026, 2, 1), new Date(2026, 3, 2)),
    ).toBe("Mar 1 – Apr 2, 2026");
    expect(
      formatDateRangeDisplay(new Date(2025, 11, 31), new Date(2026, 0, 2)),
    ).toBe("Dec 31, 2025 – Jan 2, 2026");
  });

  it("formats string inputs", () => {
    expect(formatDateRangeDisplay("2026-03-01", "2026-03-01")).toMatch(/2026/);
  });
});

describe("extractDateRangeFromContent", () => {
  it("extracts ISO date range", () => {
    const range = extractDateRangeFromContent("2024-01-01 to 2024-01-31");
    expect(range).not.toBeNull();
    expect(range!.start.toISOString()).toContain("2024-01-01");
    expect(range!.end.toISOString()).toContain("2024-01-31");
  });
  it("returns null when no pattern matches", () => {
    expect(extractDateRangeFromContent("no dates")).toBeNull();
  });

  it("extracts a long-form range", () => {
    const range = extractDateRangeFromContent(
      "Window January 1, 2024 – February 2, 2024",
    );
    expect(range).not.toBeNull();
    expect(range!.start.getFullYear()).toBe(2024);
    expect(range!.end.getMonth()).toBe(1);
  });

  it("extracts a period range when the ISO pair is not a real day", () => {
    const range = extractDateRangeFromContent(
      "2024-99-99 to 2024-99-99 period: 2024-03-01 - 2024-03-15",
    );
    expect(range).not.toBeNull();
    expect(range!.start.toISOString()).toContain("2024-03-01");
    expect(range!.end.toISOString()).toContain("2024-03-15");
  });

  it("returns null when every matched pair is invalid", () => {
    expect(
      extractDateRangeFromContent("Notamonth 1, 2024 - Notamonth 2, 2024"),
    ).toBeNull();
  });
});
