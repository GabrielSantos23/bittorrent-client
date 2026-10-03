import { describe, expect, it } from "vitest";
import { formatBytes, formatEta, formatRate, formatRatio } from "./format";

describe("formatBytes", () => {
  it("formats zero and small values in bytes", () => {
    expect(formatBytes(0)).toBe("0 B");
    expect(formatBytes(5)).toBe("5 B");
    expect(formatBytes(1023)).toBe("1023 B");
  });

  it("formats exact unit boundaries", () => {
    expect(formatBytes(1024)).toBe("1.00 KiB");
    expect(formatBytes(1024 ** 2)).toBe("1.00 MiB");
    expect(formatBytes(1024 ** 3)).toBe("1.00 GiB");
    expect(formatBytes(1024 ** 4)).toBe("1.00 TiB");
  });

  it("rounds to two decimals", () => {
    expect(formatBytes(1536)).toBe("1.50 KiB");
    expect(formatBytes(1048575)).toBe("1024.00 KiB");
    expect(formatBytes(7_927_234_56)).toBe("756.00 MiB");
  });

  it("stops scaling at the largest unit", () => {
    expect(formatBytes(1024 ** 5)).toBe("1024.00 TiB");
  });
});

describe("formatRate", () => {
  it("appends per second", () => {
    expect(formatRate(0)).toBe("0 B/s");
    expect(formatRate(2048)).toBe("2.00 KiB/s");
    expect(formatRate(157_286_400)).toBe("150.00 MiB/s");
  });
});

describe("formatEta", () => {
  it("shows a dash without an estimate", () => {
    expect(formatEta(null)).toBe("—");
  });

  it("formats seconds below a minute", () => {
    expect(formatEta(0)).toBe("0s");
    expect(formatEta(45)).toBe("45s");
    expect(formatEta(59)).toBe("59s");
  });

  it("formats minutes", () => {
    expect(formatEta(60)).toBe("1m 0s");
    expect(formatEta(125)).toBe("2m 5s");
    expect(formatEta(3599)).toBe("59m 59s");
  });

  it("formats hours and drops seconds", () => {
    expect(formatEta(3600)).toBe("1h 0m");
    expect(formatEta(7325)).toBe("2h 2m");
  });
});

describe("formatRatio", () => {
  it("formats the share ratio with two decimals", () => {
    expect(formatRatio(0)).toBe("0.00×");
    expect(formatRatio(0.5)).toBe("0.50×");
    expect(formatRatio(12.345)).toBe("12.35×");
  });
});
