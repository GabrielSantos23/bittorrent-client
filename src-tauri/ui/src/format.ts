export function byteParts(bytes: number): { value: number; unit: string } {
  if (bytes < 1024) return { value: bytes, unit: "B" };
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return { value, unit: units[unit] };
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(2)} ${units[unit]}`;
}

export function formatRate(bytesPerSecond: number): string {
  return `${formatBytes(bytesPerSecond)}/s`;
}

export function formatEta(seconds: number | null): string {
  if (seconds === null) return "—";
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}

export function formatRatio(ratio: number): string {
  return `${ratio.toFixed(2)}×`;
}

export function formatAnnounceTime(unixSeconds: number | null, timeZone?: string): string {
  if (unixSeconds === null) return "—";
  return new Intl.DateTimeFormat("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    timeZone,
  }).format(unixSeconds * 1000);
}

export const stateColor: Record<string, string> = {
  Checking: "text-info",
  FetchingMetadata: "text-info",
  Downloading: "text-accent",
  Paused: "text-muted-foreground",
  Completed: "text-accent",
  Stopped: "text-muted-foreground",
  Error: "",
  Seeding: "text-info",
};

export type BadgeVariant =
  | "default"
  | "secondary"
  | "destructive"
  | "info"
  | "success";

export const badgeVariantByState: Record<string, BadgeVariant> = {
  Checking: "info",
  FetchingMetadata: "info",
  Downloading: "success",
  Seeding: "info",
  Completed: "success",
  Paused: "secondary",
  Stopped: "secondary",
  Error: "destructive",
};

// Short display labels for states whose raw enum name is too wide for the
// fixed State column.
export const stateLabel: Record<string, string> = {
  FetchingMetadata: "Fetching",
};
