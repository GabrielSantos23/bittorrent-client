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

export function formatAnnounceTime(unixSeconds: number | null): string {
  if (unixSeconds === null) return "—";
  return new Date(unixSeconds * 1000).toISOString().slice(11, 19);
}

export const stateColor: Record<string, string> = {
  Checking: "text-info",
  Downloading: "text-primary",
  Paused: "text-muted-foreground",
  Completed: "text-primary",
  Stopped: "text-muted-foreground",
  Error: "text-destructive",
  Seeding: "text-info",
};
