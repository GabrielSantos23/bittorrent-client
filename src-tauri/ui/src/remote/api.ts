import type { FilePriority } from "../../../../bt-core/bindings/FilePriority";
import type { TorrentDetail } from "../../../../bt-core/bindings/TorrentDetail";
import type { TorrentSummary } from "../../../../bt-core/bindings/TorrentSummary";

// Fetch-based twin of the desktop's api.ts: the phone browser has no Tauri
// IPC, so every call goes through the remote server's JSON API, always
// authenticated with the token the QR link was opened with.

export class RemoteLinkError extends Error {
  constructor() {
    super("This link is no longer valid. Scan the QR code again.");
    this.name = "RemoteLinkError";
  }
}

function currentToken(): string {
  return new URLSearchParams(window.location.search).get("token") ?? "";
}

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const separator = path.includes("?") ? "&" : "?";
  const url =
    `/api/${path}${separator}token=${encodeURIComponent(currentToken())}`;
  const response = await fetch(url, init);
  if (response.status === 401) {
    throw new RemoteLinkError();
  }
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(text || `${response.status} ${response.statusText}`);
  }
  return (await response.json()) as T;
}

export const remoteApi = {
  list: () => call<TorrentSummary[]>("torrents"),
  detail: (id: string) =>
    call<TorrentDetail>(`torrents/${encodeURIComponent(id)}`),
  pause: (id: string) =>
    call(`torrents/${encodeURIComponent(id)}/pause`, { method: "POST" }),
  resume: (id: string) =>
    call(`torrents/${encodeURIComponent(id)}/resume`, { method: "POST" }),
  remove: (id: string, deleteFiles: boolean) =>
    call(`torrents/${encodeURIComponent(id)}/remove?delete_files=${deleteFiles}`, {
      method: "POST",
    }),
  setPriorities: (id: string, priorities: Array<[number, FilePriority]>) =>
    call(`torrents/${encodeURIComponent(id)}/priorities`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ priorities }),
    }),
  addMagnet: (uri: string) =>
    call<{ id: string }>("magnet", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ uri }),
    }),
};
