import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { open } from "@tauri-apps/plugin-dialog";
import { getCurrentWebview } from "@tauri-apps/api/webview";
import type { Settings } from "../../bindings/Settings";
import type { TorrentDetail } from "../../../bt-core/bindings/TorrentDetail";
import type { TorrentSummary } from "../../../bt-core/bindings/TorrentSummary";
import type { ListenerStatus } from "../../../bt-core/bindings/ListenerStatus";
import type { DhtStatus } from "../../../bt-core/bindings/DhtStatus";

export const api = {
  list: () => invoke<TorrentSummary[]>("list_torrents"),
  getListenerStatus: () => invoke<ListenerStatus>("get_listener_status"),
  getDhtStatus: () => invoke<DhtStatus>("get_dht_status"),
  add: (path: string) => invoke<string>("add_torrent", { path }),
  addMagnet: (uri: string) => invoke<string>("add_magnet", { uri }),
  pause: (id: string) => invoke<void>("pause_torrent", { id }),
  resume: (id: string) => invoke<void>("resume_torrent", { id }),
  remove: (id: string, deleteFiles: boolean) =>
    invoke<void>("remove_torrent", { id, deleteFiles }),
  select: (id: string | null) => invoke<void>("select_torrent", { id }),
  getSettings: () => invoke<Settings>("get_settings"),
  setSettings: (settings: {
    downloadDir: string;
    listenPort: number;
    uploadLimitBps: number;
    dhtEnabled: boolean;
    dhtPort: number;
  }) => invoke<void>("set_settings", settings),
  openOutputDir: (id: string) => invoke<void>("open_output_dir", { id }),
  onSummaries: (handler: (summaries: TorrentSummary[]) => void) =>
    listen<TorrentSummary[]>("session://summaries", (event) =>
      handler(event.payload),
    ),
  onDetail: (handler: (detail: TorrentDetail | null) => void) =>
    listen<TorrentDetail | null>("torrent://detail", (event) =>
      handler(event.payload),
    ),
  onListener: (handler: (status: ListenerStatus) => void) =>
    listen<ListenerStatus>("session://listener", (event) =>
      handler(event.payload),
    ),
  onDht: (handler: (status: DhtStatus) => void) =>
    listen<DhtStatus>("session://dht", (event) => handler(event.payload)),
  pickTorrent: () =>
    open({
      multiple: false,
      filters: [{ name: "Torrent", extensions: ["torrent"] }],
    }),
  pickDirectory: () => open({ directory: true }),
  onDragEnter: (handler: (paths: string[]) => void) =>
    getCurrentWebview().onDragDropEvent((event) => {
      if (event.payload.type === "enter") {
        handler(event.payload.paths);
      }
    }),
  onDragLeave: (handler: () => void) =>
    getCurrentWebview().onDragDropEvent((event) => {
      if (event.payload.type === "leave") {
        handler();
      }
    }),
  onTorrentDrop: (handler: (paths: string[]) => void) =>
    getCurrentWebview().onDragDropEvent((event) => {
      if (event.payload.type === "drop") {
        handler(event.payload.paths);
      }
    }),
};
