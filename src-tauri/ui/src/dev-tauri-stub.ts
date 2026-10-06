// Stand-in for the Tauri IPC so the UI can run in a plain browser during
// development. Installed only when the page is NOT running inside Tauri
// (window.__TAURI_INTERNALS__ absent), so the packaged app is unaffected.

type InvokeArgs = Record<string, unknown>;

// Toggle state for the "Stop Seeding When Done" preview toggle, so flipping
// it in the context menu is reflected in the stub's torrent summary.
let stubStopAfterComplete = false;
let stubCallbackId = 0;
let stubSummariesHandler: number | null = null;
let stubRemoteRunning = false;

function stubRemoteStatus(): Record<string, unknown> {
  return {
    running: stubRemoteRunning,
    connected: false,
    url: stubRemoteRunning
      ? "http://192.168.1.10:8420/?token=dev-stub-token"
      : null,
    token: "dev-stub-token",
    port: 8420,
  };
}

function stubSummary(): Record<string, unknown> {
  return { ...STUB_SUMMARY, stop_after_complete: stubStopAfterComplete };
}

// Replays the summaries event like the real backend's publish tick, so UI
// changes (e.g. the stop-seeding toggle) show up without a manual refetch.
function stubEmitSummaries(): void {
  if (stubSummariesHandler === null) return;
  const handler = (window as unknown as Record<string, unknown>)[
    `_${stubSummariesHandler}`
  ];
  if (typeof handler === "function") {
    (handler as (event: unknown) => void)({
      event: "session://summaries",
      id: 0,
      payload: [stubSummary()],
    });
  }
}

// Sample row so the dev preview has a torrent to exercise the table,
// tooltips and the context menu. Real backend data replaces it in the app.
const STUB_SUMMARY = {
  id: "stub0000000000000000000000000000000000000000000000000000000stub",
  name: "Sample Pack [dev stub] with a deliberately long name to force truncation",
  state: "Downloading",
  total_length: 262144,
  verified_bytes: 110592,
  wanted_bytes: 262144,
  progress: 0.42,
  download_rate: 524288,
  session_uploaded: 65536,
  upload_rate: 32768,
  ratio: 0.0625,
  eta_seconds: 2,
  peer_count: 7,
  output_dir: "C:\\downloads",
  error: null,
  error_retryable: false,
  dht_waiting: false,
  stop_after_complete: false,
};

function cannedInvoke(cmd: string, args?: InvokeArgs): Promise<unknown> {
  switch (cmd) {
    case "list_torrents":
      return Promise.resolve([stubSummary()]);
    case "get_listener_status":
      return Promise.resolve({ active: false, port: 0, error: null });
    case "get_dht_status":
      return Promise.resolve({ active: false, node_count: 0, port: 0, error: null });
    case "get_settings":
      return Promise.resolve({
        download_dir: "C:\\downloads",
        listen_port: 6881,
        upload_limit_bps: 0,
        dht_enabled: false,
        dht_port: 6881,
        remote_token: "dev-stub-token",
        remote_port: 8420,
      });
    case "remote_start":
      stubRemoteRunning = true;
      return Promise.resolve(stubRemoteStatus());
    case "remote_stop":
      stubRemoteRunning = false;
      return Promise.resolve(stubRemoteStatus());
    case "remote_status":
      return Promise.resolve(stubRemoteStatus());
    case "remote_refresh_token":
      return Promise.resolve(stubRemoteStatus());
    case "select_torrent":
    case "pause_torrent":
    case "resume_torrent":
    case "remove_torrent":
    case "set_stop_after_complete":
      stubStopAfterComplete = Boolean(args?.stop);
      stubEmitSummaries();
      return Promise.resolve();
    case "set_settings":
    case "set_file_priorities":
    case "force_recheck":
    case "open_output_dir":
    case "set_listen_port":
    case "set_upload_limit":
    case "set_dht":
      return Promise.resolve();
    case "plugin:dialog|open":
      return Promise.resolve("C:\\sample\\sample.torrent");
    case "plugin:window|minimize":
    case "plugin:window|toggle_maximize":
    case "plugin:window|close":
    case "plugin:window|start_dragging":
      // Caption buttons are no-ops in the browser preview.
      return Promise.resolve();
    case "plugin:window|is_maximized":
      return Promise.resolve(false);
    case "add_magnet":
    case "add_torrent":
      // Pretend the torrent was accepted; the browser preview has no backend.
      return Promise.resolve(
        "stub0000000000000000000000000000000000000000000000000000000stub",
      );
    case "inspect_torrent":
      return Promise.resolve({
        name: "Sample Pack",
        total_length: 49152,
        files: [
          { index: 0, path: ["one.txt"], length: 16384 },
          { index: 1, path: ["sub", "two.bin"], length: 16384 },
          { index: 2, path: ["sub", "three.bin"], length: 16400 },
        ],
      });
    default:
      if (cmd === "plugin:event|listen" && args?.event === "session://summaries") {
        stubSummariesHandler = Number(args.handler);
        return Promise.resolve(0);
      }
      if (cmd.startsWith("plugin:event|")) return Promise.resolve(0);
      return Promise.reject(new Error(`dev stub: unhandled command ${cmd}`));
  }
}

export function installTauriDevStub(): void {
  if (typeof window === "undefined") return;
  const w = window as unknown as Record<string, unknown>;
  if (w.__TAURI_INTERNALS__ !== undefined) return; // running inside Tauri
  w.__TAURI_INTERNALS__ = {
    metadata: {
      currentWindow: { label: "main" },
      currentWebview: { windowLabel: "main", label: "main" },
    },
    transformCallback: (callback: unknown) => {
      stubCallbackId += 1;
      (w as Record<string, unknown>)[`_${stubCallbackId}`] = callback;
      return stubCallbackId;
    },
    invoke: (cmd: string, args?: InvokeArgs) => cannedInvoke(cmd, args),
  };
}
