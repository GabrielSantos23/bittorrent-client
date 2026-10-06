import { useEffect, useState } from "react";
import { ArrowDownIcon, ArrowUpIcon, FolderOpenIcon } from "lucide-react";
import { api } from "./api";
import { formatRate } from "./format";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/animate-ui/components/radix/checkbox";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/animate-ui/components/radix/alert-dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Folder } from "@/components/ui/folder-component";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/animate-ui/components/radix/tooltip";
import { SidebarProvider } from "@/components/animate-ui/components/radix/sidebar";
import AppSidebar from "./components/AppSidebar";
import DetailPanel from "./components/DetailPanel";
import TitleBar, { TitleBarDivider } from "./components/TitleBar";
import {
  AddTorrentModal,
  type AddDecision,
  type PendingTorrent,
} from "./components/AddTorrentModal";
import { RemoteAccessModal } from "./components/RemoteAccessModal";
import TorrentTable from "./TorrentTable";
import type { TorrentSummary } from "../../../bt-core/bindings/TorrentSummary";
import type { TorrentDetail } from "../../../bt-core/bindings/TorrentDetail";
import type { ListenerStatus } from "../../../bt-core/bindings/ListenerStatus";
import type { DhtStatus } from "../../../bt-core/bindings/DhtStatus";
import type { Settings } from "../../bindings/Settings";

// TEMPORARY DIAGNOSTIC STAMP: bump on every UI change so a running window
// can be told apart from a stale one at a glance.
const UI_BUILD_STAMP = "ui-20261005-1225";

function isTorrentPath(path: string): boolean {
  return path.toLowerCase().endsWith(".torrent");
}

interface SettingsForm {
  downloadDir: string;
  listenPort: number;
  uploadLimitBps: number;
  dhtEnabled: boolean;
  dhtPort: number;
}

function toForm(settings: Settings): SettingsForm {
  return {
    downloadDir: settings.download_dir,
    listenPort: settings.listen_port,
    uploadLimitBps: settings.upload_limit_bps,
    dhtEnabled: settings.dht_enabled,
    dhtPort: settings.dht_port,
  };
}

export default function App() {
  const [summaries, setSummaries] = useState<TorrentSummary[]>([]);
  const [detail, setDetail] = useState<TorrentDetail | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [pendingRemove, setPendingRemove] = useState<TorrentSummary | null>(null);
  const [deleteFiles, setDeleteFiles] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [dragActive, setDragActive] = useState(false);
  const [listener, setListener] = useState<ListenerStatus | null>(null);
  const [dht, setDht] = useState<DhtStatus | null>(null);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [settingsForm, setSettingsForm] = useState<SettingsForm | null>(null);
  const [settingsError, setSettingsError] = useState<string | null>(null);
  const [addOpen, setAddOpen] = useState(false);
  const [pending, setPending] = useState<PendingTorrent[]>([]);
  const [stateFilter, setStateFilter] = useState<string | null>(null);
  const [remoteOpen, setRemoteOpen] = useState(false);

  useEffect(() => {
    api.list().then(setSummaries).catch(setNotice);
    api.getListenerStatus().then(setListener).catch((err) => setNotice(String(err)));
    api.getDhtStatus().then(setDht).catch((err) => setNotice(String(err)));
    const unSummaries = api.onSummaries((next) => {
      setSummaries(next);
    });
    const unDetail = api.onDetail(setDetail);
    const unListener = api.onListener(setListener);
    const unDht = api.onDht(setDht);
    const unDragEnter = api.onDragEnter((paths) => {
      setDragActive(paths.some(isTorrentPath));
    });
    const unDragLeave = api.onDragLeave(() => setDragActive(false));
    const unDrop = api.onTorrentDrop((paths) => {
      setDragActive(false);
      setNotice(`add dialog: drop received with ${paths.length} path(s)`);
      openAddModalForFiles(paths.filter(isTorrentPath));
    });

    return () => {
      unSummaries.then((stop) => stop());
      unDetail.then((stop) => stop());
      unListener.then((stop) => stop());
      unDht.then((stop) => stop());
      unDragEnter.then((stop) => stop());
      unDragLeave.then((stop) => stop());
      unDrop.then((stop) => stop());
    };
  }, []);

  // Pasting a magnet URI anywhere outside a text field opens the add dialog
  // with it pre-filled, like qBittorrent's clipboard handling. Registered in
  // its own effect so it stays active even if the Tauri listeners above fail.
  useEffect(() => {
    const onPaste = (event: ClipboardEvent) => {
      const target =
        event.target instanceof HTMLElement ? event.target : null;
      if (target?.closest("input, textarea, [role='dialog']")) return;
      const text = event.clipboardData?.getData("text") ?? "";
      if (!text.trim().startsWith("magnet:?")) return;
      event.preventDefault();
      setPending([{ kind: "magnet", uri: text.trim() }]);
      setAddOpen(true);
    };
    window.addEventListener("paste", onPaste);
    return () => window.removeEventListener("paste", onPaste);
  }, []);

  const totalRate = summaries.reduce((acc, torrent) => acc + torrent.download_rate, 0);
  const totalUpload = summaries.reduce((acc, torrent) => acc + torrent.upload_rate, 0);
  const filteredSummaries =
    stateFilter === null
      ? summaries
      : summaries.filter((torrent) => torrent.state === stateFilter);
  const selected = summaries.find((torrent) => torrent.id === selectedId) ?? null;

  const handleSelect = (id: string) => {
    const next = selectedId === id ? null : id;
    setSelectedId(next);
    void api.select(next);
  };

  const handlePauseResume = (summary: TorrentSummary) => {
    const action = summary.state === "Paused" ? api.resume(summary.id) : api.pause(summary.id);
    action.catch((err) => setNotice(String(err)));
  };

  const handleConfirmRemove = () => {
    if (pendingRemove === null) return;
    api
      .remove(pendingRemove.id, deleteFiles)
      .then(() => {
        if (selectedId === pendingRemove.id) {
          setSelectedId(null);
          void api.select(null);
        }
      })
      .catch((err) => setNotice(String(err)))
      .finally(() => {
        setPendingRemove(null);
        setDeleteFiles(false);
      });
  };

  const openAddModalForFiles = (rawPaths: string[]) => {
    // Every add goes through the modal; dedupe repeated drops of the same
    // file within one batch.
    const paths = [...new Set(rawPaths)];
    if (paths.length === 0) {
      setNotice("add dialog: drop contained no .torrent files");
      return;
    }
    setNotice(`add dialog: inspecting ${paths.length} file(s)...`);
    Promise.all(
      paths.map((path) =>
        api
          .inspectTorrent(path)
          .then(
            (inspection): PendingTorrent => ({
              kind: "file",
              path,
              inspection,
            }),
          )
          .catch(
            (err): PendingTorrent => ({
              kind: "file-unreadable",
              path,
              reason: String(err),
            }),
          ),
      ),
    ).then((pendingTorrents) => {
      if (pendingTorrents.length > 0) {
        setNotice("add dialog: opened");
        setPending(pendingTorrents);
        setAddOpen(true);
      }
    });
  };

  const handleAdd = () => {
    api
      .pickTorrent()
      .then((path) => {
        if (typeof path === "string") {
          openAddModalForFiles([path]);
        }
      })
      .catch((err) => setNotice(String(err)));
  };

  const openMagnet = () => {
    setPending([{ kind: "magnet", uri: "" }]);
    setAddOpen(true);
  };

  // Two-phase magnet add: the torrent is added paused right away and its id
  // is returned so the modal can show the file tree once metadata arrives.
  // Selecting it routes the detail events (with the file list) to the modal.
  const handleAddMagnetUri = (uri: string): Promise<string> =>
    api.addMagnet(uri, true).then((id) => {
      setSelectedId(id);
      void api.select(id);
      return id;
    });

  const handleConfirmAdd = (decisions: AddDecision[]): Promise<void> => {
    // Close the modal up front; the adds dispatch immediately and any
    // failure surfaces in the notice bar instead of the modal.
    setAddOpen(false);
    setPending([]);
    return Promise.all(
      decisions.map((decision) => {
        const run = (): Promise<unknown> => {
          switch (decision.kind) {
            case "file":
              return api.add(decision.path, decision.filePriorities);
            case "file-plain":
              return api.add(decision.path);
            case "magnet":
              return api.addMagnet(
                decision.uri,
                decision.pauseAfterMetadata,
              );
            case "magnet-files":
              return api
                .setFilePriorities(decision.id, decision.filePriorities)
                .then(() => api.resume(decision.id));
            case "magnet-keep-paused":
              return Promise.resolve();
          }
        };
        return run().catch((err) => setNotice(String(err)));
      }),
    ).then(() => undefined);
  };

  const openSettings = () => {
    setSettingsError(null);
    api
      .getSettings()
      .then((settings) => {
        setSettingsForm(toForm(settings));
        setSettingsOpen(true);
      })
      .catch((err) => setNotice(String(err)));
  };

  const handleSaveSettings = () => {
    if (settingsForm === null) return;
    setSettingsError(null);
    api
      .setSettings({
        downloadDir: settingsForm.downloadDir,
        listenPort: settingsForm.listenPort,
        uploadLimitBps: settingsForm.uploadLimitBps,
        dhtEnabled: settingsForm.dhtEnabled,
        dhtPort: settingsForm.dhtPort,
      })
      .then(() => setSettingsOpen(false))
      .catch((err) => setSettingsError(String(err)));
  };

  return (
    <SidebarProvider className="h-full">
      <AppSidebar
        summaries={summaries}
        value={stateFilter}
        onChange={setStateFilter}
        detail={detail}
        onOpenRemote={() => setRemoteOpen(true)}
        onOpenSettings={openSettings}
      />
      <div
        className="flex h-full min-w-0 flex-1 flex-col bg-background text-foreground"
        onDragOver={(event) => event.preventDefault()}
      >
        <TitleBar
          trailing={
            <div className="flex items-center gap-3 pr-1.5">
              <TitleBarDivider />
              <span
                className="flex items-center gap-1.5 text-sm tabular-nums text-muted-foreground"
                data-tauri-drag-region
              >
                <ArrowDownIcon className="size-3.5" />
                {formatRate(totalRate)}
              </span>
              <span
                className="flex items-center gap-1.5 text-sm tabular-nums text-muted-foreground"
                data-tauri-drag-region
              >
                <ArrowUpIcon className="size-3.5" />
                {formatRate(totalUpload)}
              </span>
            </div>
          }
        >
          <Tooltip>
            <TooltipTrigger asChild>
              <button
                type="button"
                className="flex h-8 w-9 cursor-pointer items-center justify-center rounded-md transition-colors hover:bg-foreground/10 focus-visible:outline-none"
                onClick={handleAdd}
                aria-label="Add torrent"
              >
                <Folder color="blue" size="sm" className="scale-[0.135]" />
              </button>
            </TooltipTrigger>
            <TooltipContent side="bottom">Add torrent</TooltipContent>
          </Tooltip>
          <Button variant="ghost" size="sm" onClick={openMagnet}>
            Add magnet
          </Button>
          <TitleBarDivider />
        </TitleBar>

      {notice !== null && (
        <div className="flex items-center justify-between border-b border-destructive bg-card px-4 py-1.5 text-sm text-destructive">
          <span className="truncate">{notice}</span>
          <Button variant="ghost" size="sm" onClick={() => setNotice(null)}>
            dismiss
          </Button>
        </div>
      )}

      <main className="flex min-h-0 flex-1 flex-col">
        <div className="min-h-0 flex-1 overflow-auto px-2">
          <TorrentTable
            summaries={filteredSummaries}
            selectedId={selectedId}
            onSelect={handleSelect}
            onPauseResume={handlePauseResume}
            onRemove={setPendingRemove}
            onOpenFolder={(id) => {
              api.openOutputDir(id).catch((err) => setNotice(String(err)));
            }}
            onRecheck={(id) => {
              api
                .forceRecheck(id)
                .then(() => setNotice(`re-check started`))
                .catch((err) => setNotice(String(err)));
            }}
            onToggleStopSeeding={(summary) => {
              api
                .setStopAfterComplete(summary.id, !summary.stop_after_complete)
                .catch((err) => setNotice(String(err)));
            }}
          />
        </div>
        <DetailPanel
          summary={selected}
          detail={detail}
          dht={dht}
          dhtWaiting={
            selectedId !== null &&
            (summaries.find((torrent) => torrent.id === selectedId)?.dht_waiting ??
              false)
          }
        />
      </main>

      <footer className="flex items-center gap-4 border-t border-border px-2 py-1.5 text-xs text-muted-foreground">
        {dht !== null && (
          <span
            className={dht.active ? "text-accent" : "text-destructive"}
            title={dht.active ? undefined : (dht.error ?? undefined)}
            role="status"
          >
            {dht.active ? `DHT: ${dht.node_count} nodes` : "DHT inactive"}
          </span>
        )}
        {listener !== null && (
          <span
            className={listener.active ? "text-accent" : "text-destructive"}
            title={
              listener.active
                ? "The port is bound locally, but reachability from outside is not verified. Router/NAT port forwarding may be needed for remote peers to connect."
                : listener.error ?? undefined
            }
            role="status"
          >
            {listener.active
              ? `Listening on :${listener.port} (local)`
              : "Listener inactive"}
          </span>
        )}
        <div className="flex-1" />
        <span role="status">
          {stateFilter === null
            ? `${summaries.length} torrent${summaries.length === 1 ? "" : "s"}`
            : `${filteredSummaries.length} of ${summaries.length} torrents`}
        </span>
        <span
          className="rounded border border-border px-1 py-0.5 text-[10px] tabular-nums"
          data-ui-build={UI_BUILD_STAMP}
        >
          {UI_BUILD_STAMP}
        </span>
      </footer>

      {dragActive && (
        <div
          className="pointer-events-none fixed inset-0 z-20 flex items-center justify-center bg-background/80"
          role="status"
        >
          <div className="rounded-lg border-2 border-dashed border-accent bg-card px-8 py-6 text-sm text-accent">
            Drop .torrent files to add them
          </div>
        </div>
      )}

      <AlertDialog
        open={pendingRemove !== null}
        onOpenChange={(open) => {
          if (!open) {
            setPendingRemove(null);
            setDeleteFiles(false);
          }
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              Remove{" "}
              <span className="inline-block max-w-full truncate align-bottom">
                “{pendingRemove?.name}”
              </span>{" "}
              from the session?
            </AlertDialogTitle>
            <AlertDialogDescription>
              The torrent will stop and be removed from the list.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <label className="flex items-center gap-2 text-sm text-muted-foreground">
            <Checkbox
              checked={deleteFiles}
              onCheckedChange={(checked) => setDeleteFiles(checked === true)}
            />
            Also delete downloaded files
          </label>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onClick={handleConfirmRemove}
            >
              Remove
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AddTorrentModal
        open={addOpen}
        torrents={pending}
        detail={detail}
        onAdd={handleConfirmAdd}
        onAddMagnet={handleAddMagnetUri}
        onClose={() => {
          setAddOpen(false);
          setPending([]);
        }}
      />

      <RemoteAccessModal open={remoteOpen} onClose={() => setRemoteOpen(false)} />

      <AlertDialog
        open={settingsOpen}
        onOpenChange={(open) => {
          setSettingsOpen(open);
          if (!open) setSettingsError(null);
        }}
      >
        <AlertDialogContent className="w-full max-w-xl gap-0 overflow-hidden rounded-xl border-sidebar-border bg-sidebar p-0">
          <div className="border-b border-sidebar-border px-6 py-4">
            <AlertDialogTitle className="text-base font-semibold">
              Settings
            </AlertDialogTitle>
            <AlertDialogDescription className="mt-1 text-xs">
              Changes are applied to the running session immediately.
            </AlertDialogDescription>
          </div>
          {settingsForm !== null && (
            <div className="grid gap-6 px-6 py-5">
              <section className="grid gap-3">
                <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                  Downloads
                </span>
                <div className="grid gap-2">
                  <Label htmlFor="download-dir">Download directory</Label>
                  <div className="flex gap-2">
                    <Input
                      id="download-dir"
                      className="flex-1"
                      value={settingsForm.downloadDir}
                      onChange={(event) =>
                        setSettingsForm({
                          ...settingsForm,
                          downloadDir: event.target.value,
                        })
                      }
                    />
                    <Button
                      variant="outline"
                      className="shrink-0 self-start"
                      onClick={() => {
                        api
                          .pickDirectory()
                          .then((path) => {
                            if (typeof path === "string") {
                              setSettingsForm((form) =>
                                form === null
                                  ? form
                                  : { ...form, downloadDir: path },
                              );
                            }
                          })
                          .catch((err) => setSettingsError(String(err)));
                      }}
                    >
                      <FolderOpenIcon className="size-4" />
                      Browse
                    </Button>
                  </div>
                </div>
                <div className="grid gap-2">
                  <Label htmlFor="upload-limit">
                    Upload limit (B/s, 0 = unlimited)
                  </Label>
                  <Input
                    id="upload-limit"
                    type="number"
                    min={0}
                    value={settingsForm.uploadLimitBps}
                    onChange={(event) =>
                      setSettingsForm({
                        ...settingsForm,
                        uploadLimitBps: Number(event.target.value),
                      })
                    }
                  />
                </div>
              </section>
              <section className="grid gap-3">
                <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                  Network
                </span>
                <div className="grid grid-cols-2 gap-3">
                  <div className="grid gap-2">
                    <Label htmlFor="listen-port">Listen port</Label>
                    <Input
                      id="listen-port"
                      type="number"
                      min={1024}
                      max={65535}
                      value={settingsForm.listenPort}
                      onChange={(event) =>
                        setSettingsForm({
                          ...settingsForm,
                          listenPort: Number(event.target.value),
                        })
                      }
                    />
                  </div>
                  <div className="grid gap-2">
                    <Label htmlFor="dht-port">DHT port</Label>
                    <Input
                      id="dht-port"
                      type="number"
                      min={1024}
                      max={65535}
                      value={settingsForm.dhtPort}
                      onChange={(event) =>
                        setSettingsForm({
                          ...settingsForm,
                          dhtPort: Number(event.target.value),
                        })
                      }
                    />
                  </div>
                </div>
                <label className="flex items-center gap-2 text-sm text-muted-foreground">
                  <Checkbox
                    checked={settingsForm.dhtEnabled}
                    onCheckedChange={(checked) =>
                      setSettingsForm({
                        ...settingsForm,
                        dhtEnabled: checked === true,
                      })
                    }
                  />
                  Enable DHT peer discovery (BEP 5)
                </label>
              </section>
              {settingsError !== null && (
                <p role="alert" className="text-sm text-destructive">
                  {settingsError}
                </p>
              )}
            </div>
          )}
          <div className="flex justify-end gap-2 border-t border-sidebar-border px-6 py-4">
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <Button
              className="bg-accent text-accent-foreground hover:bg-accent/90"
              onClick={handleSaveSettings}
            >
              Save changes
            </Button>
          </div>
        </AlertDialogContent>
      </AlertDialog>
      </div>
    </SidebarProvider>
  );
}
