import { useEffect, useState } from "react";
import { api } from "./api";
import { formatRate } from "./format";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { SpeedGraph } from "./SpeedGraph";
import TorrentTable from "./TorrentTable";
import DetailPanel from "./DetailPanel";
import type { TorrentSummary } from "../../../bt-core/bindings/TorrentSummary";
import type { TorrentDetail } from "../../../bt-core/bindings/TorrentDetail";
import type { ListenerStatus } from "../../../bt-core/bindings/ListenerStatus";
import type { DhtStatus } from "../../../bt-core/bindings/DhtStatus";
import type { Settings } from "../../bindings/Settings";

const HISTORY_LIMIT = 240;

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
  const [history, setHistory] = useState<number[]>([]);
  const [pendingRemove, setPendingRemove] = useState<TorrentSummary | null>(null);
  const [deleteFiles, setDeleteFiles] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [dragActive, setDragActive] = useState(false);
  const [listener, setListener] = useState<ListenerStatus | null>(null);
  const [dht, setDht] = useState<DhtStatus | null>(null);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [settingsForm, setSettingsForm] = useState<SettingsForm | null>(null);
  const [settingsError, setSettingsError] = useState<string | null>(null);
  const [magnetOpen, setMagnetOpen] = useState(false);
  const [magnetUri, setMagnetUri] = useState("");
  const [magnetError, setMagnetError] = useState<string | null>(null);

  useEffect(() => {
    api.list().then(setSummaries).catch(setNotice);
    const unSummaries = api.onSummaries((next) => {
      setSummaries(next);
      const total = next.reduce((acc, torrent) => acc + torrent.download_rate, 0);
      setHistory((current) => [...current.slice(-(HISTORY_LIMIT - 1)), total]);
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
      for (const path of paths.filter(isTorrentPath)) {
        api.add(path).catch((err) => setNotice(String(err)));
      }
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

  const totalRate = summaries.reduce((acc, torrent) => acc + torrent.download_rate, 0);
  const totalUpload = summaries.reduce((acc, torrent) => acc + torrent.upload_rate, 0);

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

  const handleAdd = () => {
    api
      .pickTorrent()
      .then((path) => {
        if (typeof path === "string") {
          return api.add(path);
        }
        return null;
      })
      .catch((err) => setNotice(String(err)));
  };

  const openMagnet = () => {
    setMagnetUri("");
    setMagnetError(null);
    setMagnetOpen(true);
  };

  const handleAddMagnet = () => {
    const uri = magnetUri.trim();
    if (!uri.startsWith("magnet:?")) {
      setMagnetError("not a magnet uri");
      return;
    }
    setMagnetError(null);
    api
      .addMagnet(uri)
      .then(() => setMagnetOpen(false))
      .catch((err) => setMagnetError(String(err)));
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
    <div
      className="flex h-full flex-col bg-background text-foreground"
      onDragOver={(event) => event.preventDefault()}
    >
      <header className="flex items-center gap-4 border-b border-border px-4 py-3">
        <span className="text-lg font-semibold">BitTorrent Client</span>
        <Button variant="outline" size="sm" onClick={handleAdd}>
          Add torrent
        </Button>
        <Button variant="ghost" size="sm" onClick={openMagnet}>
          Add magnet
        </Button>
        <Button variant="ghost" size="sm" onClick={openSettings}>
          Settings
        </Button>
        <span className="text-sm text-muted-foreground">
          ↓ {formatRate(totalRate)}
        </span>
        <span className="text-sm text-muted-foreground">
          ↑ {formatRate(totalUpload)}
        </span>
        <div className="flex-1" />
        {dht !== null && (
          <span
            className={dht.active ? "text-sm text-primary" : "text-sm text-destructive"}
            title={dht.active ? undefined : (dht.error ?? undefined)}
            role="status"
          >
            {dht.active ? `DHT: ${dht.node_count} nodes` : "DHT inactive"}
          </span>
        )}
        {listener !== null && (
          <span
            className={
              listener.active
                ? "text-sm text-primary"
                : "text-sm text-destructive"
            }
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
        <SpeedGraph history={history} />
      </header>

      {notice !== null && (
        <div className="flex items-center justify-between border-b border-destructive bg-card px-4 py-1.5 text-sm text-destructive">
          <span className="truncate">{notice}</span>
          <Button variant="ghost" size="sm" onClick={() => setNotice(null)}>
            dismiss
          </Button>
        </div>
      )}

      <main className="flex min-h-0 flex-1 flex-col">
        <div className="flex-1 overflow-auto">
          <TorrentTable
            summaries={summaries}
            selectedId={selectedId}
            onSelect={handleSelect}
            onPauseResume={handlePauseResume}
            onRemove={setPendingRemove}
          />
        </div>
        <DetailPanel
          detail={selectedId === null ? null : detail}
          dht={dht}
          dhtWaiting={
            selectedId !== null &&
            (summaries.find((torrent) => torrent.id === selectedId)?.dht_waiting ??
              false)
          }
        />
      </main>

      {dragActive && (
        <div
          className="pointer-events-none fixed inset-0 z-20 flex items-center justify-center bg-background/80"
          role="status"
        >
          <div className="rounded-lg border-2 border-dashed border-primary bg-card px-8 py-6 text-sm text-primary">
            Drop .torrent files to add them
          </div>
        </div>
      )}

      <Dialog open={pendingRemove !== null}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              Remove “{pendingRemove?.name}” from the session?
            </DialogTitle>
            <DialogDescription>
              The torrent will stop and be removed from the list.
            </DialogDescription>
          </DialogHeader>
          <label className="flex items-center gap-2 text-sm text-muted-foreground">
            <Checkbox
              checked={deleteFiles}
              onCheckedChange={(checked) => setDeleteFiles(checked === true)}
            />
            Also delete downloaded files
          </label>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setPendingRemove(null);
                setDeleteFiles(false);
              }}
            >
              Cancel
            </Button>
            <Button variant="destructive" onClick={handleConfirmRemove}>
              Remove
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={magnetOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add magnet</DialogTitle>
            <DialogDescription>
              Fetch metadata from peers sharing the info hash.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-2">
            <div className="grid gap-2">
              <Label htmlFor="magnet-uri">Magnet uri</Label>
              <Input
                id="magnet-uri"
                data-autofocus
                value={magnetUri}
                onChange={(event) => setMagnetUri(event.target.value)}
                placeholder="magnet:?xt=urn:btih:…"
              />
            </div>
            {magnetError !== null && (
              <p role="alert" className="text-sm text-destructive">
                {magnetError}
              </p>
            )}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setMagnetOpen(false)}>
              Cancel
            </Button>
            <Button onClick={handleAddMagnet}>Add</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={settingsOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Settings</DialogTitle>
            <DialogDescription>
              Changes are applied to the running session immediately.
            </DialogDescription>
          </DialogHeader>
          {settingsForm !== null && (
            <div className="grid gap-4 py-2">
              <div className="grid gap-2">
                <Label htmlFor="download-dir">Download directory</Label>
                <Input
                  id="download-dir"
                  value={settingsForm.downloadDir}
                  onChange={(event) =>
                    setSettingsForm({
                      ...settingsForm,
                      downloadDir: event.target.value,
                    })
                  }
                />
              </div>
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
              <div className="grid gap-2">
                <Label htmlFor="upload-limit">Upload limit (B/s, 0 = unlimited)</Label>
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
              {settingsError !== null && (
                <p role="alert" className="text-sm text-destructive">
                  {settingsError}
                </p>
              )}
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setSettingsOpen(false)}>
              Cancel
            </Button>
            <Button onClick={handleSaveSettings}>Save</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
