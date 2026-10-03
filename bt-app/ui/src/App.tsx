import { useEffect, useState } from "react";
import { api } from "./api";
import { formatRate } from "./format";
import type { TorrentSummary } from "../../../bt-core/bindings/TorrentSummary";
import type { TorrentDetail } from "../../../bt-core/bindings/TorrentDetail";
import DetailPanel from "./DetailPanel";
import SpeedGraph from "./SpeedGraph";
import TorrentTable from "./TorrentTable";

const HISTORY_LIMIT = 240;

function isTorrentPath(path: string): boolean {
  return path.toLowerCase().endsWith(".torrent");
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

  useEffect(() => {
    api.list().then(setSummaries).catch(setNotice);
    const unSummaries = api.onSummaries((next) => {
      setSummaries(next);
      const total = next.reduce((acc, torrent) => acc + torrent.download_rate, 0);
      setHistory((current) => [...current.slice(-(HISTORY_LIMIT - 1)), total]);
    });
    const unDetail = api.onDetail(setDetail);
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
      unDragEnter.then((stop) => stop());
      unDragLeave.then((stop) => stop());
      unDrop.then((stop) => stop());
    };
  }, []);

  const totalRate = summaries.reduce((acc, torrent) => acc + torrent.download_rate, 0);

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

  return (
    <div
      className="flex h-full flex-col bg-bg text-text"
      onDragOver={(event) => event.preventDefault()}
    >
      <header className="flex items-center gap-4 border-b border-border px-4 py-3">
        <span className="text-lg font-semibold">BitTorrent Client</span>
        <button
          type="button"
          className="rounded border border-accent px-3 py-1 text-sm text-accent hover:bg-surface"
          onClick={handleAdd}
        >
          Add torrent
        </button>
        <span className="text-sm text-muted">
          ↓ {formatRate(totalRate)}
        </span>
        <div className="flex-1" />
        <SpeedGraph history={history} />
      </header>

      {notice !== null && (
        <div className="flex items-center justify-between border-b border-danger bg-surface px-4 py-1.5 text-sm text-danger">
          <span className="truncate">{notice}</span>
          <button type="button" onClick={() => setNotice(null)} className="ml-4">
            dismiss
          </button>
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
        <DetailPanel detail={selectedId === null ? null : detail} />
      </main>

      {dragActive && (
        <div
          className="pointer-events-none fixed inset-0 z-20 flex items-center justify-center bg-bg/80"
          role="status"
        >
          <div className="rounded border-2 border-dashed border-accent bg-surface px-8 py-6 text-sm text-accent">
            Drop .torrent files to add them
          </div>
        </div>
      )}

      {pendingRemove !== null && (
        <div className="fixed inset-0 z-10 flex items-center justify-center bg-bg/80">
          <div className="w-[420px] rounded border border-border bg-surface p-4">
            <p className="mb-3 text-sm">
              Remove “{pendingRemove.name}” from the session?
            </p>
            <label className="mb-4 flex items-center gap-2 text-sm text-muted">
              <input
                type="checkbox"
                checked={deleteFiles}
                onChange={(event) => setDeleteFiles(event.target.checked)}
              />
              Also delete downloaded files
            </label>
            <div className="flex justify-end gap-2">
              <button
                type="button"
                className="rounded border border-border px-3 py-1 text-sm"
                onClick={() => {
                  setPendingRemove(null);
                  setDeleteFiles(false);
                }}
              >
                Cancel
              </button>
              <button
                type="button"
                className="rounded border border-danger px-3 py-1 text-sm text-danger hover:bg-bg"
                onClick={handleConfirmRemove}
              >
                Remove
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
