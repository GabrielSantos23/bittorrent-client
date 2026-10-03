import { formatBytes, formatEta, formatRate, stateColor } from "./format";
import type { TorrentSummary } from "../../../bt-core/bindings/TorrentSummary";

interface ProgressBarProps {
  progress: number;
  label: string;
}

function ProgressBar({ progress, label }: ProgressBarProps) {
  const percent = Math.min(Math.max(progress * 100, 0), 100);
  return (
    <svg
      width="110"
      height="8"
      viewBox="0 0 110 8"
      role="progressbar"
      aria-valuenow={Math.round(percent)}
      aria-valuemin={0}
      aria-valuemax={100}
      aria-label={label}
    >
      <rect x="0" y="0" width="110" height="8" rx="2" className="fill-border" />
      <rect
        x="1"
        y="1"
        width={Math.max((108 * percent) / 100, percent > 0 ? 2 : 0)}
        height="6"
        rx="1.5"
        className="fill-accent"
      />
    </svg>
  );
}

interface TorrentTableProps {
  summaries: TorrentSummary[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onPauseResume: (summary: TorrentSummary) => void;
  onRemove: (summary: TorrentSummary) => void;
}

export default function TorrentTable({
  summaries,
  selectedId,
  onSelect,
  onPauseResume,
  onRemove,
}: TorrentTableProps) {
  return (
    <table className="w-full text-left text-sm">
      <thead>
        <tr className="border-b border-border text-muted">
          <th className="px-3 py-2 font-medium">Name</th>
          <th className="px-3 py-2 font-medium">Size</th>
          <th className="px-3 py-2 font-medium">Progress</th>
          <th className="px-3 py-2 font-medium">State</th>
          <th className="px-3 py-2 font-medium">Rate</th>
          <th className="px-3 py-2 font-medium">ETA</th>
          <th className="px-3 py-2 font-medium">Peers</th>
          <th className="px-3 py-2 font-medium">Actions</th>
        </tr>
      </thead>
      <tbody>
        {summaries.length === 0 ? (
          <tr>
            <td colSpan={8} className="px-3 py-8 text-center text-muted">
              No torrents yet — use “Add torrent” or drop a .torrent file here.
            </td>
          </tr>
        ) : (
          summaries.map((summary) => {
            const pausable =
              summary.state === "Downloading" || summary.state === "Checking";
            const resumable = summary.state === "Paused";
            return (
              <tr
                key={summary.id}
                onClick={() => onSelect(summary.id)}
                className={`cursor-pointer border-b border-border hover:bg-surface/70 ${
                  selectedId === summary.id ? "bg-surface" : ""
                }`}
              >
                <td className="max-w-[240px] truncate px-3 py-2" title={summary.name}>
                  {summary.name}
                  {summary.error !== null && (
                    <span className="ml-2 text-danger" title={summary.error}>
                      ⚠
                    </span>
                  )}
                </td>
                <td className="px-3 py-2">{formatBytes(summary.total_length)}</td>
                <td className="px-3 py-2">
                  <div className="flex items-center gap-2">
                    <ProgressBar
                      progress={summary.progress}
                      label={`${summary.name} progress`}
                    />
                    <span className="text-muted">
                      {(summary.progress * 100).toFixed(1)}%
                    </span>
                  </div>
                </td>
                <td className={`px-3 py-2 ${stateColor[summary.state] ?? ""}`}>
                  {summary.state}
                  {summary.error !== null && (
                    <div className="max-w-[220px] truncate text-xs text-danger">
                      {summary.error}
                    </div>
                  )}
                </td>
                <td className="px-3 py-2">{formatRate(summary.download_rate)}</td>
                <td className="px-3 py-2">{formatEta(summary.eta_seconds)}</td>
                <td className="px-3 py-2">{summary.peer_count}</td>
                <td className="px-3 py-2">
                  <div className="flex gap-2">
                    {pausable && (
                      <button
                        type="button"
                        className="rounded border border-border px-2 py-0.5 hover:bg-surface"
                        onClick={(event) => {
                          event.stopPropagation();
                          onPauseResume(summary);
                        }}
                      >
                        Pause
                      </button>
                    )}
                    {resumable && (
                      <button
                        type="button"
                        className="rounded border border-border px-2 py-0.5 hover:bg-surface"
                        onClick={(event) => {
                          event.stopPropagation();
                          onPauseResume(summary);
                        }}
                      >
                        Resume
                      </button>
                    )}
                    <button
                      type="button"
                      className="rounded border border-danger px-2 py-0.5 text-danger hover:bg-surface"
                      onClick={(event) => {
                        event.stopPropagation();
                        onRemove(summary);
                      }}
                    >
                      Remove
                    </button>
                  </div>
                </td>
              </tr>
            );
          })
        )}
      </tbody>
    </table>
  );
}
