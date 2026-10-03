import { useState } from "react";
import { formatBytes, formatRate } from "./format";
import type { TorrentDetail } from "../../../bt-core/bindings/TorrentDetail";

type Tab = "Peers" | "Files" | "Info";

interface DetailPanelProps {
  detail: TorrentDetail | null;
}

export default function DetailPanel({ detail }: DetailPanelProps) {
  const [tab, setTab] = useState<Tab>("Peers");
  if (detail === null) {
    return (
      <div className="flex items-center justify-center border-t border-border px-4 py-6 text-muted">
        Select a torrent to see its details.
      </div>
    );
  }
  const tabs: Tab[] = ["Peers", "Files", "Info"];
  return (
    <div className="flex min-h-0 flex-1 flex-col border-t border-border">
      <div className="flex items-center gap-1 border-b border-border px-3 pt-2">
        {tabs.map((name) => (
          <button
            key={name}
            type="button"
            className={`rounded-t px-3 py-1.5 text-sm ${
              tab === name
                ? "border-x border-t border-border bg-surface text-text"
                : "text-muted hover:text-text"
            }`}
            onClick={() => setTab(name)}
          >
            {name}
          </button>
        ))}
        <div className="flex-1" />
      </div>
      <div className="min-h-0 flex-1 overflow-auto p-3">
        {tab === "Peers" && (
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-border text-muted">
                <th className="px-2 py-1 font-medium">Address</th>
                <th className="px-2 py-1 font-medium">Client</th>
                <th className="px-2 py-1 font-medium">Rate</th>
                <th className="px-2 py-1 font-medium">Choked</th>
              </tr>
            </thead>
            <tbody>
              {detail.peers.length === 0 ? (
                <tr>
                  <td colSpan={4} className="px-2 py-4 text-muted">
                    No peers connected.
                  </td>
                </tr>
              ) : (
                detail.peers.map((peer) => (
                  <tr key={peer.addr} className="border-b border-border/50">
                    <td className="px-2 py-1">{peer.addr}</td>
                    <td className="px-2 py-1">{peer.client}</td>
                    <td className="px-2 py-1">{formatRate(peer.rate)}</td>
                    <td className="px-2 py-1">{peer.choked ? "yes" : "no"}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        )}
        {tab === "Files" && (
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-border text-muted">
                <th className="px-2 py-1 font-medium">Path</th>
                <th className="px-2 py-1 font-medium">Size</th>
              </tr>
            </thead>
            <tbody>
              {detail.files.map((file) => (
                <tr key={file.path} className="border-b border-border/50">
                  <td className="px-2 py-1">{file.path}</td>
                  <td className="px-2 py-1">{formatBytes(file.length)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {tab === "Info" && (
          <dl className="grid grid-cols-[140px_1fr] gap-y-1 text-sm">
            <dt className="text-muted">Info hash</dt>
            <dd className="break-all">{detail.info_hash}</dd>
            <dt className="text-muted">Output dir</dt>
            <dd className="break-all">{detail.output_dir}</dd>
            <dt className="text-muted">Comment</dt>
            <dd>{detail.comment ?? "—"}</dd>
            <dt className="text-muted">Trackers</dt>
            <dd className="flex flex-col gap-1">
              {detail.trackers.length === 0 ? (
                <span className="text-muted">none</span>
              ) : (
                detail.trackers.map((tracker) => (
                  <span key={tracker} className="break-all">
                    {tracker}
                  </span>
                ))
              )}
            </dd>
          </dl>
        )}
      </div>
    </div>
  );
}
