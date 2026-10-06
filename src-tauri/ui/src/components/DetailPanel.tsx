import type { ReactNode } from "react";

import {
  formatAnnounceTime,
  formatBytes,
  formatEta,
  formatRate,
  formatRatio,
  stateColor,
} from "../format";
import type { TorrentSummary } from "../../../../bt-core/bindings/TorrentSummary";
import type { TorrentDetail } from "../../../../bt-core/bindings/TorrentDetail";
import type { DhtStatus } from "../../../../bt-core/bindings/DhtStatus";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import {
  Tabs,
  TabsContent,
  TabsContents,
  TabsList,
  TabsTrigger,
} from "@/components/animate-ui/components/animate/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import TorrentFileTree from "./TorrentFileTree";

interface DetailPanelProps {
  summary: TorrentSummary | null;
  detail: TorrentDetail | null;
  dht: DhtStatus | null;
  dhtWaiting: boolean;
}

function Stat({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="flex min-w-0 flex-col gap-0.5">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className="truncate text-sm">{children}</span>
    </div>
  );
}

function directionLabel(direction: "Incoming" | "Outgoing"): string {
  return direction === "Incoming" ? "in" : "out";
}

export default function DetailPanel({
  summary,
  detail,
  dht,
  dhtWaiting,
}: DetailPanelProps) {
  if (summary === null) {
    return (
      <div className="mx-2 mb-2 flex h-10 shrink-0 items-center justify-center rounded-lg border border-sidebar-border bg-sidebar/80 text-sm text-muted-foreground shadow-sm backdrop-blur-md">
        Select a torrent to see its details.
      </div>
    );
  }
  const liveDetail = detail !== null && detail.id === summary.id ? detail : null;

  return (
    <div className="mx-2 mb-2 flex h-72 shrink-0 flex-col rounded-lg border border-sidebar-border bg-sidebar/80 text-sidebar-foreground shadow-sm backdrop-blur-md">
      <div className="flex items-center gap-3 border-b border-sidebar-border px-3 py-2">
        <span
          className="min-w-0 truncate text-sm font-semibold"
          title={summary.name}
        >
          {summary.name}
        </span>
        <Badge
          variant={
            summary.state === "Error"
              ? "destructive"
              : summary.state === "Downloading" || summary.state === "Completed"
                ? "success"
                : "info"
          }
          className={`shrink-0 ${stateColor[summary.state] ?? ""}`}
        >
          {summary.state}
        </Badge>
        <Progress
          value={Math.min(Math.max(summary.progress * 100, 0), 100)}
          aria-label={`${summary.name} progress`}
          className="h-1.5 w-40 shrink-0"
        />
        <span className="shrink-0 text-xs text-muted-foreground">
          {(summary.progress * 100).toFixed(1)}%
        </span>
      </div>
      <Tabs defaultValue="Status" className="flex min-h-0 flex-1 flex-col gap-0 px-3 py-2">
        <TabsList className="self-start">
          <TabsTrigger value="Status">Status</TabsTrigger>
          <TabsTrigger value="Files">Files</TabsTrigger>
          <TabsTrigger value="Trackers">Trackers</TabsTrigger>
          <TabsTrigger value="Peers">Peers</TabsTrigger>
          <TabsTrigger value="Info">Info</TabsTrigger>
        </TabsList>
        {liveDetail === null ? (
          <TabsContents className="min-h-0 flex-1">
            <TabsContent value="Status" className="h-full overflow-auto">
              <div className="grid grid-cols-2 gap-x-8 gap-y-2 py-2 lg:grid-cols-4">
                <Stat label="Size">
                  {formatBytes(summary.wanted_bytes)}
                  {summary.wanted_bytes !== summary.total_length && (
                    <span className="text-muted-foreground">
                      {' of '}{formatBytes(summary.total_length)}
                    </span>
                  )}
                </Stat>
                <Stat label="Downloaded">
                  {formatBytes(summary.verified_bytes)}
                </Stat>
                <Stat label="Down speed">
                  {formatRate(summary.download_rate)}
                </Stat>
                <Stat label="Up speed">{formatRate(summary.upload_rate)}</Stat>
                <Stat label="Uploaded">
                  {formatBytes(summary.session_uploaded)}
                </Stat>
                <Stat label="Share ratio">{formatRatio(summary.ratio)}</Stat>
                <Stat label="ETA">{formatEta(summary.eta_seconds)}</Stat>
                <Stat label="Peers">{summary.peer_count}</Stat>
              </div>
            </TabsContent>
          </TabsContents>
        ) : (
          <TabsContents className="min-h-0 flex-1">
            <TabsContent value="Status" className="h-full overflow-auto">
                <div className="grid grid-cols-2 gap-x-8 gap-y-2 py-2 lg:grid-cols-4">
                  <Stat label="Size">
                  {formatBytes(summary.wanted_bytes)}
                  {summary.wanted_bytes !== summary.total_length && (
                    <span className="text-muted-foreground">
                      {' of '}{formatBytes(summary.total_length)}
                    </span>
                  )}
                </Stat>
                  <Stat label="Downloaded">
                    {formatBytes(summary.verified_bytes)}
                  </Stat>
                  <Stat label="Down speed">
                    {formatRate(summary.download_rate)}
                  </Stat>
                  <Stat label="Up speed">{formatRate(summary.upload_rate)}</Stat>
                  <Stat label="Uploaded">
                    {formatBytes(summary.session_uploaded)}
                  </Stat>
                  <Stat label="Share ratio">{formatRatio(summary.ratio)}</Stat>
                  <Stat label="ETA">{formatEta(summary.eta_seconds)}</Stat>
                  <Stat label="Peers">{summary.peer_count}</Stat>
                </div>
            </TabsContent>
            <TabsContent value="Files" className="h-full overflow-auto">
              <TorrentFileTree key={liveDetail.id} files={liveDetail.files} />
            </TabsContent>
            <TabsContent value="Trackers" className="h-full overflow-auto py-2">
              <div className="flex flex-col gap-2">
                {liveDetail.trackers.length === 0 ? (
                  <p className="text-sm text-muted-foreground">No trackers.</p>
                ) : (
                  liveDetail.trackers.map((tracker) => (
                    <div
                      key={tracker.url}
                      className="rounded-md border border-border bg-card p-2 text-sm"
                    >
                      <div className="break-all">{tracker.url}</div>
                      <div className="text-xs text-muted-foreground">
                        {tracker.state} · seeders {tracker.seeders} · leechers{" "}
                        {tracker.leechers} · last announce{" "}
                        {formatAnnounceTime(tracker.last_announce)}
                      </div>
                      {tracker.last_error !== null && (
                        <div className="text-xs text-destructive">
                          {tracker.last_error}
                        </div>
                      )}
                    </div>
                  ))
                )}
              </div>
            </TabsContent>
            <TabsContent value="Peers" className="h-full overflow-auto p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Address</TableHead>
                    <TableHead>Client</TableHead>
                    <TableHead>Direction</TableHead>
                    <TableHead>Down</TableHead>
                    <TableHead>Up</TableHead>
                    <TableHead>Choked us</TableHead>
                    <TableHead>We unchoked</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {liveDetail.peers.length === 0 ? (
                    <TableRow>
                      <TableCell
                        colSpan={7}
                        className="h-16 text-center text-muted-foreground"
                      >
                        No peers connected.
                      </TableCell>
                    </TableRow>
                  ) : (
                    liveDetail.peers.map((peer) => (
                      <TableRow key={peer.addr}>
                        <TableCell>{peer.addr}</TableCell>
                        <TableCell>{peer.client}</TableCell>
                        <TableCell>{directionLabel(peer.direction)}</TableCell>
                        <TableCell>{formatRate(peer.rate)}</TableCell>
                        <TableCell>{formatRate(peer.up_rate)}</TableCell>
                        <TableCell>{peer.choked ? "yes" : "no"}</TableCell>
                        <TableCell>{peer.unchoked ? "yes" : "no"}</TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </TabsContent>
            <TabsContent value="Info" className="h-full overflow-auto">
              <dl className="grid grid-cols-[160px_1fr] gap-y-1 py-2 text-sm">
                <dt className="text-muted-foreground">Info hash</dt>
                <dd className="break-all font-mono text-xs">
                  {liveDetail.info_hash}
                </dd>
                <dt className="text-muted-foreground">Output dir</dt>
                <dd className="break-all">{liveDetail.output_dir}</dd>
                <dt className="text-muted-foreground">Comment</dt>
                <dd className="break-all">{liveDetail.comment ?? "—"}</dd>
                <dt className="text-muted-foreground">DHT</dt>
                <dd>
                  {dht === null
                    ? "—"
                    : dht.active
                      ? `active · ${dht.node_count} nodes`
                      : "inactive"}
                  {dht !== null && !dht.active && dht.error !== null && (
                    <span className="block text-xs text-destructive">
                      {dht.error}
                    </span>
                  )}
                  {dhtWaiting && (
                    <span className="block text-xs text-muted-foreground">
                      waiting for peers from DHT
                    </span>
                  )}
                </dd>
              </dl>
            </TabsContent>
          </TabsContents>
        )}
      </Tabs>
    </div>
  );
}
