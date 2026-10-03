import { formatAnnounceTime, formatBytes, formatRate, formatRatio } from "./format";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { TorrentDetail } from "../../../bt-core/bindings/TorrentDetail";

interface DetailPanelProps {
  detail: TorrentDetail | null;
}

function directionLabel(direction: "Incoming" | "Outgoing"): string {
  return direction === "Incoming" ? "in" : "out";
}

export default function DetailPanel({ detail }: DetailPanelProps) {
  if (detail === null) {
    return (
      <div className="flex items-center justify-center border-t border-border px-4 py-6 text-muted-foreground">
        Select a torrent to see its details.
      </div>
    );
  }
  return (
    <Tabs defaultValue="Peers" className="flex min-h-0 flex-1 flex-col gap-0 border-t border-border">
      <div className="border-b border-border px-3 pt-2">
        <TabsList>
          <TabsTrigger value="Peers">Peers</TabsTrigger>
          <TabsTrigger value="Files">Files</TabsTrigger>
          <TabsTrigger value="Info">Info</TabsTrigger>
        </TabsList>
      </div>
      <TabsContent value="Peers" className="min-h-0 flex-1 overflow-auto p-0">
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
            {detail.peers.length === 0 ? (
              <TableRow>
                <TableCell
                  colSpan={7}
                  className="h-16 text-center text-muted-foreground"
                >
                  No peers connected.
                </TableCell>
              </TableRow>
            ) : (
              detail.peers.map((peer) => (
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
      <TabsContent value="Files" className="min-h-0 flex-1 overflow-auto p-0">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Path</TableHead>
              <TableHead>Size</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {detail.files.map((file) => (
              <TableRow key={file.path}>
                <TableCell>{file.path}</TableCell>
                <TableCell>{formatBytes(file.length)}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TabsContent>
      <TabsContent value="Info" className="min-h-0 flex-1 overflow-auto p-3">
        <dl className="grid grid-cols-[160px_1fr] gap-y-1 text-sm">
          <dt className="text-muted-foreground">Info hash</dt>
          <dd className="break-all">{detail.info_hash}</dd>
          <dt className="text-muted-foreground">Output dir</dt>
          <dd className="break-all">{detail.output_dir}</dd>
          <dt className="text-muted-foreground">Comment</dt>
          <dd>{detail.comment ?? "—"}</dd>
          <dt className="text-muted-foreground">Uploaded</dt>
          <dd>{formatBytes(detail.session_uploaded)}</dd>
          <dt className="text-muted-foreground">Upload rate</dt>
          <dd>{formatRate(detail.upload_rate)}</dd>
          <dt className="text-muted-foreground">Ratio</dt>
          <dd>{formatRatio(detail.ratio)}</dd>
          <dt className="text-muted-foreground">Trackers</dt>
          <dd className="flex flex-col gap-2">
            {detail.trackers.length === 0 ? (
              <span className="text-muted-foreground">none</span>
            ) : (
              detail.trackers.map((tracker) => (
                <div
                  key={tracker.url}
                  className="rounded-md border border-border bg-card p-2"
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
          </dd>
        </dl>
      </TabsContent>
    </Tabs>
  );
}
