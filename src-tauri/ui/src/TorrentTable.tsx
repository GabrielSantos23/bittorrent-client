import { formatBytes, formatEta, formatRate, formatRatio, stateColor } from "./format";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { TorrentSummary } from "../../../bt-core/bindings/TorrentSummary";

interface TorrentTableProps {
  summaries: TorrentSummary[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onPauseResume: (summary: TorrentSummary) => void;
  onRemove: (summary: TorrentSummary) => void;
}

const badgeVariantByState: Record<
  string,
  "default" | "secondary" | "destructive" | "info" | "success"
> = {
  Checking: "info",
  FetchingMetadata: "info",
  Downloading: "success",
  Seeding: "info",
  Completed: "success",
  Paused: "secondary",
  Stopped: "secondary",
  Error: "destructive",
};

export default function TorrentTable({
  summaries,
  selectedId,
  onSelect,
  onPauseResume,
  onRemove,
}: TorrentTableProps) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Name</TableHead>
          <TableHead>Size</TableHead>
          <TableHead>Progress</TableHead>
          <TableHead>State</TableHead>
          <TableHead>Down</TableHead>
          <TableHead>Up</TableHead>
          <TableHead>Ratio</TableHead>
          <TableHead>ETA</TableHead>
          <TableHead>Peers</TableHead>
          <TableHead>Actions</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {summaries.length === 0 ? (
          <TableRow>
            <TableCell
              colSpan={10}
              className="h-24 text-center text-muted-foreground"
            >
              No torrents yet — use “Add torrent” or drop a .torrent file here.
            </TableCell>
          </TableRow>
        ) : (
          summaries.map((summary) => {
            const pausable =
              summary.state === "Downloading" || summary.state === "Checking";
            const resumable = summary.state === "Paused";
            return (
              <TableRow
                key={summary.id}
                onClick={() => onSelect(summary.id)}
                data-selected={selectedId === summary.id || undefined}
                className="cursor-pointer data-[selected]:bg-accent"
              >
                <TableCell className="max-w-[240px] truncate" title={summary.name}>
                  {summary.name}
                  {summary.error !== null && (
                    <span className="ml-2 text-destructive" title={summary.error}>
                      ⚠
                    </span>
                  )}
                </TableCell>
                <TableCell>{formatBytes(summary.total_length)}</TableCell>
                <TableCell>
                  {summary.state === "FetchingMetadata" ? (
                    <span className="text-xs text-info">fetching metadata…</span>
                  ) : (
                    <div className="flex items-center gap-2">
                      <Progress
                        value={Math.min(Math.max(summary.progress * 100, 0), 100)}
                        aria-label={`${summary.name} progress`}
                        className="w-28"
                      />
                      <span className="text-muted-foreground">
                        {(summary.progress * 100).toFixed(1)}%
                      </span>
                    </div>
                  )}
                </TableCell>
                <TableCell>
                  <Badge
                    variant={badgeVariantByState[summary.state] ?? "secondary"}
                    className={stateColor[summary.state] ?? ""}
                  >
                    {summary.state}
                  </Badge>
                  {summary.error !== null && (
                    <div className="max-w-[220px] truncate text-xs text-destructive">
                      {summary.error}
                    </div>
                  )}
                </TableCell>
                <TableCell>{formatRate(summary.download_rate)}</TableCell>
                <TableCell>{formatRate(summary.upload_rate)}</TableCell>
                <TableCell>{formatRatio(summary.ratio)}</TableCell>
                <TableCell>{formatEta(summary.eta_seconds)}</TableCell>
                <TableCell>{summary.peer_count}</TableCell>
                <TableCell>
                  <div className="flex gap-2">
                    {pausable && (
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={(event) => {
                          event.stopPropagation();
                          onPauseResume(summary);
                        }}
                      >
                        Pause
                      </Button>
                    )}
                    {resumable && (
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={(event) => {
                          event.stopPropagation();
                          onPauseResume(summary);
                        }}
                      >
                        Resume
                      </Button>
                    )}
                    <Button
                      variant="outline"
                      size="sm"
                      className="text-destructive"
                      onClick={(event) => {
                        event.stopPropagation();
                        onRemove(summary);
                      }}
                    >
                      Remove
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            );
          })
        )}
      </TableBody>
    </Table>
  );
}
