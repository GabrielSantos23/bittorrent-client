import {
  badgeVariantByState,
  formatBytes,
  formatEta,
  stateColor,
  stateLabel,
} from "./format";
import {
  CheckIcon,
  FolderOpenIcon,
  MagnetIcon,
  PauseIcon,
  PlayIcon,
  RefreshCwIcon,
  Trash2Icon,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/animate-ui/components/animate/tooltip";
import {
  ContextMenu,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuSeparator,
  ContextMenuTrigger,
} from "@/components/ui/context-menu";
import {
  AnimatedCount,
  AnimatedPercent,
  AnimatedRate,
  AnimatedRatio,
} from "@/components/AnimatedValues";
import { Progress } from "@/components/ui/progress";
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
  onOpenFolder: (id: string) => void;
  onRecheck: (id: string) => void;
  onToggleStopSeeding: (summary: TorrentSummary) => void;
}

export default function TorrentTable({
  summaries,
  selectedId,
  onSelect,
  onPauseResume,
  onRemove,
  onOpenFolder,
  onRecheck,
  onToggleStopSeeding,
}: TorrentTableProps) {
  return (
    <TooltipProvider>
      <Table className="table-fixed">
      <TableHeader>
        <TableRow>
          <TableHead>Name</TableHead>
          <TableHead className="w-24">Size</TableHead>
          <TableHead className="w-44">Progress</TableHead>
          <TableHead className="w-24">State</TableHead>
          <TableHead className="w-32">Down</TableHead>
          <TableHead className="w-32">Up</TableHead>
          <TableHead className="w-24">Ratio</TableHead>
          <TableHead className="w-20">ETA</TableHead>
          <TableHead className="w-16">Peers</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {summaries.length === 0 ? (
          <TableRow>
            <TableCell
              colSpan={9}
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
              <ContextMenu key={summary.id}>
                <ContextMenuTrigger
                  render={
                    <TableRow
                      onClick={() => onSelect(summary.id)}
                      data-selected={selectedId === summary.id || undefined}
                      className="cursor-pointer data-[selected]:bg-muted data-[selected]:[&>td]:border-y data-[selected]:[&>td]:border-sidebar-border data-[selected]:[&>td:first-child]:border-l data-[selected]:[&>td:last-child]:border-r data-[selected]:[&>td:first-child]:rounded-l-lg data-[selected]:[&>td:last-child]:rounded-r-lg"
                    />
                  }
                >
                <TableCell className="min-w-0">
                  <div className="flex items-center gap-1.5">
                    <Tooltip>
                      <TooltipTrigger className="min-w-0 flex-1 cursor-default truncate">
                        {summary.name}
                      </TooltipTrigger>
                      <TooltipContent>{summary.name}</TooltipContent>
                    </Tooltip>
                    {summary.error !== null && (
                      <Tooltip>
                        <TooltipTrigger className="cursor-help text-destructive">
                          ⚠
                        </TooltipTrigger>
                        <TooltipContent>{summary.error}</TooltipContent>
                      </Tooltip>
                    )}
                  </div>
                </TableCell>
                <TableCell className="whitespace-nowrap">
                  <Tooltip>
                    <TooltipTrigger className="block max-w-full truncate">
                      {formatBytes(summary.wanted_bytes)}
                    </TooltipTrigger>
                    <TooltipContent>
                      Selected {formatBytes(summary.wanted_bytes)} of{" "}
                      {formatBytes(summary.total_length)}
                    </TooltipContent>
                  </Tooltip>
                </TableCell>
                <TableCell>
                {summary.state === "FetchingMetadata" ? (
                  <span className="block truncate text-xs text-info">
                    fetching metadata…
                  </span>
                ) : (
                    <div className="flex items-center gap-2">
                      <Progress
                        value={Math.min(Math.max(summary.progress * 100, 0), 100)}
                        aria-label={`${summary.name} progress`}
                        className="w-28"
                      />
                      <AnimatedPercent
                        value={Math.min(Math.max(summary.progress * 100, 0), 100)}
                        className="text-muted-foreground"
                      />
                    </div>
                  )}
                </TableCell>
                <TableCell className="overflow-hidden">
                  <Badge
                    variant={badgeVariantByState[summary.state] ?? "secondary"}
                    className={`max-w-full ${stateColor[summary.state] ?? ""}`}
                  >
                    <span className="min-w-0 truncate">
                      {stateLabel[summary.state] ?? summary.state}
                    </span>
                  </Badge>
                  {summary.error !== null && (
                    <Tooltip>
                      <TooltipTrigger className="max-w-full cursor-help truncate text-xs text-destructive">
                        {summary.error}
                      </TooltipTrigger>
                      <TooltipContent>{summary.error}</TooltipContent>
                    </Tooltip>
                  )}
                </TableCell>
                <TableCell className="whitespace-nowrap">
                  <AnimatedRate bytes={summary.download_rate} />
                </TableCell>
                <TableCell className="whitespace-nowrap">
                  <AnimatedRate bytes={summary.upload_rate} />
                </TableCell>
                <TableCell className="whitespace-nowrap">
                  <AnimatedRatio ratio={summary.ratio} />
                </TableCell>
                <TableCell className="whitespace-nowrap">
                  {formatEta(summary.eta_seconds)}
                </TableCell>
                <TableCell className="whitespace-nowrap">
                  <AnimatedCount value={summary.peer_count} />
                </TableCell>
                </ContextMenuTrigger>
                <ContextMenuContent className="w-56">
                  <ContextMenuItem onClick={() => onOpenFolder(summary.id)}>
                    <FolderOpenIcon />
                    Open Download Folder
                  </ContextMenuItem>
                  {pausable && (
                    <ContextMenuItem onClick={() => onPauseResume(summary)}>
                      <PauseIcon />
                      Pause
                    </ContextMenuItem>
                  )}
                  {resumable && (
                    <ContextMenuItem onClick={() => onPauseResume(summary)}>
                      <PlayIcon />
                      Resume
                    </ContextMenuItem>
                  )}
                  <ContextMenuItem onClick={() => onToggleStopSeeding(summary)}>
                    <CheckIcon
                      className={
                        summary.stop_after_complete ? "" : "opacity-0"
                      }
                    />
                    Stop Seeding When Done
                  </ContextMenuItem>
                  <ContextMenuSeparator />
                  <ContextMenuItem
                    onClick={() =>
                      navigator.clipboard.writeText(
                        `magnet:?xt=urn:btih:${summary.id}&dn=${encodeURIComponent(summary.name)}`,
                      )
                    }
                  >
                    <MagnetIcon />
                    Copy Magnet URI
                  </ContextMenuItem>
                  <ContextMenuSeparator />
                  <ContextMenuItem
                    variant="destructive"
                    onClick={() => onRemove(summary)}
                  >
                    <Trash2Icon />
                    Remove Torrent
                  </ContextMenuItem>
                  <ContextMenuSeparator />
                  <ContextMenuItem onClick={() => onRecheck(summary.id)}>
                    <RefreshCwIcon />
                    Force Re-check
                  </ContextMenuItem>
                </ContextMenuContent>
              </ContextMenu>
            );
          })
        )}
      </TableBody>
      </Table>
    </TooltipProvider>
  );
}
