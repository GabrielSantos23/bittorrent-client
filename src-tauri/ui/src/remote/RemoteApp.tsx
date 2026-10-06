import {
  useCallback,
  useEffect,
  useState,
  type FormEvent,
} from "react";
import {
  AlertCircleIcon,
  ArrowDownIcon,
  ArrowUpIcon,
  CheckIcon,
  EllipsisVerticalIcon,
  FolderIcon,
  FolderOpenIcon,
  PauseIcon,
  PlayIcon,
  PlusIcon,
  RefreshCwIcon,
  SearchIcon,
  Trash2Icon,
} from "lucide-react";

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
import { Checkbox } from "@/components/animate-ui/components/radix/checkbox";
import {
  AnimatedPercent,
  AnimatedRate,
} from "@/components/AnimatedValues";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Progress } from "@/components/ui/progress";
import { Separator } from "@/components/ui/separator";
import { Skeleton } from "@/components/ui/skeleton";
import TorrentFileTree from "@/components/TorrentFileTree";
import {
  badgeVariantByState,
  formatBytes,
  formatEta,
  stateColor,
  stateLabel,
} from "../format";
import { RemoteLinkError, remoteApi } from "./api";
import type { FilePriority } from "../../../../bt-core/bindings/FilePriority";
import type { TorrentDetail } from "../../../../bt-core/bindings/TorrentDetail";
import type { TorrentSummary } from "../../../../bt-core/bindings/TorrentSummary";

// States that can be paused; everything else (Paused, Stopped, Error) is
// started with Resume instead.
const ACTIVE_STATES = ["Downloading", "Seeding", "Checking", "FetchingMetadata"];

const OFFLINE_MESSAGE = "Cannot reach the client. Is the desktop app running?";

// Server errors are plain text, but proxies on the way may answer with HTML
// or JSON error pages nobody should read on a phone.
function friendlyError(error: unknown): string {
  if (error instanceof RemoteLinkError) {
    return error.message;
  }
  const message = error instanceof Error ? error.message : String(error);
  if (!message || message.startsWith("<") || message.startsWith("{")) {
    return OFFLINE_MESSAGE;
  }
  return message;
}

function clampPercent(summary: TorrentSummary): number {
  return Math.min(Math.max(summary.progress * 100, 0), 100);
}

// Per-state presentation, all from the shared theme: the folder tile and the
// progress fill shift with the torrent state, everything else stays put.
const TILE_BY_STATE: Record<string, { wrap: string; icon: string }> = {
  Error: { wrap: "bg-destructive/15", icon: "text-destructive" },
  Paused: { wrap: "bg-secondary", icon: "text-muted-foreground" },
  Stopped: { wrap: "bg-secondary", icon: "text-muted-foreground" },
};

const BAR_BY_STATE: Record<string, string> = {
  Error: "bg-destructive",
  Paused: "bg-muted-foreground/50",
  Stopped: "bg-muted-foreground/50",
};

const ICON_BY_STATE: Record<string, typeof ArrowDownIcon> = {
  Downloading: ArrowDownIcon,
  Seeding: ArrowUpIcon,
  Completed: CheckIcon,
  Checking: RefreshCwIcon,
  FetchingMetadata: SearchIcon,
  Paused: PauseIcon,
  Stopped: PauseIcon,
  Error: AlertCircleIcon,
};

export function RemoteApp() {
  const [torrents, setTorrents] = useState<TorrentSummary[] | null>(null);
  const [offline, setOffline] = useState<string | null>(null);
  const [menuTorrent, setMenuTorrent] = useState<TorrentSummary | null>(null);
  const [filesTorrent, setFilesTorrent] = useState<TorrentSummary | null>(null);
  const [detail, setDetail] = useState<TorrentDetail | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [pendingRemove, setPendingRemove] = useState<TorrentSummary | null>(null);
  const [deleteFiles, setDeleteFiles] = useState(false);
  const [addOpen, setAddOpen] = useState(false);
  const [magnetUri, setMagnetUri] = useState("");
  const [magnetMessage, setMagnetMessage] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const [busyIds, setBusyIds] = useState<Set<string>>(new Set());

  const refresh = useCallback(() => {
    remoteApi
      .list()
      .then((next) => {
        setTorrents(next);
        setOffline(null);
      })
      .catch((err) => setOffline(friendlyError(err)));
  }, []);

  useEffect(() => {
    let refreshing = false;
    const poll = () => {
      if (refreshing || document.hidden) return;
      refreshing = true;
      remoteApi
        .list()
        .then((next) => {
          setTorrents(next);
          setOffline(null);
        })
        .catch((err) => setOffline(friendlyError(err)))
        .finally(() => {
          refreshing = false;
        });
    };
    poll();
    const timer = window.setInterval(poll, 2000);
    const onVisibility = () => {
      if (!document.hidden) poll();
    };
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, []);

  const act = (summary: TorrentSummary, run: () => Promise<unknown>) => {
    setBusyIds((previous) => new Set(previous).add(summary.id));
    run()
      .then(refresh)
      .catch((err) => setOffline(friendlyError(err)))
      .finally(() =>
        setBusyIds((previous) => {
          const next = new Set(previous);
          next.delete(summary.id);
          return next;
        }),
      );
  };

  // The file tree stays live: poll the torrent's detail while its dialog is
  // open so verified bytes and skip states track the download.
  useEffect(() => {
    if (filesTorrent === null) return;
    let cancelled = false;
    let refreshing = false;
    const load = () => {
      if (refreshing || document.hidden) return;
      refreshing = true;
      remoteApi
        .detail(filesTorrent.id)
        .then((next) => {
          if (!cancelled) {
            setDetail(next);
            setDetailError(null);
          }
        })
        .catch((err) => {
          if (!cancelled) setDetailError(friendlyError(err));
        })
        .finally(() => {
          refreshing = false;
        });
    };
    load();
    const timer = window.setInterval(load, 2000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [filesTorrent]);

  const handleMenuFiles = () => {
    if (menuTorrent === null) return;
    setDetail(null);
    setDetailError(null);
    setFilesTorrent(menuTorrent);
    setMenuTorrent(null);
  };

  // Checkbox toggles in the file tree apply Skip/Normal to the touched files
  // (and whole folders at once). Optimistic: the live detail poll reconciles
  // with the session's truth shortly after.
  const handleToggleFiles = (indexes: number[], checked: boolean) => {
    if (filesTorrent === null || detail === null) return;
    const priority: FilePriority = checked ? "Normal" : "Skip";
    const touched = new Set(indexes);
    const nextFiles = detail.files.map((file, index) =>
      touched.has(index) ? { ...file, priority } : file,
    );
    setDetail({ ...detail, files: nextFiles });
    remoteApi
      .setPriorities(
        filesTorrent.id,
        nextFiles.map((file, index): [number, FilePriority] => [
          index,
          file.priority,
        ]),
      )
      .catch((err) => setOffline(friendlyError(err)));
  };

  const handleMenuPauseResume = () => {
    if (menuTorrent === null) return;
    const summary = menuTorrent;
    setMenuTorrent(null);
    const active = ACTIVE_STATES.includes(summary.state);
    act(summary, () =>
      active ? remoteApi.pause(summary.id) : remoteApi.resume(summary.id),
    );
  };

  const handleMenuRemove = () => {
    setPendingRemove(menuTorrent);
    setMenuTorrent(null);
  };

  const handleConfirmRemove = () => {
    if (pendingRemove === null) return;
    act(pendingRemove, () => remoteApi.remove(pendingRemove.id, deleteFiles));
    setPendingRemove(null);
    setDeleteFiles(false);
  };

  const handleAddMagnet = (event: FormEvent) => {
    event.preventDefault();
    const uri = magnetUri.trim();
    if (!uri || adding) return;
    setAdding(true);
    setMagnetMessage(null);
    remoteApi
      .addMagnet(uri)
      .then(() => {
        setAddOpen(false);
        setMagnetUri("");
        refresh();
      })
      .catch((err) => {
        setMagnetMessage(friendlyError(err));
      })
      .finally(() => setAdding(false));
  };

  const online = offline === null && torrents !== null;
  const count = torrents?.length ?? 0;

  return (
    <div className="mx-auto min-h-dvh w-full max-w-md pb-28">
      <header className="px-4 pt-[max(env(safe-area-inset-top),1.25rem)]">
        <h1 className="text-2xl font-bold tracking-tight">BitTorrent Client</h1>
        <p className="mt-1 flex items-center gap-1.5 text-sm text-muted-foreground">
          <span
            className={`size-1.5 shrink-0 rounded-full ${
              online ? "bg-accent" : "bg-destructive"
            }`}
          />
          {online
            ? `Connected · ${count} torrent${count === 1 ? "" : "s"}`
            : (offline ?? "Connecting…")}
        </p>
      </header>

      <section
        aria-label="Transfer speeds"
        className="mx-4 mt-4 flex items-stretch rounded-2xl border border-border bg-card py-5"
      >
        <div className="min-w-0 flex-1 px-3 text-center">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">
            Down speed
          </p>
          <p className="mt-1.5 text-2xl font-semibold">
            <AnimatedRate bytes={totalRate(torrents, "download_rate")} />
          </p>
        </div>
        <Separator orientation="vertical" />
        <div className="min-w-0 flex-1 px-3 text-center">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">
            Up speed
          </p>
          <p className="mt-1.5 text-2xl font-semibold">
            <AnimatedRate bytes={totalRate(torrents, "upload_rate")} />
          </p>
        </div>
      </section>

      <div className="mt-7 flex items-center justify-between px-4">
        <h2 className="text-xl font-bold tracking-tight">
          Torrents ({count})
        </h2>
        <Button
          variant="secondary"
          size="icon"
          className="size-10 rounded-full"
          onClick={refresh}
          aria-label="Refresh"
        >
          <RefreshCwIcon className="size-4.5" />
        </Button>
      </div>

      <main className="mt-3 space-y-4 px-4">
        {offline !== null && (
          <div
            role="alert"
            className="rounded-2xl border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive"
          >
            {offline}
          </div>
        )}

        {torrents === null ? (
          <LoadingCards />
        ) : torrents.length === 0 ? (
          <p className="py-20 text-center text-sm text-muted-foreground">
            No torrents yet. Tap + to add one.
          </p>
        ) : (
          torrents.map((summary) => (
            <TorrentCard
              key={summary.id}
              summary={summary}
              busy={busyIds.has(summary.id)}
              onMenu={() => setMenuTorrent(summary)}
            />
          ))
        )}
      </main>

      <Button
        variant="secondary"
        size="icon"
        onClick={() => {
          setMagnetMessage(null);
          setAddOpen(true);
        }}
        aria-label="Add magnet link"
        className="fixed bottom-[max(env(safe-area-inset-bottom),1.25rem)] right-5 z-20 size-14 rounded-2xl shadow-lg shadow-black/30"
      >
        <PlusIcon className="size-6" />
      </Button>

      <Dialog
        modal={true}
        open={addOpen}
        onOpenChange={(open) => {
          setAddOpen(open);
          if (!open) setMagnetMessage(null);
        }}
      >
        <DialogContent className="w-full max-w-sm gap-4">
          <DialogHeader>
            <DialogTitle>Add magnet</DialogTitle>
            <DialogDescription>
              The link is added to the desktop client and starts downloading.
            </DialogDescription>
          </DialogHeader>
          <form className="grid gap-3" onSubmit={handleAddMagnet}>
            <Input
              value={magnetUri}
              onChange={(event) => setMagnetUri(event.target.value)}
              placeholder="magnet:?xt=urn:btih:…"
              inputMode="url"
              autoComplete="off"
              aria-label="Magnet link"
            />
            {magnetMessage !== null && (
              <p className="text-sm text-destructive">{magnetMessage}</p>
            )}
            <div className="flex justify-end gap-2">
              <Button
                type="button"
                variant="outline"
                onClick={() => setAddOpen(false)}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={adding || magnetUri.trim() === ""}>
                Add
              </Button>
            </div>
          </form>
        </DialogContent>
      </Dialog>

      <Dialog
        modal={true}
        open={menuTorrent !== null}
        onOpenChange={(open) => {
          if (!open) setMenuTorrent(null);
        }}
      >
        <DialogContent
          className="w-full max-w-xs gap-3"
          aria-describedby={undefined}
        >
          <DialogHeader>
            <DialogTitle className="truncate pr-4 text-sm">
              {menuTorrent?.name}
            </DialogTitle>
            <DialogDescription className="sr-only">
              Torrent actions
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-2">
            <Button variant="outline" onClick={handleMenuFiles}>
              <FolderOpenIcon />
              Files
            </Button>
            {menuTorrent !== null &&
            ACTIVE_STATES.includes(menuTorrent.state) ? (
              <Button variant="outline" onClick={handleMenuPauseResume}>
                <PauseIcon />
                Pause
              </Button>
            ) : (
              <Button variant="outline" onClick={handleMenuPauseResume}>
                <PlayIcon />
                Resume
              </Button>
            )}
            <Button
              variant="outline"
              className="text-destructive hover:text-destructive"
              onClick={handleMenuRemove}
            >
              <Trash2Icon />
              Remove
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      <Dialog
        modal={true}
        open={filesTorrent !== null}
        onOpenChange={(open) => {
          if (!open) setFilesTorrent(null);
        }}
      >
        <DialogContent className="max-h-[85vh] w-full max-w-md overflow-auto">
          <DialogHeader>
            <DialogTitle className="truncate pr-4 text-sm">
              {filesTorrent?.name}
            </DialogTitle>
            <DialogDescription>
              {detailError !== null
                ? "Could not load the file list."
                : detail === null
                  ? "Loading files…"
                  : `${detail.files.length} file${detail.files.length === 1 ? "" : "s"}`}
            </DialogDescription>
          </DialogHeader>
          {detailError !== null ? (
            <div className="grid gap-3">
              <p className="text-sm text-destructive">{detailError}</p>
              <Button variant="outline" onClick={() => setFilesTorrent(null)}>
                Close
              </Button>
            </div>
          ) : detail === null ? (
            <div className="space-y-2">
              <Skeleton className="h-10 w-full" />
              <Skeleton className="ml-6 h-10 w-5/6" />
              <Skeleton className="h-10 w-full" />
            </div>
          ) : detail.files.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No files yet — the metadata is still being fetched.
            </p>
          ) : (
            <TorrentFileTree
              files={detail.files}
              onToggleFile={handleToggleFiles}
            />
          )}
        </DialogContent>
      </Dialog>

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
            <AlertDialogTitle className="min-w-0">
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
    </div>
  );
}

function totalRate(
  torrents: TorrentSummary[] | null,
  key: "download_rate" | "upload_rate",
): number {
  return (torrents ?? []).reduce((acc, torrent) => acc + torrent[key], 0);
}

function LoadingCards() {
  return (
    <>
      {[0, 1, 2].map((index) => (
        <div
          key={index}
          className="rounded-2xl border border-border bg-card p-4"
        >
          <div className="flex items-start gap-3">
            <Skeleton className="size-11 rounded-xl" />
            <div className="flex-1 space-y-2 py-0.5">
              <Skeleton className="h-4 w-3/4" />
              <Skeleton className="h-5 w-28 rounded-md" />
            </div>
          </div>
          <Skeleton className="mt-3 h-3.5 w-1/2" />
          <Skeleton className="mt-2.5 h-1.5 w-full" />
          <Skeleton className="mt-2.5 h-3.5 w-2/3" />
        </div>
      ))}
    </>
  );
}

interface TorrentCardProps {
  summary: TorrentSummary;
  busy: boolean;
  onMenu: () => void;
}

function TorrentCard({ summary, busy, onMenu }: TorrentCardProps) {
  const pct = clampPercent(summary);
  const tile = TILE_BY_STATE[summary.state] ?? {
    wrap: "bg-accent/15",
    icon: "text-accent",
  };
  const StateIcon = ICON_BY_STATE[summary.state];
  return (
    <article
      className={`rounded-2xl border border-border bg-card p-4 ${
        busy ? "opacity-70" : ""
      }`}
    >
      <div className="flex items-start gap-3">
        <div
          className={`flex size-11 shrink-0 items-center justify-center rounded-xl ${tile.wrap}`}
        >
          <FolderIcon className={`size-5.5 ${tile.icon}`} />
        </div>
        <div className="min-w-0 flex-1">
          <h3 className="truncate text-sm font-semibold leading-snug" title={summary.name}>
            {summary.name}
          </h3>
          <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
            <Badge
              variant={badgeVariantByState[summary.state] ?? "secondary"}
              className={`gap-1 ${stateColor[summary.state] ?? ""}`}
            >
              {StateIcon && <StateIcon className="size-3" />}
              <span className="min-w-0 truncate">
                {stateLabel[summary.state] ?? summary.state}
              </span>
            </Badge>
          </div>
        </div>
        <Button
          variant="ghost"
          size="icon"
          className="-mr-1.5 -mt-1 size-8 shrink-0 rounded-full text-muted-foreground"
          disabled={busy}
          onClick={onMenu}
          aria-label="Torrent actions"
        >
          <EllipsisVerticalIcon className="size-4.5" />
        </Button>
      </div>

      <p className="mt-3 truncate text-sm tabular-nums text-muted-foreground">
        {formatBytes(summary.verified_bytes)} / {formatBytes(summary.total_length)}
        {" • "}
        {summary.state === "FetchingMetadata"
          ? "fetching metadata…"
          : `ETA ${formatEta(summary.eta_seconds)}`}
      </p>

      <Progress
        value={pct}
        aria-label={`${summary.name} progress`}
        indicatorClassName={BAR_BY_STATE[summary.state]}
        className="mt-2.5 h-1.5"
      />

      <div className="mt-2.5 flex items-center justify-between text-sm">
        <p className="min-w-0 tabular-nums text-muted-foreground">
          <AnimatedRate bytes={summary.download_rate} />
          <span className="mx-1 text-muted-foreground/50">/</span>
          <AnimatedRate bytes={summary.upload_rate} />
        </p>
        <AnimatedPercent
          value={pct}
          className="shrink-0 tabular-nums text-muted-foreground"
        />
      </div>

      {summary.error !== null && (
        <p className="mt-2 text-xs text-destructive">{summary.error}</p>
      )}
    </article>
  );
}
