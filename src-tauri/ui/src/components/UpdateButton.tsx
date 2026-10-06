import { useCallback, useEffect, useRef, useState } from "react";
import { check, type Update } from "@tauri-apps/plugin-updater";
import { relaunch } from "@tauri-apps/plugin-process";
import {
  DownloadIcon,
  ExternalLinkIcon,
  RefreshCwIcon,
} from "lucide-react";

import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from "@/components/animate-ui/components/radix/hover-card";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/animate-ui/components/radix/tooltip";
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

// Where "more changes on GitHub" points when a release carries extra notes.
const RELEASE_PAGE = "https://github.com/GabrielSantos23/bittorrent-client/releases/latest";

// The release check also runs on its own so the dot shows up without the
// user asking for it.
const CHECK_INTERVAL_MS = 30 * 60 * 1000;

type Phase = "idle" | "checking" | "available" | "downloading" | "downloaded" | "installing";

interface ChangeEntry {
  message: string;
}

// Release bodies are markdown commit lists ("- fix(core): ..."); each bullet
// becomes one changelog entry. Anything else in the body is ignored.
function changelogFrom(update: Update): ChangeEntry[] {
  const lines = (update.body ?? "")
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.startsWith("- ") || line.startsWith("* "))
    .map((line) => ({ message: line.replace(/^[-*]\s+/, "").trim() }));
  if (lines.length > 0) return lines;
  const fallback = (update.body ?? "").trim();
  return fallback ? [{ message: fallback }] : [];
}

const captionButton =
  "relative flex h-full w-[46px] shrink-0 items-center justify-center text-foreground/90 transition-colors hover:bg-foreground/10 active:bg-foreground/[0.15] focus-visible:outline-none disabled:pointer-events-none disabled:opacity-60";

const RING_RADIUS = 11;
const RING_CIRCUMFERENCE = 2 * Math.PI * RING_RADIUS;

export default function UpdateButton() {
  const [phase, setPhase] = useState<Phase>("idle");
  const [progress, setProgress] = useState(0);
  const [update, setUpdate] = useState<Update | null>(null);
  const [installOpen, setInstallOpen] = useState(false);
  const updateRef = useRef<Update | null>(null);

  const runCheck = useCallback(async () => {
    setPhase((current) => {
      // Never interrupt a download/install or override a found update with
      // a periodic check that comes back empty-handed.
      if (current !== "idle" && current !== "checking") return current;
      return "checking";
    });
    try {
      const found = await check();
      if (found) {
        updateRef.current = found;
        setUpdate(found);
        setPhase((current) =>
          current === "checking" || current === "idle" ? "available" : current,
        );
      } else {
        setPhase((current) => (current === "checking" ? "idle" : current));
      }
    } catch (err) {
      // No release published yet, offline, or GitHub unreachable: stay quiet.
      console.warn("[update] check failed", err);
      setPhase((current) => (current === "checking" ? "idle" : current));
    }
  }, []);

  // Periodic background check alongside the button.
  useEffect(() => {
    const timer = window.setInterval(() => {
      void runCheck();
    }, CHECK_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, [runCheck]);

  const startDownload = async () => {
    const current = updateRef.current;
    if (current === null || phase === "downloading") return;
    setPhase("downloading");
    setProgress(0);
    try {
      let total: number | null = null;
      let downloaded = 0;
      await current.downloadAndInstall((event) => {
        switch (event.event) {
          case "Started":
            total = event.data.contentLength ?? null;
            break;
          case "Progress":
            downloaded += event.data.chunkLength;
            if (total !== null && total > 0) {
              setProgress(Math.min(100, (downloaded / total) * 100));
            }
            break;
          case "Finished":
            setProgress(100);
            break;
        }
      });
      setProgress(100);
      setPhase("downloaded");
    } catch (err) {
      console.error("[update] download failed", err);
      setPhase("available");
    }
  };

  const startInstall = async () => {
    setInstallOpen(false);
    setPhase("installing");
    // On Windows the NSIS installer replaces the app; relaunching afterwards
    // (or by the installer itself) brings the new version up.
    try {
      await relaunch();
    } catch (err) {
      console.error("[update] relaunch failed", err);
      setPhase("idle");
    }
  };

  const busy = phase === "checking" || phase === "installing";

  const icon = busy ? (
    <RefreshCwIcon className="size-3.5 animate-spin" />
  ) : (
    <DownloadIcon className="size-3.5" />
  );

  const button = (
    <button
      type="button"
      className={captionButton}
      disabled={busy || phase === "downloading"}
      aria-label={ariaLabelFor(phase)}
      onClick={
        phase === "idle"
          ? () => void runCheck()
          : phase === "available"
            ? () => void startDownload()
            : phase === "downloaded"
              ? () => setInstallOpen(true)
              : undefined
      }
    >
      {phase === "downloading" ? (
        <span className="pointer-events-none absolute inset-0 m-auto flex h-7 w-7 items-center justify-center">
          <svg viewBox="0 0 28 28" className="size-7 -rotate-90">
            <circle
              cx="14"
              cy="14"
              r={RING_RADIUS}
              fill="none"
              strokeWidth="2.5"
              className="stroke-border"
            />
            <circle
              cx="14"
              cy="14"
              r={RING_RADIUS}
              fill="none"
              strokeWidth="2.5"
              strokeLinecap="round"
              strokeDasharray={RING_CIRCUMFERENCE}
              strokeDashoffset={RING_CIRCUMFERENCE * (1 - progress / 100)}
              className="stroke-accent transition-[stroke-dashoffset] duration-100 ease-linear"
            />
          </svg>
          <DownloadIcon className="absolute size-3.5" />
        </span>
      ) : (
        icon
      )}
      {(phase === "available" || phase === "downloaded") && (
        <span
          aria-hidden
          className={`absolute right-2 top-1.5 size-2 rounded-full ring-2 ring-background ${
            phase === "downloaded" ? "bg-accent" : "bg-white"
          }`}
        />
      )}
    </button>
  );

  // Update ready (or already downloaded): the hover card with the changelog
  // rides on the button, exactly like the reference layout.
  if (phase === "available" || phase === "downloaded") {
    const changes = update === null ? [] : changelogFrom(update);
    return (
      <>
        <HoverCard openDelay={100} closeDelay={150}>
          <HoverCardTrigger asChild>{button}</HoverCardTrigger>
          <HoverCardContent
            side="bottom"
            align="end"
            className="w-[380px] gap-0 p-4"
          >
            <p className="text-sm font-semibold">Update ready to download</p>
            <p className="mt-0.5 text-xs text-muted-foreground">
              {update?.version}
            </p>
            <div className="mt-3 max-h-72 overflow-y-auto pr-1">
              <p className="text-sm font-semibold">What's changed</p>
              {changes.length > 0 ? (
                <ul className="mt-2 list-disc space-y-3 pl-4 text-[13px] leading-relaxed">
                  {changes.map((change, index) => (
                    <li key={index}>{change.message}</li>
                  ))}
                </ul>
              ) : (
                <p className="mt-2 text-[13px] text-muted-foreground">
                  No release notes were provided for this version.
                </p>
              )}
            </div>
            <a
              href={RELEASE_PAGE}
              target="_blank"
              rel="noreferrer"
              className="mt-3 flex items-center gap-1 border-t border-border pt-3 text-xs text-muted-foreground underline underline-offset-2 hover:text-foreground"
            >
              See this release on GitHub
              <ExternalLinkIcon className="size-3" />
            </a>
          </HoverCardContent>
        </HoverCard>
        <InstallDialog
          open={installOpen}
          version={update?.version ?? ""}
          onOpenChange={setInstallOpen}
          onConfirm={() => void startInstall()}
        />
      </>
    );
  }

  return (
    <>
      <Tooltip>
        <TooltipTrigger asChild>{button}</TooltipTrigger>
        <TooltipContent side="bottom">{tooltipLabelFor(phase)}</TooltipContent>
      </Tooltip>
      <InstallDialog
        open={installOpen}
        version={update?.version ?? ""}
        onOpenChange={setInstallOpen}
        onConfirm={() => void startInstall()}
      />
    </>
  );
}

function ariaLabelFor(phase: Phase): string {
  switch (phase) {
    case "idle":
      return "Check for updates";
    case "checking":
      return "Checking for updates";
    case "available":
      return "Update available — download";
    case "downloading":
      return "Downloading update";
    case "downloaded":
      return "Update downloaded — click to install";
    case "installing":
      return "Installing update";
  }
}

function tooltipLabelFor(phase: Phase): string {
  switch (phase) {
    case "idle":
      return "Check for updates";
    case "checking":
      return "Checking for updates…";
    case "installing":
      return "Installing update…";
    default:
      return ariaLabelFor(phase);
  }
}

function InstallDialog({
  open,
  version,
  onOpenChange,
  onConfirm,
}: {
  open: boolean;
  version: string;
  onOpenChange: (open: boolean) => void;
  onConfirm: () => void;
}) {
  return (
    <AlertDialog
      open={open}
      onOpenChange={(next) => {
        if (!next) onOpenChange(false);
      }}
    >
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>
            Install update {version} and restart BitTorrent Client?
          </AlertDialogTitle>
          <AlertDialogDescription>
            Any running tasks will be interrupted. Make sure you're ready
            before continuing.
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>Cancel</AlertDialogCancel>
          <AlertDialogAction
            className="bg-accent text-accent-foreground hover:bg-accent/90"
            onClick={onConfirm}
          >
            Confirm
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
