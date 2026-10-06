import { useEffect, useState } from "react";
import {
  CopyIcon,
  CheckIcon,
  LinkIcon,
  LoaderCircleIcon,
  PlayIcon,
  RotateCwIcon,
  SmartphoneIcon,
  UnlinkIcon,
} from "lucide-react";
import QRCode from "react-qr-code";

import { api } from "../api";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import type { RemoteStatus } from "../../../bindings/RemoteStatus";

interface RemoteAccessModalProps {
  open: boolean;
  onClose: () => void;
}

type Phase = "loading" | "running" | "stopped" | "error";

// Visual states for the status card, mirroring the "Waiting for phone /
// Ready" pairing card: the badge dot is amber while waiting for the first
// phone connection, green once a device has paired.
function phaseOf(status: RemoteStatus | null, error: string | null): Phase {
  if (status === null) return error === null ? "loading" : "error";
  return status.running ? "running" : "stopped";
}

function StatusBadge({
  label,
  tone,
}: {
  label: string;
  tone: "waiting" | "connected" | "off" | "error";
}) {
  const dot =
    tone === "connected"
      ? "bg-emerald-500"
      : tone === "waiting"
        ? "bg-amber-500"
        : tone === "error"
          ? "bg-destructive"
          : "bg-muted-foreground";
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full border border-border px-2 py-0.5 text-xs text-muted-foreground">
      <span className={`size-1.5 rounded-full ${dot}`} />
      {label}
    </span>
  );
}

export function RemoteAccessModal({ open, onClose }: RemoteAccessModalProps) {
  const [status, setStatus] = useState<RemoteStatus | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const [busyAction, setBusyAction] = useState<string | null>(null);

  // Opening the modal starts the server automatically; it keeps running
  // afterwards so the phone stays connected until Stop is pressed.
  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    setStatus(null);
    setError(null);
    api
      .remoteStart()
      .then((next) => {
        if (!cancelled) setStatus(next);
      })
      .catch((err) => {
        if (!cancelled) setError(String(err));
      });
    const unlisten = api.onRemoteStatus((next) => {
      if (!cancelled) {
        setStatus(next);
        setError(null);
      }
    });
    return () => {
      cancelled = true;
      unlisten.then((stop) => stop());
    };
  }, [open]);

  const run = (name: string, action: () => Promise<RemoteStatus>) => {
    setBusyAction(name);
    setError(null);
    action()
      .then((next) => setStatus(next))
      .catch((err) => setError(String(err)))
      .finally(() => setBusyAction(null));
  };

  const handleCopy = () => {
    if (status?.url == null) return;
    navigator.clipboard.writeText(status.url).then(() => {
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    });
  };

  const phase = phaseOf(status, error);
  const running = phase === "running" && status !== null;
  const connected = running && status.connected;
  const url = running ? status.url : null;

  const title = running
    ? connected
      ? "Connected"
      : "Waiting for phone"
    : phase === "stopped"
      ? "Remote access is off"
      : phase === "error"
        ? "Couldn't start remote access"
        : "Starting remote access…";
  const badge =
    phase === "error" ? (
      <StatusBadge label="Error" tone="error" />
    ) : running ? (
      connected ? (
        <StatusBadge label="Connected" tone="connected" />
      ) : (
        <StatusBadge label="Ready" tone="waiting" />
      )
    ) : phase === "stopped" ? (
      <StatusBadge label="Stopped" tone="off" />
    ) : (
      <StatusBadge label="Starting" tone="waiting" />
    );
  const description = running
    ? connected
      ? "Your phone is controlling this client. Keep this window's Stop button in mind when you walk away."
      : "Scan the QR code or open the link on your phone."
    : phase === "stopped"
      ? "Press Start to allow control from your phone on the same network."
      : phase === "error"
        ? (error ?? "Something went wrong.")
        : "Preparing the QR code…";

  return (
    <Dialog
      modal={true}
      open={open}
      onOpenChange={(dialogOpen) => {
        if (!dialogOpen) onClose();
      }}
    >
      <DialogContent className="w-full max-w-md gap-4">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-base">
            <SmartphoneIcon className="size-4.5 text-accent" />
            Scan from phone
          </DialogTitle>
          <DialogDescription>
            Use your phone camera to open this client remotely.
          </DialogDescription>
        </DialogHeader>

        <div className="rounded-lg border border-border bg-card p-4">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <span className="text-sm font-semibold">{title}</span>
                {badge}
              </div>
              <p className="mt-1 text-sm text-muted-foreground">{description}</p>
            </div>
            {running ? (
              <Button
                variant="outline"
                size="sm"
                className="shrink-0"
                disabled={busyAction !== null}
                onClick={() => run("stop", api.remoteStop)}
              >
                <UnlinkIcon className="size-4" />
                Stop
              </Button>
            ) : (
              <Button
                size="sm"
                className="shrink-0 bg-accent text-accent-foreground hover:bg-accent/90"
                disabled={busyAction !== null}
                onClick={() => run("start", api.remoteStart)}
              >
                {phase === "loading" ? (
                  <LoaderCircleIcon className="size-4 animate-spin" />
                ) : (
                  <PlayIcon className="size-4" />
                )}
                Start
              </Button>
            )}
          </div>

          {running && (
            <div className="mt-3 flex items-center justify-between gap-3 border-t border-border pt-3">
              <p className="min-w-0 text-sm text-muted-foreground">
                Can't scan? Open the link on your phone.
              </p>
              <div className="flex shrink-0 gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  disabled={busyAction !== null}
                  title="Generates a new link; the previous one stops working"
                  onClick={() => run("refresh", api.remoteRefreshToken)}
                >
                  <RotateCwIcon className="size-4" />
                  Refresh QR
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={handleCopy}
                >
                  {copied ? (
                    <CheckIcon className="size-4 text-emerald-500" />
                  ) : (
                    <CopyIcon className="size-4" />
                  )}
                  {copied ? "Copied" : "Copy link"}
                </Button>
              </div>
            </div>
          )}
        </div>

        <div className="flex min-h-80 items-center justify-center rounded-lg border border-dashed border-border bg-sidebar p-4">
          {running && url !== null ? (
            <div className="rounded-xl bg-white p-3 shadow-lg">
              <QRCode value={url} size={224} bgColor="#ffffff" fgColor="#0b0d12" />
            </div>
          ) : phase === "loading" ? (
            <div className="flex flex-col items-center gap-3 text-muted-foreground">
              <LoaderCircleIcon className="size-6 animate-spin" />
              <span className="text-sm">Preparing the QR code…</span>
            </div>
          ) : (
            <div className="flex flex-col items-center gap-3 px-6 text-center text-muted-foreground">
              <LinkIcon className="size-6" />
              <span className="text-sm">
                {phase === "error"
                  ? "Fix the problem below and press Start to show the QR code."
                  : "Start remote access to show the QR code."}
              </span>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
