import { useMemo, useState } from "react";
import { FileWarningIcon, Link2Icon, LoaderCircleIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/animate-ui/components/radix/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Files,
  FolderItem,
  FolderContent,
} from "@/components/animate-ui/components/radix/files";
import {
  FolderHeader as FolderHeaderPrimitive,
  FolderTrigger as FolderTriggerPrimitive,
  FolderHighlight as FolderHighlightPrimitive,
  Folder as FolderPrimitive,
  FolderIcon as FolderIconPrimitive,
  FileHighlight as FileHighlightPrimitive,
  File as FilePrimitive,
  FileLabel as FileLabelPrimitive,
} from "@/components/animate-ui/primitives/radix/files";
import { FolderIcon, FolderOpenIcon } from "lucide-react";
import { formatBytes } from "../format";
import type { TorrentInspection } from "../../../bindings/TorrentInspection";
import type { TorrentDetail } from "../../../../bt-core/bindings/TorrentDetail";
import type { FilePriority } from "../../../../bt-core/bindings/FilePriority";

export type PendingTorrent =
  | { kind: "file"; path: string; inspection: TorrentInspection }
  // A .torrent that could not be inspected (unreadable, or a backend that
  // predates inspect_torrent); the modal still shows it and can add it plain.
  | { kind: "file-unreadable"; path: string; reason: string }
  | { kind: "magnet"; uri: string };

export type AddDecision =
  | { kind: "file"; path: string; filePriorities: [number, FilePriority][] }
  | { kind: "file-plain"; path: string }
  // Checked "start immediately": add and download as soon as metadata lands.
  | { kind: "magnet"; uri: string; pauseAfterMetadata: boolean }
  // Two-phase magnet: already added paused; apply the picked priorities and
  // resume.
  | { kind: "magnet-files"; id: string; filePriorities: [number, FilePriority][] }
  // Two-phase magnet: metadata never arrived; leave it paused in the list.
  | { kind: "magnet-keep-paused"; id: string };

interface AddTorrentModalProps {
  open: boolean;
  torrents: PendingTorrent[];
  detail: TorrentDetail | null;
  onAdd: (decisions: AddDecision[]) => Promise<void>;
  onAddMagnet: (uri: string) => Promise<string>;
  onClose: () => void;
}

interface TreeNode {
  name: string;
  path: string;
  folders: TreeNode[];
  files: { index: number; name: string; length: number }[];
}

interface TreeSource {
  name: string;
  total_length: number;
  files: { index: number; path: string[]; length: number }[];
}

function buildTree(inspection: TreeSource): TreeNode {
  const root: TreeNode = {
    name: inspection.name,
    path: "",
    folders: [],
    files: [],
  };
  for (const file of inspection.files) {
    let node = root;
    for (let depth = 0; depth < file.path.length - 1; depth++) {
      const path = file.path.slice(0, depth + 1).join("/");
      let next = node.folders.find((folder) => folder.path === path);
      if (next === undefined) {
        next = {
          name: file.path[depth],
          path,
          folders: [],
          files: [],
        };
        node.folders.push(next);
      }
      node = next;
    }
    node.files.push({
      index: file.index,
      name: file.path[file.path.length - 1],
      length: file.length,
    });
  }
  return root;
}

function descendantIndices(node: TreeNode, into: number[]): number[] {
  for (const file of node.files) {
    into.push(file.index);
  }
  for (const folder of node.folders) {
    descendantIndices(folder, into);
  }
  return into;
}

function sortedValues(set: Set<number>): number[] {
  return [...set].sort((a, b) => a - b);
}

function displayNameForUri(uri: string): string {
  const match = uri.match(/[?&]dn=([^&]+)/);
  if (match === null) return "Magnet torrent";
  try {
    return decodeURIComponent(match[1]).replace(/\n/g, " ").trim() || "Magnet torrent";
  } catch {
    return "Magnet torrent";
  }
}

export function AddTorrentModal({
  open,
  torrents,
  detail,
  onAdd,
  onAddMagnet,
  onClose,
}: AddTorrentModalProps) {
  return (
    <Dialog
      modal={true}
      open={open && torrents.length > 0}
      onOpenChange={(dialogOpen) => {
        if (!dialogOpen) onClose();
      }}
    >
      <DialogContent className="max-h-[85vh] w-full max-w-2xl overflow-auto">
        {torrents.length > 0 && (
          <AddTorrentBody
            torrents={torrents}
            detail={detail}
            onAdd={onAdd}
            onAddMagnet={onAddMagnet}
            onClose={onClose}
          />
        )}
      </DialogContent>
    </Dialog>
  );
}

function AddTorrentBody({
  torrents,
  detail,
  onAdd,
  onAddMagnet,
  onClose,
}: {
  torrents: PendingTorrent[];
  detail: TorrentDetail | null;
  onAdd: (decisions: AddDecision[]) => Promise<void>;
  onAddMagnet: (uri: string) => Promise<string>;
  onClose: () => void;
}) {
  const [selected, setSelected] = useState(0);
  // File indices to skip (priority Skip) per pending torrent; everything
  // checked by default.
  const [skipped, setSkipped] = useState<Record<number, Set<number>>>({});
  const [uris, setUris] = useState<Record<number, string>>({});
  // Torrent ids for magnets added in two-phase mode (added paused, waiting
  // for the metadata so the file tree can be picked in this modal).
  const [addedMagnets, setAddedMagnets] = useState<Record<number, string>>({});
  const [startImmediately, setStartImmediately] = useState<
    Record<number, boolean>
  >({});
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const uriFor = (index: number) =>
    uris[index] ?? (torrents[index].kind === "magnet" ? torrents[index].uri : "");

  const current = torrents[selected];
  const currentSkipped = skipped[selected] ?? new Set<number>();

  const magnetId =
    current?.kind === "magnet" ? addedMagnets[selected] : undefined;
  const magnetDetail =
    magnetId !== undefined && detail !== null && detail.id === magnetId
      ? detail
      : null;
  const magnetReady =
    magnetDetail !== null && magnetDetail.files.length > 0;

  const treeSource: TreeSource | null = useMemo(() => {
    if (current === undefined) return null;
    if (current.kind === "file") {
      return {
        name: current.inspection.name,
        total_length: current.inspection.total_length,
        files: current.inspection.files.map((file) => ({
          index: file.index,
          path: file.path,
          length: file.length,
        })),
      };
    }
    if (current.kind === "magnet" && magnetReady && magnetDetail !== null) {
      const files = magnetDetail.files.map((file, index) => ({
        index,
        path: file.path.split("/"),
        length: file.length,
      }));
      return {
        name: displayNameForUri(uriFor(selected)),
        total_length: files.reduce((total, file) => total + file.length, 0),
        files,
      };
    }
    return null;
    // uriFor(selected) is derived from uris state; including it directly
    // keeps the display name fresh while typing.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [current, magnetReady, magnetDetail, selected, uris]);

  const tree = treeSource !== null ? buildTree(treeSource) : null;

  const allReady = torrents.every((torrent, index) => {
    if (torrent.kind !== "magnet") return true;
    const id = addedMagnets[index];
    if (id === undefined) {
      return uriFor(index).trim().startsWith("magnet:?");
    }
    const magnetState =
      detail !== null && detail.id === id && detail.files.length > 0;
    return magnetState;
  });

  const selectedBytes = (() => {
    if (treeSource === null) return null;
    return treeSource.files
      .filter((file) => !currentSkipped.has(file.index))
      .reduce((total, file) => total + file.length, 0);
  })();

  const toggleFile = (index: number, checked: boolean) => {
    setSkipped((previous) => {
      const next = new Set(previous[selected] ?? []);
      if (checked) {
        next.delete(index);
      } else {
        next.add(index);
      }
      return { ...previous, [selected]: next };
    });
  };

  const toggleFolder = (node: TreeNode, checked: boolean) => {
    const indices = new Set(descendantIndices(node, []));
    setSkipped((previous) => {
      const next = new Set(previous[selected] ?? []);
      for (const index of indices) {
        if (checked) {
          next.delete(index);
        } else {
          next.add(index);
        }
      }
      return { ...previous, [selected]: next };
    });
  };

  const folderState = (node: TreeNode): boolean | "indeterminate" => {
    const indices = descendantIndices(node, []);
    const skippedCount = indices.filter((index) =>
      currentSkipped.has(index),
    ).length;
    if (skippedCount === 0) return true;
    if (skippedCount === indices.length) return false;
    return "indeterminate";
  };

  // The checkbox sits OUTSIDE the accordion trigger so that selecting a
  // folder never expands or collapses it, and expanding never selects.
  const renderFolder = (node: TreeNode) => (
    <FolderItem key={node.path || node.name} value={node.path || node.name}>
      <FolderHeaderPrimitive>
        <div className="flex w-full items-center gap-2 p-2">
          <Checkbox
            className="shrink-0"
            checked={folderState(node)}
            onCheckedChange={(checked) => toggleFolder(node, checked === true)}
          />
          <FolderTriggerPrimitive className="w-full text-start">
            <FolderHighlightPrimitive>
              <FolderPrimitive className="flex items-center gap-2 p-1 pointer-events-none">
                <FolderIconPrimitive
                  closeIcon={<FolderIcon className="size-4.5" />}
                  openIcon={<FolderOpenIcon className="size-4.5" />}
                />
                <FileLabelPrimitive className="text-sm">
                  {node.name}
                </FileLabelPrimitive>
              </FolderPrimitive>
            </FolderHighlightPrimitive>
          </FolderTriggerPrimitive>
        </div>
      </FolderHeaderPrimitive>
      <FolderContent>
        {node.folders.map(renderFolder)}
        {node.files.map((file) => (
          <FileHighlightPrimitive key={file.index}>
            <FilePrimitive className="flex items-center gap-2 p-2 pl-8 pointer-events-none">
              <Checkbox
                className="pointer-events-auto shrink-0"
                checked={!currentSkipped.has(file.index)}
                onCheckedChange={(checked) =>
                  toggleFile(file.index, checked === true)
                }
              />
              <FileLabelPrimitive className="text-sm">
                {file.name}
              </FileLabelPrimitive>
              <span className="text-xs text-muted-foreground">
                {formatBytes(file.length)}
              </span>
            </FilePrimitive>
          </FileHighlightPrimitive>
        ))}
      </FolderContent>
    </FolderItem>
  );

  const isMagnetFetching =
    current?.kind === "magnet" && magnetId !== undefined && !magnetReady;

  const handleAdd = () => {
    setSubmitting(true);
    setError(null);
    const decisions: AddDecision[] = torrents.map((torrent, index) => {
      if (torrent.kind === "file-unreadable") {
        return { kind: "file-plain", path: torrent.path };
      }
      if (torrent.kind === "file") {
        const skip = skipped[index] ?? new Set<number>();
        return {
          kind: "file",
          path: torrent.path,
          filePriorities: torrent.inspection.files.map((file) => [
            file.index,
            (skip.has(file.index) ? "Skip" : "Normal") as FilePriority,
          ]),
        };
      }
      const id = addedMagnets[index];
      if (id !== undefined) {
        if (detail !== null && detail.id === id && detail.files.length > 0) {
          const skip = skipped[index] ?? new Set<number>();
          return {
            kind: "magnet-files",
            id,
            filePriorities: detail.files.map((_, fileIndex) => [
              fileIndex,
              (skip.has(fileIndex) ? "Skip" : "Normal") as FilePriority,
            ]),
          };
        }
        return { kind: "magnet-keep-paused", id };
      }
      return {
        kind: "magnet",
        uri: uriFor(index).trim(),
        pauseAfterMetadata: startImmediately[index] ?? false,
      };
    });
    onAdd(decisions)
      .then(() => onClose())
      .catch((err) => setError(String(err)))
      .finally(() => setSubmitting(false));
  };

  const handleAddMagnet = () => {
    setError(null);
    onAddMagnet(uriFor(selected).trim())
      .then((id) => {
        setAddedMagnets((previous) => ({ ...previous, [selected]: id }));
      })
      .catch((err) => setError(String(err)));
  };

  const addButtonLabel = (() => {
    if (submitting) return "Adding…";
    if (isMagnetFetching) return "Keep paused";
    if (current?.kind === "magnet" && magnetReady) return "Start";
    return "Add";
  })();

  return (
    <div className="grid gap-4">
      <DialogHeader>
        <DialogTitle>
          Add torrent{torrents.length === 1 ? "" : `s (${torrents.length})`}
        </DialogTitle>
        <DialogDescription>
          Unchecked files are skipped and never downloaded.
        </DialogDescription>
      </DialogHeader>

      {torrents.length > 1 && (
        <div className="flex flex-wrap gap-2">
          {torrents.map((torrent, index) => (
            <button
              key={
                torrent.kind === "magnet" ? `magnet-${index}` : torrent.path
              }
              type="button"
              onClick={() => setSelected(index)}
              className={
                index === selected
                  ? "flex max-w-[16rem] items-center gap-2 rounded-md bg-accent px-3 py-2 text-start text-sm text-accent-foreground"
                  : "flex max-w-[16rem] items-center gap-2 rounded-md border px-3 py-2 text-start text-sm"
              }
            >
              {torrent.kind === "magnet" ? (
                <Link2Icon className="size-4 shrink-0" />
              ) : (
                <FileWarningIcon className="size-4 shrink-0" />
              )}
              <span className="truncate">
                {torrent.kind === "file"
                  ? torrent.inspection.name
                  : torrent.kind === "file-unreadable"
                    ? torrent.path.split(/[\\/]/).pop()
                    : uriFor(index).trim() || "New magnet"}
              </span>
            </button>
          ))}
        </div>
      )}

      {treeSource !== null && tree !== null && (
        <div className="grid gap-2">
          <div className="flex items-center justify-between text-sm text-muted-foreground">
            <span>
              {formatBytes(treeSource.total_length)} total
              {selectedBytes !== null &&
                ` · ${formatBytes(selectedBytes)} selected`}
            </span>
            {sortedValues(currentSkipped).length > 0 && (
              <span>
                {currentSkipped.size} file
                {currentSkipped.size === 1 ? "" : "s"} skipped
              </span>
            )}
          </div>
          <div className="max-h-72 overflow-auto rounded-md border">
            <Files defaultOpen={[tree.name]}>{renderFolder(tree)}</Files>
          </div>
        </div>
      )}

      {current?.kind === "file-unreadable" && (
        <div className="grid gap-2">
          <p className="text-sm text-muted-foreground">
            <span className="font-medium text-foreground">
              {current.path}
            </span>{" "}
            could not be inspected: {current.reason}
          </p>
          <p className="text-sm text-muted-foreground">
            You can still add it with all files, without picking priorities.
          </p>
        </div>
      )}

      {current?.kind === "magnet" && magnetId === undefined && (
        <div className="grid gap-3">
          <div className="grid gap-2">
            <Label htmlFor="add-magnet-uri">Magnet uri</Label>
            <Input
              id="add-magnet-uri"
              value={uriFor(selected)}
              onChange={(event) =>
                setUris((previous) => ({
                  ...previous,
                  [selected]: event.target.value,
                }))
              }
              placeholder="magnet:?xt=urn:btih:…"
            />
          </div>
          <label className="flex items-center gap-2 text-sm text-muted-foreground">
            <Checkbox
              checked={startImmediately[selected] ?? false}
              onCheckedChange={(checked) =>
                setStartImmediately((previous) => ({
                  ...previous,
                  [selected]: checked === true,
                }))
              }
            />
            Start downloading as soon as the metadata arrives (skip file
            picking)
          </label>
        </div>
      )}

      {current?.kind === "magnet" && isMagnetFetching && (
        <div className="flex items-center gap-3 rounded-md border border-border bg-card px-4 py-3 text-sm text-muted-foreground">
          <LoaderCircleIcon className="size-4 animate-spin text-accent" />
          <span>
            Fetching metadata — the torrent is added and paused. The file tree
            appears here as soon as it arrives.
          </span>
        </div>
      )}

      {current?.kind === "magnet" && magnetReady && (
        <p className="text-sm text-muted-foreground">
          Metadata fetched — the torrent is paused. Uncheck anything you do not
          want, then press Start.
        </p>
      )}

      {error !== null && (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      )}

      <div className="flex justify-end gap-2">
        <Button variant="outline" onClick={onClose}>
          Cancel
        </Button>
        <Button
          disabled={!allReady || submitting}
          onClick={
            isMagnetFetching
              ? handleKeepPaused
              : current?.kind === "magnet" &&
                  magnetId === undefined &&
                  !(startImmediately[selected] ?? false)
                ? handleAddMagnet
                : handleAdd
          }
        >
          {addButtonLabel}
        </Button>
      </div>
    </div>
  );

  function handleKeepPaused() {
    setSubmitting(true);
    setError(null);
    const id = magnetId;
    if (id === undefined) {
      setSubmitting(false);
      return;
    }
    onAdd([{ kind: "magnet-keep-paused", id }])
      .then(() => onClose())
      .catch((err) => setError(String(err)))
      .finally(() => setSubmitting(false));
  }
}
