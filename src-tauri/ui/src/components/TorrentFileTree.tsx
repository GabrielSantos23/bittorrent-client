import {
  FileArchiveIcon,
  FileAudioIcon,
  FileIcon,
  FileImageIcon,
  FileTextIcon,
  FileVideoIcon,
  FolderClosedIcon,
  FolderOpenIcon,
} from "lucide-react";

import { formatBytes } from "../format";
import type { FileSummary } from "../../../../bt-core/bindings/FileSummary";
import { Checkbox } from "@/components/animate-ui/components/radix/checkbox";
import {
  Files,
  FolderContent,
  FolderItem,
  SubFiles,
} from "@/components/animate-ui/components/radix/files";
import {
  File as FileRow,
  FileHighlight,
  FileIcon as FileIconSlot,
  FileLabel,
  Folder as FolderShell,
  FolderHeader,
  FolderHighlight,
  FolderIcon as FolderIconSlot,
  FolderLabel,
  FolderTrigger as FolderTriggerPrimitive,
} from "@/components/animate-ui/primitives/radix/files";

interface TreeNode {
  name: string;
  path: string;
  file?: FileSummary;
  /** Position of the file in the source array; set for file nodes only. */
  index?: number;
  children: TreeNode[];
}

function buildTree(files: FileSummary[]): TreeNode[] {
  const rootChildren: TreeNode[] = [];
  const dirs = new Map<string, TreeNode>();

  const dirNode = (parent: TreeNode[], path: string, name: string): TreeNode => {
    const key = parent === rootChildren ? name : path;
    const existing = dirs.get(key);
    if (existing) return existing;
    const node: TreeNode = { name, path, children: [] };
    dirs.set(key, node);
    parent.push(node);
    return node;
  };

  files.forEach((file, index) => {
    const parts = file.path.split("/");
    let parent = rootChildren;
    let dirPath = "";
    for (let i = 0; i < parts.length - 1; i++) {
      dirPath = dirPath === "" ? parts[i] : `${dirPath}/${parts[i]}`;
      parent = dirNode(parent, dirPath, parts[i]).children;
    }
    parent.push({
      name: parts[parts.length - 1],
      path: file.path,
      file,
      index,
      children: [],
    });
  });

  const sortNodes = (nodes: TreeNode[]) => {
    nodes.sort((a, b) => {
      if (!!a.file !== !!b.file) return a.file ? 1 : -1;
      return a.name.localeCompare(b.name);
    });
    for (const node of nodes) {
      if (node.children.length > 0) sortNodes(node.children);
    }
  };
  sortNodes(rootChildren);
  return rootChildren;
}

function nodeStats(node: TreeNode): { size: number; verified: number } {
  if (node.file) {
    return { size: node.file.length, verified: node.file.verified_bytes };
  }
  let size = 0;
  let verified = 0;
  for (const child of node.children) {
    const stats = nodeStats(child);
    size += stats.size;
    verified += stats.verified;
  }
  return { size, verified };
}

function percent(verified: number, size: number): string {
  if (size <= 0) return "100%";
  return `${Math.min(100, Math.round((verified / size) * 100))}%`;
}

const VIDEO = new Set([
  "mp4", "mkv", "avi", "mov", "webm", "wmv", "flv", "m4v", "ts", "mpg", "mpeg",
]);
const AUDIO = new Set(["mp3", "flac", "aac", "ogg", "wav", "m4a", "opus", "wma"]);
const IMAGE = new Set(["jpg", "jpeg", "png", "gif", "bmp", "webp", "svg", "ico"]);
const ARCHIVE = new Set(["zip", "rar", "7z", "tar", "gz", "bz2", "xz", "zst"]);
const TEXT = new Set([
  "txt", "md", "nfo", "pdf", "doc", "docx", "srt", "ass", "ssa", "sub", "idx",
  "cue", "log", "sfv", "url", "ini", "json", "xml", "html",
]);

function iconFor(name: string) {
  const extension = name.split(".").pop()?.toLowerCase() ?? "";
  if (VIDEO.has(extension)) return FileVideoIcon;
  if (AUDIO.has(extension)) return FileAudioIcon;
  if (IMAGE.has(extension)) return FileImageIcon;
  if (ARCHIVE.has(extension)) return FileArchiveIcon;
  if (TEXT.has(extension)) return FileTextIcon;
  return FileIcon;
}

function topLevelFolderPaths(nodes: TreeNode[]): string[] {
  return nodes.filter((node) => node.file === undefined).map((node) => node.path);
}

// Checkboxes mirror the add-dialog's file selection: checked = downloaded,
// unchecked = skipped, folders show a tri-state over their descendants.
function descendantIndexes(node: TreeNode, into: number[] = []): number[] {
  if (node.file && node.index !== undefined) {
    into.push(node.index);
  }
  for (const child of node.children) {
    descendantIndexes(child, into);
  }
  return into;
}

function nodeChecked(node: TreeNode): boolean | "indeterminate" {
  if (node.file) return node.file.priority !== "Skip";
  let checked = 0;
  let total = 0;
  for (const child of node.children) {
    const state = nodeChecked(child);
    if (state === "indeterminate") return "indeterminate";
    total += 1;
    if (state) checked += 1;
  }
  if (checked === 0) return false;
  if (checked === total) return true;
  return "indeterminate";
}

function folderSkipped(node: TreeNode): boolean {
  if (node.file) return node.file.priority === "Skip";
  return node.children.every(folderSkipped);
}

function FileLeaf({
  node,
  onToggleFile,
}: {
  node: TreeNode;
  onToggleFile?: (indexes: number[], checked: boolean) => void;
}) {
  const file = node.file;
  if (!file) return null;
  const Icon = iconFor(node.name);
  const skipped = file.priority === "Skip";
  return (
    <FileHighlight>
      <FileRow
        className={`flex items-center justify-between gap-2 p-2${skipped ? " opacity-50" : ""}`}
      >
        <div className="flex min-w-0 items-center gap-2">
          {onToggleFile !== undefined && node.index !== undefined && (
            <Checkbox
              className="shrink-0"
              checked={file.priority !== "Skip"}
              onCheckedChange={(checked) =>
                onToggleFile([node.index as number], checked === true)
              }
            />
          )}
          <FileIconSlot>
            <Icon className="size-4 shrink-0 text-muted-foreground" />
          </FileIconSlot>
          <FileLabel className="truncate text-sm">{node.name}</FileLabel>
          {skipped && (
            <span className="shrink-0 rounded border border-border px-1 text-[10px] uppercase tracking-wide text-muted-foreground">
              skipped
            </span>
          )}
        </div>
        <span className="shrink-0 text-xs text-muted-foreground">
          {formatBytes(file.length)} · {percent(file.verified_bytes, file.length)}
        </span>
      </FileRow>
    </FileHighlight>
  );
}

function FolderBranch({
  node,
  onToggleFile,
}: {
  node: TreeNode;
  onToggleFile?: (indexes: number[], checked: boolean) => void;
}) {
  const stats = nodeStats(node);
  const skipped = folderSkipped(node);
  // In checkbox mode the checkbox sits OUTSIDE the trigger so picking a
  // folder never expands it, mirroring the add-dialog's tree.
  const header = onToggleFile !== undefined ? (
    <div className="flex w-full items-center gap-2 p-2">
      <Checkbox
        className="shrink-0"
        checked={nodeChecked(node)}
        onCheckedChange={(checked) =>
          onToggleFile(descendantIndexes(node), checked === true)
        }
      />
      <FolderTriggerPrimitive className="w-full text-start">
        <FolderHighlight>
          <FolderShell className="flex items-center justify-between gap-2 p-1">
            <FolderNodeLabel node={node} skipped={skipped} />
            <span className="shrink-0 text-xs text-muted-foreground">
              {formatBytes(stats.size)} · {percent(stats.verified, stats.size)}
            </span>
          </FolderShell>
        </FolderHighlight>
      </FolderTriggerPrimitive>
    </div>
  ) : (
    <FolderTriggerPrimitive className="w-full text-start">
      <FolderHighlight>
        <FolderShell className="flex items-center justify-between gap-2 p-2">
          <FolderNodeLabel node={node} skipped={skipped} />
          <span className="shrink-0 text-xs text-muted-foreground">
            {formatBytes(stats.size)} · {percent(stats.verified, stats.size)}
          </span>
        </FolderShell>
      </FolderHighlight>
    </FolderTriggerPrimitive>
  );
  return (
    <FolderItem value={node.path}>
      <FolderHeader>
        {header}
      </FolderHeader>
      <FolderContent>
        <SubFiles>
          {node.children.map((child) =>
            child.file === undefined ? (
              <FolderBranch key={child.path} node={child} onToggleFile={onToggleFile} />
            ) : (
              <FileLeaf key={child.path} node={child} onToggleFile={onToggleFile} />
            ),
          )}
        </SubFiles>
      </FolderContent>
    </FolderItem>
  );
}

function FolderNodeLabel({
  node,
  skipped,
}: {
  node: TreeNode;
  skipped: boolean;
}) {
  return (
    <div className="flex items-center gap-2">
      <FolderIconSlot
        closeIcon={<FolderClosedIcon className="size-4 text-accent" />}
        openIcon={<FolderOpenIcon className="size-4 text-accent" />}
      />
      <FolderLabel
        className={`text-sm font-medium${skipped ? " opacity-50" : ""}`}
      >
        {node.name}
      </FolderLabel>
      {skipped && (
        <span className="rounded border border-border px-1 text-[10px] uppercase tracking-wide text-muted-foreground">
          skipped
        </span>
      )}
    </div>
  );
}

interface TorrentFileTreeProps {
  files: FileSummary[];
  /**
   * When provided, rows render checkboxes bound to each file's priority so
   * callers can exclude files from the download (Skip) or include them back
   * (Normal), like the add-dialog's file picker. Omitted = read-only.
   */
  onToggleFile?: (indexes: number[], checked: boolean) => void;
}

export default function TorrentFileTree({
  files,
  onToggleFile,
}: TorrentFileTreeProps) {
  const tree = buildTree(files);
  if (tree.length === 0) {
    return (
      <p className="px-2 py-3 text-sm text-muted-foreground">No files.</p>
    );
  }
  return (
    <Files defaultOpen={topLevelFolderPaths(tree)}>
      {tree.map((node) =>
        node.file === undefined ? (
          <FolderBranch key={node.path} node={node} onToggleFile={onToggleFile} />
        ) : (
          <FileLeaf key={node.path} node={node} onToggleFile={onToggleFile} />
        ),
      )}
    </Files>
  );
}
