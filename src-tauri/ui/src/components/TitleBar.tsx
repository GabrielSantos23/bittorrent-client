import { useEffect, useState, type ReactNode } from "react";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { useSidebar } from "@/components/animate-ui/components/radix/sidebar";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/animate-ui/components/radix/tooltip";

// Caption glyphs are drawn on a 10×10 grid with 1px strokes to match the
// Segoe Fluent caption icons; lucide's 24-grid outlines look too heavy at
// caption size.
function SidebarGlyph() {
  return (
    <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden="true">
      <rect
        x="0.5"
        y="0.5"
        width="9"
        height="9"
        rx="1.5"
        fill="none"
        stroke="currentColor"
        strokeWidth="1"
      />
      <rect x="2" y="2" width="2.4" height="6" rx="0.6" fill="currentColor" />
    </svg>
  );
}

function MinimizeGlyph() {
  return (
    <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden="true">
      <path d="M0 5h10" stroke="currentColor" strokeWidth="1" />
    </svg>
  );
}

function MaximizeGlyph({ maximized }: { maximized: boolean }) {
  return maximized ? (
    <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden="true">
      <path
        d="M2.5 2.5v-2h7v7h-2"
        fill="none"
        stroke="currentColor"
        strokeWidth="1"
      />
      <rect
        x="0.5"
        y="2.5"
        width="7"
        height="7"
        fill="none"
        stroke="currentColor"
        strokeWidth="1"
      />
    </svg>
  ) : (
    <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden="true">
      <rect
        x="0.5"
        y="0.5"
        width="9"
        height="9"
        fill="none"
        stroke="currentColor"
        strokeWidth="1"
      />
    </svg>
  );
}

function CloseGlyph() {
  return (
    <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden="true">
      <path d="M0 0l10 10M10 0L0 10" stroke="currentColor" strokeWidth="1" />
    </svg>
  );
}

const captionButton =
  "flex h-full w-[46px] shrink-0 items-center justify-center text-foreground/90 transition-colors hover:bg-foreground/10 active:bg-foreground/[0.15] focus-visible:outline-none";
const closeButton =
  "flex h-full w-[46px] shrink-0 items-center justify-center text-foreground/90 transition-colors hover:bg-[#c42b1c] hover:text-white active:bg-[#c42b1c]/90 active:text-white focus-visible:outline-none";

// Vertical rule between the bar's control groups, like Windows 11 command bars.
export function TitleBarDivider() {
  return (
    <div
      data-tauri-drag-region
      aria-hidden="true"
      className="mx-1.5 h-5 w-px shrink-0 self-center bg-border"
    />
  );
}

// Top bar of the content column, following the Windows 11 command-bar
// pattern: actions grouped on the left (children), status info right-aligned
// before the caption buttons (trailing). The bar is a Tauri drag region
// (double-click toggles maximize); interactive children opt out by not
// carrying the drag attribute.
export default function TitleBar({
  children,
  trailing,
}: {
  children?: ReactNode;
  trailing?: ReactNode;
}) {
  const { toggleSidebar } = useSidebar();
  const [maximized, setMaximized] = useState(false);

  // Track the maximized state so the caption glyph swaps to the restore icon.
  // Resizes are the only way the state can change while this window exists.
  useEffect(() => {
    const win = getCurrentWindow();
    let disposed = false;
    let unlisten: (() => void) | undefined;
    const sync = () => {
      win
        .isMaximized()
        .then((value) => {
          if (!disposed) setMaximized(value);
        })
        .catch(() => {});
    };
    sync();
    win
      .onResized(sync)
      .then((stop) => {
        if (disposed) stop();
        else unlisten = stop;
      })
      .catch(() => {});
    return () => {
      disposed = true;
      unlisten?.();
    };
  }, []);

  // Window commands reject in the plain-browser dev preview (no Tauri
  // backend); swallow that so the preview stays usable.
  const run = (action: () => Promise<void>) => {
    action().catch(() => {});
  };

  return (
    <header
      data-tauri-drag-region
      className="flex h-11 shrink-0 select-none items-center border-b border-border bg-background pl-2"
    >
      <div data-tauri-drag-region className="flex min-w-0 items-center gap-1">
        {children}
      </div>
      <div data-tauri-drag-region className="min-w-6 flex-1 self-stretch" />
      {trailing}
      <div className="flex h-full items-stretch">
        <Tooltip>
          <TooltipTrigger asChild>
            <button
              type="button"
              className={captionButton}
              aria-label="Toggle sidebar"
              onClick={toggleSidebar}
            >
              <SidebarGlyph />
            </button>
          </TooltipTrigger>
          <TooltipContent side="bottom">Toggle sidebar</TooltipContent>
        </Tooltip>
        <button
          type="button"
          className={captionButton}
          title="Minimize"
          aria-label="Minimize"
          onClick={() => run(() => getCurrentWindow().minimize())}
        >
          <MinimizeGlyph />
        </button>
        <button
          type="button"
          className={captionButton}
          title={maximized ? "Restore" : "Maximize"}
          aria-label={maximized ? "Restore" : "Maximize"}
          onClick={() => run(() => getCurrentWindow().toggleMaximize())}
        >
          <MaximizeGlyph maximized={maximized} />
        </button>
        <button
          type="button"
          className={closeButton}
          title="Close"
          aria-label="Close"
          onClick={() => run(() => getCurrentWindow().close())}
        >
          <CloseGlyph />
        </button>
      </div>
    </header>
  );
}
