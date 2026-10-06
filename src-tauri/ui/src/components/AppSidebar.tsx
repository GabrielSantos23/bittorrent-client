import {
  AlertCircleIcon,
  CheckCircle2Icon,
  DownloadIcon,
  ListIcon,
  MagnetIcon,
  PauseIcon,
  RefreshCwIcon,
  SearchIcon,
  SettingsIcon,
  SmartphoneIcon,
  UploadIcon,
} from "lucide-react";

import type { TorrentSummary } from "../../../../bt-core/bindings/TorrentSummary";
import type { TorrentDetail } from "../../../../bt-core/bindings/TorrentDetail";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
} from "@/components/animate-ui/components/radix/sidebar";

interface StateFilter {
  value: string | null;
  label: string;
  Icon: typeof ListIcon;
}

const STATE_FILTERS: StateFilter[] = [
  { value: null, label: "All", Icon: ListIcon },
  { value: "Downloading", label: "Downloading", Icon: DownloadIcon },
  { value: "Seeding", label: "Seeding", Icon: UploadIcon },
  { value: "Completed", label: "Completed", Icon: CheckCircle2Icon },
  { value: "Checking", label: "Checking", Icon: RefreshCwIcon },
  { value: "FetchingMetadata", label: "Fetching metadata", Icon: SearchIcon },
  { value: "Paused", label: "Paused", Icon: PauseIcon },
  { value: "Error", label: "Error", Icon: AlertCircleIcon },
];

function trackerHost(url: string): string {
  try {
    return new URL(url).hostname;
  } catch {
    return url;
  }
}

interface AppSidebarProps {
  summaries: TorrentSummary[];
  value: string | null;
  onChange: (state: string | null) => void;
  detail: TorrentDetail | null;
  onOpenRemote: () => void;
  onOpenSettings: () => void;
}

export default function AppSidebar({
  summaries,
  value,
  onChange,
  detail,
  onOpenRemote,
  onOpenSettings,
}: AppSidebarProps) {
  const countByState = new Map<string, number>();
  for (const summary of summaries) {
    countByState.set(summary.state, (countByState.get(summary.state) ?? 0) + 1);
  }

  return (
    <Sidebar variant="floating" collapsible="icon">
      <SidebarHeader>
        <div className="flex items-center gap-2 px-2 py-1.5 group-data-[collapsible=icon]:justify-center">
          <MagnetIcon className="size-5 shrink-0 text-accent" />
          <span className="truncate text-sm font-semibold group-data-[collapsible=icon]:hidden">
            BitTorrent Client
          </span>
        </div>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>States</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {STATE_FILTERS.map(({ value: state, label, Icon }) => {
                const count =
                  state === null
                    ? summaries.length
                    : (countByState.get(state) ?? 0);
                return (
                  <SidebarMenuItem key={label}>
                    <SidebarMenuButton
                      isActive={value === state}
                      onClick={() => onChange(state)}
                      tooltip={label}
                    >
                      <Icon />
                      <span className="group-data-[collapsible=icon]:hidden">
                        {label}
                      </span>
                      <span
                        className={`ml-auto text-xs tabular-nums text-muted-foreground group-data-[collapsible=icon]:hidden${
                          count === 0 && state !== null ? " opacity-50" : ""
                        }`}
                      >
                        {count}
                      </span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                );
              })}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
        <SidebarGroup>
          <SidebarGroupLabel>Trackers</SidebarGroupLabel>
          <SidebarGroupContent>
            {detail === null || detail.trackers.length === 0 ? (
              <p className="px-2 py-1 text-xs text-muted-foreground group-data-[collapsible=icon]:hidden">
                {detail === null
                  ? "Select a torrent to see its trackers."
                  : "No trackers."}
              </p>
            ) : (
              <SidebarMenu>
                {detail.trackers.map((tracker) => (
                  <SidebarMenuItem key={tracker.url}>
                    <SidebarMenuButton tooltip={tracker.url} className="cursor-default">
                      <span
                        className={
                          tracker.last_error === null
                            ? "size-2 shrink-0 rounded-full bg-accent"
                            : "size-2 shrink-0 rounded-full bg-destructive"
                        }
                      />
                      <span className="truncate group-data-[collapsible=icon]:hidden">
                        {trackerHost(tracker.url)}
                      </span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                ))}
              </SidebarMenu>
            )}
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton onClick={onOpenRemote} tooltip="Remote access">
              <SmartphoneIcon />
              <span className="group-data-[collapsible=icon]:hidden">
                Remote access
              </span>
            </SidebarMenuButton>
          </SidebarMenuItem>
          <SidebarMenuItem>
            <SidebarMenuButton onClick={onOpenSettings} tooltip="Settings">
              <SettingsIcon />
              <span className="group-data-[collapsible=icon]:hidden">
                Settings
              </span>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarFooter>
      <SidebarRail />
    </Sidebar>
  );
}
