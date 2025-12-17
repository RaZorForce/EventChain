import {
    Calendar,
    Home,
    LineChart,
    Settings,
    User,
    BookOpen,
    History,
    TrendingUp,
    School,
    Pencil,
    RotateCcw,
    Bell
} from "lucide-react"
import {
    Sidebar,
    SidebarContent,
    SidebarFooter,
    SidebarHeader,
    SidebarMenu,
    SidebarMenuButton,
    SidebarMenuItem,
    SidebarProvider,
    SidebarRail,
    SidebarTrigger,
    useSidebar,
} from "@/components/ui/sidebar"
import React from "react";

// Menu items for the secondary sidebar
const navMain = [
    {
        title: "Dashboard",
        url: "#",
        icon: Home,
        isActive: true,
    },
    {
        title: "Daily Journal",
        url: "#",
        icon: BookOpen,
    },
    {
        title: "Trades",
        url: "#",
        icon: History,
    },
    {
        title: "Notebook",
        url: "#",
        icon: Pencil,
    },
    {
        title: "Reports",
        url: "#",
        icon: LineChart,
        badge: "NEW",
    },
    {
        title: "Playbooks",
        url: "#",
        icon: BookOpen,
    },
    {
        title: "Progress Tracker",
        url: "#",
        icon: TrendingUp,
        badge: "BETA",
    },
    {
        title: "Trade Replay",
        url: "#",
        icon: RotateCcw,
    },
    {
        title: "Resource Center",
        url: "#",
        icon: School,
    },
]

export function AppSidebar() {
    const [activeTab, setActiveTab] = React.useState("track")

    return (
        <SidebarProvider
            style={{
                "--sidebar-width": "350px",
            } as React.CSSProperties}
        >
            {/* Primary Icon Sidebar (Leftmost) */}
            <Sidebar collapsible="none" className="!w-[calc(var(--sidebar-width-icon)_+_1px)] border-r bg-sidebar">
                <SidebarHeader>
                    <SidebarMenu>
                        <SidebarMenuItem>
                            <SidebarMenuButton size="lg" asChild className="md:h-8 md:p-0">
                                <a href="#">
                                    <div className="flex aspect-square size-8 items-center justify-center rounded-lg bg-primary text-primary-foreground">
                                        <TrendingUp className="size-4" />
                                    </div>
                                    <div className="grid flex-1 text-left text-sm leading-tight">
                                        <span className="truncate font-semibold">EventChain</span>
                                        <span className="truncate text-xs">Trader</span>
                                    </div>
                                </a>
                            </SidebarMenuButton>
                        </SidebarMenuItem>
                    </SidebarMenu>
                </SidebarHeader>
                <SidebarContent>
                    <SidebarMenu>
                        <SidebarMenuItem>
                            <SidebarMenuButton
                                isActive={activeTab === "track"}
                                onClick={() => setActiveTab("track")}
                                tooltip="Tracking"
                                className="justify-center"
                            >
                                <Pencil className="size-5" />
                            </SidebarMenuButton>
                        </SidebarMenuItem>
                        <SidebarMenuItem>
                            <SidebarMenuButton
                                isActive={activeTab === "backtest"}
                                onClick={() => setActiveTab("backtest")}
                                tooltip="Backtesting"
                                className="justify-center"
                            >
                                <RotateCcw className="size-5" />
                            </SidebarMenuButton>
                        </SidebarMenuItem>
                        <SidebarMenuItem>
                            <SidebarMenuButton
                                isActive={activeTab === "mentor"}
                                onClick={() => setActiveTab("mentor")}
                                tooltip="Mentor Mode"
                                className="justify-center"
                            >
                                <User className="size-5" />
                            </SidebarMenuButton>
                        </SidebarMenuItem>
                        <SidebarMenuItem>
                            <SidebarMenuButton
                                isActive={activeTab === "university"}
                                onClick={() => setActiveTab("university")}
                                tooltip="University"
                                className="justify-center"
                            >
                                <School className="size-5" />
                            </SidebarMenuButton>
                        </SidebarMenuItem>
                    </SidebarMenu>
                </SidebarContent>
                <SidebarFooter>
                    <SidebarMenu>
                        <SidebarMenuItem>
                            <SidebarMenuButton tooltip="Notifications" className="justify-center">
                                <Bell className="size-5" />
                            </SidebarMenuButton>
                        </SidebarMenuItem>
                        <SidebarMenuItem>
                            <SidebarMenuButton tooltip="Account" className="justify-center">
                                <User className="size-5" />
                            </SidebarMenuButton>
                        </SidebarMenuItem>
                    </SidebarMenu>
                </SidebarFooter>
            </Sidebar>

            {/* Secondary Sidebar (Collapsible Menu) */}
            <Sidebar collapsible="none" className="hidden flex-1 md:flex bg-sidebar-accent/10">
                <SidebarHeader className="gap-3.5 border-b p-4">
                    <div className="flex w-full items-center justify-between">
                        <div className="text-base font-medium text-foreground">
                            {activeTab === "track" && "Tracking"}
                            {activeTab === "backtest" && "Backtesting"}
                            {activeTab === "mentor" && "Mentor Mode"}
                            {activeTab === "university" && "University"}
                        </div>
                        {/* <SidebarTrigger className="-mr-2 ml-auto" /> */}
                    </div>
                </SidebarHeader>
                <SidebarContent>
                    <div className="p-2">
                        {/* Dynamic Content based on Active Tab */}
                        {activeTab === "track" && (
                            <SidebarMenu>
                                {navMain.map((item) => (
                                    <SidebarMenuItem key={item.title}>
                                        <SidebarMenuButton isActive={item.isActive} asChild>
                                            <a href={item.url} className="flex items-center gap-2">
                                                <item.icon />
                                                <span>{item.title}</span>
                                                {item.badge && (
                                                    <span className="ml-auto text-xs bg-primary/20 text-primary px-1.5 py-0.5 rounded-full">
                                                        {item.badge}
                                                    </span>
                                                )}
                                            </a>
                                        </SidebarMenuButton>
                                    </SidebarMenuItem>
                                ))}
                            </SidebarMenu>
                        )}
                        {activeTab !== "track" && (
                            <div className="p-4 text-sm text-muted-foreground">
                                Module not implemented yet.
                            </div>
                        )}
                    </div>
                </SidebarContent>
                <SidebarFooter>
                    {/* Add Trade Button */}
                    <div className="p-2">
                        <button className="w-full bg-primary text-primary-foreground hover:bg-primary/90 h-10 px-4 py-2 rounded-md font-medium text-sm transition-colors">
                            + Add Trade
                        </button>
                    </div>
                </SidebarFooter>
            </Sidebar>
        </SidebarProvider>
    )
}
