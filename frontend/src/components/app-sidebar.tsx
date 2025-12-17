import {
    Home,
    LineChart,
    User,
    BookOpen,
    History,
    TrendingUp,
    School,
    Pencil,
    RotateCcw,
    Bell,
    ChevronLeft,
    ChevronRight
} from "lucide-react"
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
    const [isSecondaryCollapsed, setIsSecondaryCollapsed] = React.useState(false)

    return (
        <div className="flex h-screen sticky top-0">
            {/* Primary Icon Sidebar (Always Visible) */}
            <div className="w-14 min-w-14 border-r bg-sidebar flex flex-col shrink-0 h-full">
                {/* Logo */}
                <div className="flex items-center justify-center p-3 border-b border-sidebar-border">
                    <div className="flex aspect-square size-8 items-center justify-center rounded-lg bg-primary text-primary-foreground">
                        <TrendingUp className="size-4" />
                    </div>
                </div>

                {/* Primary Navigation Icons */}
                <div className="flex-1 py-2">
                    <div className="flex flex-col gap-1 px-2">
                        <button
                            className={`flex items-center justify-center p-2.5 rounded-md transition-colors ${
                                activeTab === "track"
                                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                                    : "text-sidebar-foreground/70 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                            }`}
                            onClick={() => setActiveTab("track")}
                            title="Tracking"
                        >
                            <Pencil className="size-5" />
                        </button>
                        <button
                            className={`flex items-center justify-center p-2.5 rounded-md transition-colors ${
                                activeTab === "backtest"
                                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                                    : "text-sidebar-foreground/70 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                            }`}
                            onClick={() => setActiveTab("backtest")}
                            title="Backtesting"
                        >
                            <RotateCcw className="size-5" />
                        </button>
                        <button
                            className={`flex items-center justify-center p-2.5 rounded-md transition-colors ${
                                activeTab === "mentor"
                                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                                    : "text-sidebar-foreground/70 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                            }`}
                            onClick={() => setActiveTab("mentor")}
                            title="Mentor Mode"
                        >
                            <User className="size-5" />
                        </button>
                        <button
                            className={`flex items-center justify-center p-2.5 rounded-md transition-colors ${
                                activeTab === "university"
                                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                                    : "text-sidebar-foreground/70 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                            }`}
                            onClick={() => setActiveTab("university")}
                            title="University"
                        >
                            <School className="size-5" />
                        </button>
                    </div>
                </div>

                {/* Footer Icons */}
                <div className="py-2 border-t border-sidebar-border">
                    <div className="flex flex-col gap-1 px-2">
                        <button
                            className="flex items-center justify-center p-2.5 rounded-md text-sidebar-foreground/70 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground transition-colors"
                            title="Notifications"
                        >
                            <Bell className="size-5" />
                        </button>
                        <button
                            className="flex items-center justify-center p-2.5 rounded-md text-sidebar-foreground/70 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground transition-colors"
                            title="Account"
                        >
                            <User className="size-5" />
                        </button>
                    </div>
                </div>
            </div>

            {/* Secondary Sidebar Container - always rendered for smooth transition */}
            <div className="relative flex h-full">
                {/* Secondary Sidebar (Collapsible) */}
                <div
                    className="bg-sidebar border-r border-sidebar-border flex flex-col h-full overflow-hidden"
                    style={{
                        width: isSecondaryCollapsed ? 0 : 256,
                        minWidth: isSecondaryCollapsed ? 0 : 256,
                        transition: 'width 300ms ease-in-out, min-width 300ms ease-in-out',
                    }}
                >
                    {/* Inner content wrapper with fixed width to prevent content squishing */}
                    <div className="w-64 min-w-64 flex flex-col h-full">
                        {/* Header with collapse button */}
                        <div className="flex items-center justify-between p-4 border-b border-sidebar-border">
                            <div className="text-sm font-semibold text-sidebar-foreground whitespace-nowrap">
                                Event Chain Trader
                            </div>
                            <button
                                onClick={() => setIsSecondaryCollapsed(true)}
                                className="flex items-center justify-center p-1 rounded-md text-sidebar-foreground/70 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground transition-colors"
                                aria-label="Collapse sidebar"
                            >
                                <ChevronLeft className="size-4" />
                            </button>
                        </div>

                        {/* Content */}
                        <div className="flex-1 overflow-y-auto py-2">
                            {/* Add Trade button at the top */}
                            <div className="p-2">
                                <button className="w-full bg-primary text-primary-foreground hover:bg-primary/90 h-9 px-4 rounded-md font-medium text-sm transition-colors whitespace-nowrap">
                                    + Add Trade
                                </button>
                            </div>
                            
                            {activeTab === "track" && (
                                <nav className="flex flex-col gap-1 px-2">
                                    {navMain.map((item) => (
                                        <a
                                            key={item.title}
                                            href={item.url}
                                            className={`flex items-center gap-3 px-3 py-2 rounded-md text-sm transition-colors whitespace-nowrap ${
                                                item.isActive
                                                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                                                    : "text-sidebar-foreground/70 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                                            }`}
                                        >
                                            <item.icon className="size-4 shrink-0" />
                                            <span>{item.title}</span>
                                            {item.badge && (
                                                <span className="ml-auto text-[10px] bg-primary/20 text-primary px-1.5 py-0.5 rounded-full font-medium">
                                                    {item.badge}
                                                </span>
                                            )}
                                        </a>
                                    ))}
                                </nav>
                            )}
                            {activeTab !== "track" && (
                                <div className="px-4 py-2 text-sm text-sidebar-foreground/70 whitespace-nowrap">
                                    Module not implemented yet.
                                </div>
                            )}
                        </div>
                    </div>
                </div>

                {/* Expand button - always rendered, visibility controlled by opacity */}
                <div
                    className="bg-sidebar border-r border-sidebar-border flex items-start h-full"
                    style={{
                        width: isSecondaryCollapsed ? 'auto' : 0,
                        opacity: isSecondaryCollapsed ? 1 : 0,
                        overflow: 'hidden',
                        transition: 'opacity 300ms ease-in-out',
                        pointerEvents: isSecondaryCollapsed ? 'auto' : 'none',
                    }}
                >
                    <button
                        onClick={() => setIsSecondaryCollapsed(false)}
                        className="flex items-center justify-center p-2 m-1 rounded-md text-sidebar-foreground/70 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground transition-colors"
                        aria-label="Expand sidebar"
                    >
                        <ChevronRight className="size-4" />
                    </button>
                </div>
            </div>
        </div>
    )
}
