import { Link, useLocation } from "react-router-dom";
import { cn } from "@/lib/utils";
import {
  LayoutDashboard,
  Activity,
  ShieldCheck,
  GitBranch,
  Menu,
  Server,
  Cable,
  PanelLeftClose,
  PanelLeftOpen,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { ThemeToggle } from "@/components/ui/theme-toggle";
import { useState } from "react";

const sidebarItems = [
  { icon: LayoutDashboard, label: "Pipeline Monitor", href: "/" },
  { icon: Activity, label: "Error Intelligence", href: "/errors" },
  { icon: GitBranch, label: "Flow Explorer", href: "/flow" },
  { icon: Cable, label: "Source Manager", href: "/sources" },
  { icon: ShieldCheck, label: "Admin & Governance", href: "/admin" },
];

export function AppLayout({ children }: { children: React.ReactNode }) {
  const location = useLocation();
  const [isMobileOpen, setIsMobileOpen] = useState(false);
  const [isCollapsed, setIsCollapsed] = useState(false);

  const sidebarWidth = isCollapsed ? "w-16" : "w-64";
  const mainMargin = isCollapsed ? "md:ml-16" : "md:ml-64";

  const NavContent = () => (
    <div className="flex flex-col h-full bg-sidebar text-sidebar-foreground border-r border-sidebar-border">
      <div className={cn("p-4", !isCollapsed && "p-6")}>
        <div className={cn("flex items-center gap-2 mb-8", isCollapsed && "justify-center mb-6")}>
          <div className="h-8 w-8 rounded-[var(--radius-md)] flex items-center justify-center overflow-hidden flex-shrink-0">
             <img src="/icons/fabric.svg" alt="Fabric" className="h-8 w-8" />
          </div>
          {!isCollapsed && (
            <div>
              <h1 className="font-display font-semibold text-sm leading-none tracking-tight">FMD Data</h1>
              <span className="text-[10px] text-sidebar-foreground/60 font-medium tracking-wider uppercase">Pipeline Control</span>
            </div>
          )}
        </div>

        <nav className="space-y-1">
          {sidebarItems.map((item) => {
            const isActive = location.pathname === item.href;
            return (
              <Link key={item.href} to={item.href} title={isCollapsed ? item.label : undefined}>
                <div className={cn(
                  "flex items-center gap-3 px-3 py-2 rounded-[var(--radius-md)] text-sm transition-all group relative overflow-hidden cursor-pointer",
                  "duration-[var(--duration-fast)]",
                  isCollapsed && "justify-center px-2",
                  isActive
                    ? "bg-sidebar-primary text-sidebar-primary-foreground font-medium shadow-[var(--shadow-sm)]"
                    : "text-sidebar-foreground/80 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                )}>
                  <item.icon className={cn("h-4 w-4 flex-shrink-0", isActive ? "text-sidebar-primary-foreground" : "text-sidebar-foreground/60 group-hover:text-sidebar-accent-foreground")} />
                  {!isCollapsed && item.label}
                  {isActive && (
                    <div className="absolute inset-0 bg-white/10 mix-blend-overlay pointer-events-none" />
                  )}
                </div>
              </Link>
            );
          })}
        </nav>
      </div>

      {/* Collapse toggle */}
      <div className={cn("px-4 py-2", isCollapsed && "px-2")}>
        <button
          onClick={() => setIsCollapsed(!isCollapsed)}
          className="flex items-center gap-2 px-3 py-1.5 rounded-[var(--radius-md)] text-sidebar-foreground/50 hover:text-sidebar-foreground hover:bg-sidebar-accent/50 transition-all text-xs w-full cursor-pointer"
          title={isCollapsed ? "Expand sidebar" : "Collapse sidebar"}
        >
          {isCollapsed ? (
            <PanelLeftOpen className="h-4 w-4 mx-auto" />
          ) : (
            <>
              <PanelLeftClose className="h-4 w-4" />
              <span>Collapse</span>
            </>
          )}
        </button>
      </div>

      <div className={cn("mt-auto p-6 border-t border-sidebar-border", isCollapsed && "p-3")}>
        <div className={cn("flex items-center gap-3", isCollapsed && "justify-center")}>
          <div className="h-8 w-8 rounded-full bg-sidebar-accent flex items-center justify-center text-[10px] font-medium text-sidebar-accent-foreground flex-shrink-0">
            SN
          </div>
          {!isCollapsed && (
            <div className="flex flex-col">
              <span className="text-sm font-medium">Steve Nahrup</span>
              <span className="text-[10px] text-sidebar-foreground/60 tracking-wider uppercase">Data Engineer</span>
            </div>
          )}
        </div>
      </div>
    </div>
  );

  return (
    <div className="flex min-h-screen bg-background text-foreground font-sans">
      {/* Desktop Sidebar */}
      <aside className={cn("hidden md:block fixed inset-y-0 z-50 transition-all duration-200", sidebarWidth)}>
        <NavContent />
      </aside>

      {/* Mobile Sidebar Overlay */}
      {isMobileOpen && (
        <div className="fixed inset-0 z-50 md:hidden">
          <div className="fixed inset-0 bg-black/40 backdrop-blur-[4px]" onClick={() => setIsMobileOpen(false)} />
          <div className="fixed inset-y-0 left-0 w-64 z-50 animate-[slideIn_0.25s_var(--ease-claude)]">
            <NavContent />
          </div>
        </div>
      )}

      {/* Main Content */}
      <main className={cn("flex-1 flex flex-col min-h-screen transition-all duration-200", mainMargin)}>
        <header className="sticky top-0 z-40 h-12 bg-background/80 backdrop-blur-md border-b border-border flex items-center justify-between px-6">
           <div className="flex items-center gap-4">
             <Button variant="ghost" size="icon" className="md:hidden h-8 w-8" onClick={() => setIsMobileOpen(true)}>
               <Menu className="h-4 w-4" />
             </Button>
             <div className="flex items-center gap-2 text-xs text-muted-foreground">
               <div className="h-1.5 w-1.5 rounded-full status-running animate-[pulse-status_2s_ease-in-out_infinite]" />
               <span className="font-medium text-foreground text-sm">System Operational</span>
               <span className="mx-2 text-border">|</span>
               <span className="font-mono text-[10px]">Last updated: just now</span>
             </div>
           </div>

           <div className="flex items-center gap-2">
             <ThemeToggle />
             <Button variant="outline" size="sm" className="hidden sm:flex gap-2 h-8 text-xs">
               <Server className="h-3.5 w-3.5 text-muted-foreground" />
               <span>Production</span>
             </Button>
           </div>
        </header>

        <div className="flex-1 cowork-grid">
          <div className={cn(
            "w-full animate-[fadeIn_0.25s_var(--ease-claude)]",
            location.pathname === "/flow"
              ? "p-0"
              : "p-6 md:p-8 max-w-7xl mx-auto"
          )}>
            {children}
          </div>
        </div>
      </main>
    </div>
  );
}
