import { Link, useLocation } from "wouter";
import { cn } from "@/lib/utils";
import { 
  LayoutDashboard, 
  Activity, 
  ShieldCheck, 
  Settings, 
  Menu,
  Database,
  Server
} from "lucide-react";
import { Sheet, SheetContent, SheetTrigger } from "@/components/ui/sheet";
import { Button } from "@/components/ui/button";
import { useState } from "react";

const sidebarItems = [
  { icon: LayoutDashboard, label: "Pipeline Monitor", href: "/" },
  { icon: Activity, label: "Error Intelligence", href: "/errors" },
  { icon: ShieldCheck, label: "Admin & Governance", href: "/admin" },
];

export function AppLayout({ children }: { children: React.ReactNode }) {
  const [location] = useLocation();
  const [isMobileOpen, setIsMobileOpen] = useState(false);

  const NavContent = () => (
    <div className="flex flex-col h-full bg-sidebar text-sidebar-foreground border-r border-sidebar-border">
      <div className="p-6">
        <div className="flex items-center gap-2 mb-8">
          <div className="h-8 w-8 bg-primary rounded-lg flex items-center justify-center">
             <Database className="h-5 w-5 text-primary-foreground" />
          </div>
          <div>
            <h1 className="font-display font-bold text-lg leading-none tracking-tight">FMD Data</h1>
            <span className="text-xs text-sidebar-foreground/60 font-medium">Pipeline Control</span>
          </div>
        </div>
        
        <nav className="space-y-1">
          {sidebarItems.map((item) => {
            const isActive = location === item.href;
            return (
              <Link key={item.href} href={item.href}>
                <div className={cn(
                  "flex items-center gap-3 px-3 py-2.5 rounded-md text-sm font-medium transition-all duration-200 group relative overflow-hidden cursor-pointer",
                  isActive 
                    ? "bg-sidebar-primary text-sidebar-primary-foreground shadow-md" 
                    : "text-sidebar-foreground/80 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                )}>
                  <item.icon className={cn("h-4 w-4", isActive ? "text-white" : "text-sidebar-foreground/60 group-hover:text-sidebar-accent-foreground")} />
                  {item.label}
                  {isActive && (
                    <div className="absolute inset-0 bg-white/10 mix-blend-overlay pointer-events-none" />
                  )}
                </div>
              </Link>
            );
          })}
        </nav>
      </div>

      <div className="mt-auto p-6 border-t border-sidebar-border">
        <div className="flex items-center gap-3">
          <div className="h-8 w-8 rounded-full bg-sidebar-accent flex items-center justify-center text-xs font-bold text-sidebar-accent-foreground">
            JS
          </div>
          <div className="flex flex-col">
            <span className="text-sm font-medium">Jane Smith</span>
            <span className="text-xs text-sidebar-foreground/60">Data Steward</span>
          </div>
        </div>
      </div>
    </div>
  );

  return (
    <div className="flex min-h-screen bg-background text-foreground font-sans">
      {/* Desktop Sidebar */}
      <aside className="hidden md:block w-64 fixed inset-y-0 z-50">
        <NavContent />
      </aside>

      {/* Mobile Sidebar */}
      <Sheet open={isMobileOpen} onOpenChange={setIsMobileOpen}>
        <SheetContent side="left" className="p-0 w-64 border-r-0">
          <NavContent />
        </SheetContent>
      </Sheet>

      {/* Main Content */}
      <main className="flex-1 md:ml-64 flex flex-col min-h-screen transition-all duration-300 ease-in-out">
        <header className="sticky top-0 z-40 h-16 bg-background/80 backdrop-blur-md border-b border-border flex items-center justify-between px-6">
           <div className="flex items-center gap-4">
             <Button variant="ghost" size="icon" className="md:hidden" onClick={() => setIsMobileOpen(true)}>
               <Menu className="h-5 w-5" />
             </Button>
             <div className="flex items-center gap-2 text-sm text-muted-foreground">
               <div className="h-2 w-2 rounded-full bg-success animate-pulse" />
               <span className="font-medium text-foreground">System Operational</span>
               <span className="mx-2 text-border">|</span>
               <span>Last updated: just now</span>
             </div>
           </div>
           
           <div className="flex items-center gap-4">
             <Button variant="outline" size="sm" className="hidden sm:flex gap-2">
               <Server className="h-4 w-4 text-muted-foreground" />
               <span>Production Environment</span>
             </Button>
           </div>
        </header>
        
        <div className="p-6 md:p-8 max-w-7xl mx-auto w-full animate-in fade-in slide-in-from-bottom-4 duration-500">
          {children}
        </div>
      </main>
    </div>
  );
}
