import { Link, useLocation } from "wouter";
import { LayoutDashboard, Plug, Database, Settings, Terminal, Activity } from "lucide-react";
import { cn } from "@/lib/utils";
import bgPattern from "@assets/generated_images/subtle_dark_technical_grid_pattern_background.png";

export default function Layout({ children }: { children: React.ReactNode }) {
  const [location] = useLocation();

  const navItems = [
    { href: "/", icon: LayoutDashboard, label: "Dashboard" },
    { href: "/connectors", icon: Plug, label: "Connectors" },
    { href: "/queries", icon: Terminal, label: "Query Builder" },
    { href: "/analysis", icon: Activity, label: "Analysis" },
  ];

  return (
    <div className="min-h-screen bg-background font-sans flex relative overflow-hidden">
      {/* Background Texture */}
      <div 
        className="absolute inset-0 pointer-events-none opacity-5 mix-blend-overlay z-0"
        style={{ backgroundImage: `url(${bgPattern})`, backgroundSize: 'cover' }}
      />

      {/* Sidebar */}
      <aside className="w-64 border-r border-sidebar-border bg-sidebar z-10 flex flex-col">
        <div className="p-6 border-b border-sidebar-border">
          <div className="flex items-center gap-2 text-primary">
            <Database className="h-6 w-6" />
            <h1 className="font-bold text-lg tracking-tight">DataNexus</h1>
          </div>
          <p className="text-xs text-muted-foreground mt-1 font-mono">v2.4.0-RC1</p>
        </div>

        <nav className="flex-1 p-4 space-y-1">
          {navItems.map((item) => {
            const Icon = item.icon;
            const isActive = location === item.href;
            
            return (
              <Link key={item.href} href={item.href}>
                <div className={cn(
                  "flex items-center gap-3 px-3 py-2 rounded-md text-sm font-medium transition-colors cursor-pointer",
                  isActive 
                    ? "bg-sidebar-accent text-sidebar-accent-foreground" 
                    : "text-muted-foreground hover:bg-sidebar-accent/50 hover:text-foreground"
                )}>
                  <Icon className="h-4 w-4" />
                  {item.label}
                </div>
              </Link>
            );
          })}
        </nav>

        <div className="p-4 border-t border-sidebar-border">
          <button className="flex items-center gap-3 px-3 py-2 w-full rounded-md text-sm font-medium text-muted-foreground hover:bg-sidebar-accent/50 hover:text-foreground transition-colors">
            <Settings className="h-4 w-4" />
            Settings
          </button>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 overflow-auto z-10 relative">
        <div className="max-w-7xl mx-auto p-8">
          {children}
        </div>
      </main>
    </div>
  );
}
