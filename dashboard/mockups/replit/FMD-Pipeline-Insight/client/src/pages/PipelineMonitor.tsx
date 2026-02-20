import { useState } from "react";
import { mockPipelines, Layer, Status } from "@/lib/mockData";
import { StatusBadge } from "@/components/ui/status-badge";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { format } from "date-fns";
import { Calendar as CalendarIcon, Filter, RefreshCw, ArrowRight, Play, Check, AlertTriangle, X } from "lucide-react";
import { cn } from "@/lib/utils";
import { Bar, BarChart, ResponsiveContainer, XAxis, YAxis, Tooltip } from "recharts";

const layers: Layer[] = ["Ingestion", "Standardization", "Business Ready", "Analytics"];

function PipelineProgress({ currentLayer, status }: { currentLayer: Layer, status: Status }) {
  const currentIndex = layers.indexOf(currentLayer);

  return (
    <div className="flex flex-col gap-1.5 mt-3 w-full max-w-md">
       <div className="flex items-center justify-between text-[10px] uppercase tracking-wider text-muted-foreground font-semibold">
          <span>Ingestion</span>
          <span>Analytics</span>
       </div>
       <div className="flex items-center gap-1">
        {layers.map((layer, index) => {
          let color = "bg-muted";
          
          if (index < currentIndex) {
            color = "bg-emerald-500/80"; // Previous steps completed
          } else if (index === currentIndex) {
            if (status === 'failed') color = "bg-rose-500";
            else if (status === 'running') color = "bg-blue-500 animate-pulse";
            else if (status === 'success') color = "bg-emerald-500"; // Completed this step
            else if (status === 'warning') color = "bg-amber-500";
          }

          return (
            <div 
              key={layer} 
              className={cn("h-2 flex-1 rounded-full transition-all duration-500", color)} 
              title={layer} 
            />
          );
        })}
      </div>
    </div>
  );
}

export default function PipelineMonitor() {
  const [searchTerm, setSearchTerm] = useState("");
  const [layerFilter, setLayerFilter] = useState<Layer | "All">("All");
  const [date, setDate] = useState<Date | undefined>(new Date());

  const filteredPipelines = mockPipelines.filter((pipeline) => {
    const matchesSearch = pipeline.name.toLowerCase().includes(searchTerm.toLowerCase()) || 
                          pipeline.sourceSystem.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesLayer = layerFilter === "All" || pipeline.layer === layerFilter;
    return matchesSearch && matchesLayer;
  });

  const chartData = [
    { name: "Mon", success: 40, failed: 2 },
    { name: "Tue", success: 45, failed: 1 },
    { name: "Wed", success: 42, failed: 3 },
    { name: "Thu", success: 48, failed: 0 },
    { name: "Fri", success: 38, failed: 4 },
    { name: "Sat", success: 20, failed: 0 },
    { name: "Sun", success: 22, failed: 0 },
  ];

  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-display font-bold tracking-tight text-foreground">Pipeline Monitor</h1>
          <p className="text-muted-foreground mt-1">Real-time status of your data ingestion and transformation flows.</p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" className="gap-2">
            <RefreshCw className="h-4 w-4" />
            Refresh
          </Button>
          <Button className="gap-2 bg-primary text-primary-foreground hover:bg-primary/90">
             <Filter className="h-4 w-4" />
             Export Report
          </Button>
        </div>
      </div>

      {/* Stats Overview */}
      <div className="grid gap-4 md:grid-cols-4">
        <Card className="bg-card shadow-sm border-border/60 hover:shadow-md transition-shadow">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Total Pipelines</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold font-display">42</div>
            <p className="text-xs text-muted-foreground">+2 from last week</p>
          </CardContent>
        </Card>
        <Card className="bg-card shadow-sm border-border/60 hover:shadow-md transition-shadow">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Success Rate</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold font-display text-emerald-600">98.2%</div>
            <p className="text-xs text-muted-foreground">+0.4% from last week</p>
          </CardContent>
        </Card>
        <Card className="bg-card shadow-sm border-border/60 hover:shadow-md transition-shadow">
           <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Avg. Duration</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold font-display">14m 32s</div>
            <p className="text-xs text-muted-foreground">-1m from last week</p>
          </CardContent>
        </Card>
        <Card className="bg-card shadow-sm border-border/60 hover:shadow-md transition-shadow">
           <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Active Alerts</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold font-display text-amber-600">3</div>
            <p className="text-xs text-muted-foreground">2 Warnings, 1 Critical</p>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-8 md:grid-cols-3">
        {/* Main List */}
        <div className="md:col-span-2 space-y-6">
          <div className="flex flex-col sm:flex-row items-center gap-4 bg-card p-4 rounded-lg border border-border shadow-sm">
            <Input 
              placeholder="Search pipelines..." 
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="max-w-sm"
            />
            <div className="flex items-center gap-2 w-full sm:w-auto">
              <Select value={layerFilter} onValueChange={(v) => setLayerFilter(v as Layer | "All")}>
                <SelectTrigger className="w-[180px]">
                  <SelectValue placeholder="Filter by Layer" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="All">All Layers</SelectItem>
                  <SelectItem value="Ingestion">Ingestion</SelectItem>
                  <SelectItem value="Standardization">Standardization</SelectItem>
                  <SelectItem value="Business Ready">Business Ready</SelectItem>
                  <SelectItem value="Analytics">Analytics</SelectItem>
                </SelectContent>
              </Select>
              <Popover>
                <PopoverTrigger asChild>
                  <Button variant={"outline"} className={cn("w-[240px] justify-start text-left font-normal", !date && "text-muted-foreground")}>
                    <CalendarIcon className="mr-2 h-4 w-4" />
                    {date ? format(date, "PPP") : <span>Pick a date</span>}
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-auto p-0" align="start">
                  <Calendar mode="single" selected={date} onSelect={setDate} initialFocus />
                </PopoverContent>
              </Popover>
            </div>
          </div>

          <div className="space-y-4">
            {filteredPipelines.map((pipeline) => (
              <div 
                key={pipeline.id} 
                className="group flex flex-col p-5 bg-card rounded-xl border border-border shadow-sm hover:shadow-md transition-all duration-200 hover:border-primary/20"
              >
                <div className="flex flex-col sm:flex-row sm:items-start justify-between gap-4">
                  <div className="space-y-1.5">
                    <div className="flex items-center gap-3">
                      <h3 className="font-semibold text-lg text-foreground group-hover:text-primary transition-colors">{pipeline.name}</h3>
                      <StatusBadge status={pipeline.status} />
                    </div>
                    <div className="flex items-center gap-4 text-sm text-muted-foreground">
                      <span className="flex items-center gap-1.5 font-medium text-foreground/80">
                        <span className="w-2 h-2 rounded-full bg-indigo-400"></span>
                        {pipeline.sourceSystem}
                      </span>
                      <span className="text-border">|</span>
                      <span>Last run: {format(new Date(pipeline.startTime), "MMM d, h:mm a")}</span>
                    </div>
                  </div>
                  
                  <div className="flex items-center gap-6 self-end sm:self-auto">
                     <div className="text-right">
                       <div className="text-sm font-medium text-foreground">{pipeline.duration}</div>
                       <div className="text-xs text-muted-foreground">Duration</div>
                     </div>
                     <div className="text-right">
                       <div className="text-sm font-medium text-emerald-600">{pipeline.successRate}%</div>
                       <div className="text-xs text-muted-foreground">Success Rate</div>
                     </div>
                     <Button variant="ghost" size="icon" className="h-8 w-8">
                       <ArrowRight className="h-4 w-4 text-muted-foreground" />
                     </Button>
                  </div>
                </div>

                {/* Timeline Visualization */}
                <PipelineProgress currentLayer={pipeline.layer} status={pipeline.status} />
              </div>
            ))}
          </div>
        </div>

        {/* Sidebar / Trends */}
        <div className="space-y-6">
           <Card className="bg-card shadow-sm border-border">
             <CardHeader>
               <CardTitle>7-Day Performance</CardTitle>
               <CardDescription>Success vs Failure rates</CardDescription>
             </CardHeader>
             <CardContent>
               <div className="h-[200px] w-full">
                 <ResponsiveContainer width="100%" height="100%">
                   <BarChart data={chartData}>
                     <XAxis dataKey="name" fontSize={12} tickLine={false} axisLine={false} />
                     <YAxis fontSize={12} tickLine={false} axisLine={false} tickFormatter={(value) => `${value}`} />
                     <Tooltip 
                        cursor={{fill: 'transparent'}}
                        contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                     />
                     <Bar dataKey="success" fill="hsl(var(--primary))" radius={[4, 4, 0, 0]} stackId="a" />
                     <Bar dataKey="failed" fill="hsl(var(--destructive))" radius={[4, 4, 0, 0]} stackId="a" />
                   </BarChart>
                 </ResponsiveContainer>
               </div>
             </CardContent>
           </Card>

           <Card className="bg-gradient-to-br from-primary/5 to-transparent border-primary/10 shadow-sm overflow-hidden relative">
             <div className="absolute top-0 right-0 p-4 opacity-5">
                <Activity className="w-24 h-24 text-primary" />
             </div>
             <CardHeader>
               <CardTitle className="text-primary flex items-center gap-2">
                 <Activity className="w-5 h-5" />
                 Pipeline Health
               </CardTitle>
               <CardDescription>Overall system status by layer</CardDescription>
             </CardHeader>
             <CardContent className="space-y-5 relative z-10">
               {[
                 { label: "Ingestion", status: "healthy", icon: Check },
                 { label: "Standardization", status: "working", icon: Play },
                 { label: "Business Ready", status: "warning", icon: AlertTriangle },
                 { label: "Analytics", status: "healthy", icon: Check }
               ].map((item, i) => (
                 <div key={item.label} className="flex items-center justify-between group">
                    <div className="flex items-center gap-3">
                      <div className={cn(
                        "w-8 h-8 rounded-full flex items-center justify-center text-white shadow-sm transition-transform group-hover:scale-110",
                        item.status === 'healthy' && "bg-emerald-500",
                        item.status === 'working' && "bg-blue-500 animate-pulse",
                        item.status === 'warning' && "bg-amber-500",
                        item.status === 'error' && "bg-rose-500",
                      )}>
                        <item.icon className="w-4 h-4" />
                      </div>
                      <span className="text-sm font-medium text-foreground">{item.label}</span>
                    </div>
                    {/* Visual Connector Line for all but last */}
                    {i < 3 && (
                      <div className="absolute left-[2.25rem] h-6 w-0.5 bg-border -z-10 translate-y-6" style={{ top: `${(i * 3.5) + 5}rem` }} />
                    )}
                 </div>
               ))}
             </CardContent>
           </Card>
        </div>
      </div>
    </div>
  );
}

function Activity({ className }: { className?: string }) {
  return (
    <svg 
      xmlns="http://www.w3.org/2000/svg" 
      width="24" 
      height="24" 
      viewBox="0 0 24 24" 
      fill="none" 
      stroke="currentColor" 
      strokeWidth="2" 
      strokeLinecap="round" 
      strokeLinejoin="round" 
      className={className}
    >
      <path d="M22 12h-4l-3 9L9 3l-3 9H2" />
    </svg>
  );
}
