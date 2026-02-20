import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { CheckCircle2, AlertCircle, Database, Server, Shield, Activity, GitBranch } from "lucide-react";
import { mockHealth } from "@/lib/mockData";

export default function AdminGovernance() {
  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-display font-bold tracking-tight text-foreground">Admin & Governance</h1>
          <p className="text-muted-foreground mt-1">System health, data lineage, and connection registry.</p>
        </div>
      </div>

      {/* Health Scorecard */}
      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
        <Card className="bg-card shadow-sm border-border">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Health Score</CardTitle>
            <Activity className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold font-display">{mockHealth.overallScore}/100</div>
            <Progress value={mockHealth.overallScore} className="h-2 mt-2 bg-muted text-primary" />
            <p className="text-xs text-muted-foreground mt-2">Optimal performance</p>
          </CardContent>
        </Card>
        
        <Card className="bg-card shadow-sm border-border">
           <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Data Freshness</CardTitle>
            <ClockIcon className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold font-display">{mockHealth.freshness}</div>
            <p className="text-xs text-muted-foreground mt-2">Last sync completed</p>
          </CardContent>
        </Card>

        <Card className="bg-card shadow-sm border-border">
           <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Active Pipelines</CardTitle>
            <GitBranch className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold font-display">{mockHealth.activePipelines}</div>
            <div className="flex items-center gap-2 mt-2">
              <Badge variant="outline" className="text-xs font-normal bg-emerald-50 text-emerald-700 border-emerald-200">41 Healthy</Badge>
              <Badge variant="outline" className="text-xs font-normal bg-rose-50 text-rose-700 border-rose-200">1 Critical</Badge>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-card shadow-sm border-border">
           <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Security</CardTitle>
            <Shield className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold font-display">Secure</div>
            <p className="text-xs text-muted-foreground mt-2">No active threats detected</p>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-8 md:grid-cols-3">
        {/* Source System Registry */}
        <Card className="md:col-span-2 shadow-sm border-border">
          <CardHeader>
            <CardTitle>Source System Registry</CardTitle>
            <CardDescription>Connection status and health of upstream data sources.</CardDescription>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>System Name</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Last Ping</TableHead>
                  <TableHead className="text-right">Action</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {[
                  { name: "Salesforce CRM", type: "SaaS", status: "Active", ping: "2ms" },
                  { name: "SAP ERP", type: "On-Premise", status: "Active", ping: "45ms" },
                  { name: "Google Analytics", type: "API", status: "Warning", ping: "450ms" },
                  { name: "Oracle Finance", type: "Database", status: "Active", ping: "12ms" },
                  { name: "HubSpot", type: "SaaS", status: "Active", ping: "140ms" },
                ].map((system) => (
                  <TableRow key={system.name}>
                    <TableCell className="font-medium flex items-center gap-2">
                      <div className="p-1.5 bg-muted rounded-md">
                        <Database className="w-3 h-3 text-muted-foreground" />
                      </div>
                      {system.name}
                    </TableCell>
                    <TableCell>{system.type}</TableCell>
                    <TableCell>
                      {system.status === 'Active' ? (
                        <span className="flex items-center gap-1.5 text-emerald-600 text-xs font-medium">
                          <CheckCircle2 className="w-3.5 h-3.5" /> Healthy
                        </span>
                      ) : (
                        <span className="flex items-center gap-1.5 text-amber-600 text-xs font-medium">
                          <AlertCircle className="w-3.5 h-3.5" /> High Latency
                        </span>
                      )}
                    </TableCell>
                    <TableCell className="text-muted-foreground">{system.ping}</TableCell>
                    <TableCell className="text-right">
                      <Button variant="ghost" size="sm" className="h-8 text-xs">Configure</Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        {/* Data Lineage Visualization (Simplified) */}
        <Card className="shadow-sm border-border">
           <CardHeader>
            <CardTitle>Data Lineage</CardTitle>
            <CardDescription>Flow across layers</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="relative border-l-2 border-dashed border-border ml-4 space-y-8 py-2">
              <div className="relative pl-8">
                <div className="absolute -left-[9px] top-1 w-4 h-4 rounded-full bg-slate-200 border-2 border-white dark:border-slate-900" />
                <h4 className="font-semibold text-sm">Landing Zone</h4>
                <p className="text-xs text-muted-foreground mt-1">Raw file ingestion (S3/Blob)</p>
                <div className="mt-2 flex gap-2">
                  <Badge variant="secondary" className="text-[10px]">JSON</Badge>
                  <Badge variant="secondary" className="text-[10px]">CSV</Badge>
                </div>
              </div>
              
              <div className="relative pl-8">
                <div className="absolute -left-[9px] top-1 w-4 h-4 rounded-full bg-blue-200 border-2 border-white dark:border-slate-900" />
                <h4 className="font-semibold text-sm">Bronze Layer</h4>
                <p className="text-xs text-muted-foreground mt-1">Raw tables, history preserved</p>
                <div className="mt-2 text-xs font-mono bg-muted p-1 rounded w-fit">24 Tables</div>
              </div>

              <div className="relative pl-8">
                <div className="absolute -left-[9px] top-1 w-4 h-4 rounded-full bg-indigo-200 border-2 border-white dark:border-slate-900" />
                <h4 className="font-semibold text-sm">Silver Layer</h4>
                <p className="text-xs text-muted-foreground mt-1">Cleaned, deduped, standardized</p>
                <div className="mt-2 text-xs font-mono bg-muted p-1 rounded w-fit">18 Tables</div>
              </div>

              <div className="relative pl-8">
                <div className="absolute -left-[9px] top-1 w-4 h-4 rounded-full bg-emerald-200 border-2 border-white dark:border-slate-900" />
                <h4 className="font-semibold text-sm">Gold Layer</h4>
                <p className="text-xs text-muted-foreground mt-1">Business aggregates & dimensions</p>
                <div className="mt-2 text-xs font-mono bg-muted p-1 rounded w-fit">12 Models</div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function ClockIcon({ className }: { className?: string }) {
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
      <circle cx="12" cy="12" r="10" />
      <polyline points="12 6 12 12 16 14" />
    </svg>
  );
}
