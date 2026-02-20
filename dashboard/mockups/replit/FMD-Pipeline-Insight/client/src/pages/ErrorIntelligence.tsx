import { mockErrors, Severity } from "@/lib/mockData";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { AlertTriangle, Info, ShieldAlert, Sparkles, CheckCircle, ExternalLink } from "lucide-react";
import { cn } from "@/lib/utils";
import { format } from "date-fns";

export default function ErrorIntelligence() {
  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-display font-bold tracking-tight text-foreground">Error Intelligence</h1>
          <p className="text-muted-foreground mt-1">AI-powered diagnostics and resolution suggestions.</p>
        </div>
      </div>

      {/* AI Insight Hero */}
      <div className="bg-gradient-to-r from-indigo-50 to-purple-50 dark:from-indigo-950/20 dark:to-purple-950/20 p-6 rounded-xl border border-indigo-100 dark:border-indigo-900/50 shadow-sm relative overflow-hidden">
        <div className="absolute top-0 right-0 p-4 opacity-10">
          <Sparkles className="w-48 h-48 text-indigo-500" />
        </div>
        
        <div className="relative z-10 flex gap-4">
          <div className="p-3 bg-white dark:bg-card rounded-lg shadow-sm h-fit">
            <Sparkles className="w-6 h-6 text-indigo-600" />
          </div>
          <div className="space-y-2 max-w-2xl">
            <h3 className="text-lg font-semibold text-indigo-950 dark:text-indigo-100">AI Weekly Summary</h3>
            <p className="text-indigo-900/80 dark:text-indigo-200/80 text-sm leading-relaxed">
              We've detected a recurring schema mismatch pattern in the <span className="font-medium">Google Analytics</span> source system. 
              This accounts for 60% of critical failures this week. It appears to be related to the new 'session_quality' field release.
            </p>
            <div className="pt-2">
              <Button size="sm" className="bg-indigo-600 hover:bg-indigo-700 text-white shadow-sm border-0">
                View Detailed Analysis
              </Button>
            </div>
          </div>
        </div>
      </div>

      <div className="grid gap-6">
        {mockErrors.map((error) => (
          <Card key={error.id} className="border-l-4 border-l-destructive shadow-sm hover:shadow-md transition-shadow">
            <CardHeader className="flex flex-row items-start justify-between space-y-0 pb-2">
              <div className="space-y-1">
                <div className="flex items-center gap-2">
                  <CardTitle className="text-lg font-medium">{error.summary}</CardTitle>
                  <Badge variant={error.severity === 'Critical' ? 'destructive' : 'secondary'} className="uppercase text-[10px] tracking-wider font-bold">
                    {error.severity}
                  </Badge>
                </div>
                <CardDescription>
                  Pipeline: <span className="font-medium text-foreground">{error.pipelineName}</span> • Source: <span className="font-medium text-foreground">{error.sourceSystem}</span>
                </CardDescription>
              </div>
              <div className="text-xs text-muted-foreground flex items-center gap-1 bg-muted px-2 py-1 rounded-md">
                <ClockIcon className="w-3 h-3" />
                {format(new Date(error.timestamp), "MMM d, h:mm a")}
              </div>
            </CardHeader>
            <CardContent>
              <div className="grid md:grid-cols-2 gap-6 mt-4">
                <div className="space-y-3">
                   <div className="flex items-start gap-2 text-sm">
                     <AlertTriangle className="w-4 h-4 text-amber-500 mt-0.5 shrink-0" />
                     <div>
                       <span className="font-medium text-foreground">Root Cause:</span>
                       <p className="text-muted-foreground mt-0.5">{error.rootCause}</p>
                     </div>
                   </div>
                   <div className="flex items-start gap-2 text-sm">
                     <Info className="w-4 h-4 text-blue-500 mt-0.5 shrink-0" />
                     <div>
                       <span className="font-medium text-foreground">Frequency:</span>
                       <p className="text-muted-foreground mt-0.5">Occurred {error.frequency} times in the last 24h</p>
                     </div>
                   </div>
                </div>

                <div className="bg-emerald-50/50 dark:bg-emerald-950/10 p-4 rounded-lg border border-emerald-100 dark:border-emerald-900/20">
                  <div className="flex items-start gap-2 text-sm">
                    <CheckCircle className="w-4 h-4 text-emerald-600 mt-0.5 shrink-0" />
                    <div className="space-y-2">
                      <span className="font-semibold text-emerald-900 dark:text-emerald-100">Suggested Fix</span>
                      <p className="text-emerald-800 dark:text-emerald-200/80">{error.suggestedFix}</p>
                      <Button variant="outline" size="sm" className="h-7 text-xs border-emerald-200 hover:bg-emerald-100 text-emerald-700 mt-2">
                        Apply Fix <ExternalLink className="w-3 h-3 ml-1" />
                      </Button>
                    </div>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
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
