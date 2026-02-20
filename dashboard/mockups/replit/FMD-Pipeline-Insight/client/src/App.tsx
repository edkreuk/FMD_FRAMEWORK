import { Switch, Route } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import NotFound from "@/pages/not-found";
import PipelineMonitor from "@/pages/PipelineMonitor";
import ErrorIntelligence from "@/pages/ErrorIntelligence";
import AdminGovernance from "@/pages/AdminGovernance";
import { AppLayout } from "@/components/layout/AppLayout";

function Router() {
  return (
    <AppLayout>
      <Switch>
        <Route path="/" component={PipelineMonitor} />
        <Route path="/errors" component={ErrorIntelligence} />
        <Route path="/admin" component={AdminGovernance} />
        <Route component={NotFound} />
      </Switch>
    </AppLayout>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <Toaster />
        <Router />
      </TooltipProvider>
    </QueryClientProvider>
  );
}

export default App;
