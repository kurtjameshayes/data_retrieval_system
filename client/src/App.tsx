import { Switch, Route } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import NotificationToast from "@/components/NotificationToast";
import NotFound from "@/pages/not-found";
import Layout from "@/components/Layout";
import Home from "@/pages/Home";
import Connectors from "@/pages/Connectors";
import Queries from "@/pages/Queries";
import Analysis from "@/pages/Analysis";
import AnalysisPlans from "@/pages/AnalysisPlans";
import DataPreview from "@/pages/DataPreview";

function Router() {
  return (
    <Layout>
      <Switch>
        <Route path="/" component={Home} />
        <Route path="/connectors" component={Connectors} />
        <Route path="/queries" component={Queries} />
        <Route path="/analysis" component={Analysis} />
        <Route path="/analysis-plans" component={AnalysisPlans} />
        <Route path="/data-preview" component={DataPreview} />
        <Route component={NotFound} />
      </Switch>
    </Layout>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <Router />
        <Toaster />
        <NotificationToast />
      </TooltipProvider>
    </QueryClientProvider>
  );
}

export default App;
