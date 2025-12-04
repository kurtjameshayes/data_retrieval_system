import { useAppStore, api } from "@/lib/store";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Activity, Database, Plug, ArrowRight, Clock } from "lucide-react";
import { Link } from "wouter";
import { formatDistanceToNow } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { useEffect } from "react";

export default function Home() {
  const { connectors, queries, setConnectors, setQueries } = useAppStore();

  const { data: connectorsData } = useQuery({
    queryKey: ['connectors'],
    queryFn: api.getConnectors,
  });

  const { data: queriesData } = useQuery({
    queryKey: ['queries'],
    queryFn: api.getQueries,
  });

  useEffect(() => {
    if (connectorsData) setConnectors(connectorsData);
  }, [connectorsData, setConnectors]);

  useEffect(() => {
    if (queriesData) setQueries(queriesData);
  }, [queriesData, setQueries]);

  const successQueries = queries.filter(q => q.status === 'success').length;
  const totalConnectors = connectors.length;

  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between bg-card/50 backdrop-blur border rounded-lg px-4 py-2">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <div className="h-2 w-2 rounded-full bg-green-500 animate-pulse" />
            <span className="text-sm font-medium text-green-500">System Operational</span>
          </div>
          <span className="text-xs text-muted-foreground border-l pl-3">All systems nominal</span>
        </div>
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <span>v2.4.0-RC1</span>
        </div>
      </div>

      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">System Overview</h1>
          <p className="text-muted-foreground mt-1">
            Data retrieval status and active connections.
          </p>
        </div>
        <div className="flex gap-3">
           <Link href="/queries">
             <Button data-testid="button-new-query">
               <Activity className="mr-2 h-4 w-4" /> New Query
             </Button>
           </Link>
           <Link href="/connectors">
             <Button variant="outline" data-testid="button-manage-connectors">
               <Plug className="mr-2 h-4 w-4" /> Manage Connectors
             </Button>
           </Link>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2">
        <Card className="bg-card/50 backdrop-blur">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Active Connectors</CardTitle>
            <Plug className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold" data-testid="text-connector-count">{totalConnectors}</div>
            <p className="text-xs text-muted-foreground">
              Ready for queries
            </p>
          </CardContent>
        </Card>
        <Card className="bg-card/50 backdrop-blur">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total Queries</CardTitle>
            <Database className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold" data-testid="text-query-count">{queries.length}</div>
            <p className="text-xs text-muted-foreground">
              {successQueries} successful executions
            </p>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-7">
        <Card className="col-span-4 bg-card/50 backdrop-blur">
          <CardHeader>
            <CardTitle>Recent Queries</CardTitle>
            <CardDescription>
              Latest data retrieval operations executed by the system.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {queries.slice(0, 5).map((query) => (
                <div key={query.id} className="flex items-center justify-between p-3 border rounded-lg bg-background/50" data-testid={`query-item-${query.id}`}>
                  <div className="flex items-center gap-4">
                    <div className={`w-2 h-2 rounded-full ${
                      query.status === 'success' ? 'bg-green-500' : 
                      query.status === 'error' ? 'bg-red-500' : 'bg-gray-300'
                    }`} />
                    <div>
                      <p className="text-sm font-medium leading-none">{query.name}</p>
                      <p className="text-xs text-muted-foreground font-mono mt-1">
                        {query.method} {query.endpoint}
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center gap-4 text-sm text-muted-foreground">
                    {query.lastRun && (
                      <span className="flex items-center gap-1 text-xs">
                        <Clock className="h-3 w-3" />
                        {formatDistanceToNow(new Date(query.lastRun), { addSuffix: true })}
                      </span>
                    )}
                    <Badge variant="outline">{query.status || 'idle'}</Badge>
                  </div>
                </div>
              ))}
              {queries.length === 0 && (
                <div className="text-center py-8 text-muted-foreground">
                  No queries executed yet.
                </div>
              )}
            </div>
          </CardContent>
        </Card>

        <Card className="col-span-3 bg-card/50 backdrop-blur">
          <CardHeader>
            <CardTitle>Connectors</CardTitle>
            <CardDescription>
              Available data sources.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {connectors.map((connector) => (
                <div key={connector.id} className="flex items-start justify-between space-x-4" data-testid={`connector-item-${connector.id}`}>
                  <div className="space-y-1">
                    <p className="text-sm font-medium leading-none">{connector.name}</p>
                    <p className="text-xs text-muted-foreground line-clamp-1">
                      {connector.baseUrl}
                    </p>
                  </div>
                  <Badge variant="secondary" className="text-xs">{connector.type}</Badge>
                </div>
              ))}
              {connectors.length === 0 && (
                <div className="text-center py-4 text-muted-foreground text-sm">
                  No connectors configured yet.
                </div>
              )}
            </div>
            <div className="mt-6">
               <Link href="/connectors">
                 <Button variant="ghost" className="w-full text-xs" data-testid="link-view-connectors">
                   View All Connectors <ArrowRight className="ml-2 h-3 w-3" />
                 </Button>
               </Link>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
