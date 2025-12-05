import { useAppStore, api, type Query, type Connector, useNotificationStore } from "@/lib/store";
import QueryBuilder from "@/components/QueryBuilder";
import EditQueryDialog from "@/components/EditQueryDialog";
import DataTablePreview from "@/components/DataTablePreview";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Play, Loader2, Pencil, Table, AlertCircle } from "lucide-react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

function extractDataByPath(data: any, path: string | null | undefined): any[] {
  if (!path || !data) return [];
  
  const parts = path.split('.');
  let current = data;
  
  for (const part of parts) {
    if (current && typeof current === 'object' && part in current) {
      current = current[part];
    } else {
      return [];
    }
  }
  
  return Array.isArray(current) ? current : [];
}

export default function Queries() {
  const { queries, setQueries, connectors, setConnectors } = useAppStore();
  const { addNotification, updateNotification } = useNotificationStore();
  const queryClient = useQueryClient();
  const [runningQueries, setRunningQueries] = useState<Set<string>>(new Set());
  const [notificationIds, setNotificationIds] = useState<Map<string, string>>(new Map());
  const [editQuery, setEditQuery] = useState<Query | null>(null);
  const [resultDialog, setResultDialog] = useState<{ open: boolean; query: Query | null; result: any; pythonResult?: any; connector?: Connector | null }>({
    open: false,
    query: null,
    result: null,
    pythonResult: null,
    connector: null,
  });

  const { data: queriesData } = useQuery({
    queryKey: ['queries'],
    queryFn: api.getQueries,
  });

  const { data: connectorsData } = useQuery({
    queryKey: ['connectors'],
    queryFn: api.getConnectors,
  });

  useEffect(() => {
    if (queriesData) setQueries(queriesData);
  }, [queriesData, setQueries]);

  useEffect(() => {
    if (connectorsData) setConnectors(connectorsData);
  }, [connectorsData, setConnectors]);

  const runMutation = useMutation({
    mutationFn: api.runQuery,
    onMutate: (id) => {
      setRunningQueries(prev => new Set(prev).add(id));
      const query = queries.find(q => q.id === id);
      const notifId = addNotification({
        type: 'loading',
        title: 'Query Running',
        message: 'Executing query via Python QueryEngine...',
        queryName: query?.queryName || id,
        queryId: id,
      });
      setNotificationIds(prev => new Map(prev).set(id, notifId));
    },
    onSuccess: (data, id) => {
      setRunningQueries(prev => {
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
      
      const notifId = notificationIds.get(id);
      if (notifId) {
        const pythonResult = (data as any).pythonResult;
        const recordCount = pythonResult?.record_count;
        const source = pythonResult?.source;
        
        updateNotification(notifId, {
          type: 'success',
          title: 'Query Completed',
          message: recordCount 
            ? `Retrieved ${recordCount} records${source === 'cache' ? ' (from cache)' : ''}`
            : 'Query executed successfully',
          duration: 5000,
        });
        setNotificationIds(prev => {
          const next = new Map(prev);
          next.delete(id);
          return next;
        });
      }
      
      const pythonResult = (data as any).pythonResult;
      const queryConnector = connectors.find(c => c.sourceId === data.query.connectorId);
      setResultDialog({ open: true, query: data.query, result: data.result, pythonResult, connector: queryConnector });
      queryClient.invalidateQueries({ queryKey: ['queries'] });
    },
    onError: (error, id) => {
      setRunningQueries(prev => {
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
      
      const notifId = notificationIds.get(id);
      if (notifId) {
        updateNotification(notifId, {
          type: 'error',
          title: 'Query Failed',
          message: error instanceof Error ? error.message : 'An error occurred while executing the query',
          duration: 8000,
        });
        setNotificationIds(prev => {
          const next = new Map(prev);
          next.delete(id);
          return next;
        });
      }
    },
  });

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Query Builder</h1>
        <p className="text-muted-foreground">
          Construct and execute data retrieval operations.
        </p>
      </div>

      <QueryBuilder />

      <div className="mt-12">
        <h3 className="text-lg font-medium mb-4">Query Library</h3>
        <div className="border rounded-lg bg-card/50 overflow-hidden">
          <div className="grid grid-cols-12 gap-4 p-4 border-b bg-muted/30 text-xs font-medium text-muted-foreground">
            <div className="col-span-4">NAME</div>
            <div className="col-span-3">QUERY ID</div>
            <div className="col-span-3">CONNECTOR</div>
            <div className="col-span-2 text-right">ACTIONS</div>
          </div>
          
          <div className="divide-y max-h-[500px] overflow-y-auto">
            {queries.map((query) => (
              <div key={query.id} className="grid grid-cols-12 gap-4 p-4 items-center hover:bg-muted/20 transition-colors" data-testid={`query-row-${query.id}`}>
                <div className="col-span-4">
                  <p className="font-medium text-sm truncate">{query.queryName}</p>
                  <p className="text-xs text-muted-foreground line-clamp-1">{query.description}</p>
                </div>
                <div className="col-span-3 text-xs font-mono text-muted-foreground truncate">
                  {query.queryId}
                </div>
                <div className="col-span-3">
                  <Badge variant="outline" className="font-mono text-xs">{query.connectorId}</Badge>
                </div>
                <div className="col-span-2 text-right flex items-center justify-end gap-1">
                  <Button 
                    size="sm" 
                    variant="ghost"
                    onClick={() => setEditQuery(query)}
                    data-testid={`button-edit-query-${query.id}`}
                    title="Edit query"
                  >
                    <Pencil className="h-3 w-3" />
                  </Button>
                  <Button 
                    size="sm" 
                    variant="outline" 
                    onClick={() => runMutation.mutate(query.id)}
                    disabled={runningQueries.has(query.id)}
                    data-testid={`button-run-query-${query.id}`}
                  >
                    {runningQueries.has(query.id) ? (
                      <><Loader2 className="h-3 w-3 mr-2 animate-spin" /> Running</>
                    ) : (
                      <><Play className="h-3 w-3 mr-2" /> Run</>
                    )}
                  </Button>
                </div>
              </div>
            ))}
            {queries.length === 0 && (
              <div className="p-8 text-center text-muted-foreground text-sm">
                No queries saved in the library.
              </div>
            )}
          </div>
        </div>
      </div>

      <Dialog open={resultDialog.open} onOpenChange={(open) => setResultDialog(prev => ({ ...prev, open }))}>
        <DialogContent className="max-w-5xl max-h-[85vh] overflow-hidden flex flex-col">
          <DialogHeader>
            <DialogTitle>Query Result: {resultDialog.query?.queryName}</DialogTitle>
            <DialogDescription className="flex items-center gap-4 flex-wrap">
              <span>Executed at {resultDialog.result?.executedAt ? new Date(resultDialog.result.executedAt).toLocaleString() : 'N/A'}</span>
              {resultDialog.pythonResult?.record_count && (
                <Badge variant="secondary">{resultDialog.pythonResult.record_count} records</Badge>
              )}
              {resultDialog.pythonResult?.source && (
                <Badge variant="outline">{resultDialog.pythonResult.source}</Badge>
              )}
              {resultDialog.connector?.dataPath && (
                <Badge variant="outline" className="font-mono text-xs">data: {resultDialog.connector.dataPath}</Badge>
              )}
            </DialogDescription>
          </DialogHeader>

          {resultDialog.pythonResult?.error && (
            <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-4 flex items-start gap-3">
              <AlertCircle className="h-5 w-5 text-red-500 flex-shrink-0 mt-0.5" />
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-red-500">Query Failed</p>
                <p className="text-sm text-red-400 mt-1 break-words">{resultDialog.pythonResult.error}</p>
                {resultDialog.pythonResult.traceback && (
                  <pre className="mt-2 text-xs text-red-300 bg-red-500/10 p-2 rounded overflow-x-auto max-h-32">
                    {resultDialog.pythonResult.traceback}
                  </pre>
                )}
              </div>
            </div>
          )}
          
          <Tabs defaultValue="table" className="mt-2 flex-1 flex flex-col min-h-0">
            <TabsList>
              <TabsTrigger value="table" data-testid="result-tab-table">
                <Table className="h-3 w-3 mr-2" />
                Table View
              </TabsTrigger>
              <TabsTrigger value="json" data-testid="result-tab-json">
                JSON
              </TabsTrigger>
            </TabsList>
            
            <TabsContent value="table" className="mt-4 flex-1 min-h-0">
              {(() => {
                const dataPath = resultDialog.connector?.dataPath;
                const rawData = resultDialog.pythonResult?.data;
                const tableData = dataPath && rawData ? extractDataByPath(rawData, dataPath) : rawData;
                
                if (tableData && (Array.isArray(tableData) ? tableData.length > 0 : true)) {
                  return <DataTablePreview data={tableData} maxRows={200} />;
                }
                return (
                  <div className="text-center text-muted-foreground py-8">
                    No tabular data available
                  </div>
                );
              })()}
            </TabsContent>
            
            <TabsContent value="json" className="mt-4 flex-1 min-h-0 overflow-hidden">
              <ScrollArea className="h-full max-h-[50vh]">
                <pre className="p-4 bg-muted rounded-lg text-xs font-mono whitespace-pre-wrap break-words">
                  {JSON.stringify(resultDialog.pythonResult || resultDialog.result?.result, null, 2)}
                </pre>
              </ScrollArea>
            </TabsContent>
          </Tabs>
        </DialogContent>
      </Dialog>

      <EditQueryDialog
        query={editQuery}
        open={!!editQuery}
        onOpenChange={(open) => !open && setEditQuery(null)}
      />
    </div>
  );
}
