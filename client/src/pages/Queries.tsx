import { useAppStore, api, type Query, useNotificationStore } from "@/lib/store";
import QueryBuilder from "@/components/QueryBuilder";
import EditQueryDialog from "@/components/EditQueryDialog";
import DataTablePreview from "@/components/DataTablePreview";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Play, Loader2, CheckCircle2, XCircle, Eye, Tag, Pencil, Table } from "lucide-react";
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

export default function Queries() {
  const { queries, setQueries } = useAppStore();
  const { addNotification, updateNotification } = useNotificationStore();
  const queryClient = useQueryClient();
  const [runningQueries, setRunningQueries] = useState<Set<string>>(new Set());
  const [notificationIds, setNotificationIds] = useState<Map<string, string>>(new Map());
  const [editQuery, setEditQuery] = useState<Query | null>(null);
  const [resultDialog, setResultDialog] = useState<{ open: boolean; query: Query | null; result: any; pythonResult?: any }>({
    open: false,
    query: null,
    result: null,
    pythonResult: null,
  });

  const { data: queriesData } = useQuery({
    queryKey: ['queries'],
    queryFn: api.getQueries,
  });

  useEffect(() => {
    if (queriesData) setQueries(queriesData);
  }, [queriesData, setQueries]);

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
      setResultDialog({ open: true, query: data.query, result: data.result, pythonResult });
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
            <div className="col-span-3">NAME</div>
            <div className="col-span-2">QUERY ID</div>
            <div className="col-span-2">CONNECTOR</div>
            <div className="col-span-3">TAGS</div>
            <div className="col-span-2 text-right">ACTIONS</div>
          </div>
          
          <div className="divide-y max-h-[500px] overflow-y-auto">
            {queries.map((query) => (
              <div key={query.id} className="grid grid-cols-12 gap-4 p-4 items-center hover:bg-muted/20 transition-colors" data-testid={`query-row-${query.id}`}>
                <div className="col-span-3">
                  <p className="font-medium text-sm truncate">{query.queryName}</p>
                  <p className="text-xs text-muted-foreground line-clamp-1">{query.description}</p>
                </div>
                <div className="col-span-2 text-xs font-mono text-muted-foreground truncate">
                  {query.queryId}
                </div>
                <div className="col-span-2">
                  <Badge variant="outline" className="font-mono text-xs">{query.connectorId}</Badge>
                </div>
                <div className="col-span-3 flex flex-wrap gap-1">
                  {query.tags?.slice(0, 3).map((tag) => (
                    <Badge key={tag} variant="secondary" className="text-xs">{tag}</Badge>
                  ))}
                  {query.tags && query.tags.length > 3 && (
                    <Badge variant="secondary" className="text-xs">+{query.tags.length - 3}</Badge>
                  )}
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
        <DialogContent className="max-w-5xl max-h-[85vh]">
          <DialogHeader>
            <DialogTitle>Query Result: {resultDialog.query?.queryName}</DialogTitle>
            <DialogDescription className="flex items-center gap-4">
              <span>Executed at {resultDialog.result?.executedAt ? new Date(resultDialog.result.executedAt).toLocaleString() : 'N/A'}</span>
              {resultDialog.pythonResult?.record_count && (
                <Badge variant="secondary">{resultDialog.pythonResult.record_count} records</Badge>
              )}
              {resultDialog.pythonResult?.source && (
                <Badge variant="outline">{resultDialog.pythonResult.source}</Badge>
              )}
            </DialogDescription>
          </DialogHeader>
          
          <Tabs defaultValue="table" className="mt-2">
            <TabsList>
              <TabsTrigger value="table" data-testid="result-tab-table">
                <Table className="h-3 w-3 mr-2" />
                Table View
              </TabsTrigger>
              <TabsTrigger value="json" data-testid="result-tab-json">
                JSON
              </TabsTrigger>
            </TabsList>
            
            <TabsContent value="table" className="mt-4">
              {resultDialog.pythonResult?.data ? (
                <DataTablePreview data={resultDialog.pythonResult.data} maxRows={200} />
              ) : (
                <div className="text-center text-muted-foreground py-8">
                  No tabular data available
                </div>
              )}
            </TabsContent>
            
            <TabsContent value="json" className="mt-4">
              <ScrollArea className="max-h-[50vh]">
                <pre className="p-4 bg-muted rounded-lg text-xs font-mono overflow-x-auto">
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
