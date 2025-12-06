import { useState, useEffect } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { api, useNotificationStore, type Query, type AnalysisPlan } from "@/lib/store";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Loader2, Play, Database, Table2, Eye, ChevronRight, Columns3 } from "lucide-react";

interface DataPreviewResult {
  success: boolean;
  plan_id?: string;
  total_rows: number;
  total_columns: number;
  preview_rows: number;
  columns: string[];
  column_types: Record<string, string>;
  data: Record<string, any>[];
  error?: string;
}

export default function DataPreview() {
  const { addNotification } = useNotificationStore();
  
  const [selectedPlan, setSelectedPlan] = useState<AnalysisPlan | null>(null);
  const [previewData, setPreviewData] = useState<DataPreviewResult | null>(null);
  const [recordDetailOpen, setRecordDetailOpen] = useState(false);
  const [selectedRecord, setSelectedRecord] = useState<Record<string, any> | null>(null);
  const [previewLimit, setPreviewLimit] = useState(100);
  const [isLoadingPreview, setIsLoadingPreview] = useState(false);

  const { data: plansData, isLoading: plansLoading } = useQuery({
    queryKey: ['analysis-plans'],
    queryFn: api.getAnalysisPlans,
  });

  const { data: queriesData } = useQuery({
    queryKey: ['queries'],
    queryFn: api.getQueries,
  });

  const plans = plansData || [];
  const queries = queriesData || [];

  const getQueryName = (queryId: string) => {
    const query = queries.find((q: Query) => q.queryId === queryId);
    return query?.queryName || queryId;
  };

  const loadPreview = async (planId: string, limit: number = 100) => {
    setIsLoadingPreview(true);
    try {
      const response = await fetch(`/api/data-preview/plan/${planId}?limit=${limit}`);
      const result = await response.json();
      
      if (result.success) {
        setPreviewData(result);
        addNotification({
          type: 'success',
          title: 'Data Loaded',
          message: `Loaded ${result.preview_rows} of ${result.total_rows} rows`,
          duration: 3000
        });
      } else {
        addNotification({
          type: 'error',
          title: 'Failed to load data',
          message: result.error,
          duration: 5000
        });
        setPreviewData(null);
      }
    } catch (error: any) {
      addNotification({
        type: 'error',
        title: 'Failed to load data',
        message: error.message,
        duration: 5000
      });
      setPreviewData(null);
    } finally {
      setIsLoadingPreview(false);
    }
  };

  const handlePlanSelect = (planId: string) => {
    const plan = plans.find((p: AnalysisPlan) => p.plan_id === planId);
    setSelectedPlan(plan || null);
    setPreviewData(null);
  };

  const handleRowDoubleClick = (record: Record<string, any>) => {
    setSelectedRecord(record);
    setRecordDetailOpen(true);
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight font-mono" data-testid="page-title">
            Data Preview
          </h1>
          <p className="text-muted-foreground mt-1">
            View joined query data from analysis plans
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        <Card className="lg:col-span-1">
          <CardHeader>
            <CardTitle className="text-lg flex items-center gap-2">
              <Database className="h-5 w-5" />
              Analysis Plans
            </CardTitle>
            <CardDescription>
              Select a plan to preview its joined data
            </CardDescription>
          </CardHeader>
          <CardContent className="p-0">
            {plansLoading ? (
              <div className="flex items-center justify-center p-8">
                <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
              </div>
            ) : plans.length === 0 ? (
              <div className="p-4 text-center text-muted-foreground text-sm">
                No analysis plans found
              </div>
            ) : (
              <ScrollArea className="h-[400px]">
                <div className="p-2 space-y-1">
                  {plans.map((plan: AnalysisPlan) => (
                    <button
                      key={plan.plan_id}
                      onClick={() => handlePlanSelect(plan.plan_id)}
                      className={`w-full text-left p-3 rounded-md transition-colors ${
                        selectedPlan?.plan_id === plan.plan_id
                          ? 'bg-primary/10 border border-primary'
                          : 'hover:bg-muted border border-transparent'
                      }`}
                      data-testid={`plan-select-${plan.plan_id}`}
                    >
                      <div className="flex items-center justify-between">
                        <span className="font-medium text-sm">{plan.plan_name}</span>
                        <ChevronRight className="h-4 w-4 text-muted-foreground" />
                      </div>
                      <div className="flex items-center gap-2 mt-1">
                        <Badge variant="outline" className="text-xs">
                          {plan.queries?.length || 0} queries
                        </Badge>
                        {!plan.active && (
                          <Badge variant="secondary" className="text-xs">Inactive</Badge>
                        )}
                      </div>
                    </button>
                  ))}
                </div>
              </ScrollArea>
            )}
          </CardContent>
        </Card>

        <Card className="lg:col-span-3">
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle className="text-lg flex items-center gap-2">
                  <Table2 className="h-5 w-5" />
                  {selectedPlan ? selectedPlan.plan_name : 'Data Preview'}
                </CardTitle>
                <CardDescription>
                  {selectedPlan 
                    ? `Joined data from ${selectedPlan.queries?.length || 0} queries`
                    : 'Select an analysis plan to view data'}
                </CardDescription>
              </div>
              {selectedPlan && (
                <div className="flex items-center gap-2">
                  <Select
                    value={String(previewLimit)}
                    onValueChange={(val) => setPreviewLimit(Number(val))}
                  >
                    <SelectTrigger className="w-32" data-testid="limit-select">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="20">20 rows</SelectItem>
                      <SelectItem value="50">50 rows</SelectItem>
                      <SelectItem value="100">100 rows</SelectItem>
                      <SelectItem value="250">250 rows</SelectItem>
                      <SelectItem value="500">500 rows</SelectItem>
                    </SelectContent>
                  </Select>
                  <Button
                    onClick={() => loadPreview(selectedPlan.plan_id, previewLimit)}
                    disabled={isLoadingPreview}
                    data-testid="load-preview-btn"
                  >
                    {isLoadingPreview ? (
                      <Loader2 className="h-4 w-4 animate-spin mr-2" />
                    ) : (
                      <Play className="h-4 w-4 mr-2" />
                    )}
                    Load Data
                  </Button>
                </div>
              )}
            </div>
          </CardHeader>
          <CardContent>
            {!selectedPlan ? (
              <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
                <Database className="h-12 w-12 mb-4 opacity-50" />
                <p>Select an analysis plan from the left to preview joined data</p>
              </div>
            ) : !previewData ? (
              <div className="space-y-4">
                <div className="border rounded-lg p-4">
                  <h4 className="font-medium mb-3">Queries in this plan:</h4>
                  <div className="space-y-2">
                    {selectedPlan.queries?.map((q: any, i: number) => (
                      <div key={i} className="flex items-center gap-2 text-sm">
                        <Badge variant="outline">{i + 1}</Badge>
                        <span className="font-mono">{q.query_id}</span>
                        <span className="text-muted-foreground">→ {getQueryName(q.query_id)}</span>
                        {q.join_column && (
                          <Badge variant="secondary" className="ml-auto">
                            Join: {q.join_column}
                          </Badge>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
                <div className="flex items-center justify-center py-8 text-muted-foreground">
                  <p>Click "Load Data" to fetch and join query results</p>
                </div>
              </div>
            ) : (
              <Tabs defaultValue="table" className="space-y-4">
                <div className="flex items-center justify-between">
                  <TabsList>
                    <TabsTrigger value="table" data-testid="tab-table">
                      <Table2 className="h-4 w-4 mr-2" />
                      Data Table
                    </TabsTrigger>
                    <TabsTrigger value="columns" data-testid="tab-columns">
                      <Columns3 className="h-4 w-4 mr-2" />
                      Columns ({previewData.columns.length})
                    </TabsTrigger>
                  </TabsList>
                  <div className="flex items-center gap-4 text-sm text-muted-foreground">
                    <span>
                      Showing <strong>{previewData.preview_rows}</strong> of{' '}
                      <strong>{previewData.total_rows}</strong> rows
                    </span>
                    <span>•</span>
                    <span>
                      <strong>{previewData.total_columns}</strong> columns
                    </span>
                  </div>
                </div>

                <TabsContent value="table" className="m-0">
                  <div className="border rounded-lg overflow-hidden">
                    <ScrollArea className="h-[500px]" data-testid="data-table-scroll">
                      <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                          <thead className="bg-muted/50 sticky top-0 z-10">
                            <tr>
                              <th className="text-left p-3 font-mono text-xs text-muted-foreground border-r bg-muted/50 sticky left-0 z-20">
                                #
                              </th>
                              {previewData.columns.map((col, i) => (
                                <th
                                  key={i}
                                  className="text-left p-3 font-mono text-xs whitespace-nowrap border-r last:border-r-0"
                                >
                                  {col}
                                </th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {previewData.data.map((row, rowIdx) => (
                              <tr
                                key={rowIdx}
                                className="border-t hover:bg-muted/30 cursor-pointer transition-colors"
                                onDoubleClick={() => handleRowDoubleClick(row)}
                                data-testid={`data-row-${rowIdx}`}
                              >
                                <td className="p-3 font-mono text-xs text-muted-foreground border-r bg-muted/20 sticky left-0 z-10">
                                  {rowIdx + 1}
                                </td>
                                {previewData.columns.map((col, colIdx) => (
                                  <td
                                    key={colIdx}
                                    className="p-3 font-mono text-xs whitespace-nowrap border-r last:border-r-0 max-w-[200px] truncate"
                                    title={String(row[col] ?? '')}
                                  >
                                    {row[col] === null || row[col] === undefined
                                      ? <span className="text-muted-foreground italic">null</span>
                                      : String(row[col])}
                                  </td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </ScrollArea>
                  </div>
                  <p className="text-xs text-muted-foreground mt-2">
                    Double-click a row to view full record details
                  </p>
                </TabsContent>

                <TabsContent value="columns" className="m-0">
                  <div className="border rounded-lg overflow-hidden">
                    <ScrollArea className="h-[500px]">
                      <table className="w-full text-sm">
                        <thead className="bg-muted/50 sticky top-0">
                          <tr>
                            <th className="text-left p-3 font-mono text-xs">#</th>
                            <th className="text-left p-3 font-mono text-xs">Column Name</th>
                            <th className="text-left p-3 font-mono text-xs">Data Type</th>
                            <th className="text-left p-3 font-mono text-xs">Sample Value</th>
                          </tr>
                        </thead>
                        <tbody>
                          {previewData.columns.map((col, i) => (
                            <tr key={i} className="border-t hover:bg-muted/30">
                              <td className="p-3 font-mono text-xs text-muted-foreground">
                                {i + 1}
                              </td>
                              <td className="p-3 font-mono text-xs font-medium">
                                {col}
                              </td>
                              <td className="p-3 font-mono text-xs">
                                <Badge variant="outline" className="text-xs">
                                  {previewData.column_types[col] || 'unknown'}
                                </Badge>
                              </td>
                              <td className="p-3 font-mono text-xs text-muted-foreground max-w-[300px] truncate">
                                {previewData.data[0]?.[col] === null
                                  ? 'null'
                                  : String(previewData.data[0]?.[col] ?? '')}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </ScrollArea>
                  </div>
                </TabsContent>
              </Tabs>
            )}
          </CardContent>
        </Card>
      </div>

      <Dialog open={recordDetailOpen} onOpenChange={setRecordDetailOpen}>
        <DialogContent className="max-w-2xl max-h-[80vh] flex flex-col">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <Eye className="h-5 w-5" />
              Record Details
            </DialogTitle>
          </DialogHeader>
          <ScrollArea className="flex-1 min-h-0 max-h-[60vh]">
            <div className="space-y-2 pr-4">
              {selectedRecord && Object.entries(selectedRecord).map(([key, value]) => (
                <div key={key} className="flex gap-4 py-2 border-b last:border-b-0">
                  <span className="font-mono text-xs font-medium min-w-[150px] text-muted-foreground">
                    {key}
                  </span>
                  <span className="font-mono text-xs flex-1 break-all">
                    {value === null || value === undefined
                      ? <span className="text-muted-foreground italic">null</span>
                      : typeof value === 'object'
                        ? JSON.stringify(value)
                        : String(value)}
                  </span>
                </div>
              ))}
            </div>
          </ScrollArea>
        </DialogContent>
      </Dialog>
    </div>
  );
}
