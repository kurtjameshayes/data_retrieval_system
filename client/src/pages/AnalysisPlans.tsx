import { useState, useEffect, useCallback } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, useAppStore, useNotificationStore, type AnalysisPlan, type Query, type QueryConfig, type AnalysisConfig } from "@/lib/store";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
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
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Plus, Play, Pencil, Trash2, Loader2, AlertCircle, CheckCircle2, X, FlaskConical, Database, Settings2 } from "lucide-react";

interface QueryWithColumns {
  query_id: string;
  alias: string;
  join_column: string;
  columns: string[];
}

interface PlanFormState {
  plan_id: string;
  plan_name: string;
  description: string;
  queries: QueryWithColumns[];
  analysis_plan: AnalysisConfig;
  tags: string[];
}

const defaultAnalysisConfig: AnalysisConfig = {
  basic_statistics: true,
  exploratory: true,
};

const defaultFormState: PlanFormState = {
  plan_id: "",
  plan_name: "",
  description: "",
  queries: [],
  analysis_plan: { ...defaultAnalysisConfig },
  tags: [],
};

export default function AnalysisPlans() {
  const queryClient = useQueryClient();
  const { queries, setQueries } = useAppStore();
  const { addNotification, updateNotification, removeNotification } = useNotificationStore();
  
  const [createDialogOpen, setCreateDialogOpen] = useState(false);
  const [editDialogOpen, setEditDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [resultDialogOpen, setResultDialogOpen] = useState(false);
  const [selectedPlan, setSelectedPlan] = useState<AnalysisPlan | null>(null);
  const [executionResult, setExecutionResult] = useState<any>(null);
  const [formState, setFormState] = useState<PlanFormState>({ ...defaultFormState });
  const [availableColumns, setAvailableColumns] = useState<string[]>([]);
  const [loadingColumns, setLoadingColumns] = useState(false);
  const [validationErrors, setValidationErrors] = useState<string[]>([]);
  const [runningPlans, setRunningPlans] = useState<Set<string>>(new Set());

  const { data: plansData, isLoading: plansLoading } = useQuery({
    queryKey: ['analysis-plans'],
    queryFn: api.getAnalysisPlans,
  });

  const { data: queriesData } = useQuery({
    queryKey: ['queries'],
    queryFn: api.getQueries,
  });

  useEffect(() => {
    if (queriesData) setQueries(queriesData);
  }, [queriesData, setQueries]);

  const createMutation = useMutation({
    mutationFn: api.createAnalysisPlan,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['analysis-plans'] });
      setCreateDialogOpen(false);
      setFormState({ ...defaultFormState });
      addNotification({ type: 'success', title: 'Analysis Plan Created', duration: 3000 });
    },
    onError: (error: any) => {
      addNotification({ type: 'error', title: 'Failed to create plan', message: error.message, duration: 5000 });
    },
  });

  const updateMutation = useMutation({
    mutationFn: ({ planId, updates }: { planId: string; updates: Partial<AnalysisPlan> }) =>
      api.updateAnalysisPlan(planId, updates),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['analysis-plans'] });
      setEditDialogOpen(false);
      setSelectedPlan(null);
      addNotification({ type: 'success', title: 'Analysis Plan Updated', duration: 3000 });
    },
    onError: (error: any) => {
      addNotification({ type: 'error', title: 'Failed to update plan', message: error.message, duration: 5000 });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: api.deleteAnalysisPlan,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['analysis-plans'] });
      setDeleteDialogOpen(false);
      setSelectedPlan(null);
      addNotification({ type: 'success', title: 'Analysis Plan Deleted', duration: 3000 });
    },
    onError: (error: any) => {
      addNotification({ type: 'error', title: 'Failed to delete plan', message: error.message, duration: 5000 });
    },
  });

  const executeMutation = useMutation({
    mutationFn: (planId: string) => api.executeAnalysisPlan(planId),
    onMutate: (planId: string) => {
      setRunningPlans(prev => new Set(prev).add(planId));
    },
    onSuccess: (data, planId: string) => {
      setRunningPlans(prev => {
        const next = new Set(prev);
        next.delete(planId);
        return next;
      });
      queryClient.invalidateQueries({ queryKey: ['analysis-plans'] });
      setExecutionResult(data);
      setResultDialogOpen(true);
      addNotification({ type: 'success', title: 'Analysis Complete', message: `Processed ${data.record_count} records`, duration: 5000 });
    },
    onError: (error: any, planId: string) => {
      setRunningPlans(prev => {
        const next = new Set(prev);
        next.delete(planId);
        return next;
      });
      addNotification({ type: 'error', title: 'Execution Failed', message: error.message, duration: 5000 });
    },
  });

  const fetchColumnsForQueries = useCallback(async (queryConfigs: QueryWithColumns[]) => {
    if (queryConfigs.length === 0) {
      setAvailableColumns([]);
      return;
    }

    setLoadingColumns(true);
    try {
      const apiQueries = queryConfigs.map(q => ({
        query_id: q.query_id,
        alias: q.alias || q.query_id,
        join_column: q.join_column || undefined,
      }));
      
      const result = await api.getJoinedColumns(apiQueries);
      const uniqueColumns = Array.from(new Set(result.columns));
      setAvailableColumns(uniqueColumns);
      
      const updatedQueries = queryConfigs.map(q => ({
        ...q,
        columns: uniqueColumns,
      }));
      setFormState(prev => ({ ...prev, queries: updatedQueries }));
    } catch (error) {
      console.error('Failed to fetch columns:', error);
      setAvailableColumns([]);
    } finally {
      setLoadingColumns(false);
    }
  }, []);

  const addQueryToForm = () => {
    const newQuery: QueryWithColumns = {
      query_id: "",
      alias: `query_${formState.queries.length + 1}`,
      join_column: "",
      columns: [],
    };
    setFormState(prev => ({
      ...prev,
      queries: [...prev.queries, newQuery],
    }));
  };

  const removeQueryFromForm = (index: number) => {
    const updated = formState.queries.filter((_, i) => i !== index);
    setFormState(prev => ({ ...prev, queries: updated }));
    if (updated.length > 0 && updated.every(q => q.query_id)) {
      fetchColumnsForQueries(updated);
    } else {
      setAvailableColumns([]);
    }
  };

  const updateQueryInForm = (index: number, field: keyof QueryWithColumns, value: string) => {
    const updated = [...formState.queries];
    updated[index] = { ...updated[index], [field]: value };
    setFormState(prev => ({ ...prev, queries: updated }));
    
    if (field === 'query_id' && value && updated.every(q => q.query_id)) {
      fetchColumnsForQueries(updated);
    }
  };

  const updateAnalysisConfig = (key: keyof AnalysisConfig, value: any) => {
    setFormState(prev => ({
      ...prev,
      analysis_plan: {
        ...prev.analysis_plan,
        [key]: value,
      },
    }));
  };

  const validateForm = (): boolean => {
    const errors: string[] = [];
    
    if (!formState.plan_id.trim()) errors.push("Plan ID is required");
    if (!formState.plan_name.trim()) errors.push("Plan name is required");
    if (formState.queries.length === 0) errors.push("At least one query is required");
    if (formState.queries.some(q => !q.query_id)) errors.push("All queries must be selected");
    
    const config = formState.analysis_plan;
    if (config.linear_regression) {
      if (!config.linear_regression.target) errors.push("Linear regression requires a target column");
      if (!config.linear_regression.features?.length) errors.push("Linear regression requires at least one feature");
    }
    if (config.random_forest) {
      if (!config.random_forest.target) errors.push("Random forest requires a target column");
      if (!config.random_forest.features?.length) errors.push("Random forest requires at least one feature");
    }
    if (config.predictive) {
      if (!config.predictive.target) errors.push("Predictive analysis requires a target column");
      if (!config.predictive.features?.length) errors.push("Predictive analysis requires at least one feature");
    }
    
    setValidationErrors(errors);
    return errors.length === 0;
  };

  const handleCreatePlan = () => {
    if (!validateForm()) {
      addNotification({ type: 'error', title: 'Validation Error', message: 'Please fix the errors in the form', duration: 3000 });
      return;
    }
    
    const plan: Omit<AnalysisPlan, 'created_at' | 'updated_at' | 'last_run_at' | 'last_run_status'> = {
      plan_id: formState.plan_id.trim().toLowerCase().replace(/\s+/g, '_'),
      plan_name: formState.plan_name.trim(),
      description: formState.description.trim() || undefined,
      queries: formState.queries.map(q => ({
        query_id: q.query_id,
        alias: q.alias || undefined,
        join_column: q.join_column || undefined,
      })),
      analysis_plan: formState.analysis_plan,
      tags: formState.tags.length > 0 ? formState.tags : undefined,
      active: true,
    };
    
    createMutation.mutate(plan);
  };

  const handleUpdatePlan = () => {
    if (!selectedPlan) return;
    if (!validateForm()) {
      addNotification({ type: 'error', title: 'Validation Error', message: 'Please fix the errors in the form', duration: 3000 });
      return;
    }
    
    const updates: Partial<AnalysisPlan> = {
      plan_name: formState.plan_name.trim(),
      description: formState.description.trim() || undefined,
      queries: formState.queries.map(q => ({
        query_id: q.query_id,
        alias: q.alias || undefined,
        join_column: q.join_column || undefined,
      })),
      analysis_plan: formState.analysis_plan,
      tags: formState.tags.length > 0 ? formState.tags : undefined,
    };
    
    updateMutation.mutate({ planId: selectedPlan.plan_id, updates });
  };

  const openEditDialog = async (plan: AnalysisPlan) => {
    setSelectedPlan(plan);
    
    const queriesWithCols: QueryWithColumns[] = plan.queries.map(q => ({
      query_id: q.query_id,
      alias: q.alias || q.query_id,
      join_column: q.join_column || "",
      columns: [],
    }));
    
    setFormState({
      plan_id: plan.plan_id,
      plan_name: plan.plan_name,
      description: plan.description || "",
      queries: queriesWithCols,
      analysis_plan: plan.analysis_plan || { ...defaultAnalysisConfig },
      tags: plan.tags || [],
    });
    
    setEditDialogOpen(true);
    
    if (queriesWithCols.length > 0 && queriesWithCols.every(q => q.query_id)) {
      await fetchColumnsForQueries(queriesWithCols);
    }
  };

  const openDeleteDialog = (plan: AnalysisPlan) => {
    setSelectedPlan(plan);
    setDeleteDialogOpen(true);
  };

  const handleExecute = (plan: AnalysisPlan) => {
    executeMutation.mutate(plan.plan_id);
  };

  const resetFormState = () => {
    setFormState({ ...defaultFormState });
    setAvailableColumns([]);
    setValidationErrors([]);
  };

  const getQueryName = (queryId: string): string => {
    const query = queries.find(q => q.queryId === queryId);
    return query?.queryName || queryId;
  };

  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Analysis Plans</h1>
          <p className="text-muted-foreground">
            Configure multi-query joins and ML-based analysis workflows.
          </p>
        </div>
        <Button 
          onClick={() => { resetFormState(); setCreateDialogOpen(true); }}
          data-testid="button-create-plan"
        >
          <Plus className="mr-2 h-4 w-4" />
          New Plan
        </Button>
      </div>

      {plansLoading ? (
        <div className="flex items-center justify-center h-[200px]">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : !plansData || plansData.length === 0 ? (
        <div className="flex flex-col items-center justify-center h-[300px] border-2 border-dashed rounded-lg text-muted-foreground">
          <FlaskConical className="h-12 w-12 mb-4 opacity-50" />
          <p>No analysis plans configured.</p>
          <p className="text-sm">Create a plan to run configurable analysis on your data.</p>
        </div>
      ) : (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {plansData.map((plan) => (
            <Card key={plan.plan_id} className="group" data-testid={`card-plan-${plan.plan_id}`}>
              <CardHeader className="pb-3">
                <div className="flex items-start justify-between">
                  <div className="space-y-1">
                    <CardTitle className="text-lg">{plan.plan_name}</CardTitle>
                    <CardDescription className="font-mono text-xs">
                      {plan.plan_id}
                    </CardDescription>
                  </div>
                  <div className="flex gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-8 w-8"
                      onClick={() => openEditDialog(plan)}
                      data-testid={`button-edit-${plan.plan_id}`}
                    >
                      <Pencil className="h-4 w-4" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-8 w-8 text-destructive"
                      onClick={() => openDeleteDialog(plan)}
                      data-testid={`button-delete-${plan.plan_id}`}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                </div>
              </CardHeader>
              <CardContent className="space-y-4">
                {plan.description && (
                  <p className="text-sm text-muted-foreground line-clamp-2">
                    {plan.description}
                  </p>
                )}
                
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-xs text-muted-foreground">
                    <Database className="h-3.5 w-3.5" />
                    <span>{plan.queries.length} {plan.queries.length === 1 ? 'query' : 'queries'}</span>
                  </div>
                  <div className="flex flex-wrap gap-1">
                    {plan.queries.slice(0, 3).map((q, i) => (
                      <Badge key={i} variant="secondary" className="text-xs font-mono">
                        {getQueryName(q.query_id)}
                      </Badge>
                    ))}
                    {plan.queries.length > 3 && (
                      <Badge variant="outline" className="text-xs">
                        +{plan.queries.length - 3} more
                      </Badge>
                    )}
                  </div>
                </div>

                <div className="flex items-center justify-between pt-2 border-t">
                  {plan.last_run_status ? (
                    <div className="flex items-center gap-1.5 text-xs">
                      {plan.last_run_status === 'success' ? (
                        <CheckCircle2 className="h-3.5 w-3.5 text-green-500" />
                      ) : (
                        <AlertCircle className="h-3.5 w-3.5 text-red-500" />
                      )}
                      <span className="text-muted-foreground">
                        {plan.last_run_at ? new Date(plan.last_run_at).toLocaleDateString() : 'Never run'}
                      </span>
                    </div>
                  ) : (
                    <span className="text-xs text-muted-foreground">Never run</span>
                  )}
                  
                  <Button
                    size="sm"
                    onClick={() => handleExecute(plan)}
                    disabled={runningPlans.has(plan.plan_id)}
                    data-testid={`button-run-${plan.plan_id}`}
                  >
                    {runningPlans.has(plan.plan_id) ? (
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    ) : (
                      <Play className="mr-2 h-4 w-4" />
                    )}
                    Run
                  </Button>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      <Dialog open={createDialogOpen} onOpenChange={(open) => { if (!open) resetFormState(); setCreateDialogOpen(open); }}>
        <DialogContent className="max-w-3xl max-h-[90vh] flex flex-col">
          <DialogHeader className="flex-shrink-0">
            <DialogTitle>Create Analysis Plan</DialogTitle>
            <DialogDescription>
              Configure queries, joins, and analysis options.
            </DialogDescription>
          </DialogHeader>
          
          <div className="flex-1 overflow-y-auto min-h-0">
            <PlanForm
              formState={formState}
              setFormState={setFormState}
              queries={queries}
              availableColumns={availableColumns}
              loadingColumns={loadingColumns}
              validationErrors={validationErrors}
              onAddQuery={addQueryToForm}
              onRemoveQuery={removeQueryFromForm}
              onUpdateQuery={updateQueryInForm}
              onUpdateConfig={updateAnalysisConfig}
              isEdit={false}
            />
          </div>
          
          <DialogFooter className="flex-shrink-0">
            <Button variant="outline" onClick={() => setCreateDialogOpen(false)}>
              Cancel
            </Button>
            <Button 
              onClick={handleCreatePlan}
              disabled={createMutation.isPending}
              data-testid="button-save-plan"
            >
              {createMutation.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Create Plan
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={editDialogOpen} onOpenChange={(open) => { if (!open) { setSelectedPlan(null); resetFormState(); } setEditDialogOpen(open); }}>
        <DialogContent className="max-w-3xl max-h-[90vh] flex flex-col">
          <DialogHeader className="flex-shrink-0">
            <DialogTitle>Edit Analysis Plan</DialogTitle>
            <DialogDescription>
              Update the analysis configuration for {selectedPlan?.plan_name}.
            </DialogDescription>
          </DialogHeader>
          
          <div className="flex-1 overflow-y-auto min-h-0">
            <PlanForm
              formState={formState}
              setFormState={setFormState}
              queries={queries}
              availableColumns={availableColumns}
              loadingColumns={loadingColumns}
              validationErrors={validationErrors}
              onAddQuery={addQueryToForm}
              onRemoveQuery={removeQueryFromForm}
              onUpdateQuery={updateQueryInForm}
              onUpdateConfig={updateAnalysisConfig}
              isEdit={true}
            />
          </div>
          
          <DialogFooter className="flex-shrink-0">
            <Button variant="outline" onClick={() => setEditDialogOpen(false)}>
              Cancel
            </Button>
            <Button 
              onClick={handleUpdatePlan}
              disabled={updateMutation.isPending}
              data-testid="button-update-plan"
            >
              {updateMutation.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Save Changes
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Analysis Plan</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete "{selectedPlan?.plan_name}"? This action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => selectedPlan && deleteMutation.mutate(selectedPlan.plan_id)}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              data-testid="button-confirm-delete"
            >
              {deleteMutation.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <Dialog open={resultDialogOpen} onOpenChange={setResultDialogOpen}>
        <DialogContent className="max-w-4xl max-h-[90vh] overflow-hidden flex flex-col">
          <DialogHeader>
            <DialogTitle>Analysis Results</DialogTitle>
            <DialogDescription>
              {executionResult?.plan_name} - {executionResult?.record_count} records processed
            </DialogDescription>
          </DialogHeader>
          
          <ScrollArea className="flex-1 -mx-6 px-6">
            {executionResult && (
              <Tabs defaultValue="summary" className="w-full">
                <TabsList>
                  <TabsTrigger value="summary">Summary</TabsTrigger>
                  <TabsTrigger value="data">Data Preview</TabsTrigger>
                  <TabsTrigger value="statistics">Statistics</TabsTrigger>
                  <TabsTrigger value="raw">Raw JSON</TabsTrigger>
                </TabsList>
                
                <TabsContent value="summary" className="space-y-4 mt-4">
                  <div className="grid gap-4 md:grid-cols-3">
                    <Card>
                      <CardHeader className="pb-2">
                        <CardTitle className="text-sm font-medium">Records</CardTitle>
                      </CardHeader>
                      <CardContent>
                        <div className="text-2xl font-bold">{executionResult.record_count}</div>
                      </CardContent>
                    </Card>
                    <Card>
                      <CardHeader className="pb-2">
                        <CardTitle className="text-sm font-medium">Columns</CardTitle>
                      </CardHeader>
                      <CardContent>
                        <div className="text-2xl font-bold">{executionResult.columns?.length || 0}</div>
                      </CardContent>
                    </Card>
                    <Card>
                      <CardHeader className="pb-2">
                        <CardTitle className="text-sm font-medium">Analyses</CardTitle>
                      </CardHeader>
                      <CardContent>
                        <div className="text-2xl font-bold">{Object.keys(executionResult.analysis || {}).length}</div>
                      </CardContent>
                    </Card>
                  </div>
                  
                  <Card>
                    <CardHeader>
                      <CardTitle className="text-sm">Available Columns</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="flex flex-wrap gap-1.5">
                        {executionResult.columns?.map((col: string, i: number) => (
                          <Badge key={i} variant="outline" className="text-xs font-mono">
                            {col}
                          </Badge>
                        ))}
                      </div>
                    </CardContent>
                  </Card>
                </TabsContent>
                
                <TabsContent value="data" className="mt-4">
                  <Card>
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm font-medium">
                        First {executionResult.data_sample?.length || 0} rows of {executionResult.record_count} total
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="p-0">
                      <div className="overflow-auto max-h-[400px] border rounded-md">
                        <table className="w-full text-xs">
                          <thead className="bg-muted sticky top-0">
                            <tr>
                              <th className="px-3 py-2 text-left font-medium border-b">#</th>
                              {executionResult.columns?.slice(0, 10).map((col: string, i: number) => (
                                <th key={i} className="px-3 py-2 text-left font-medium border-b whitespace-nowrap max-w-[200px] truncate" title={col}>
                                  {col.length > 30 ? col.substring(0, 30) + '...' : col}
                                </th>
                              ))}
                              {(executionResult.columns?.length || 0) > 10 && (
                                <th className="px-3 py-2 text-left font-medium border-b text-muted-foreground">
                                  +{executionResult.columns.length - 10} more
                                </th>
                              )}
                            </tr>
                          </thead>
                          <tbody>
                            {executionResult.data_sample?.map((row: Record<string, any>, rowIndex: number) => (
                              <tr key={rowIndex} className="border-b hover:bg-muted/50">
                                <td className="px-3 py-2 text-muted-foreground">{rowIndex + 1}</td>
                                {executionResult.columns?.slice(0, 10).map((col: string, colIndex: number) => (
                                  <td key={colIndex} className="px-3 py-2 whitespace-nowrap max-w-[200px] truncate" title={String(row[col] ?? '')}>
                                    {String(row[col] ?? '')}
                                  </td>
                                ))}
                                {(executionResult.columns?.length || 0) > 10 && (
                                  <td className="px-3 py-2 text-muted-foreground">...</td>
                                )}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                      {(executionResult.columns?.length || 0) > 10 && (
                        <p className="text-xs text-muted-foreground mt-2 px-4 pb-2">
                          Showing first 10 of {executionResult.columns?.length} columns. View Raw JSON for complete data.
                        </p>
                      )}
                    </CardContent>
                  </Card>
                </TabsContent>
                
                <TabsContent value="statistics" className="mt-4">
                  <div className="space-y-4">
                    {Object.entries(executionResult.analysis || {}).map(([key, value]) => (
                      <Card key={key}>
                        <CardHeader className="pb-2">
                          <CardTitle className="text-sm font-medium capitalize">
                            {key.replace(/_/g, ' ')}
                          </CardTitle>
                        </CardHeader>
                        <CardContent>
                          <pre className="text-xs font-mono bg-muted p-3 rounded overflow-auto max-h-[300px]">
                            {JSON.stringify(value, null, 2)}
                          </pre>
                        </CardContent>
                      </Card>
                    ))}
                  </div>
                </TabsContent>
                
                <TabsContent value="raw" className="mt-4">
                  <pre className="text-xs font-mono bg-muted p-4 rounded overflow-auto max-h-[500px]">
                    {JSON.stringify(executionResult, null, 2)}
                  </pre>
                </TabsContent>
              </Tabs>
            )}
          </ScrollArea>
          
          <DialogFooter>
            <Button variant="outline" onClick={() => setResultDialogOpen(false)}>
              Close
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

interface PlanFormProps {
  formState: PlanFormState;
  setFormState: React.Dispatch<React.SetStateAction<PlanFormState>>;
  queries: Query[];
  availableColumns: string[];
  loadingColumns: boolean;
  validationErrors: string[];
  onAddQuery: () => void;
  onRemoveQuery: (index: number) => void;
  onUpdateQuery: (index: number, field: keyof QueryWithColumns, value: string) => void;
  onUpdateConfig: (key: keyof AnalysisConfig, value: any) => void;
  isEdit: boolean;
}

function PlanForm({
  formState,
  setFormState,
  queries,
  availableColumns,
  loadingColumns,
  validationErrors,
  onAddQuery,
  onRemoveQuery,
  onUpdateQuery,
  onUpdateConfig,
  isEdit,
}: PlanFormProps) {
  return (
    <div className="space-y-6 py-4 pr-2">
        {validationErrors.length > 0 && (
          <div className="bg-destructive/10 border border-destructive/20 rounded-md p-3 space-y-1">
            {validationErrors.map((error, i) => (
              <div key={i} className="text-sm text-destructive flex items-center gap-2">
                <AlertCircle className="h-4 w-4 flex-shrink-0" />
                {error}
              </div>
            ))}
          </div>
        )}

        <div className="grid gap-4 md:grid-cols-2">
          <div className="space-y-2">
            <Label htmlFor="plan_id">Plan ID</Label>
            <Input
              id="plan_id"
              placeholder="my_analysis_plan"
              value={formState.plan_id}
              onChange={(e) => setFormState(prev => ({ ...prev, plan_id: e.target.value }))}
              disabled={isEdit}
              className="font-mono"
              data-testid="input-plan-id"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="plan_name">Plan Name</Label>
            <Input
              id="plan_name"
              placeholder="My Analysis Plan"
              value={formState.plan_name}
              onChange={(e) => setFormState(prev => ({ ...prev, plan_name: e.target.value }))}
              data-testid="input-plan-name"
            />
          </div>
        </div>

        <div className="space-y-2">
          <Label htmlFor="description">Description</Label>
          <Input
            id="description"
            placeholder="Describe what this analysis does..."
            value={formState.description}
            onChange={(e) => setFormState(prev => ({ ...prev, description: e.target.value }))}
            data-testid="input-description"
          />
        </div>

        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <Label>Queries</Label>
            <Button variant="outline" size="sm" onClick={onAddQuery} data-testid="button-add-query">
              <Plus className="mr-1 h-3.5 w-3.5" />
              Add Query
            </Button>
          </div>
          
          {loadingColumns && (
            <div className="flex items-center gap-2 text-sm text-muted-foreground bg-muted/50 p-3 rounded-lg">
              <Loader2 className="h-4 w-4 animate-spin" />
              Fetching latest column definitions...
            </div>
          )}
          
          {formState.queries.length === 0 ? (
            <div className="text-sm text-muted-foreground text-center py-6 border-2 border-dashed rounded-lg">
              No queries added. Click "Add Query" to select data sources.
            </div>
          ) : (
            <div className="space-y-3">
              {formState.queries.map((queryConfig, index) => (
                <div key={index} className="border rounded-lg p-4 space-y-3 relative">
                  <Button
                    variant="ghost"
                    size="icon"
                    className="absolute top-2 right-2 h-6 w-6 text-muted-foreground hover:text-destructive"
                    onClick={() => onRemoveQuery(index)}
                    data-testid={`button-remove-query-${index}`}
                  >
                    <X className="h-4 w-4" />
                  </Button>
                  
                  <div className="grid gap-3 md:grid-cols-3 pr-8">
                    <div className="space-y-1.5">
                      <Label className="text-xs">Query</Label>
                      <Select
                        value={queryConfig.query_id}
                        onValueChange={(value) => onUpdateQuery(index, 'query_id', value)}
                      >
                        <SelectTrigger data-testid={`select-query-${index}`}>
                          <SelectValue placeholder="Select query..." />
                        </SelectTrigger>
                        <SelectContent>
                          {queries.map((q) => (
                            <SelectItem key={q.queryId} value={q.queryId}>
                              {q.queryName}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    
                    <div className="space-y-1.5">
                      <Label className="text-xs">Alias</Label>
                      <Input
                        placeholder="query_alias"
                        value={queryConfig.alias}
                        onChange={(e) => onUpdateQuery(index, 'alias', e.target.value)}
                        className="font-mono text-sm"
                        data-testid={`input-alias-${index}`}
                      />
                    </div>
                    
                    <div className="space-y-1.5">
                      <Label className="text-xs">Join Column</Label>
                      <Select
                        value={queryConfig.join_column || "__none__"}
                        onValueChange={(value) => onUpdateQuery(index, 'join_column', value === "__none__" ? "" : value)}
                        disabled={availableColumns.length === 0}
                      >
                        <SelectTrigger data-testid={`select-join-${index}`}>
                          <SelectValue placeholder={loadingColumns ? "Loading..." : "Select column..."} />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="__none__">None</SelectItem>
                          {availableColumns.map((col) => (
                            <SelectItem key={col} value={col}>
                              {col}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="space-y-4">
          <Label className="flex items-center gap-2">
            <Settings2 className="h-4 w-4" />
            Analysis Configuration
          </Label>
          
          <div className="grid gap-4 md:grid-cols-2">
            <div className="flex items-center space-x-2">
              <Checkbox
                id="basic_statistics"
                checked={!!formState.analysis_plan.basic_statistics}
                onCheckedChange={(checked) => onUpdateConfig('basic_statistics', checked)}
                data-testid="checkbox-basic-stats"
              />
              <Label htmlFor="basic_statistics" className="text-sm font-normal">
                Basic Statistics (mean, median, correlation)
              </Label>
            </div>
            
            <div className="flex items-center space-x-2">
              <Checkbox
                id="exploratory"
                checked={!!formState.analysis_plan.exploratory}
                onCheckedChange={(checked) => onUpdateConfig('exploratory', checked)}
                data-testid="checkbox-exploratory"
              />
              <Label htmlFor="exploratory" className="text-sm font-normal">
                Exploratory Analysis (types, distributions)
              </Label>
            </div>
          </div>

          <div className="border rounded-lg p-4 space-y-4">
            <div className="flex items-center justify-between">
              <Label className="text-sm">Linear Regression</Label>
              <Checkbox
                checked={!!formState.analysis_plan.linear_regression}
                onCheckedChange={(checked) => {
                  if (checked) {
                    onUpdateConfig('linear_regression', { features: [], target: '' });
                  } else {
                    onUpdateConfig('linear_regression', undefined);
                  }
                }}
                data-testid="checkbox-linear-regression"
              />
            </div>
            
            {formState.analysis_plan.linear_regression && (
              <div className="grid gap-3 md:grid-cols-2">
                <div className="space-y-1.5">
                  <Label className="text-xs">Target Column</Label>
                  <Select
                    value={formState.analysis_plan.linear_regression.target || "__none__"}
                    onValueChange={(value) => onUpdateConfig('linear_regression', {
                      ...formState.analysis_plan.linear_regression,
                      target: value === "__none__" ? "" : value,
                    })}
                    disabled={availableColumns.length === 0}
                  >
                    <SelectTrigger data-testid="select-lr-target">
                      <SelectValue placeholder={loadingColumns ? "Loading..." : "Select target..."} />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="__none__">Select target...</SelectItem>
                      {availableColumns.map((col) => (
                        <SelectItem key={col} value={col}>{col}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                
                <div className="space-y-1.5">
                  <Label className="text-xs">Feature Columns</Label>
                  <MultiColumnSelect
                    columns={availableColumns}
                    selected={formState.analysis_plan.linear_regression.features || []}
                    onChange={(features) => onUpdateConfig('linear_regression', {
                      ...formState.analysis_plan.linear_regression,
                      features,
                    })}
                    testId="select-lr-features"
                  />
                </div>
              </div>
            )}
          </div>

          <div className="border rounded-lg p-4 space-y-4">
            <div className="flex items-center justify-between">
              <Label className="text-sm">Random Forest</Label>
              <Checkbox
                checked={!!formState.analysis_plan.random_forest}
                onCheckedChange={(checked) => {
                  if (checked) {
                    onUpdateConfig('random_forest', { features: [], target: '', n_estimators: 200 });
                  } else {
                    onUpdateConfig('random_forest', undefined);
                  }
                }}
                data-testid="checkbox-random-forest"
              />
            </div>
            
            {formState.analysis_plan.random_forest && (
              <div className="grid gap-3 md:grid-cols-2">
                <div className="space-y-1.5">
                  <Label className="text-xs">Target Column</Label>
                  <Select
                    value={formState.analysis_plan.random_forest.target || "__none__"}
                    onValueChange={(value) => onUpdateConfig('random_forest', {
                      ...formState.analysis_plan.random_forest,
                      target: value === "__none__" ? "" : value,
                    })}
                    disabled={availableColumns.length === 0}
                  >
                    <SelectTrigger data-testid="select-rf-target">
                      <SelectValue placeholder={loadingColumns ? "Loading..." : "Select target..."} />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="__none__">Select target...</SelectItem>
                      {availableColumns.map((col) => (
                        <SelectItem key={col} value={col}>{col}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                
                <div className="space-y-1.5">
                  <Label className="text-xs">Feature Columns</Label>
                  <MultiColumnSelect
                    columns={availableColumns}
                    selected={formState.analysis_plan.random_forest.features || []}
                    onChange={(features) => onUpdateConfig('random_forest', {
                      ...formState.analysis_plan.random_forest,
                      features,
                    })}
                    testId="select-rf-features"
                  />
                </div>
              </div>
            )}
          </div>

          <div className="border rounded-lg p-4 space-y-4">
            <div className="flex items-center justify-between">
              <Label className="text-sm">Multivariate (PCA)</Label>
              <Checkbox
                checked={!!formState.analysis_plan.multivariate}
                onCheckedChange={(checked) => {
                  if (checked) {
                    onUpdateConfig('multivariate', { features: [], n_components: 2 });
                  } else {
                    onUpdateConfig('multivariate', undefined);
                  }
                }}
                data-testid="checkbox-multivariate"
              />
            </div>
            
            {formState.analysis_plan.multivariate && (
              <div className="grid gap-3 md:grid-cols-2">
                <div className="space-y-1.5">
                  <Label className="text-xs">Feature Columns</Label>
                  <MultiColumnSelect
                    columns={availableColumns}
                    selected={formState.analysis_plan.multivariate.features || []}
                    onChange={(features) => onUpdateConfig('multivariate', {
                      ...formState.analysis_plan.multivariate,
                      features,
                    })}
                    testId="select-pca-features"
                  />
                </div>
                
                <div className="space-y-1.5">
                  <Label className="text-xs">Components</Label>
                  <Input
                    type="number"
                    min="1"
                    max="10"
                    value={formState.analysis_plan.multivariate.n_components || 2}
                    onChange={(e) => onUpdateConfig('multivariate', {
                      ...formState.analysis_plan.multivariate,
                      n_components: parseInt(e.target.value) || 2,
                    })}
                    data-testid="input-pca-components"
                  />
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
  );
}

interface MultiColumnSelectProps {
  columns: string[];
  selected: string[];
  onChange: (selected: string[]) => void;
  testId?: string;
}

function MultiColumnSelect({ columns, selected, onChange, testId }: MultiColumnSelectProps) {
  const [open, setOpen] = useState(false);
  
  const toggleColumn = (col: string) => {
    if (selected.includes(col)) {
      onChange(selected.filter(c => c !== col));
    } else {
      onChange([...selected, col]);
    }
  };
  
  return (
    <div className="relative">
      <Button
        variant="outline"
        className="w-full justify-start font-normal"
        onClick={() => setOpen(!open)}
        disabled={columns.length === 0}
        data-testid={testId}
      >
        {selected.length === 0 ? (
          <span className="text-muted-foreground">Select columns...</span>
        ) : (
          <span className="truncate">{selected.length} column(s) selected</span>
        )}
      </Button>
      
      {open && columns.length > 0 && (
        <div className="absolute z-50 top-full left-0 right-0 mt-1 bg-popover border rounded-md shadow-md max-h-[200px] overflow-auto">
          {columns.map((col) => (
            <div
              key={col}
              className="flex items-center gap-2 px-3 py-2 hover:bg-accent cursor-pointer"
              onClick={() => toggleColumn(col)}
            >
              <Checkbox
                checked={selected.includes(col)}
                className="pointer-events-none"
              />
              <span className="text-sm truncate">{col}</span>
            </div>
          ))}
        </div>
      )}
      
      {selected.length > 0 && (
        <div className="flex flex-wrap gap-1 mt-2">
          {selected.map((col) => (
            <Badge key={col} variant="secondary" className="text-xs">
              {col}
              <button
                className="ml-1 hover:text-destructive"
                onClick={(e) => { e.stopPropagation(); toggleColumn(col); }}
              >
                <X className="h-3 w-3" />
              </button>
            </Badge>
          ))}
        </div>
      )}
    </div>
  );
}
