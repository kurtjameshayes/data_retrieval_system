import { useState, useEffect } from "react";
import { api, type Query, useNotificationStore } from "@/lib/store";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Switch } from "@/components/ui/switch";
import { Badge } from "@/components/ui/badge";
import { Loader2, Save, Play, Database, X } from "lucide-react";
import JsonEditor from "./JsonEditor";
import DataTablePreview from "./DataTablePreview";

interface EditQueryDialogProps {
  query: Query | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export default function EditQueryDialog({ query, open, onOpenChange }: EditQueryDialogProps) {
  const queryClient = useQueryClient();
  const { addNotification, updateNotification } = useNotificationStore();
  
  const [formData, setFormData] = useState<Partial<Query>>({});
  const [jsonData, setJsonData] = useState<any>({});
  const [activeTab, setActiveTab] = useState<"form" | "json" | "test">("form");
  const [tagInput, setTagInput] = useState("");
  const [testResult, setTestResult] = useState<any>(null);
  const [isTesting, setIsTesting] = useState(false);
  const [saveToResults, setSaveToResults] = useState(true);

  useEffect(() => {
    if (query) {
      setFormData({ ...query });
      setJsonData({ ...query });
      setTestResult(null);
    }
  }, [query]);

  const updateMutation = useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<Query> }) => 
      api.updateQuery(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['queries'] });
      onOpenChange(false);
      addNotification({
        type: 'success',
        title: 'Query Updated',
        message: `${formData.queryName || 'Query'} has been updated successfully`,
        duration: 4000,
      });
    },
    onError: (error) => {
      addNotification({
        type: 'error',
        title: 'Update Failed',
        message: error instanceof Error ? error.message : 'Failed to update query',
        duration: 6000,
      });
    },
  });

  const handleTest = async () => {
    if (!query) return;
    
    setIsTesting(true);
    setTestResult(null);
    
    const notifId = addNotification({
      type: 'loading',
      title: 'Testing Query',
      message: 'Executing query via Python QueryEngine...',
      queryName: query.queryName,
    });
    
    try {
      const result = await api.runQuery(query.id, undefined, { saveToResults });
      const pythonResult = (result as any).pythonResult;
      
      setTestResult({
        success: pythonResult?.success,
        data: pythonResult?.data,
        recordCount: pythonResult?.record_count,
        source: pythonResult?.source,
        error: pythonResult?.error,
      });
      
      updateNotification(notifId, {
        type: pythonResult?.success ? 'success' : 'error',
        title: pythonResult?.success ? 'Query Executed' : 'Query Failed',
        message: pythonResult?.success 
          ? `Retrieved ${pythonResult?.record_count || 0} records` 
          : pythonResult?.error,
        duration: 4000,
      });
      
      if (pythonResult?.success) {
        setActiveTab("test");
      }
    } catch (error) {
      setTestResult({
        success: false,
        error: error instanceof Error ? error.message : 'Test failed',
      });
      
      updateNotification(notifId, {
        type: 'error',
        title: 'Test Failed',
        message: error instanceof Error ? error.message : 'Query test failed',
        duration: 5000,
      });
    } finally {
      setIsTesting(false);
    }
  };

  const handleSave = () => {
    if (!query) return;
    
    const dataToSave = activeTab === "json" ? jsonData : formData;
    const { id, createdAt, updatedAt, ...updates } = dataToSave;
    
    updateMutation.mutate({ id: query.id, data: updates });
  };

  const handleFormChange = (field: keyof Query, value: any) => {
    const newData = { ...formData, [field]: value };
    setFormData(newData);
    setJsonData(newData);
  };

  const handleJsonChange = (value: any) => {
    setJsonData(value);
    setFormData(value);
  };

  const addTag = () => {
    if (!tagInput.trim()) return;
    const currentTags = formData.tags || [];
    if (!currentTags.includes(tagInput.trim())) {
      handleFormChange("tags", [...currentTags, tagInput.trim()]);
    }
    setTagInput("");
  };

  const removeTag = (tag: string) => {
    const currentTags = formData.tags || [];
    handleFormChange("tags", currentTags.filter(t => t !== tag));
  };

  if (!query) return null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Edit Query: {query.queryName}</DialogTitle>
          <DialogDescription>
            Modify query settings, test execution, or edit JSON directly.
          </DialogDescription>
        </DialogHeader>

        <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as "form" | "json" | "test")}>
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="form" data-testid="tab-form">Form Editor</TabsTrigger>
            <TabsTrigger value="json" data-testid="tab-json">JSON Editor</TabsTrigger>
            <TabsTrigger value="test" data-testid="tab-test">
              Test Results
              {testResult?.recordCount && (
                <Badge variant="secondary" className="ml-2 text-xs">
                  {testResult.recordCount}
                </Badge>
              )}
            </TabsTrigger>
          </TabsList>

          <TabsContent value="form" className="space-y-4 mt-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="queryId">Query ID</Label>
                <Input
                  id="queryId"
                  value={formData.queryId || ""}
                  onChange={(e) => handleFormChange("queryId", e.target.value)}
                  className="font-mono text-sm"
                  data-testid="input-queryId"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="queryName">Query Name</Label>
                <Input
                  id="queryName"
                  value={formData.queryName || ""}
                  onChange={(e) => handleFormChange("queryName", e.target.value)}
                  data-testid="input-queryName"
                />
              </div>
            </div>

            <div className="space-y-2">
              <Label htmlFor="connectorId">Connector ID</Label>
              <Input
                id="connectorId"
                value={formData.connectorId || ""}
                onChange={(e) => handleFormChange("connectorId", e.target.value)}
                className="font-mono text-sm"
                data-testid="input-connectorId"
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="description">Description</Label>
              <Textarea
                id="description"
                value={formData.description || ""}
                onChange={(e) => handleFormChange("description", e.target.value || null)}
                rows={2}
                data-testid="input-description"
              />
            </div>

            <div className="space-y-2">
              <Label>Tags</Label>
              <div className="flex flex-wrap gap-2 mb-2">
                {(formData.tags || []).map((tag) => (
                  <Badge key={tag} variant="secondary" className="gap-1">
                    {tag}
                    <button onClick={() => removeTag(tag)} className="hover:text-destructive">
                      <X className="h-3 w-3" />
                    </button>
                  </Badge>
                ))}
              </div>
              <div className="flex gap-2">
                <Input
                  value={tagInput}
                  onChange={(e) => setTagInput(e.target.value)}
                  placeholder="Add tag..."
                  onKeyDown={(e) => e.key === "Enter" && (e.preventDefault(), addTag())}
                  className="flex-1"
                  data-testid="input-tag"
                />
                <Button type="button" variant="outline" onClick={addTag} data-testid="button-add-tag">
                  Add
                </Button>
              </div>
            </div>

            <div className="space-y-2">
              <Label>Parameters (JSON)</Label>
              <Textarea
                value={JSON.stringify(formData.parameters || {}, null, 2)}
                onChange={(e) => {
                  try {
                    handleFormChange("parameters", JSON.parse(e.target.value));
                  } catch {}
                }}
                className="font-mono text-xs min-h-[150px]"
                data-testid="input-parameters"
              />
            </div>
          </TabsContent>

          <TabsContent value="json" className="mt-4">
            <JsonEditor
              value={jsonData}
              onChange={handleJsonChange}
            />
          </TabsContent>

          <TabsContent value="test" className="mt-4">
            {testResult?.success && testResult.data ? (
              <DataTablePreview data={testResult.data} maxRows={100} />
            ) : testResult?.error ? (
              <div className="border rounded-lg p-8 text-center">
                <p className="text-red-500 mb-2">Query execution failed</p>
                <p className="text-sm text-muted-foreground">{testResult.error}</p>
              </div>
            ) : (
              <div className="border rounded-lg p-8 text-center text-muted-foreground">
                <Database className="h-8 w-8 mx-auto mb-2 opacity-50" />
                <p>Run a test to preview data here</p>
              </div>
            )}
          </TabsContent>
        </Tabs>

        <div className="border rounded-lg p-4 bg-muted/30 space-y-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <span className="text-sm font-medium">Test Query Execution</span>
              <div className="flex items-center gap-2">
                <Switch
                  id="saveResults"
                  checked={saveToResults}
                  onCheckedChange={setSaveToResults}
                  data-testid="switch-save-results"
                />
                <Label htmlFor="saveResults" className="text-xs text-muted-foreground cursor-pointer">
                  Save to query_results
                </Label>
              </div>
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={handleTest}
              disabled={isTesting}
              data-testid="button-test-query"
            >
              {isTesting ? (
                <><Loader2 className="h-4 w-4 mr-2 animate-spin" /> Running...</>
              ) : (
                <><Play className="h-4 w-4 mr-2" /> Run Test</>
              )}
            </Button>
          </div>
          
          {testResult && (
            <div className={`text-sm ${testResult.success ? 'text-green-500' : 'text-red-500'}`}>
              {testResult.success 
                ? `Retrieved ${testResult.recordCount || 0} records${testResult.source === 'cache' ? ' (from cache)' : ''}`
                : testResult.error
              }
            </div>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} data-testid="button-cancel">
            Cancel
          </Button>
          <Button 
            onClick={handleSave} 
            disabled={updateMutation.isPending}
            data-testid="button-save-query"
          >
            {updateMutation.isPending ? (
              <><Loader2 className="h-4 w-4 mr-2 animate-spin" /> Saving...</>
            ) : (
              <><Save className="h-4 w-4 mr-2" /> Save Changes</>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
