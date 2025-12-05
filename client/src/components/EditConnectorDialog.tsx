import { useState, useEffect } from "react";
import { api, type Connector, useNotificationStore } from "@/lib/store";
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
import { Loader2, Save, Zap, CheckCircle2, XCircle, Clock } from "lucide-react";
import JsonEditor from "./JsonEditor";

interface EditConnectorDialogProps {
  connector: Connector | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export default function EditConnectorDialog({ connector, open, onOpenChange }: EditConnectorDialogProps) {
  const queryClient = useQueryClient();
  const { addNotification, updateNotification } = useNotificationStore();
  
  const [formData, setFormData] = useState<Partial<Connector>>({});
  const [jsonData, setJsonData] = useState<any>({});
  const [activeTab, setActiveTab] = useState<"form" | "json">("form");
  const [testResult, setTestResult] = useState<any>(null);
  const [isTesting, setIsTesting] = useState(false);

  useEffect(() => {
    if (connector) {
      setFormData({ ...connector });
      setJsonData({ ...connector });
      setTestResult(null);
    }
  }, [connector]);

  const updateMutation = useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<Connector> }) => 
      api.updateConnector(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['connectors'] });
      onOpenChange(false);
      addNotification({
        type: 'success',
        title: 'Connector Updated',
        message: `${formData.sourceName || 'Connector'} has been updated successfully`,
        duration: 4000,
      });
    },
    onError: (error) => {
      addNotification({
        type: 'error',
        title: 'Update Failed',
        message: error instanceof Error ? error.message : 'Failed to update connector',
        duration: 6000,
      });
    },
  });

  const handleTest = async () => {
    if (!connector) return;
    
    setIsTesting(true);
    setTestResult(null);
    
    try {
      const result = await api.testConnector(connector.id);
      setTestResult(result);
    } catch (error) {
      setTestResult({
        success: false,
        error: error instanceof Error ? error.message : 'Test failed',
      });
    } finally {
      setIsTesting(false);
    }
  };

  const handleSave = () => {
    if (!connector) return;
    
    const dataToSave = activeTab === "json" ? jsonData : formData;
    const { id, createdAt, updatedAt, ...updates } = dataToSave;
    
    updateMutation.mutate({ id: connector.id, data: updates });
  };

  const handleFormChange = (field: keyof Connector, value: any) => {
    const newData = { ...formData, [field]: value };
    setFormData(newData);
    setJsonData(newData);
  };

  const handleJsonChange = (value: any) => {
    setJsonData(value);
    setFormData(value);
  };

  if (!connector) return null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[85vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Edit Connector: {connector.sourceName}</DialogTitle>
          <DialogDescription>
            Modify connector settings using the form or edit JSON directly.
          </DialogDescription>
        </DialogHeader>

        <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as "form" | "json")}>
          <TabsList className="grid w-full grid-cols-2">
            <TabsTrigger value="form" data-testid="tab-form">Form Editor</TabsTrigger>
            <TabsTrigger value="json" data-testid="tab-json">JSON Editor</TabsTrigger>
          </TabsList>

          <TabsContent value="form" className="space-y-4 mt-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="sourceId">Source ID</Label>
                <Input
                  id="sourceId"
                  value={formData.sourceId || ""}
                  onChange={(e) => handleFormChange("sourceId", e.target.value)}
                  className="font-mono text-sm"
                  data-testid="input-sourceId"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="sourceName">Source Name</Label>
                <Input
                  id="sourceName"
                  value={formData.sourceName || ""}
                  onChange={(e) => handleFormChange("sourceName", e.target.value)}
                  data-testid="input-sourceName"
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="connectorType">Connector Type</Label>
                <Input
                  id="connectorType"
                  value={formData.connectorType || ""}
                  onChange={(e) => handleFormChange("connectorType", e.target.value)}
                  className="font-mono text-sm"
                  data-testid="input-connectorType"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="format">Format</Label>
                <Input
                  id="format"
                  value={formData.format || ""}
                  onChange={(e) => handleFormChange("format", e.target.value)}
                  className="font-mono text-sm"
                  data-testid="input-format"
                />
              </div>
            </div>

            <div className="space-y-2">
              <Label htmlFor="url">URL</Label>
              <Input
                id="url"
                value={formData.url || ""}
                onChange={(e) => handleFormChange("url", e.target.value)}
                className="font-mono text-sm"
                data-testid="input-url"
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="apiKey">API Key</Label>
              <Input
                id="apiKey"
                type="password"
                value={formData.apiKey || ""}
                onChange={(e) => handleFormChange("apiKey", e.target.value || null)}
                placeholder="Leave empty if not required"
                className="font-mono text-sm"
                data-testid="input-apiKey"
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
              <Label htmlFor="documentation">Documentation URL</Label>
              <Input
                id="documentation"
                value={formData.documentation || ""}
                onChange={(e) => handleFormChange("documentation", e.target.value || null)}
                placeholder="https://..."
                className="font-mono text-sm"
                data-testid="input-documentation"
              />
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="maxRetries">Max Retries</Label>
                <Input
                  id="maxRetries"
                  type="number"
                  value={formData.maxRetries || 3}
                  onChange={(e) => handleFormChange("maxRetries", parseInt(e.target.value))}
                  data-testid="input-maxRetries"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="retryDelay">Retry Delay (ms)</Label>
                <Input
                  id="retryDelay"
                  type="number"
                  value={formData.retryDelay || 1000}
                  onChange={(e) => handleFormChange("retryDelay", parseInt(e.target.value))}
                  data-testid="input-retryDelay"
                />
              </div>
            </div>

            <div className="space-y-2">
              <Label htmlFor="notes">Notes</Label>
              <Textarea
                id="notes"
                value={formData.notes || ""}
                onChange={(e) => handleFormChange("notes", e.target.value || null)}
                rows={2}
                data-testid="input-notes"
              />
            </div>
          </TabsContent>

          <TabsContent value="json" className="mt-4">
            <JsonEditor
              value={jsonData}
              onChange={handleJsonChange}
            />
          </TabsContent>
        </Tabs>

        <div className="border rounded-lg p-4 bg-muted/30 space-y-3">
          <div className="flex items-center justify-between">
            <span className="text-sm font-medium">Test Connection</span>
            <Button
              variant="outline"
              size="sm"
              onClick={handleTest}
              disabled={isTesting}
              data-testid="button-test-connector"
            >
              {isTesting ? (
                <><Loader2 className="h-4 w-4 mr-2 animate-spin" /> Testing...</>
              ) : (
                <><Zap className="h-4 w-4 mr-2" /> Test URL</>
              )}
            </Button>
          </div>
          
          {testResult && (
            <div className={`p-3 rounded-lg text-sm ${testResult.success ? 'bg-green-500/10 border border-green-500/30' : 'bg-red-500/10 border border-red-500/30'}`}>
              <div className="flex items-center gap-2 mb-1">
                {testResult.success ? (
                  <CheckCircle2 className="h-4 w-4 text-green-500" />
                ) : (
                  <XCircle className="h-4 w-4 text-red-500" />
                )}
                <span className={testResult.success ? 'text-green-500' : 'text-red-500'}>
                  {testResult.success ? testResult.message : testResult.error}
                </span>
              </div>
              {testResult.responseTime && (
                <div className="flex items-center gap-1 text-xs text-muted-foreground">
                  <Clock className="h-3 w-3" />
                  Response time: {testResult.responseTime}ms
                </div>
              )}
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
            data-testid="button-save-connector"
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
