import { useState, useEffect } from "react";
import { useAppStore, api } from "@/lib/store";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardFooter,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Plus, Trash2, Save, Terminal, X, Code } from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

export default function QueryBuilder() {
  const { connectors, setConnectors } = useAppStore();
  const queryClient = useQueryClient();
  const { toast } = useToast();
  
  const [selectedConnectorId, setSelectedConnectorId] = useState<string>("");
  const [queryName, setQueryName] = useState("");
  const [description, setDescription] = useState("");
  const [queryId, setQueryId] = useState("");
  const [tags, setTags] = useState<string[]>([]);
  const [tagInput, setTagInput] = useState("");
  const [parameters, setParameters] = useState<{ key: string; value: string }[]>([]);
  const [notesText, setNotesText] = useState("");
  const [jsonInput, setJsonInput] = useState("");
  
  const { data: connectorsData } = useQuery({
    queryKey: ['connectors'],
    queryFn: api.getConnectors,
  });

  useEffect(() => {
    if (connectorsData) setConnectors(connectorsData);
  }, [connectorsData, setConnectors]);

  const selectedConnector = connectors.find(c => c.sourceId === selectedConnectorId);

  const createMutation = useMutation({
    mutationFn: api.createQuery,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['queries'] });
      toast({
        title: "Query Saved",
        description: "Query has been added to the library.",
      });
      resetForm();
    },
    onError: () => {
      toast({
        variant: "destructive",
        title: "Save Failed",
        description: "Failed to save the query.",
      });
    },
  });

  const resetForm = () => {
    setQueryName("");
    setDescription("");
    setQueryId("");
    setTags([]);
    setParameters([]);
    setNotesText("");
    setJsonInput("");
  };

  const handleAddParam = () => {
    setParameters([...parameters, { key: "", value: "" }]);
  };

  const handleRemoveParam = (index: number) => {
    setParameters(parameters.filter((_, i) => i !== index));
  };

  const handleUpdateParam = (index: number, field: "key" | "value", value: string) => {
    const newParams = [...parameters];
    newParams[index][field] = value;
    setParameters(newParams);
  };

  const handleAddTag = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && tagInput.trim()) {
      e.preventDefault();
      if (!tags.includes(tagInput.trim())) {
        setTags([...tags, tagInput.trim()]);
      }
      setTagInput("");
    }
  };

  const handleRemoveTag = (tagToRemove: string) => {
    setTags(tags.filter(tag => tag !== tagToRemove));
  };

  const handleJsonImport = () => {
    try {
      const parsed = JSON.parse(jsonInput);
      if (parsed.query_id) setQueryId(parsed.query_id);
      if (parsed.queryId) setQueryId(parsed.queryId);
      if (parsed.query_name) setQueryName(parsed.query_name);
      if (parsed.queryName) setQueryName(parsed.queryName);
      if (parsed.connector_id) setSelectedConnectorId(parsed.connector_id);
      if (parsed.connectorId) setSelectedConnectorId(parsed.connectorId);
      if (parsed.description) setDescription(parsed.description);
      if (parsed.tags) setTags(parsed.tags);
      if (parsed.parameters) {
        const paramList = Object.entries(parsed.parameters).map(([key, value]) => ({
          key,
          value: String(value),
        }));
        setParameters(paramList);
      }
      if (parsed.notes) {
        setNotesText(typeof parsed.notes === 'string' ? parsed.notes : JSON.stringify(parsed.notes, null, 2));
      }
      toast({
        title: "Import Successful",
        description: "Form populated from JSON.",
      });
    } catch (e) {
      toast({
        variant: "destructive",
        title: "Import Failed",
        description: "Invalid JSON format.",
      });
    }
  };

  const handleSaveQuery = () => {
    if (!selectedConnectorId || !queryName || !queryId) {
      toast({
        variant: "destructive",
        title: "Validation Error",
        description: "Please fill in all required fields (Query ID, Name, and Connector).",
      });
      return;
    }

    if (/\s/.test(queryId)) {
      toast({
        variant: "destructive",
        title: "Validation Error",
        description: "Query ID must not contain spaces.",
      });
      return;
    }

    const paramsObject: Record<string, any> = {};
    parameters.forEach(p => {
      if (p.key) {
        paramsObject[p.key] = p.value;
      }
    });

    let notesValue: any = null;
    if (notesText) {
      try {
        notesValue = JSON.parse(notesText);
      } catch {
        notesValue = notesText;
      }
    }

    createMutation.mutate({
      queryId,
      queryName,
      connectorId: selectedConnectorId,
      description: description || null,
      parameters: paramsObject,
      tags: tags.length > 0 ? tags : [],
      notes: notesValue,
    });
  };

  return (
    <div className="grid gap-6 lg:grid-cols-12">
      <div className="lg:col-span-5 flex flex-col gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Terminal className="h-5 w-5 text-primary" />
              Query Definition
            </CardTitle>
          </CardHeader>
          <CardContent>
            <Tabs defaultValue="manual" className="w-full">
              <TabsList className="grid w-full grid-cols-2 mb-4">
                <TabsTrigger value="manual">Manual Entry</TabsTrigger>
                <TabsTrigger value="json">JSON Import</TabsTrigger>
              </TabsList>

              <TabsContent value="manual" className="space-y-4">
                <div className="space-y-2">
                  <Label>Source Connector</Label>
                  <Select onValueChange={setSelectedConnectorId} value={selectedConnectorId}>
                    <SelectTrigger data-testid="select-source-connector">
                      <SelectValue placeholder="Select a data source..." />
                    </SelectTrigger>
                    <SelectContent>
                      {connectors.map((c) => (
                        <SelectItem key={c.id} value={c.sourceId}>
                          {c.sourceName} <span className="text-muted-foreground ml-2 text-xs">({c.sourceId})</span>
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <Separator />

                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label>Query ID (No Spaces)</Label>
                    <Input 
                      value={queryId} 
                      onChange={(e) => setQueryId(e.target.value.replace(/\s/g, '_').toLowerCase())} 
                      placeholder="e.g. get_snap_data" 
                      className="font-mono text-sm"
                      data-testid="input-query-id"
                    />
                  </div>

                  <div className="space-y-2">
                    <Label>Query Name</Label>
                    <Input 
                      value={queryName} 
                      onChange={(e) => setQueryName(e.target.value)} 
                      placeholder="e.g. SNAP Data by ZIP" 
                      data-testid="input-query-name"
                    />
                  </div>
                </div>

                <div className="space-y-2">
                  <Label>Description</Label>
                  <Textarea 
                    value={description} 
                    onChange={(e) => setDescription(e.target.value)} 
                    placeholder="What this query does..." 
                    className="h-16 resize-none"
                    data-testid="input-query-description"
                  />
                </div>

                <div className="space-y-2">
                  <Label>Tags</Label>
                  <div className="space-y-2">
                    <Input 
                      value={tagInput} 
                      onChange={(e) => setTagInput(e.target.value)} 
                      onKeyDown={handleAddTag}
                      placeholder="Type tag and press Enter..." 
                      data-testid="input-query-tags"
                    />
                    <div className="flex flex-wrap gap-2 min-h-[24px]">
                      {tags.map((tag) => (
                        <Badge key={tag} variant="secondary" className="flex items-center gap-1 hover:bg-secondary/80" data-testid={`tag-${tag}`}>
                          {tag}
                          <X 
                            className="h-3 w-3 cursor-pointer hover:text-destructive" 
                            onClick={() => handleRemoveTag(tag)}
                          />
                        </Badge>
                      ))}
                    </div>
                  </div>
                </div>
                
                <div className="space-y-2">
                  <div className="flex items-center justify-between">
                    <Label>Query Parameters</Label>
                    <Button variant="ghost" size="sm" onClick={handleAddParam} className="h-6 w-6 p-0" data-testid="button-add-param">
                      <Plus className="h-4 w-4" />
                    </Button>
                  </div>
                  
                  <div className="space-y-2 max-h-[200px] overflow-y-auto">
                    {parameters.map((param, index) => (
                      <div key={index} className="flex gap-2 items-center">
                        <Input 
                          value={param.key} 
                          onChange={(e) => handleUpdateParam(index, "key", e.target.value)}
                          placeholder="Key"
                          className="h-8 font-mono text-xs"
                        />
                        <Input 
                          value={param.value} 
                          onChange={(e) => handleUpdateParam(index, "value", e.target.value)}
                          placeholder="Value"
                          className="h-8 font-mono text-xs"
                        />
                        <Button 
                          variant="ghost" 
                          size="icon" 
                          className="h-8 w-8 shrink-0"
                          onClick={() => handleRemoveParam(index)}
                        >
                          <Trash2 className="h-3 w-3 text-muted-foreground" />
                        </Button>
                      </div>
                    ))}
                    {parameters.length === 0 && (
                      <p className="text-xs text-muted-foreground text-center py-2 border border-dashed rounded">
                        No parameters defined
                      </p>
                    )}
                  </div>
                </div>

                <div className="space-y-2">
                  <Label>Notes (Optional - JSON or Text)</Label>
                  <Textarea 
                    value={notesText} 
                    onChange={(e) => setNotesText(e.target.value)} 
                    placeholder='{"variables": {"B22010_001E": "Total households"}, "source": "ACS"}' 
                    className="h-20 resize-none font-mono text-xs"
                    data-testid="textarea-query-notes"
                  />
                </div>
              </TabsContent>

              <TabsContent value="json">
                <div className="space-y-4">
                  <div className="space-y-2">
                    <Label>Paste Query JSON</Label>
                    <div className="relative">
                      <Textarea
                        value={jsonInput}
                        onChange={(e) => setJsonInput(e.target.value)}
                        className="font-mono text-xs min-h-[300px] bg-muted/50"
                        placeholder={`{
  "query_id": "snap_all_attributes",
  "query_name": "SNAP - All Attributes",
  "connector_id": "census_api",
  "description": "Retrieve SNAP data",
  "parameters": {
    "dataset": "2022/acs/acs5",
    "get": "NAME,B22010_001E"
  },
  "tags": ["census", "snap"]
}`}
                        data-testid="textarea-json-import"
                      />
                      <Code className="absolute top-3 right-3 h-4 w-4 text-muted-foreground opacity-50" />
                    </div>
                  </div>
                  <Button onClick={handleJsonImport} variant="secondary" className="w-full" data-testid="button-import-json">
                    Parse & Populate Form
                  </Button>
                </div>
              </TabsContent>
            </Tabs>
          </CardContent>
          <CardFooter>
            <Button onClick={handleSaveQuery} className="w-full" disabled={createMutation.isPending} data-testid="button-save-query">
              <Save className="h-4 w-4 mr-2" /> {createMutation.isPending ? "Saving..." : "Save Query"}
            </Button>
          </CardFooter>
        </Card>
      </div>

      <div className="lg:col-span-7 flex flex-col">
        <Card className="flex-1 flex flex-col overflow-hidden border-sidebar-border/50 bg-card/50 backdrop-blur-sm">
          <div className="p-4 border-b border-sidebar-border bg-sidebar/50 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Badge variant="outline" className="font-mono text-xs bg-background">
                {selectedConnector?.connectorType || "connector"}
              </Badge>
              <span className="font-mono text-sm text-muted-foreground truncate">
                {selectedConnector ? selectedConnector.url : "Select a connector..."}
              </span>
            </div>
            <Badge variant="secondary">Preview Mode</Badge>
          </div>
          
          <div className="flex-1 p-6 overflow-auto">
            {selectedConnector ? (
              <div className="space-y-4">
                <div>
                  <h4 className="text-sm font-medium mb-2">Connector Details</h4>
                  <div className="bg-muted/50 rounded-lg p-4 space-y-2 text-sm">
                    <p><span className="text-muted-foreground">Name:</span> {selectedConnector.sourceName}</p>
                    <p><span className="text-muted-foreground">Type:</span> {selectedConnector.connectorType}</p>
                    <p><span className="text-muted-foreground">URL:</span> <code className="text-xs">{selectedConnector.url}</code></p>
                    <p><span className="text-muted-foreground">API Key:</span> {selectedConnector.apiKey ? '********' : 'Not configured'}</p>
                  </div>
                </div>
                
                {parameters.length > 0 && (
                  <div>
                    <h4 className="text-sm font-medium mb-2">Parameters Preview</h4>
                    <pre className="bg-muted/50 rounded-lg p-4 text-xs font-mono overflow-auto">
                      {JSON.stringify(
                        parameters.reduce((acc, p) => {
                          if (p.key) acc[p.key] = p.value;
                          return acc;
                        }, {} as Record<string, string>),
                        null,
                        2
                      )}
                    </pre>
                  </div>
                )}
              </div>
            ) : (
              <div className="h-full flex items-center justify-center">
                <div className="text-center space-y-4 max-w-md">
                  <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center mx-auto">
                    <Terminal className="h-8 w-8 text-primary" />
                  </div>
                  <h3 className="text-xl font-medium">Ready to Build</h3>
                  <p className="text-muted-foreground">
                    Select a connector and configure your query parameters. Once saved, you can execute it from the library.
                  </p>
                </div>
              </div>
            )}
          </div>
        </Card>
      </div>
    </div>
  );
}
