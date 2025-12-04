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
import { Play, Plus, Trash2, Save, Terminal, X } from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";

export default function QueryBuilder() {
  const { connectors, setConnectors } = useAppStore();
  const queryClient = useQueryClient();
  const { toast } = useToast();
  
  const [selectedConnectorId, setSelectedConnectorId] = useState<string>("");
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [notes, setNotes] = useState("");
  const [tags, setTags] = useState<string[]>([]);
  const [tagInput, setTagInput] = useState("");
  const [queryId, setQueryId] = useState("");
  const [endpoint, setEndpoint] = useState("");
  const [method, setMethod] = useState<"GET" | "POST" | "PUT" | "DELETE">("GET");
  const [params, setParams] = useState<{ key: string; value: string; enabled: boolean }[]>([]);
  
  const { data: connectorsData } = useQuery({
    queryKey: ['connectors'],
    queryFn: api.getConnectors,
  });

  useEffect(() => {
    if (connectorsData) setConnectors(connectorsData);
  }, [connectorsData, setConnectors]);

  const selectedConnector = connectors.find(c => c.id === selectedConnectorId);

  const createMutation = useMutation({
    mutationFn: api.createQuery,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['queries'] });
      toast({
        title: "Query Saved",
        description: "Query has been added to the library.",
      });
      setName("");
      setDescription("");
      setNotes("");
      setTags([]);
      setQueryId("");
    },
    onError: () => {
      toast({
        variant: "destructive",
        title: "Save Failed",
        description: "Failed to save the query.",
      });
    },
  });

  const handleAddParam = () => {
    setParams([...params, { key: "", value: "", enabled: true }]);
  };

  const handleRemoveParam = (index: number) => {
    setParams(params.filter((_, i) => i !== index));
  };

  const handleUpdateParam = (index: number, field: "key" | "value", value: string) => {
    const newParams = [...params];
    newParams[index][field] = value;
    setParams(newParams);
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

  const handleSaveQuery = () => {
    if (!selectedConnectorId || !name || !endpoint || !queryId) {
      toast({
        variant: "destructive",
        title: "Validation Error",
        description: "Please fill in all required fields (including Query ID).",
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

    createMutation.mutate({
      connectorId: selectedConnectorId,
      name,
      description: description || null,
      notes: notes || null,
      tags: tags.length > 0 ? tags : null,
      queryId,
      endpoint,
      method,
      params: params.map((p, i) => ({ ...p, id: `p_${i}` })),
    });
  };

  return (
    <div className="grid gap-6 lg:grid-cols-12 h-[calc(100vh-8rem)]">
      <div className="lg:col-span-4 flex flex-col gap-6 h-full overflow-y-auto pr-2">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Terminal className="h-5 w-5 text-primary" />
              Query Definition
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label>Source Connector</Label>
              <Select onValueChange={setSelectedConnectorId} value={selectedConnectorId}>
                <SelectTrigger data-testid="select-source-connector">
                  <SelectValue placeholder="Select a data source..." />
                </SelectTrigger>
                <SelectContent>
                  {connectors.map((c) => (
                    <SelectItem key={c.id} value={c.id}>
                      {c.name} <span className="text-muted-foreground ml-2 text-xs">({c.type})</span>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <Separator />

            <div className="space-y-2">
              <Label>Query Name</Label>
              <Input 
                value={name} 
                onChange={(e) => setName(e.target.value)} 
                placeholder="e.g. Get Active Users" 
                data-testid="input-query-name"
              />
            </div>

            <div className="space-y-2">
              <Label>Query ID (No Spaces)</Label>
              <Input 
                value={queryId} 
                onChange={(e) => setQueryId(e.target.value.replace(/\s/g, ''))} 
                placeholder="e.g. get_active_users" 
                className="font-mono text-sm"
                data-testid="input-query-id"
              />
            </div>

            <div className="space-y-2">
              <Label>Description</Label>
              <Input 
                value={description} 
                onChange={(e) => setDescription(e.target.value)} 
                placeholder="Brief explanation of what this query does..." 
                data-testid="input-query-description"
              />
            </div>

            <div className="space-y-2">
              <Label>Notes</Label>
              <Textarea 
                value={notes} 
                onChange={(e) => setNotes(e.target.value)} 
                placeholder="Detailed notes, implementation details, or usage instructions..." 
                className="h-20 resize-none"
                data-testid="textarea-query-notes"
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

            <div className="grid grid-cols-4 gap-2">
              <div className="col-span-1">
                <Label>Method</Label>
                <Select value={method} onValueChange={(v: any) => setMethod(v)}>
                  <SelectTrigger data-testid="select-query-method">
                     <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="GET">GET</SelectItem>
                    <SelectItem value="POST">POST</SelectItem>
                    <SelectItem value="PUT">PUT</SelectItem>
                    <SelectItem value="DELETE">DEL</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="col-span-3">
                <Label>Endpoint Path</Label>
                <Input 
                  value={endpoint} 
                  onChange={(e) => setEndpoint(e.target.value)} 
                  placeholder="/users/active" 
                  className="font-mono text-sm"
                  data-testid="input-query-endpoint"
                />
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
                {params.map((param, index) => (
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
                {params.length === 0 && (
                  <p className="text-xs text-muted-foreground text-center py-2 border border-dashed rounded">
                    No parameters defined
                  </p>
                )}
              </div>
            </div>
          </CardContent>
          <CardFooter>
            <Button onClick={handleSaveQuery} className="w-full" disabled={createMutation.isPending} data-testid="button-save-query">
              <Save className="h-4 w-4 mr-2" /> {createMutation.isPending ? "Saving..." : "Save Query"}
            </Button>
          </CardFooter>
        </Card>
      </div>

      <div className="lg:col-span-8 flex flex-col h-full">
        <Card className="flex-1 flex flex-col overflow-hidden border-sidebar-border/50 bg-card/50 backdrop-blur-sm">
          <div className="p-4 border-b border-sidebar-border bg-sidebar/50 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Badge variant="outline" className="font-mono text-xs bg-background">
                {method}
              </Badge>
              <span className="font-mono text-sm text-muted-foreground">
                {selectedConnector ? selectedConnector.baseUrl : "..."}{endpoint}
              </span>
            </div>
            <Badge variant="secondary">Preview Mode</Badge>
          </div>
          
          <div className="flex-1 p-8 flex items-center justify-center bg-black/5">
            <div className="text-center space-y-4 max-w-md">
              <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center mx-auto">
                <Terminal className="h-8 w-8 text-primary" />
              </div>
              <h3 className="text-xl font-medium">Ready to Build</h3>
              <p className="text-muted-foreground">
                Configure your query on the left. Once saved, you can execute it from the library or dashboard.
              </p>
            </div>
          </div>
        </Card>
      </div>
    </div>
  );
}
