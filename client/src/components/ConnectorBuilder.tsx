import { useState } from "react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import * as z from "zod";
import { api } from "@/lib/store";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Code, Plug } from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import { useMutation, useQueryClient } from "@tanstack/react-query";

const connectorSchema = z.object({
  sourceId: z.string().min(1, "Source ID is required").regex(/^[a-z0-9_]+$/, "Only lowercase letters, numbers, and underscores"),
  sourceName: z.string().min(2, "Source name is required"),
  connectorType: z.string().min(1, "Connector type is required"),
  url: z.string().url("Must be a valid URL"),
  apiKey: z.string().optional(),
  format: z.string().optional(),
  description: z.string().optional(),
  documentation: z.string().optional(),
  notes: z.string().optional(),
  maxRetries: z.number().optional(),
  retryDelay: z.number().optional(),
});

type ConnectorFormValues = z.infer<typeof connectorSchema>;

export default function ConnectorBuilder() {
  const queryClient = useQueryClient();
  const { toast } = useToast();
  const [jsonInput, setJsonInput] = useState("");

  const form = useForm<ConnectorFormValues>({
    resolver: zodResolver(connectorSchema),
    defaultValues: {
      sourceId: "",
      sourceName: "",
      connectorType: "",
      url: "",
      apiKey: "",
      format: "JSON",
      description: "",
      documentation: "",
      notes: "",
      maxRetries: 3,
      retryDelay: 1,
    },
  });

  const createMutation = useMutation({
    mutationFn: api.createConnector,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['connectors'] });
      toast({
        title: "Connector Created",
        description: "The connector has been added to the system.",
      });
      form.reset();
    },
    onError: () => {
      toast({
        variant: "destructive",
        title: "Creation Failed",
        description: "Failed to create the connector.",
      });
    },
  });

  const onSubmit = (data: ConnectorFormValues) => {
    createMutation.mutate({
      sourceId: data.sourceId,
      sourceName: data.sourceName,
      connectorType: data.connectorType,
      url: data.url,
      apiKey: data.apiKey || null,
      format: data.format || "JSON",
      description: data.description || null,
      documentation: data.documentation || null,
      notes: data.notes || null,
      maxRetries: data.maxRetries || 3,
      retryDelay: data.retryDelay || 1,
    });
  };

  const handleJsonImport = () => {
    try {
      const parsed = JSON.parse(jsonInput);
      if (parsed.source_id) form.setValue("sourceId", parsed.source_id);
      if (parsed.sourceId) form.setValue("sourceId", parsed.sourceId);
      if (parsed.source_name) form.setValue("sourceName", parsed.source_name);
      if (parsed.sourceName) form.setValue("sourceName", parsed.sourceName);
      if (parsed.connector_type) form.setValue("connectorType", parsed.connector_type);
      if (parsed.connectorType) form.setValue("connectorType", parsed.connectorType);
      if (parsed.url) form.setValue("url", parsed.url);
      if (parsed.api_key) form.setValue("apiKey", parsed.api_key);
      if (parsed.apiKey) form.setValue("apiKey", parsed.apiKey);
      if (parsed.format) form.setValue("format", parsed.format);
      if (parsed.description) form.setValue("description", parsed.description);
      if (parsed.documentation) form.setValue("documentation", parsed.documentation);
      if (parsed.notes) form.setValue("notes", parsed.notes);
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

  return (
    <div className="grid gap-6 lg:grid-cols-2">
      <Card className="border-sidebar-border/50 bg-card/50 backdrop-blur-sm">
        <CardHeader>
          <CardTitle>Connector Configuration</CardTitle>
          <CardDescription>
            Define the connection parameters for the external API.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Tabs defaultValue="manual" className="w-full">
            <TabsList className="grid w-full grid-cols-2 mb-4">
              <TabsTrigger value="manual">Manual Entry</TabsTrigger>
              <TabsTrigger value="json">JSON Import</TabsTrigger>
            </TabsList>

            <TabsContent value="manual">
              <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="sourceId">Source ID</Label>
                    <Input 
                      id="sourceId" 
                      {...form.register("sourceId")} 
                      placeholder="e.g. census_api" 
                      className="font-mono text-xs"
                      data-testid="input-source-id" 
                    />
                    {form.formState.errors.sourceId && (
                      <p className="text-xs text-destructive">{form.formState.errors.sourceId.message}</p>
                    )}
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="connectorType">Connector Type</Label>
                    <Input 
                      id="connectorType" 
                      {...form.register("connectorType")} 
                      placeholder="e.g. census, rest, fbi_crime" 
                      className="font-mono text-xs"
                      data-testid="input-connector-type" 
                    />
                  </div>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="sourceName">Source Name</Label>
                  <Input 
                    id="sourceName" 
                    {...form.register("sourceName")} 
                    placeholder="e.g. US Census Bureau API" 
                    data-testid="input-source-name" 
                  />
                  {form.formState.errors.sourceName && (
                    <p className="text-xs text-destructive">{form.formState.errors.sourceName.message}</p>
                  )}
                </div>

                <div className="space-y-2">
                  <Label htmlFor="url">API URL</Label>
                  <Input 
                    id="url" 
                    {...form.register("url")} 
                    placeholder="https://api.example.com/data" 
                    className="font-mono text-xs" 
                    data-testid="input-url" 
                  />
                  {form.formState.errors.url && (
                    <p className="text-xs text-destructive">{form.formState.errors.url.message}</p>
                  )}
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="apiKey">API Key (Optional)</Label>
                    <Input 
                      id="apiKey" 
                      type="password"
                      {...form.register("apiKey")} 
                      placeholder="Your API key" 
                      className="font-mono text-xs"
                      data-testid="input-api-key" 
                    />
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="format">Response Format</Label>
                    <Input 
                      id="format" 
                      {...form.register("format")} 
                      placeholder="JSON" 
                      className="font-mono text-xs"
                      data-testid="input-format" 
                    />
                  </div>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="description">Description</Label>
                  <Textarea 
                    id="description" 
                    {...form.register("description")} 
                    placeholder="Brief description of this data source..." 
                    className="resize-none h-16"
                    data-testid="textarea-description"
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="documentation">Documentation URL</Label>
                  <Input 
                    id="documentation" 
                    {...form.register("documentation")} 
                    placeholder="https://docs.example.com/api" 
                    className="font-mono text-xs"
                    data-testid="input-documentation" 
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="notes">Notes</Label>
                  <Textarea 
                    id="notes" 
                    {...form.register("notes")} 
                    placeholder="Additional notes, usage tips, etc." 
                    className="resize-none h-16"
                    data-testid="textarea-notes"
                  />
                </div>

                <Button type="submit" className="w-full" disabled={createMutation.isPending} data-testid="button-create-connector">
                  {createMutation.isPending ? "Creating..." : "Create Connector"}
                </Button>
              </form>
            </TabsContent>

            <TabsContent value="json">
              <div className="space-y-4">
                <div className="space-y-2">
                  <Label>Paste Configuration JSON</Label>
                  <div className="relative">
                    <Textarea
                      value={jsonInput}
                      onChange={(e) => setJsonInput(e.target.value)}
                      className="font-mono text-xs min-h-[300px] bg-muted/50"
                      placeholder={`{
  "source_id": "my_api",
  "source_name": "My API",
  "connector_type": "rest",
  "url": "https://api.example.com",
  "api_key": "your-api-key",
  "description": "API description",
  "documentation": "https://docs.example.com"
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
      </Card>

      <div className="hidden lg:block space-y-6">
        <Card className="border-sidebar-border/50 bg-card/50 h-full flex items-center justify-center border-dashed">
           <div className="text-center text-muted-foreground">
              <Plug className="h-12 w-12 mx-auto mb-4 opacity-20" />
              <h3 className="text-lg font-medium">Connector Preview</h3>
              <p className="text-sm max-w-xs mx-auto mt-2">
                Configure the connector details on the left to register a new data source.
              </p>
           </div>
        </Card>
      </div>
    </div>
  );
}
