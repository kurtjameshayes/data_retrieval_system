import { useState } from "react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import * as z from "zod";
import { useAppStore } from "@/lib/store";
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
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Plus, Trash2, Code, Plug } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

const connectorSchema = z.object({
  name: z.string().min(2, "Name is required"),
  baseUrl: z.string().url("Must be a valid URL"),
  type: z.enum(["REST", "GRAPHQL"]),
  description: z.string().optional(),
  authType: z.enum(["None", "Bearer", "ApiKey"]),
  authKey: z.string().optional(),
});

type ConnectorFormValues = z.infer<typeof connectorSchema>;

export default function ConnectorBuilder() {
  const { addConnector } = useAppStore();
  const { toast } = useToast();
  const [jsonInput, setJsonInput] = useState("");
  const [headers, setHeaders] = useState<{ key: string; value: string }[]>([]);

  const form = useForm<ConnectorFormValues>({
    resolver: zodResolver(connectorSchema),
    defaultValues: {
      type: "REST",
      authType: "None",
      name: "",
      baseUrl: "",
      description: "",
    },
  });

  const onSubmit = (data: ConnectorFormValues) => {
    addConnector({
      ...data,
      description: data.description || "",
      headers: headers.map((h, i) => ({ 
        id: `h_${i}`, 
        key: h.key, 
        value: h.value 
      })),
    });

    toast({
      title: "Connector Created",
      description: `${data.name} has been added to the system.`,
    });
    
    form.reset();
    setHeaders([]);
  };

  const handleJsonImport = () => {
    try {
      const parsed = JSON.parse(jsonInput);
      // Basic validation and mapping
      if (parsed.name) form.setValue("name", parsed.name);
      if (parsed.baseUrl) form.setValue("baseUrl", parsed.baseUrl);
      if (parsed.description) form.setValue("description", parsed.description);
      if (parsed.headers) {
        const newHeaders = Object.entries(parsed.headers).map(([key, value]) => ({
          key,
          value: String(value),
        }));
        setHeaders(newHeaders);
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

  const addHeader = () => setHeaders([...headers, { key: "", value: "" }]);
  const removeHeader = (index: number) => setHeaders(headers.filter((_, i) => i !== index));
  const updateHeader = (index: number, field: "key" | "value", value: string) => {
    const newHeaders = [...headers];
    newHeaders[index][field] = value;
    setHeaders(newHeaders);
  };

  return (
    <div className="grid gap-6 lg:grid-cols-2">
      {/* Builder Column */}
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
                <div className="space-y-2">
                  <Label htmlFor="name">Connector Name</Label>
                  <Input id="name" {...form.register("name")} placeholder="e.g. Stripe API" />
                  {form.formState.errors.name && (
                    <p className="text-xs text-destructive">{form.formState.errors.name.message}</p>
                  )}
                </div>

                <div className="space-y-2">
                  <Label htmlFor="baseUrl">Base URL</Label>
                  <Input id="baseUrl" {...form.register("baseUrl")} placeholder="https://api.example.com/v1" className="font-mono text-xs" />
                  {form.formState.errors.baseUrl && (
                    <p className="text-xs text-destructive">{form.formState.errors.baseUrl.message}</p>
                  )}
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label>Type</Label>
                    <Select 
                      onValueChange={(val: any) => form.setValue("type", val)}
                      defaultValue={form.getValues("type")}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select type" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="REST">REST</SelectItem>
                        <SelectItem value="GRAPHQL">GraphQL</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>

                  <div className="space-y-2">
                    <Label>Auth Type</Label>
                    <Select 
                      onValueChange={(val: any) => form.setValue("authType", val)}
                      defaultValue={form.getValues("authType")}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select auth" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="None">None</SelectItem>
                        <SelectItem value="Bearer">Bearer Token</SelectItem>
                        <SelectItem value="ApiKey">API Key</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                <div className="space-y-2">
                    <Label>Default Headers</Label>
                    <div className="space-y-2">
                      {headers.map((header, index) => (
                        <div key={index} className="flex gap-2">
                          <Input 
                            placeholder="Key" 
                            value={header.key} 
                            onChange={(e) => updateHeader(index, "key", e.target.value)}
                            className="font-mono text-xs h-8"
                          />
                          <Input 
                            placeholder="Value" 
                            value={header.value} 
                            onChange={(e) => updateHeader(index, "value", e.target.value)}
                            className="font-mono text-xs h-8"
                          />
                          <Button type="button" variant="ghost" size="icon" className="h-8 w-8" onClick={() => removeHeader(index)}>
                            <Trash2 className="h-4 w-4 text-muted-foreground" />
                          </Button>
                        </div>
                      ))}
                      <Button type="button" variant="outline" size="sm" onClick={addHeader} className="w-full border-dashed">
                        <Plus className="h-3 w-3 mr-2" /> Add Header
                      </Button>
                    </div>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="description">Description (Optional)</Label>
                  <Textarea 
                    id="description" 
                    {...form.register("description")} 
                    placeholder="Brief description of this data source..." 
                    className="resize-none h-20"
                  />
                </div>

                <Button type="submit" className="w-full">Create Connector</Button>
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
  "name": "My API",
  "baseUrl": "https://api.example.com",
  "headers": {
    "Authorization": "Bearer 123"
  }
}`}
                    />
                    <Code className="absolute top-3 right-3 h-4 w-4 text-muted-foreground opacity-50" />
                  </div>
                </div>
                <Button onClick={handleJsonImport} variant="secondary" className="w-full">
                  Parse & Populate Form
                </Button>
              </div>
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>

      {/* Preview Column - could be used for something else later */}
      <div className="hidden lg:block space-y-6">
        <Card className="border-sidebar-border/50 bg-card/50 h-full flex items-center justify-center border-dashed">
           <div className="text-center text-muted-foreground">
              <Plug className="h-12 w-12 mx-auto mb-4 opacity-20" />
              <h3 className="text-lg font-medium">Connector Preview</h3>
              <p className="text-sm max-w-xs mx-auto mt-2">
                Configure the connector details on the left to register a new data source in the nexus.
              </p>
           </div>
        </Card>
      </div>
    </div>
  );
}
