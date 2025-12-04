import { useAppStore, api } from "@/lib/store";
import ConnectorBuilder from "@/components/ConnectorBuilder";
import { Card, CardContent, CardHeader, CardTitle, CardDescription, CardFooter } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Trash2, Globe, Key, FileText, ExternalLink } from "lucide-react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useEffect } from "react";
import { useToast } from "@/hooks/use-toast";

export default function Connectors() {
  const { connectors, setConnectors } = useAppStore();
  const queryClient = useQueryClient();
  const { toast } = useToast();

  const { data: connectorsData } = useQuery({
    queryKey: ['connectors'],
    queryFn: api.getConnectors,
  });

  useEffect(() => {
    if (connectorsData) setConnectors(connectorsData);
  }, [connectorsData, setConnectors]);

  const deleteMutation = useMutation({
    mutationFn: api.deleteConnector,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['connectors'] });
      queryClient.invalidateQueries({ queryKey: ['queries'] });
      toast({
        title: "Connector Deactivated",
        description: "The connector has been deactivated.",
      });
    },
    onError: () => {
      toast({
        variant: "destructive",
        title: "Delete Failed",
        description: "Failed to delete the connector.",
      });
    },
  });

  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Data Connectors</h1>
          <p className="text-muted-foreground">
            Manage API connections and data sources.
          </p>
        </div>
      </div>

      <ConnectorBuilder />

      <div className="mt-12">
        <h3 className="text-lg font-medium mb-4">Active Connectors</h3>
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {connectors.map((connector) => (
            <Card key={connector.id} className="bg-card/50 hover:bg-card/80 transition-colors border-sidebar-border" data-testid={`connector-card-${connector.id}`}>
              <CardHeader className="pb-3">
                <div className="flex justify-between items-start">
                  <CardTitle className="text-base font-semibold">{connector.sourceName}</CardTitle>
                  <Badge variant="outline" className="font-mono text-xs">{connector.connectorType}</Badge>
                </div>
                <CardDescription className="text-xs font-mono truncate" title={connector.url}>
                  {connector.url}
                </CardDescription>
              </CardHeader>
              <CardContent className="pb-3 space-y-3">
                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                  {connector.apiKey ? (
                    <>
                      <Key className="h-3 w-3 text-green-500" />
                      <span className="text-green-500">API Key configured</span>
                    </>
                  ) : (
                    <>
                      <Globe className="h-3 w-3" />
                      <span>No API key</span>
                    </>
                  )}
                </div>
                <p className="text-xs text-muted-foreground line-clamp-2 h-8">
                  {connector.description || "No description provided."}
                </p>
                {connector.documentation && (
                  <a 
                    href={connector.documentation} 
                    target="_blank" 
                    rel="noopener noreferrer"
                    className="flex items-center gap-1 text-xs text-primary hover:underline"
                  >
                    <FileText className="h-3 w-3" />
                    Documentation
                    <ExternalLink className="h-3 w-3" />
                  </a>
                )}
                {connector.notes && (
                  <p className="text-xs text-muted-foreground italic line-clamp-1">
                    Note: {connector.notes}
                  </p>
                )}
              </CardContent>
              <CardFooter className="pt-0 flex justify-between items-center">
                <span className="text-xs text-muted-foreground font-mono">{connector.sourceId}</span>
                 <Button 
                   variant="ghost" 
                   size="sm" 
                   className="text-destructive hover:text-destructive hover:bg-destructive/10"
                   onClick={() => deleteMutation.mutate(connector.id)}
                   disabled={deleteMutation.isPending}
                   data-testid={`button-delete-connector-${connector.id}`}
                 >
                   <Trash2 className="h-4 w-4" />
                 </Button>
              </CardFooter>
            </Card>
          ))}
          {connectors.length === 0 && (
            <div className="col-span-full text-center py-12 text-muted-foreground">
              No connectors configured yet. Create one above to get started.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
