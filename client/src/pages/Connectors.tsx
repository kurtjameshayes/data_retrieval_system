import { useAppStore } from "@/lib/store";
import ConnectorBuilder from "@/components/ConnectorBuilder";
import { Card, CardContent, CardHeader, CardTitle, CardDescription, CardFooter } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Trash2, Globe, Lock, ShieldCheck } from "lucide-react";

export default function Connectors() {
  const { connectors, deleteConnector } = useAppStore();

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
            <Card key={connector.id} className="bg-card/50 hover:bg-card/80 transition-colors border-sidebar-border">
              <CardHeader className="pb-3">
                <div className="flex justify-between items-start">
                  <CardTitle className="text-base font-semibold">{connector.name}</CardTitle>
                  <Badge variant="outline" className="font-mono text-xs">{connector.type}</Badge>
                </div>
                <CardDescription className="text-xs truncate" title={connector.baseUrl}>
                  {connector.baseUrl}
                </CardDescription>
              </CardHeader>
              <CardContent className="pb-3">
                <div className="flex items-center gap-2 text-xs text-muted-foreground mb-2">
                  {connector.authType === 'None' ? (
                    <Globe className="h-3 w-3" />
                  ) : (
                    <Lock className="h-3 w-3" />
                  )}
                  <span>Auth: {connector.authType}</span>
                </div>
                <p className="text-xs text-muted-foreground line-clamp-2 h-8">
                  {connector.description || "No description provided."}
                </p>
              </CardContent>
              <CardFooter className="pt-0 flex justify-end">
                 <Button 
                   variant="ghost" 
                   size="sm" 
                   className="text-destructive hover:text-destructive hover:bg-destructive/10"
                   onClick={() => deleteConnector(connector.id)}
                 >
                   <Trash2 className="h-4 w-4" />
                 </Button>
              </CardFooter>
            </Card>
          ))}
        </div>
      </div>
    </div>
  );
}
