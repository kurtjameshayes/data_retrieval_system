import { useAppStore } from "@/lib/store";
import QueryBuilder from "@/components/QueryBuilder";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Play, Loader2, CheckCircle2, XCircle } from "lucide-react";

export default function Queries() {
  const { queries, runQuery } = useAppStore();

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Query Builder</h1>
        <p className="text-muted-foreground">
          Construct and execute data retrieval operations.
        </p>
      </div>

      <QueryBuilder />

      <div className="mt-12">
        <h3 className="text-lg font-medium mb-4">Query Library</h3>
        <div className="border rounded-lg bg-card/50 overflow-hidden">
          <div className="grid grid-cols-12 gap-4 p-4 border-b bg-muted/30 text-xs font-medium text-muted-foreground">
            <div className="col-span-4">NAME</div>
            <div className="col-span-4">ENDPOINT</div>
            <div className="col-span-2">STATUS</div>
            <div className="col-span-2 text-right">ACTIONS</div>
          </div>
          
          <div className="divide-y">
            {queries.map((query) => (
              <div key={query.id} className="grid grid-cols-12 gap-4 p-4 items-center hover:bg-muted/20 transition-colors">
                <div className="col-span-4 font-medium text-sm">{query.name}</div>
                <div className="col-span-4 text-xs font-mono text-muted-foreground truncate">
                  <span className="font-bold mr-2 text-primary">{query.method}</span>
                  {query.endpoint}
                </div>
                <div className="col-span-2">
                  {query.status === 'loading' && (
                    <Badge variant="outline" className="bg-blue-500/10 text-blue-500 border-blue-500/20">
                      <Loader2 className="h-3 w-3 mr-1 animate-spin" /> Running
                    </Badge>
                  )}
                  {query.status === 'success' && (
                    <Badge variant="outline" className="bg-green-500/10 text-green-500 border-green-500/20">
                      <CheckCircle2 className="h-3 w-3 mr-1" /> Success
                    </Badge>
                  )}
                  {query.status === 'error' && (
                    <Badge variant="outline" className="bg-red-500/10 text-red-500 border-red-500/20">
                      <XCircle className="h-3 w-3 mr-1" /> Error
                    </Badge>
                  )}
                  {query.status === 'idle' && (
                    <Badge variant="secondary">Idle</Badge>
                  )}
                </div>
                <div className="col-span-2 text-right">
                  <Button 
                    size="sm" 
                    variant="outline" 
                    onClick={() => runQuery(query.id)}
                    disabled={query.status === 'loading'}
                  >
                    <Play className="h-3 w-3 mr-2" /> Run
                  </Button>
                </div>
              </div>
            ))}
            {queries.length === 0 && (
              <div className="p-8 text-center text-muted-foreground text-sm">
                No queries saved in the library.
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
