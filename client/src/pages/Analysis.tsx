import { useAppStore, api } from "@/lib/store";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { Code } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { useEffect } from "react";

export default function Analysis() {
  const { queries, setQueries } = useAppStore();

  const { data: queriesData } = useQuery({
    queryKey: ['queries'],
    queryFn: api.getQueries,
  });

  useEffect(() => {
    if (queriesData) setQueries(queriesData);
  }, [queriesData, setQueries]);

  const successfulQueries = queries.filter(q => q.status === 'success' && q.result);

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Analysis Dashboard</h1>
        <p className="text-muted-foreground">
          Visualize and inspect query results.
        </p>
      </div>

      {successfulQueries.length === 0 ? (
        <div className="flex flex-col items-center justify-center h-[400px] border-2 border-dashed rounded-lg text-muted-foreground">
          <p>No data available for analysis.</p>
          <p className="text-sm">Run some queries to populate this view.</p>
        </div>
      ) : (
        <div className="grid gap-8">
          {successfulQueries.map((query) => (
            <Card key={query.id} className="overflow-hidden" data-testid={`analysis-card-${query.id}`}>
              <CardHeader className="bg-muted/30">
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle>{query.name}</CardTitle>
                    <CardDescription className="font-mono text-xs mt-1">
                      {query.method} {query.endpoint}
                    </CardDescription>
                  </div>
                  <Badge variant="outline" className="font-mono text-xs">
                    {query.lastRun ? new Date(query.lastRun).toLocaleTimeString() : 'N/A'}
                  </Badge>
                </div>
              </CardHeader>
              <CardContent className="p-0">
                <div className="grid md:grid-cols-2 h-[400px] divide-x">
                  <div className="p-6 flex flex-col">
                     <h4 className="text-sm font-medium mb-4 text-muted-foreground">Visualization Preview</h4>
                     <div className="flex-1 w-full min-h-0">
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart data={transformDataForChart(query.result)}>
                            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                            <XAxis dataKey="name" stroke="hsl(var(--muted-foreground))" fontSize={12} />
                            <YAxis stroke="hsl(var(--muted-foreground))" fontSize={12} />
                            <Tooltip 
                              contentStyle={{ 
                                backgroundColor: 'hsl(var(--popover))', 
                                borderColor: 'hsl(var(--border))',
                                color: 'hsl(var(--popover-foreground))'
                              }} 
                            />
                            <Bar dataKey="value" fill="hsl(var(--primary))" radius={[4, 4, 0, 0]} />
                          </BarChart>
                        </ResponsiveContainer>
                     </div>
                  </div>

                  <div className="flex flex-col bg-muted/10">
                    <div className="p-3 border-b bg-muted/20 flex items-center gap-2">
                      <Code className="h-4 w-4" />
                      <span className="text-xs font-medium">Raw JSON Response</span>
                    </div>
                    <ScrollArea className="flex-1 p-4">
                      <pre className="text-xs font-mono text-muted-foreground">
                        {JSON.stringify(query.result, null, 2)}
                      </pre>
                    </ScrollArea>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}

function transformDataForChart(data: any) {
  if (Array.isArray(data) && Array.isArray(data[0])) {
    const rows = data.slice(1);
    return rows.map((row: any[]) => ({
      name: row[0],
      value: parseFloat(row[1]) || 0
    })).slice(0, 10);
  }

  if (data?.data && Array.isArray(data.data)) {
     return data.data.map((item: any) => ({
       name: item.label || item.name || item.id || "Unknown",
       value: item.value || item.count || item.amount || 0
     }));
  }

  if (data?.breakdown && Array.isArray(data.breakdown)) {
    return data.breakdown.map((item: any) => ({
      name: item.type || item.name,
      value: item.count || item.value
    }));
  }

  return [];
}
