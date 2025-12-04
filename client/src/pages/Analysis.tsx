import { useAppStore } from "@/lib/store";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, LineChart, Line } from 'recharts';
import { Code } from "lucide-react";

export default function Analysis() {
  const { queries } = useAppStore();
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
            <Card key={query.id} className="overflow-hidden">
              <CardHeader className="bg-muted/30">
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle>{query.name}</CardTitle>
                    <CardDescription className="font-mono text-xs mt-1">
                      {query.method} {query.endpoint}
                    </CardDescription>
                  </div>
                  <Badge variant="outline" className="font-mono text-xs">
                    {new Date(query.lastRun!).toLocaleTimeString()}
                  </Badge>
                </div>
              </CardHeader>
              <CardContent className="p-0">
                <div className="grid md:grid-cols-2 h-[400px] divide-x">
                  {/* Visualizer Attempt */}
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

                  {/* Raw Data */}
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

// Helper to try and make sense of random JSON for a chart
function transformDataForChart(data: any) {
  // Case 1: Array of arrays (Census style)
  if (Array.isArray(data) && Array.isArray(data[0])) {
    const headers = data[0];
    const rows = data.slice(1);
    // Try to find a numeric column
    const numericIndex = headers.findIndex((h: string) => 
      rows.some((r: any) => !isNaN(parseFloat(r[headers.indexOf(h)])))
    ); // Wait, that logic is flawed if I use index of h inside loop.
    
    // Simple approach: Assume col 0 is name, col 1 is value if numeric
    return rows.map((row: any[]) => ({
      name: row[0],
      value: parseFloat(row[1]) || 0
    })).slice(0, 10); // Limit to 10
  }

  // Case 2: Object with "data" array
  if (data?.data && Array.isArray(data.data)) {
     return data.data.map((item: any) => ({
       name: item.label || item.name || item.id || "Unknown",
       value: item.value || item.count || item.amount || 0
     }));
  }

  // Case 3: Random object with breakdown
  if (data?.breakdown && Array.isArray(data.breakdown)) {
    return data.breakdown.map((item: any) => ({
      name: item.type || item.name,
      value: item.count || item.value
    }));
  }

  return [];
}
