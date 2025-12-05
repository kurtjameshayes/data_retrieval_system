import { useState, useMemo } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { ChevronLeft, ChevronRight, Search, Download } from "lucide-react";

interface DataTablePreviewProps {
  data: any[];
  maxRows?: number;
}

export default function DataTablePreview({ data, maxRows = 100 }: DataTablePreviewProps) {
  const [page, setPage] = useState(0);
  const [search, setSearch] = useState("");
  const pageSize = 20;

  const safeData = useMemo(() => {
    if (!data) return [];
    if (Array.isArray(data)) return data;
    if (typeof data === 'object') return [data];
    return [];
  }, [data]);

  const columns = useMemo(() => {
    if (safeData.length === 0) return [];
    const allKeys = new Set<string>();
    safeData.forEach(row => {
      if (row && typeof row === 'object') {
        Object.keys(row).forEach(key => allKeys.add(key));
      }
    });
    return Array.from(allKeys);
  }, [safeData]);

  const filteredData = useMemo(() => {
    if (safeData.length === 0) return [];
    const limited = safeData.slice(0, maxRows);
    if (!search) return limited;
    
    const searchLower = search.toLowerCase();
    return limited.filter(row => {
      if (!row || typeof row !== 'object') return false;
      return Object.values(row).some(val => 
        String(val).toLowerCase().includes(searchLower)
      );
    });
  }, [data, search, maxRows]);

  const paginatedData = useMemo(() => {
    const start = page * pageSize;
    return filteredData.slice(start, start + pageSize);
  }, [filteredData, page]);

  const totalPages = Math.ceil(filteredData.length / pageSize);

  const handleDownloadCsv = () => {
    if (safeData.length === 0) return;
    
    const headers = columns.join(",");
    const rows = safeData.slice(0, maxRows).map(row => 
      columns.map(col => {
        const val = row?.[col];
        if (val === null || val === undefined) return "";
        const str = String(val);
        return str.includes(",") || str.includes('"') || str.includes("\n") 
          ? `"${str.replace(/"/g, '""')}"` 
          : str;
      }).join(",")
    );
    
    const csv = [headers, ...rows].join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "query_results.csv";
    a.click();
    URL.revokeObjectURL(url);
  };

  if (safeData.length === 0) {
    return (
      <div className="border rounded-lg p-8 text-center text-muted-foreground">
        No data to display
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-4">
        <div className="relative flex-1 max-w-sm">
          <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search data..."
            value={search}
            onChange={(e) => {
              setSearch(e.target.value);
              setPage(0);
            }}
            className="pl-8 h-8 text-sm"
            data-testid="input-search-table"
          />
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-muted-foreground">
            {filteredData.length} of {Math.min(safeData.length, maxRows)} rows {safeData.length > maxRows && `(${safeData.length} total, showing first ${maxRows})`}
          </span>
          <Button
            variant="outline"
            size="sm"
            onClick={handleDownloadCsv}
            className="h-8"
            data-testid="button-download-csv"
          >
            <Download className="h-3 w-3 mr-1" />
            CSV
          </Button>
        </div>
      </div>

      <ScrollArea className="border rounded-lg">
        <div className="max-h-[400px] overflow-auto">
          <table className="w-full text-xs">
            <thead className="bg-muted/50 sticky top-0">
              <tr>
                {columns.map((col) => (
                  <th key={col} className="px-3 py-2 text-left font-medium text-muted-foreground whitespace-nowrap border-b">
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {paginatedData.map((row, i) => (
                <tr key={i} className="hover:bg-muted/30 transition-colors" data-testid={`table-row-${i}`}>
                  {columns.map((col) => {
                    const val = row?.[col];
                    let displayValue: string;
                    if (val === null || val === undefined) {
                      displayValue = "";
                    } else if (typeof val === 'object') {
                      displayValue = JSON.stringify(val);
                    } else {
                      displayValue = String(val);
                    }
                    return (
                      <td key={col} className="px-3 py-2 border-b border-border/50 max-w-[200px] truncate" title={displayValue}>
                        {val !== null && val !== undefined ? displayValue : <span className="text-muted-foreground/50">null</span>}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </ScrollArea>

      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <span className="text-xs text-muted-foreground">
            Page {page + 1} of {totalPages}
          </span>
          <div className="flex items-center gap-1">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setPage(p => Math.max(0, p - 1))}
              disabled={page === 0}
              className="h-7 w-7 p-0"
              data-testid="button-prev-page"
            >
              <ChevronLeft className="h-4 w-4" />
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
              disabled={page >= totalPages - 1}
              className="h-7 w-7 p-0"
              data-testid="button-next-page"
            >
              <ChevronRight className="h-4 w-4" />
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}
