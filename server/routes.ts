import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage, type InsertConnector, type InsertQuery } from "./storage";
import { z } from "zod";
import { fromZodError } from "zod-validation-error";

const insertConnectorSchema = z.object({
  sourceId: z.string().min(1),
  sourceName: z.string().min(1),
  connectorType: z.string().min(1),
  url: z.string().url(),
  apiKey: z.string().nullable().optional(),
  format: z.string().optional(),
  description: z.string().nullable().optional(),
  documentation: z.string().nullable().optional(),
  notes: z.string().nullable().optional(),
  maxRetries: z.number().optional(),
  retryDelay: z.number().optional(),
});

const insertQuerySchema = z.object({
  queryId: z.string().min(1),
  queryName: z.string().min(1),
  connectorId: z.string().min(1),
  description: z.string().nullable().optional(),
  parameters: z.record(z.any()).optional(),
  tags: z.array(z.string()).optional(),
  notes: z.any().optional(),
});

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  
  // Connector routes
  app.get("/api/connectors", async (req, res) => {
    try {
      const connectors = await storage.getConnectors();
      res.json(connectors);
    } catch (error) {
      console.error("Error fetching connectors:", error);
      res.status(500).json({ error: "Failed to fetch connectors" });
    }
  });

  app.get("/api/connectors/:id", async (req, res) => {
    try {
      const connector = await storage.getConnector(req.params.id);
      if (!connector) {
        return res.status(404).json({ error: "Connector not found" });
      }
      res.json(connector);
    } catch (error) {
      console.error("Error fetching connector:", error);
      res.status(500).json({ error: "Failed to fetch connector" });
    }
  });

  app.get("/api/connectors/source/:sourceId", async (req, res) => {
    try {
      const connector = await storage.getConnectorBySourceId(req.params.sourceId);
      if (!connector) {
        return res.status(404).json({ error: "Connector not found" });
      }
      res.json(connector);
    } catch (error) {
      console.error("Error fetching connector:", error);
      res.status(500).json({ error: "Failed to fetch connector" });
    }
  });

  app.post("/api/connectors", async (req, res) => {
    try {
      const result = insertConnectorSchema.safeParse(req.body);
      if (!result.success) {
        return res.status(400).json({ error: fromZodError(result.error).message });
      }
      
      const connector = await storage.createConnector(result.data as InsertConnector);
      res.status(201).json(connector);
    } catch (error) {
      console.error("Error creating connector:", error);
      res.status(500).json({ error: "Failed to create connector" });
    }
  });

  app.put("/api/connectors/:id", async (req, res) => {
    try {
      const connector = await storage.updateConnector(req.params.id, req.body);
      res.json(connector);
    } catch (error) {
      console.error("Error updating connector:", error);
      res.status(500).json({ error: "Failed to update connector" });
    }
  });

  app.delete("/api/connectors/:id", async (req, res) => {
    try {
      await storage.deleteConnector(req.params.id);
      res.status(204).send();
    } catch (error) {
      console.error("Error deleting connector:", error);
      res.status(500).json({ error: "Failed to delete connector" });
    }
  });

  // Query routes
  app.get("/api/queries", async (req, res) => {
    try {
      const queries = await storage.getQueries();
      res.json(queries);
    } catch (error) {
      console.error("Error fetching queries:", error);
      res.status(500).json({ error: "Failed to fetch queries" });
    }
  });

  app.get("/api/queries/:id", async (req, res) => {
    try {
      const query = await storage.getQuery(req.params.id);
      if (!query) {
        return res.status(404).json({ error: "Query not found" });
      }
      res.json(query);
    } catch (error) {
      console.error("Error fetching query:", error);
      res.status(500).json({ error: "Failed to fetch query" });
    }
  });

  app.get("/api/queries/by-query-id/:queryId", async (req, res) => {
    try {
      const query = await storage.getQueryByQueryId(req.params.queryId);
      if (!query) {
        return res.status(404).json({ error: "Query not found" });
      }
      res.json(query);
    } catch (error) {
      console.error("Error fetching query:", error);
      res.status(500).json({ error: "Failed to fetch query" });
    }
  });

  app.get("/api/queries/connector/:connectorId", async (req, res) => {
    try {
      const queries = await storage.getQueriesByConnectorId(req.params.connectorId);
      res.json(queries);
    } catch (error) {
      console.error("Error fetching queries:", error);
      res.status(500).json({ error: "Failed to fetch queries" });
    }
  });

  app.post("/api/queries", async (req, res) => {
    try {
      const result = insertQuerySchema.safeParse(req.body);
      if (!result.success) {
        return res.status(400).json({ error: fromZodError(result.error).message });
      }
      
      const query = await storage.createQuery(result.data as InsertQuery);
      res.status(201).json(query);
    } catch (error) {
      console.error("Error creating query:", error);
      res.status(500).json({ error: "Failed to create query" });
    }
  });

  app.put("/api/queries/:id", async (req, res) => {
    try {
      const query = await storage.updateQuery(req.params.id, req.body);
      res.json(query);
    } catch (error) {
      console.error("Error updating query:", error);
      res.status(500).json({ error: "Failed to update query" });
    }
  });

  app.delete("/api/queries/:id", async (req, res) => {
    try {
      await storage.deleteQuery(req.params.id);
      res.status(204).send();
    } catch (error) {
      console.error("Error deleting query:", error);
      res.status(500).json({ error: "Failed to delete query" });
    }
  });

  // Query execution
  app.post("/api/queries/:id/run", async (req, res) => {
    try {
      const id = req.params.id;

      const query = await storage.getQuery(id);
      if (!query) {
        return res.status(404).json({ error: "Query not found" });
      }

      // Get connector by source_id (connector_id in query references source_id)
      const connector = await storage.getConnectorBySourceId(query.connectorId);
      if (!connector) {
        return res.status(404).json({ error: `Connector '${query.connectorId}' not found` });
      }

      // Build the request URL with parameters
      const params = new URLSearchParams();
      if (query.parameters) {
        Object.entries(query.parameters).forEach(([key, value]) => {
          if (value !== undefined && value !== null) {
            params.append(key, String(value));
          }
        });
      }

      // Add API key if available
      if (connector.apiKey) {
        params.append('key', connector.apiKey);
      }

      const url = `${connector.url}?${params.toString()}`;

      // Build headers
      const headers: Record<string, string> = {
        'Accept': 'application/json',
      };

      // Execute the API call
      const response = await fetch(url, {
        method: 'GET',
        headers,
      });

      let data;
      const contentType = response.headers.get('content-type');
      if (contentType && contentType.includes('application/json')) {
        data = await response.json();
      } else {
        data = await response.text();
      }

      // Store result in query_results collection
      const queryResult = await storage.createQueryResult({
        queryId: query.queryId,
        result: data,
        status: response.ok ? 'success' : 'error',
        error: response.ok ? null : `HTTP ${response.status}`,
      });

      res.json({ query, result: queryResult });
    } catch (error) {
      console.error("Error running query:", error);
      
      const id = req.params.id;
      try {
        const query = await storage.getQuery(id);
        if (query) {
          await storage.createQueryResult({
            queryId: query.queryId,
            result: null,
            status: 'error',
            error: error instanceof Error ? error.message : 'Unknown error',
          });
        }
      } catch (e) {
        // Ignore update error
      }
      
      res.status(500).json({ error: "Failed to execute query" });
    }
  });

  // Query results routes
  app.get("/api/queries/:id/results", async (req, res) => {
    try {
      const query = await storage.getQuery(req.params.id);
      if (!query) {
        return res.status(404).json({ error: "Query not found" });
      }
      const results = await storage.getQueryResults(query.queryId);
      res.json(results);
    } catch (error) {
      console.error("Error fetching query results:", error);
      res.status(500).json({ error: "Failed to fetch query results" });
    }
  });

  app.get("/api/queries/:id/results/latest", async (req, res) => {
    try {
      const query = await storage.getQuery(req.params.id);
      if (!query) {
        return res.status(404).json({ error: "Query not found" });
      }
      const result = await storage.getLatestQueryResult(query.queryId);
      if (!result) {
        return res.status(404).json({ error: "No results found" });
      }
      res.json(result);
    } catch (error) {
      console.error("Error fetching latest query result:", error);
      res.status(500).json({ error: "Failed to fetch latest query result" });
    }
  });

  return httpServer;
}
