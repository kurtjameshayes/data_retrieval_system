import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage, type InsertConnector, type InsertQuery } from "./storage";
import { z } from "zod";
import { fromZodError } from "zod-validation-error";

const insertConnectorSchema = z.object({
  name: z.string().min(1),
  baseUrl: z.string().url(),
  type: z.enum(['REST', 'GRAPHQL']),
  description: z.string().nullable().optional(),
  headers: z.array(z.object({
    id: z.string(),
    key: z.string(),
    value: z.string(),
  })).nullable().optional(),
  authType: z.enum(['None', 'Bearer', 'ApiKey']),
  authKey: z.string().nullable().optional(),
});

const insertQuerySchema = z.object({
  connectorId: z.string(),
  name: z.string().min(1),
  description: z.string().nullable().optional(),
  notes: z.string().nullable().optional(),
  tags: z.array(z.string()).nullable().optional(),
  queryId: z.string().min(1),
  endpoint: z.string().min(1),
  method: z.enum(['GET', 'POST', 'PUT', 'DELETE']),
  params: z.array(z.object({
    id: z.string(),
    key: z.string(),
    value: z.string(),
    enabled: z.boolean(),
  })).nullable().optional(),
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

  app.delete("/api/connectors/:id", async (req, res) => {
    try {
      const id = req.params.id;
      await storage.deleteConnector(id);
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

  app.post("/api/queries/:id/run", async (req, res) => {
    try {
      const id = req.params.id;

      const query = await storage.getQuery(id);
      if (!query) {
        return res.status(404).json({ error: "Query not found" });
      }

      const connector = await storage.getConnector(query.connectorId);
      if (!connector) {
        return res.status(404).json({ error: "Connector not found" });
      }

      // Build the full URL
      let url = `${connector.baseUrl}${query.endpoint}`;
      
      // Add query parameters
      const enabledParams = query.params?.filter((p: any) => p.enabled) || [];
      if (enabledParams.length > 0) {
        const params = new URLSearchParams();
        enabledParams.forEach((p: any) => {
          params.append(p.key, p.value);
        });
        url += `?${params.toString()}`;
      }

      // Build headers
      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
      };
      
      // Add connector default headers
      connector.headers?.forEach((h: any) => {
        headers[h.key] = h.value;
      });

      // Add auth headers
      if (connector.authType === 'Bearer' && connector.authKey) {
        headers['Authorization'] = `Bearer ${connector.authKey}`;
      } else if (connector.authType === 'ApiKey' && connector.authKey) {
        headers[connector.authKey] = req.body.apiKey || '';
      }

      // Execute the API call
      const response = await fetch(url, {
        method: query.method,
        headers,
      });

      const data = await response.json();

      // Update query with results
      const updatedQuery = await storage.updateQuery(id, {
        lastRun: new Date(),
        status: 'success',
        result: data,
      });

      res.json(updatedQuery);
    } catch (error) {
      console.error("Error running query:", error);
      
      // Update query with error status
      const id = req.params.id;
      try {
        await storage.updateQuery(id, {
          lastRun: new Date(),
          status: 'error',
        });
      } catch (e) {
        // Ignore update error
      }
      
      res.status(500).json({ error: "Failed to execute query" });
    }
  });

  return httpServer;
}
