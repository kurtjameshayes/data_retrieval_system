import type { Express } from "express";
import { createServer, type Server } from "http";
import { spawn } from "child_process";
import path from "path";
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

  // Test connector endpoint - verify URL is reachable
  app.post("/api/connectors/:id/test", async (req, res) => {
    try {
      const connector = await storage.getConnector(req.params.id);
      if (!connector) {
        return res.status(404).json({ error: "Connector not found" });
      }

      const startTime = Date.now();
      
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 10000);
        
        const response = await fetch(connector.url, {
          method: "HEAD",
          signal: controller.signal,
          headers: connector.apiKey ? { 
            "Authorization": `Bearer ${connector.apiKey}`,
            "X-Api-Key": connector.apiKey 
          } : {},
        });
        
        clearTimeout(timeoutId);
        const responseTime = Date.now() - startTime;
        
        res.json({
          success: response.ok || response.status === 405,
          status: response.status,
          statusText: response.statusText,
          responseTime,
          url: connector.url,
          message: response.ok 
            ? `Connection successful (${response.status} ${response.statusText})`
            : response.status === 405 
              ? `Endpoint reachable (HEAD not allowed, but server responded)`
              : `Server responded with ${response.status} ${response.statusText}`,
        });
      } catch (fetchError: any) {
        const responseTime = Date.now() - startTime;
        
        if (fetchError.name === "AbortError") {
          res.json({
            success: false,
            error: "Connection timeout after 10 seconds",
            responseTime,
            url: connector.url,
          });
        } else {
          res.json({
            success: false,
            error: fetchError.message || "Failed to connect",
            responseTime,
            url: connector.url,
          });
        }
      }
    } catch (error) {
      console.error("Error testing connector:", error);
      res.status(500).json({ error: "Failed to test connector" });
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

  // Query execution via Python subprocess using QueryEngine.execute_query
  app.post("/api/queries/:id/run", async (req, res) => {
    try {
      const id = req.params.id;

      const query = await storage.getQuery(id);
      if (!query) {
        return res.status(404).json({ error: "Query not found" });
      }

      // Get the connector by sourceId (query.connectorId is actually the source_id)
      const connector = await storage.getConnectorBySourceId(query.connectorId);
      if (!connector) {
        return res.status(404).json({ error: "Connector not found for query" });
      }

      // Merge query parameters with any overrides
      const useCache = req.body?.useCache !== false;
      const parameterOverrides = req.body?.parameterOverrides || {};
      const saveToResults = req.body?.saveToResults !== false;
      
      // Deep serialize parameters to handle MongoDB-specific types (ObjectId, Date, etc)
      const serializeForJson = (obj: any): any => {
        if (obj === null || obj === undefined) return obj;
        if (typeof obj !== 'object') return obj;
        if (obj instanceof Date) return obj.toISOString();
        if (typeof obj.toJSON === 'function') return obj.toJSON();
        if (Array.isArray(obj)) return obj.map(serializeForJson);
        const result: Record<string, any> = {};
        for (const [key, value] of Object.entries(obj)) {
          result[key] = serializeForJson(value);
        }
        return result;
      };
      
      const baseParams = serializeForJson(query.parameters || {});
      
      // Deep merge function to handle nested parameter overrides
      const deepMerge = (target: any, source: any): any => {
        if (source === null || source === undefined) return target;
        if (typeof source !== 'object' || Array.isArray(source)) return source;
        if (typeof target !== 'object' || Array.isArray(target)) return source;
        
        const result = { ...target };
        for (const key of Object.keys(source)) {
          if (key in result && typeof result[key] === 'object' && typeof source[key] === 'object' 
              && !Array.isArray(result[key]) && !Array.isArray(source[key])) {
            result[key] = deepMerge(result[key], source[key]);
          } else {
            result[key] = source[key];
          }
        }
        return result;
      };
      
      // If parameterOverrides contains complete substituted params, use those; otherwise deep merge
      const mergedParameters = Object.keys(parameterOverrides).length > 0 
        ? deepMerge(baseParams, parameterOverrides)
        : baseParams;

      // Execute query via Python QueryEngine.execute_query (same as execute_query.py)
      const pythonScript = path.join(process.cwd(), "python_src", "execute_query.py");
      const args = [pythonScript, connector.sourceId, JSON.stringify(mergedParameters)];
      if (!useCache) {
        args.push("--no-cache");
      }

      const pythonProcess = spawn("python3", args, {
        env: { ...process.env },
        cwd: process.cwd(),
      });

      let stdout = "";
      let stderr = "";

      pythonProcess.stdout.on("data", (data) => {
        stdout += data.toString();
      });

      pythonProcess.stderr.on("data", (data) => {
        stderr += data.toString();
      });

      pythonProcess.on("close", async (code) => {
        console.log(`Python process exited with code ${code}`);
        if (stderr) {
          console.log("Python stderr:", stderr);
        }

        try {
          let result;
          if (stdout.trim()) {
            result = JSON.parse(stdout.trim());
          } else {
            result = {
              success: false,
              error: stderr || "No output from Python script",
            };
          }

          // Store result in query_results collection (if it fails, still return Python result)
          let queryResult = null;
          if (saveToResults) {
            try {
              queryResult = await storage.createQueryResult({
                queryId: query.queryId,
                result: result.success ? (result.data || result) : { error: result.error },
                status: result.success ? "success" : "error",
                error: result.error || null,
              });
            } catch (storageError) {
              console.error("Failed to store query result:", storageError);
              // Continue without storing - Python result is still valid
            }
          }

          if (result.success) {
            res.json({ query, result: queryResult, pythonResult: result });
          } else {
            res.status(500).json({ query, result: queryResult, pythonResult: result, error: result.error });
          }
        } catch (parseError) {
          console.error("Failed to parse Python output:", parseError);
          console.log("Raw stdout:", stdout);
          
          const queryResult = await storage.createQueryResult({
            queryId: query.queryId,
            result: { error: "Failed to parse Python output", raw: stdout.substring(0, 500) },
            status: "error",
            error: `Failed to parse Python output: ${stdout.substring(0, 500)}`,
          });

          res.status(500).json({ 
            error: "Failed to parse query result",
            query,
            result: queryResult,
          });
        }
      });

      pythonProcess.on("error", async (error) => {
        console.error("Failed to spawn Python process:", error);
        
        const queryResult = await storage.createQueryResult({
          queryId: query.queryId,
          result: { error: `Failed to spawn Python process: ${error.message}` },
          status: "error",
          error: `Failed to spawn Python process: ${error.message}`,
        });

        res.status(500).json({ 
          error: "Failed to execute Python script",
          query,
          result: queryResult,
        });
      });

    } catch (error) {
      console.error("Error running query:", error);
      
      const id = req.params.id;
      try {
        const query = await storage.getQuery(id);
        if (query) {
          await storage.createQueryResult({
            queryId: query.queryId,
            result: { error: error instanceof Error ? error.message : "Unknown error" },
            status: "error",
            error: error instanceof Error ? error.message : "Unknown error",
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

  // ============================================================
  // Python Integration Endpoints
  // These endpoints use the Python query engine for execution
  // ============================================================

  // Validate connector via Python ConnectorManager
  app.post("/api/connectors/:id/validate", async (req, res) => {
    try {
      const connector = await storage.getConnector(req.params.id);
      if (!connector) {
        return res.status(404).json({ error: "Connector not found" });
      }

      const pythonScript = path.join(process.cwd(), "python_src", "validate_connector.py");
      const pythonProcess = spawn("python3", [pythonScript, connector.sourceId], {
        env: { ...process.env },
        cwd: process.cwd(),
      });

      let stdout = "";
      let stderr = "";

      pythonProcess.stdout.on("data", (data) => {
        stdout += data.toString();
      });

      pythonProcess.stderr.on("data", (data) => {
        stderr += data.toString();
      });

      pythonProcess.on("close", (code) => {
        console.log(`Python validate_connector exited with code ${code}`);
        if (stderr) {
          console.log("Python stderr:", stderr);
        }

        try {
          const result = stdout.trim() ? JSON.parse(stdout.trim()) : {
            success: false,
            error: stderr || "No output from Python script",
          };
          res.json({ connector, pythonResult: result });
        } catch (parseError) {
          console.error("Failed to parse Python output:", parseError);
          res.status(500).json({
            error: "Failed to parse validation result",
            connector,
            raw: stdout.substring(0, 500),
          });
        }
      });

      pythonProcess.on("error", (error) => {
        console.error("Failed to spawn Python process:", error);
        res.status(500).json({ error: "Failed to execute Python script", connector });
      });

    } catch (error) {
      console.error("Error validating connector:", error);
      res.status(500).json({ error: "Failed to validate connector" });
    }
  });

  // Execute ad-hoc query via Python QueryEngine
  app.post("/api/execute", async (req, res) => {
    try {
      const { sourceId, parameters, useCache = true } = req.body;

      if (!sourceId) {
        return res.status(400).json({ error: "sourceId is required" });
      }

      if (!parameters || typeof parameters !== "object") {
        return res.status(400).json({ error: "parameters must be an object" });
      }

      const pythonScript = path.join(process.cwd(), "python_src", "execute_query.py");
      const args = [pythonScript, sourceId, JSON.stringify(parameters)];
      if (!useCache) {
        args.push("--no-cache");
      }

      const pythonProcess = spawn("python3", args, {
        env: { ...process.env },
        cwd: process.cwd(),
      });

      let stdout = "";
      let stderr = "";

      pythonProcess.stdout.on("data", (data) => {
        stdout += data.toString();
      });

      pythonProcess.stderr.on("data", (data) => {
        stderr += data.toString();
      });

      pythonProcess.on("close", (code) => {
        console.log(`Python execute_query exited with code ${code}`);
        if (stderr) {
          console.log("Python stderr:", stderr);
        }

        try {
          const result = stdout.trim() ? JSON.parse(stdout.trim()) : {
            success: false,
            error: stderr || "No output from Python script",
          };

          if (result.success) {
            res.json(result);
          } else {
            res.status(500).json(result);
          }
        } catch (parseError) {
          console.error("Failed to parse Python output:", parseError);
          res.status(500).json({
            error: "Failed to parse query result",
            raw: stdout.substring(0, 500),
          });
        }
      });

      pythonProcess.on("error", (error) => {
        console.error("Failed to spawn Python process:", error);
        res.status(500).json({ error: "Failed to execute Python script" });
      });

    } catch (error) {
      console.error("Error executing query:", error);
      res.status(500).json({ error: "Failed to execute query" });
    }
  });

  // Analyze query results via Python DataAnalysisEngine
  app.post("/api/queries/:id/analyze", async (req, res) => {
    try {
      const query = await storage.getQuery(req.params.id);
      if (!query) {
        return res.status(404).json({ error: "Query not found" });
      }

      const analysisType = req.body?.type || "basic";

      const pythonScript = path.join(process.cwd(), "python_src", "analyze_data.py");
      const pythonProcess = spawn("python3", [pythonScript, query.queryId, analysisType], {
        env: { ...process.env },
        cwd: process.cwd(),
      });

      let stdout = "";
      let stderr = "";

      pythonProcess.stdout.on("data", (data) => {
        stdout += data.toString();
      });

      pythonProcess.stderr.on("data", (data) => {
        stderr += data.toString();
      });

      pythonProcess.on("close", (code) => {
        console.log(`Python analyze_data exited with code ${code}`);
        if (stderr) {
          console.log("Python stderr:", stderr);
        }

        try {
          const result = stdout.trim() ? JSON.parse(stdout.trim()) : {
            success: false,
            error: stderr || "No output from Python script",
          };

          if (result.success) {
            res.json({ query, analysis: result });
          } else {
            res.status(500).json({ query, error: result.error });
          }
        } catch (parseError) {
          console.error("Failed to parse Python output:", parseError);
          res.status(500).json({
            error: "Failed to parse analysis result",
            query,
            raw: stdout.substring(0, 500),
          });
        }
      });

      pythonProcess.on("error", (error) => {
        console.error("Failed to spawn Python process:", error);
        res.status(500).json({ error: "Failed to execute Python script", query });
      });

    } catch (error) {
      console.error("Error analyzing query:", error);
      res.status(500).json({ error: "Failed to analyze query" });
    }
  });

  // Get cache and query statistics via Python
  app.get("/api/stats", async (req, res) => {
    try {
      const pythonScript = path.join(process.cwd(), "python_src", "get_cache_stats.py");
      const pythonProcess = spawn("python3", [pythonScript], {
        env: { ...process.env },
        cwd: process.cwd(),
      });

      let stdout = "";
      let stderr = "";

      pythonProcess.stdout.on("data", (data) => {
        stdout += data.toString();
      });

      pythonProcess.stderr.on("data", (data) => {
        stderr += data.toString();
      });

      pythonProcess.on("close", (code) => {
        console.log(`Python get_cache_stats exited with code ${code}`);
        if (stderr) {
          console.log("Python stderr:", stderr);
        }

        try {
          const result = stdout.trim() ? JSON.parse(stdout.trim()) : {
            success: false,
            error: stderr || "No output from Python script",
          };
          res.json(result);
        } catch (parseError) {
          console.error("Failed to parse Python output:", parseError);
          res.status(500).json({
            error: "Failed to parse stats result",
            raw: stdout.substring(0, 500),
          });
        }
      });

      pythonProcess.on("error", (error) => {
        console.error("Failed to spawn Python process:", error);
        res.status(500).json({ error: "Failed to execute Python script" });
      });

    } catch (error) {
      console.error("Error fetching stats:", error);
      res.status(500).json({ error: "Failed to fetch stats" });
    }
  });

  // ============================================================
  // Analysis Plans Endpoints
  // Manage and execute configurable multi-query analysis plans
  // ============================================================

  // Helper to run Python scripts and return results
  const runPythonScript = (scriptPath: string, args: string[]): Promise<any> => {
    return new Promise((resolve, reject) => {
      const pythonProcess = spawn("python3", [scriptPath, ...args], {
        env: { ...process.env },
        cwd: process.cwd(),
      });

      let stdout = "";
      let stderr = "";

      pythonProcess.stdout.on("data", (data) => {
        stdout += data.toString();
      });

      pythonProcess.stderr.on("data", (data) => {
        stderr += data.toString();
      });

      pythonProcess.on("close", (code) => {
        console.log(`Python script exited with code ${code}`);
        if (stderr) {
          console.log("Python stderr:", stderr);
        }

        try {
          const result = stdout.trim() ? JSON.parse(stdout.trim()) : {
            success: false,
            error: stderr || "No output from Python script",
          };
          resolve(result);
        } catch (parseError) {
          reject(new Error(`Failed to parse Python output: ${stdout.substring(0, 500)}`));
        }
      });

      pythonProcess.on("error", (error) => {
        reject(error);
      });
    });
  };

  // List all analysis plans
  app.get("/api/analysis-plans", async (req, res) => {
    try {
      const activeOnly = req.query.active === "true";
      const scriptPath = path.join(process.cwd(), "python_src", "manage_analysis_plan.py");
      const args = ["list"];
      if (activeOnly) args.push("--active");
      
      const result = await runPythonScript(scriptPath, args);
      
      if (result.success) {
        res.json(result.plans);
      } else {
        res.status(500).json({ error: result.error });
      }
    } catch (error) {
      console.error("Error fetching analysis plans:", error);
      res.status(500).json({ error: "Failed to fetch analysis plans" });
    }
  });

  // Get single analysis plan
  app.get("/api/analysis-plans/:planId", async (req, res) => {
    try {
      const scriptPath = path.join(process.cwd(), "python_src", "manage_analysis_plan.py");
      const result = await runPythonScript(scriptPath, ["get", req.params.planId]);
      
      if (result.success) {
        res.json(result.plan);
      } else {
        res.status(404).json({ error: result.error });
      }
    } catch (error) {
      console.error("Error fetching analysis plan:", error);
      res.status(500).json({ error: "Failed to fetch analysis plan" });
    }
  });

  // Create analysis plan (with column validation)
  app.post("/api/analysis-plans", async (req, res) => {
    try {
      const planData = req.body;
      
      // First, validate that all referenced columns exist in the query outputs
      if (planData.queries && planData.queries.length > 0 && planData.analysis_config) {
        const validateScriptPath = path.join(process.cwd(), "python_src", "execute_analysis_plan.py");
        const validationResult = await runPythonScript(validateScriptPath, ["validate_plan", JSON.stringify(planData)]);
        
        if (!validationResult.success) {
          return res.status(400).json({ error: validationResult.error });
        }
        
        if (validationResult.validation && !validationResult.validation.valid) {
          return res.status(400).json({ 
            error: "Column validation failed", 
            validation_errors: validationResult.validation.errors,
            available_columns: validationResult.available_columns
          });
        }
      }
      
      // Proceed with creation if validation passes
      const scriptPath = path.join(process.cwd(), "python_src", "manage_analysis_plan.py");
      const result = await runPythonScript(scriptPath, ["create", JSON.stringify(req.body)]);
      
      if (result.success) {
        res.status(201).json(result.plan);
      } else {
        res.status(400).json({ error: result.error });
      }
    } catch (error) {
      console.error("Error creating analysis plan:", error);
      res.status(500).json({ error: "Failed to create analysis plan" });
    }
  });

  // Update analysis plan (with column validation)
  app.put("/api/analysis-plans/:planId", async (req, res) => {
    try {
      const updates = req.body;
      
      // If queries or analysis_config are being updated, validate columns
      if ((updates.queries && updates.queries.length > 0) || updates.analysis_config) {
        // Need to fetch current plan to merge with updates for full validation
        const getScriptPath = path.join(process.cwd(), "python_src", "manage_analysis_plan.py");
        const currentPlanResult = await runPythonScript(getScriptPath, ["get", req.params.planId]);
        
        if (!currentPlanResult.success || !currentPlanResult.plan) {
          return res.status(404).json({ error: "Plan not found" });
        }
        
        // Merge current plan with updates for validation
        const mergedPlan = {
          ...currentPlanResult.plan,
          ...updates,
          queries: updates.queries || currentPlanResult.plan.queries,
          analysis_config: updates.analysis_config || currentPlanResult.plan.analysis_config
        };
        
        if (mergedPlan.queries && mergedPlan.queries.length > 0 && mergedPlan.analysis_config) {
          const validateScriptPath = path.join(process.cwd(), "python_src", "execute_analysis_plan.py");
          const validationResult = await runPythonScript(validateScriptPath, ["validate_plan", JSON.stringify(mergedPlan)]);
          
          if (!validationResult.success) {
            return res.status(400).json({ error: validationResult.error });
          }
          
          if (validationResult.validation && !validationResult.validation.valid) {
            return res.status(400).json({ 
              error: "Column validation failed", 
              validation_errors: validationResult.validation.errors,
              available_columns: validationResult.available_columns
            });
          }
        }
      }
      
      // Proceed with update if validation passes
      const scriptPath = path.join(process.cwd(), "python_src", "manage_analysis_plan.py");
      const result = await runPythonScript(scriptPath, ["update", req.params.planId, JSON.stringify(req.body)]);
      
      if (result.success) {
        res.json(result.plan);
      } else {
        res.status(400).json({ error: result.error });
      }
    } catch (error) {
      console.error("Error updating analysis plan:", error);
      res.status(500).json({ error: "Failed to update analysis plan" });
    }
  });

  // Delete analysis plan
  app.delete("/api/analysis-plans/:planId", async (req, res) => {
    try {
      const scriptPath = path.join(process.cwd(), "python_src", "manage_analysis_plan.py");
      const result = await runPythonScript(scriptPath, ["delete", req.params.planId]);
      
      if (result.success) {
        res.status(204).send();
      } else {
        res.status(404).json({ error: result.error });
      }
    } catch (error) {
      console.error("Error deleting analysis plan:", error);
      res.status(500).json({ error: "Failed to delete analysis plan" });
    }
  });

  // Get columns for a specific query (for dropdown population)
  app.get("/api/queries/:queryId/columns", async (req, res) => {
    try {
      const query = await storage.getQueryByQueryId(req.params.queryId);
      if (!query) {
        return res.status(404).json({ error: "Query not found" });
      }

      const scriptPath = path.join(process.cwd(), "python_src", "execute_analysis_plan.py");
      const result = await runPythonScript(scriptPath, ["get_columns", req.params.queryId]);
      
      if (result.success) {
        res.json({ queryId: req.params.queryId, columns: result.columns });
      } else {
        res.status(500).json({ error: result.error });
      }
    } catch (error) {
      console.error("Error fetching query columns:", error);
      res.status(500).json({ error: "Failed to fetch query columns" });
    }
  });

  // Get columns for joined queries (for dropdown population after join)
  app.post("/api/analysis-plans/joined-columns", async (req, res) => {
    try {
      const { queries } = req.body;
      
      if (!queries || !Array.isArray(queries) || queries.length === 0) {
        return res.status(400).json({ error: "queries array is required" });
      }

      const scriptPath = path.join(process.cwd(), "python_src", "execute_analysis_plan.py");
      const result = await runPythonScript(scriptPath, ["get_joined_columns", JSON.stringify(queries)]);
      
      if (result.success) {
        res.json({ columns: result.columns, recordCount: result.record_count, sample: result.sample });
      } else {
        res.status(500).json({ error: result.error });
      }
    } catch (error) {
      console.error("Error fetching joined columns:", error);
      res.status(500).json({ error: "Failed to fetch joined columns" });
    }
  });

  // Validate analysis plan configuration
  app.post("/api/analysis-plans/validate", async (req, res) => {
    try {
      const scriptPath = path.join(process.cwd(), "python_src", "execute_analysis_plan.py");
      const result = await runPythonScript(scriptPath, ["validate_plan", JSON.stringify(req.body)]);
      
      if (result.success) {
        res.json(result);
      } else {
        res.status(400).json({ error: result.error });
      }
    } catch (error) {
      console.error("Error validating analysis plan:", error);
      res.status(500).json({ error: "Failed to validate analysis plan" });
    }
  });

  // Execute analysis plan
  app.post("/api/analysis-plans/:planId/execute", async (req, res) => {
    try {
      const scriptPath = path.join(process.cwd(), "python_src", "execute_analysis_plan.py");
      const result = await runPythonScript(scriptPath, ["execute", req.params.planId]);
      
      if (result.success) {
        res.json(result);
      } else {
        res.status(500).json({ error: result.error });
      }
    } catch (error) {
      console.error("Error executing analysis plan:", error);
      res.status(500).json({ error: "Failed to execute analysis plan" });
    }
  });

  // Preview analysis plan data (execute queries and join without running analysis)
  app.post("/api/analysis-plans/:planId/preview", async (req, res) => {
    try {
      const scriptPath = path.join(process.cwd(), "python_src", "execute_analysis_plan.py");
      const result = await runPythonScript(scriptPath, ["preview", req.params.planId]);
      
      if (result.success) {
        res.json(result);
      } else {
        res.status(500).json({ error: result.error });
      }
    } catch (error) {
      console.error("Error previewing analysis plan:", error);
      res.status(500).json({ error: "Failed to preview analysis plan" });
    }
  });

  return httpServer;
}
