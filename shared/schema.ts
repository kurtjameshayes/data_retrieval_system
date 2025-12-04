import { pgTable, text, serial, timestamp, jsonb } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

export const connectors = pgTable("connectors", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  baseUrl: text("base_url").notNull(),
  type: text("type").notNull(), // 'REST' | 'GRAPHQL'
  description: text("description"),
  headers: jsonb("headers").default([]).$type<Array<{ id: string; key: string; value: string }>>(),
  authType: text("auth_type").notNull(), // 'None' | 'Bearer' | 'ApiKey'
  authKey: text("auth_key"),
  createdAt: timestamp("created_at").defaultNow().notNull(),
});

export const queries = pgTable("queries", {
  id: serial("id").primaryKey(),
  connectorId: serial("connector_id").references(() => connectors.id, { onDelete: 'cascade' }).notNull(),
  name: text("name").notNull(),
  description: text("description"),
  notes: text("notes"),
  tags: jsonb("tags").default([]).$type<string[]>(),
  queryId: text("query_id").notNull().unique(),
  endpoint: text("endpoint").notNull(),
  method: text("method").notNull(), // 'GET' | 'POST' | 'PUT' | 'DELETE'
  params: jsonb("params").default([]).$type<Array<{ id: string; key: string; value: string; enabled: boolean }>>(),
  lastRun: timestamp("last_run"),
  status: text("status"), // 'idle' | 'loading' | 'success' | 'error'
  result: jsonb("result"),
  createdAt: timestamp("created_at").defaultNow().notNull(),
});

// Connector schemas
export const insertConnectorSchema = createInsertSchema(connectors, {
  type: z.enum(['REST', 'GRAPHQL']),
  authType: z.enum(['None', 'Bearer', 'ApiKey']),
}).omit({ id: true, createdAt: true });

export type InsertConnector = z.infer<typeof insertConnectorSchema>;
export type Connector = typeof connectors.$inferSelect;

// Query schemas
export const insertQuerySchema = createInsertSchema(queries, {
  method: z.enum(['GET', 'POST', 'PUT', 'DELETE']),
  status: z.enum(['idle', 'loading', 'success', 'error']).optional(),
}).omit({ id: true, createdAt: true });

export type InsertQuery = z.infer<typeof insertQuerySchema>;
export type Query = typeof queries.$inferSelect;
