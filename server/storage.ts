import { connectors, queries, type Connector, type Query, type InsertConnector, type InsertQuery } from "@shared/schema";
import { db } from "./db";
import { eq } from "drizzle-orm";

export interface IStorage {
  // Connectors
  getConnectors(): Promise<Connector[]>;
  getConnector(id: number): Promise<Connector | undefined>;
  createConnector(connector: InsertConnector): Promise<Connector>;
  deleteConnector(id: number): Promise<void>;
  
  // Queries
  getQueries(): Promise<Query[]>;
  getQuery(id: number): Promise<Query | undefined>;
  getQueryByQueryId(queryId: string): Promise<Query | undefined>;
  createQuery(query: InsertQuery): Promise<Query>;
  updateQuery(id: number, updates: Partial<Query>): Promise<Query>;
  deleteQuery(id: number): Promise<void>;
}

export class DatabaseStorage implements IStorage {
  // Connectors
  async getConnectors(): Promise<Connector[]> {
    return await db.select().from(connectors);
  }

  async getConnector(id: number): Promise<Connector | undefined> {
    const [connector] = await db.select().from(connectors).where(eq(connectors.id, id));
    return connector || undefined;
  }

  async createConnector(connector: InsertConnector): Promise<Connector> {
    const [newConnector] = await db
      .insert(connectors)
      .values(connector)
      .returning();
    return newConnector;
  }

  async deleteConnector(id: number): Promise<void> {
    await db.delete(connectors).where(eq(connectors.id, id));
  }

  // Queries
  async getQueries(): Promise<Query[]> {
    return await db.select().from(queries);
  }

  async getQuery(id: number): Promise<Query | undefined> {
    const [query] = await db.select().from(queries).where(eq(queries.id, id));
    return query || undefined;
  }

  async getQueryByQueryId(queryId: string): Promise<Query | undefined> {
    const [query] = await db.select().from(queries).where(eq(queries.queryId, queryId));
    return query || undefined;
  }

  async createQuery(query: InsertQuery): Promise<Query> {
    const [newQuery] = await db
      .insert(queries)
      .values(query)
      .returning();
    return newQuery;
  }

  async updateQuery(id: number, updates: Partial<Query>): Promise<Query> {
    const [updatedQuery] = await db
      .update(queries)
      .set(updates)
      .where(eq(queries.id, id))
      .returning();
    return updatedQuery;
  }

  async deleteQuery(id: number): Promise<void> {
    await db.delete(queries).where(eq(queries.id, id));
  }
}

export const storage = new DatabaseStorage();
