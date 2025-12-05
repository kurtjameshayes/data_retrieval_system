import { ConnectorModel, QueryModel, QueryResultModel } from "./mongodb";

export interface Connector {
  id: string;
  sourceId: string;
  sourceName: string;
  connectorType: string;
  url: string;
  apiKey?: string | null;
  format: string;
  active: boolean;
  description: string | null;
  documentation: string | null;
  notes: string | null;
  maxRetries: number;
  retryDelay: number;
  dataPath?: string | null;
  createdAt: Date;
  updatedAt: Date;
}

export interface Query {
  id: string;
  queryId: string;
  queryName: string;
  connectorId: string;
  description: string | null;
  parameters: Record<string, any>;
  tags: string[];
  notes: any;
  active: boolean;
  createdAt: Date;
  updatedAt: Date;
}

export interface QueryResult {
  id: string;
  queryId: string;
  result: any;
  executedAt: Date;
  status: 'success' | 'error';
  error?: string | null;
}

export interface InsertConnector {
  sourceId: string;
  sourceName: string;
  connectorType: string;
  url: string;
  apiKey?: string | null;
  format?: string;
  description?: string | null;
  documentation?: string | null;
  notes?: string | null;
  maxRetries?: number;
  retryDelay?: number;
  dataPath?: string | null;
}

export interface InsertQuery {
  queryId: string;
  queryName: string;
  connectorId: string;
  description?: string | null;
  parameters?: Record<string, any>;
  tags?: string[];
  notes?: any;
}

export interface InsertQueryResult {
  queryId: string;
  result: any;
  status: 'success' | 'error';
  error?: string | null;
}

export interface IStorage {
  getConnectors(): Promise<Connector[]>;
  getConnector(id: string): Promise<Connector | undefined>;
  getConnectorBySourceId(sourceId: string): Promise<Connector | undefined>;
  createConnector(connector: InsertConnector): Promise<Connector>;
  updateConnector(id: string, updates: Partial<InsertConnector>): Promise<Connector>;
  deleteConnector(id: string): Promise<void>;
  
  getQueries(): Promise<Query[]>;
  getQuery(id: string): Promise<Query | undefined>;
  getQueryByQueryId(queryId: string): Promise<Query | undefined>;
  getQueriesByConnectorId(connectorId: string): Promise<Query[]>;
  createQuery(query: InsertQuery): Promise<Query>;
  updateQuery(id: string, updates: Partial<InsertQuery>): Promise<Query>;
  deleteQuery(id: string): Promise<void>;

  getQueryResults(queryId: string): Promise<QueryResult[]>;
  getLatestQueryResult(queryId: string): Promise<QueryResult | undefined>;
  createQueryResult(result: InsertQueryResult): Promise<QueryResult>;
}

function transformConnector(doc: any): Connector {
  return {
    id: doc._id.toString(),
    sourceId: doc.source_id,
    sourceName: doc.source_name,
    connectorType: doc.connector_type,
    url: doc.url,
    apiKey: doc.api_key,
    format: doc.format || 'JSON',
    active: doc.active !== false,
    description: doc.description || null,
    documentation: doc.documentation || null,
    notes: doc.notes || null,
    maxRetries: doc.max_retries || 3,
    retryDelay: doc.retry_delay || 1,
    dataPath: doc.data_path || null,
    createdAt: doc.created_at || new Date(),
    updatedAt: doc.updated_at || new Date(),
  };
}

function transformQuery(doc: any): Query {
  return {
    id: doc._id.toString(),
    queryId: doc.query_id,
    queryName: doc.query_name,
    connectorId: doc.connector_id,
    description: doc.description || null,
    parameters: doc.parameters || {},
    tags: doc.tags || [],
    notes: doc.notes || null,
    active: doc.active !== false,
    createdAt: doc.created_at || new Date(),
    updatedAt: doc.updated_at || new Date(),
  };
}

function transformQueryResult(doc: any): QueryResult {
  return {
    id: doc._id.toString(),
    queryId: doc.query_id,
    result: doc.result,
    executedAt: doc.executed_at || new Date(),
    status: doc.status,
    error: doc.error || null,
  };
}

export class MongoStorage implements IStorage {
  async getConnectors(): Promise<Connector[]> {
    const docs = await ConnectorModel.find({ active: { $ne: false } }).sort({ created_at: -1 });
    return docs.map(transformConnector);
  }

  async getConnector(id: string): Promise<Connector | undefined> {
    const doc = await ConnectorModel.findById(id);
    return doc ? transformConnector(doc) : undefined;
  }

  async getConnectorBySourceId(sourceId: string): Promise<Connector | undefined> {
    const doc = await ConnectorModel.findOne({ source_id: sourceId });
    return doc ? transformConnector(doc) : undefined;
  }

  async createConnector(connector: InsertConnector): Promise<Connector> {
    const doc = await ConnectorModel.create({
      source_id: connector.sourceId,
      source_name: connector.sourceName,
      connector_type: connector.connectorType,
      url: connector.url,
      api_key: connector.apiKey,
      format: connector.format || 'JSON',
      description: connector.description,
      documentation: connector.documentation,
      notes: connector.notes,
      max_retries: connector.maxRetries || 3,
      retry_delay: connector.retryDelay || 1,
      data_path: connector.dataPath,
      active: true,
      created_at: new Date(),
      updated_at: new Date(),
    });
    return transformConnector(doc);
  }

  async updateConnector(id: string, updates: Partial<InsertConnector>): Promise<Connector> {
    const updateData: any = { updated_at: new Date() };
    if (updates.sourceId !== undefined) updateData.source_id = updates.sourceId;
    if (updates.sourceName !== undefined) updateData.source_name = updates.sourceName;
    if (updates.connectorType !== undefined) updateData.connector_type = updates.connectorType;
    if (updates.url !== undefined) updateData.url = updates.url;
    if (updates.apiKey !== undefined) updateData.api_key = updates.apiKey;
    if (updates.format !== undefined) updateData.format = updates.format;
    if (updates.description !== undefined) updateData.description = updates.description;
    if (updates.documentation !== undefined) updateData.documentation = updates.documentation;
    if (updates.notes !== undefined) updateData.notes = updates.notes;
    if (updates.maxRetries !== undefined) updateData.max_retries = updates.maxRetries;
    if (updates.retryDelay !== undefined) updateData.retry_delay = updates.retryDelay;
    if (updates.dataPath !== undefined) updateData.data_path = updates.dataPath;

    const doc = await ConnectorModel.findByIdAndUpdate(id, updateData, { new: true });
    if (!doc) throw new Error('Connector not found');
    return transformConnector(doc);
  }

  async deleteConnector(id: string): Promise<void> {
    await ConnectorModel.findByIdAndUpdate(id, { active: false, updated_at: new Date() });
  }

  async getQueries(): Promise<Query[]> {
    const docs = await QueryModel.find({ active: { $ne: false } }).sort({ created_at: -1 });
    return docs.map(transformQuery);
  }

  async getQuery(id: string): Promise<Query | undefined> {
    const doc = await QueryModel.findById(id);
    return doc ? transformQuery(doc) : undefined;
  }

  async getQueryByQueryId(queryId: string): Promise<Query | undefined> {
    const doc = await QueryModel.findOne({ query_id: queryId });
    return doc ? transformQuery(doc) : undefined;
  }

  async getQueriesByConnectorId(connectorId: string): Promise<Query[]> {
    const docs = await QueryModel.find({ connector_id: connectorId, active: { $ne: false } });
    return docs.map(transformQuery);
  }

  async createQuery(query: InsertQuery): Promise<Query> {
    const doc = await QueryModel.create({
      query_id: query.queryId,
      query_name: query.queryName,
      connector_id: query.connectorId,
      description: query.description,
      parameters: query.parameters || {},
      tags: query.tags || [],
      notes: query.notes,
      active: true,
      created_at: new Date(),
      updated_at: new Date(),
    });
    return transformQuery(doc);
  }

  async updateQuery(id: string, updates: Partial<InsertQuery>): Promise<Query> {
    const updateData: any = { updated_at: new Date() };
    if (updates.queryId !== undefined) updateData.query_id = updates.queryId;
    if (updates.queryName !== undefined) updateData.query_name = updates.queryName;
    if (updates.connectorId !== undefined) updateData.connector_id = updates.connectorId;
    if (updates.description !== undefined) updateData.description = updates.description;
    if (updates.parameters !== undefined) updateData.parameters = updates.parameters;
    if (updates.tags !== undefined) updateData.tags = updates.tags;
    if (updates.notes !== undefined) updateData.notes = updates.notes;

    const doc = await QueryModel.findByIdAndUpdate(id, updateData, { new: true });
    if (!doc) throw new Error('Query not found');
    return transformQuery(doc);
  }

  async deleteQuery(id: string): Promise<void> {
    await QueryModel.findByIdAndUpdate(id, { active: false, updated_at: new Date() });
  }

  async getQueryResults(queryId: string): Promise<QueryResult[]> {
    const docs = await QueryResultModel.find({ query_id: queryId }).sort({ executed_at: -1 });
    return docs.map(transformQueryResult);
  }

  async getLatestQueryResult(queryId: string): Promise<QueryResult | undefined> {
    const doc = await QueryResultModel.findOne({ query_id: queryId }).sort({ executed_at: -1 });
    return doc ? transformQueryResult(doc) : undefined;
  }

  async createQueryResult(result: InsertQueryResult): Promise<QueryResult> {
    const doc = await QueryResultModel.create({
      query_id: result.queryId,
      result: result.result,
      status: result.status,
      error: result.error,
      executed_at: new Date(),
    });
    return transformQueryResult(doc);
  }
}

export const storage = new MongoStorage();
