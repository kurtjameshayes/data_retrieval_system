import { ConnectorModel, QueryModel, QueryResultModel } from "./mongodb";

export interface ConnectorField {
  id: string;
  key: string;
  value: string;
}

export interface Connector {
  id: string;
  name: string;
  baseUrl: string;
  type: 'REST' | 'GRAPHQL';
  description: string | null;
  headers: ConnectorField[] | null;
  authType: 'None' | 'Bearer' | 'ApiKey';
  authKey?: string | null;
  createdAt: Date;
}

export interface QueryParam {
  id: string;
  key: string;
  value: string;
  enabled: boolean;
}

export interface Query {
  id: string;
  connectorId: string;
  name: string;
  description?: string | null;
  notes?: string | null;
  tags?: string[] | null;
  queryId: string;
  endpoint: string;
  method: 'GET' | 'POST' | 'PUT' | 'DELETE';
  params: QueryParam[] | null;
  lastRun?: Date | null;
  status?: 'idle' | 'loading' | 'success' | 'error' | null;
  createdAt: Date;
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
  name: string;
  baseUrl: string;
  type: 'REST' | 'GRAPHQL';
  description?: string | null;
  headers?: ConnectorField[] | null;
  authType: 'None' | 'Bearer' | 'ApiKey';
  authKey?: string | null;
}

export interface InsertQuery {
  connectorId: string;
  name: string;
  description?: string | null;
  notes?: string | null;
  tags?: string[] | null;
  queryId: string;
  endpoint: string;
  method: 'GET' | 'POST' | 'PUT' | 'DELETE';
  params?: QueryParam[] | null;
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
  createConnector(connector: InsertConnector): Promise<Connector>;
  deleteConnector(id: string): Promise<void>;
  
  getQueries(): Promise<Query[]>;
  getQuery(id: string): Promise<Query | undefined>;
  getQueryByQueryId(queryId: string): Promise<Query | undefined>;
  createQuery(query: InsertQuery): Promise<Query>;
  updateQuery(id: string, updates: Partial<Query>): Promise<Query>;
  deleteQuery(id: string): Promise<void>;

  getQueryResults(queryId: string): Promise<QueryResult[]>;
  getLatestQueryResult(queryId: string): Promise<QueryResult | undefined>;
  createQueryResult(result: InsertQueryResult): Promise<QueryResult>;
}

function transformConnector(doc: any): Connector {
  return {
    id: doc._id.toString(),
    name: doc.name || doc.connector_name || '',
    baseUrl: doc.baseUrl || doc.base_url || '',
    type: doc.type || doc.api_type || 'REST',
    description: doc.description || null,
    headers: doc.headers || [],
    authType: doc.authType || doc.auth_type || 'None',
    authKey: doc.authKey || doc.auth_key || null,
    createdAt: doc.createdAt || doc.created_at || new Date(),
  };
}

function transformQuery(doc: any): Query {
  return {
    id: doc._id.toString(),
    connectorId: doc.connectorId ? doc.connectorId.toString() : doc.connector_id?.toString() || '',
    name: doc.name || doc.query_name || '',
    description: doc.description || null,
    notes: doc.notes || null,
    tags: doc.tags || [],
    queryId: doc.queryId || doc.query_id || '',
    endpoint: doc.endpoint || '',
    method: doc.method || 'GET',
    params: doc.params || doc.parameters || [],
    lastRun: doc.lastRun || doc.last_run || null,
    status: doc.status || 'idle',
    createdAt: doc.createdAt || doc.created_at || new Date(),
  };
}

function transformQueryResult(doc: any): QueryResult {
  return {
    id: doc._id.toString(),
    queryId: doc.queryId.toString(),
    result: doc.result,
    executedAt: doc.executedAt,
    status: doc.status,
    error: doc.error,
  };
}

export class MongoStorage implements IStorage {
  async getConnectors(): Promise<Connector[]> {
    const docs = await ConnectorModel.find().sort({ createdAt: -1 });
    return docs.map(transformConnector);
  }

  async getConnector(id: string): Promise<Connector | undefined> {
    const doc = await ConnectorModel.findById(id);
    return doc ? transformConnector(doc) : undefined;
  }

  async createConnector(connector: InsertConnector): Promise<Connector> {
    const doc = await ConnectorModel.create(connector);
    return transformConnector(doc);
  }

  async deleteConnector(id: string): Promise<void> {
    await ConnectorModel.findByIdAndDelete(id);
    await QueryModel.deleteMany({ connectorId: id });
  }

  async getQueries(): Promise<Query[]> {
    const docs = await QueryModel.find().sort({ createdAt: -1 });
    return docs.map(transformQuery);
  }

  async getQuery(id: string): Promise<Query | undefined> {
    const doc = await QueryModel.findById(id);
    return doc ? transformQuery(doc) : undefined;
  }

  async getQueryByQueryId(queryId: string): Promise<Query | undefined> {
    const doc = await QueryModel.findOne({ queryId });
    return doc ? transformQuery(doc) : undefined;
  }

  async createQuery(query: InsertQuery): Promise<Query> {
    const doc = await QueryModel.create({
      ...query,
      status: 'idle',
    });
    return transformQuery(doc);
  }

  async updateQuery(id: string, updates: Partial<Query>): Promise<Query> {
    const doc = await QueryModel.findByIdAndUpdate(id, updates, { new: true });
    if (!doc) throw new Error('Query not found');
    return transformQuery(doc);
  }

  async deleteQuery(id: string): Promise<void> {
    await QueryModel.findByIdAndDelete(id);
    await QueryResultModel.deleteMany({ queryId: id });
  }

  async getQueryResults(queryId: string): Promise<QueryResult[]> {
    const docs = await QueryResultModel.find({ queryId }).sort({ executedAt: -1 });
    return docs.map(transformQueryResult);
  }

  async getLatestQueryResult(queryId: string): Promise<QueryResult | undefined> {
    const doc = await QueryResultModel.findOne({ queryId }).sort({ executedAt: -1 });
    return doc ? transformQueryResult(doc) : undefined;
  }

  async createQueryResult(result: InsertQueryResult): Promise<QueryResult> {
    const doc = await QueryResultModel.create(result);
    return transformQueryResult(doc);
  }
}

export const storage = new MongoStorage();
