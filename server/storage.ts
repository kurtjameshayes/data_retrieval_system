import { ConnectorModel, QueryModel } from "./mongodb";

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
  result?: any;
  createdAt: Date;
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
}

function transformConnector(doc: any): Connector {
  return {
    id: doc._id.toString(),
    name: doc.name,
    baseUrl: doc.baseUrl,
    type: doc.type,
    description: doc.description,
    headers: doc.headers,
    authType: doc.authType,
    authKey: doc.authKey,
    createdAt: doc.createdAt,
  };
}

function transformQuery(doc: any): Query {
  return {
    id: doc._id.toString(),
    connectorId: doc.connectorId.toString(),
    name: doc.name,
    description: doc.description,
    notes: doc.notes,
    tags: doc.tags,
    queryId: doc.queryId,
    endpoint: doc.endpoint,
    method: doc.method,
    params: doc.params,
    lastRun: doc.lastRun,
    status: doc.status,
    result: doc.result,
    createdAt: doc.createdAt,
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
  }
}

export const storage = new MongoStorage();
