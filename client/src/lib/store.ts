import { create } from 'zustand';

export type ConnectorType = 'REST' | 'GRAPHQL';

export interface ConnectorField {
  id: string;
  key: string;
  value: string;
  description?: string;
}

export interface Connector {
  id: number;
  name: string;
  baseUrl: string;
  type: ConnectorType;
  description: string | null;
  headers: ConnectorField[] | null;
  authType: 'None' | 'Bearer' | 'ApiKey';
  authKey?: string | null;
  createdAt: string;
}

export interface QueryParam {
  id: string;
  key: string;
  value: string;
  enabled: boolean;
}

export interface Query {
  id: number;
  connectorId: number;
  name: string;
  description?: string | null;
  notes?: string | null;
  tags?: string[] | null;
  queryId: string;
  endpoint: string;
  method: 'GET' | 'POST' | 'PUT' | 'DELETE';
  params: QueryParam[] | null;
  lastRun?: string | null;
  status?: 'idle' | 'loading' | 'success' | 'error' | null;
  result?: any;
  createdAt: string;
}

// API client functions
export const api = {
  // Connectors
  async getConnectors(): Promise<Connector[]> {
    const res = await fetch('/api/connectors');
    if (!res.ok) throw new Error('Failed to fetch connectors');
    return res.json();
  },

  async createConnector(connector: Omit<Connector, 'id' | 'createdAt'>): Promise<Connector> {
    const res = await fetch('/api/connectors', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(connector),
    });
    if (!res.ok) throw new Error('Failed to create connector');
    return res.json();
  },

  async deleteConnector(id: number): Promise<void> {
    const res = await fetch(`/api/connectors/${id}`, { method: 'DELETE' });
    if (!res.ok) throw new Error('Failed to delete connector');
  },

  // Queries
  async getQueries(): Promise<Query[]> {
    const res = await fetch('/api/queries');
    if (!res.ok) throw new Error('Failed to fetch queries');
    return res.json();
  },

  async createQuery(query: Omit<Query, 'id' | 'createdAt' | 'lastRun' | 'status' | 'result'>): Promise<Query> {
    const res = await fetch('/api/queries', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(query),
    });
    if (!res.ok) throw new Error('Failed to create query');
    return res.json();
  },

  async runQuery(id: number): Promise<Query> {
    const res = await fetch(`/api/queries/${id}/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
    });
    if (!res.ok) throw new Error('Failed to run query');
    return res.json();
  },
};

// Zustand store for local state management
interface AppState {
  connectors: Connector[];
  queries: Query[];
  
  setConnectors: (connectors: Connector[]) => void;
  setQueries: (queries: Query[]) => void;
  updateQueryInStore: (id: number, updates: Partial<Query>) => void;
}

export const useAppStore = create<AppState>((set) => ({
  connectors: [],
  queries: [],
  
  setConnectors: (connectors) => set({ connectors }),
  setQueries: (queries) => set({ queries }),
  updateQueryInStore: (id, updates) => set((state) => ({
    queries: state.queries.map(q => q.id === id ? { ...q, ...updates } : q)
  })),
}));
