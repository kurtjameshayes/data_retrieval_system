import { create } from 'zustand';

// Notification types
export type NotificationType = 'info' | 'success' | 'error' | 'loading';

export interface Notification {
  id: string;
  type: NotificationType;
  title: string;
  message?: string;
  queryId?: string;
  queryName?: string;
  duration?: number;
  createdAt: number;
}

interface NotificationState {
  notifications: Notification[];
  addNotification: (notification: Omit<Notification, 'id' | 'createdAt'>) => string;
  removeNotification: (id: string) => void;
  updateNotification: (id: string, updates: Partial<Notification>) => void;
  clearAll: () => void;
}

export const useNotificationStore = create<NotificationState>((set, get) => ({
  notifications: [],
  
  addNotification: (notification) => {
    const id = `notif-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    const newNotification: Notification = {
      ...notification,
      id,
      createdAt: Date.now(),
    };
    
    set((state) => ({
      notifications: [...state.notifications, newNotification],
    }));
    
    if (notification.duration && notification.duration > 0) {
      setTimeout(() => {
        get().removeNotification(id);
      }, notification.duration);
    }
    
    return id;
  },
  
  removeNotification: (id) => {
    set((state) => ({
      notifications: state.notifications.filter((n) => n.id !== id),
    }));
  },
  
  updateNotification: (id, updates) => {
    set((state) => ({
      notifications: state.notifications.map((n) =>
        n.id === id ? { ...n, ...updates } : n
      ),
    }));
    
    if (updates.duration && updates.duration > 0) {
      setTimeout(() => {
        get().removeNotification(id);
      }, updates.duration);
    }
  },
  
  clearAll: () => set({ notifications: [] }),
}));

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
  createdAt: string;
  updatedAt: string;
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
  createdAt: string;
  updatedAt: string;
}

export interface QueryResult {
  id: string;
  queryId: string;
  result: any;
  executedAt: string;
  status: 'success' | 'error';
  error?: string | null;
}

// API client functions
export const api = {
  // Connectors
  async getConnectors(): Promise<Connector[]> {
    const res = await fetch('/api/connectors');
    if (!res.ok) throw new Error('Failed to fetch connectors');
    return res.json();
  },

  async getConnector(id: string): Promise<Connector> {
    const res = await fetch(`/api/connectors/${id}`);
    if (!res.ok) throw new Error('Failed to fetch connector');
    return res.json();
  },

  async createConnector(connector: Omit<Connector, 'id' | 'createdAt' | 'updatedAt' | 'active'>): Promise<Connector> {
    const res = await fetch('/api/connectors', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(connector),
    });
    if (!res.ok) throw new Error('Failed to create connector');
    return res.json();
  },

  async updateConnector(id: string, updates: Partial<Connector>): Promise<Connector> {
    const res = await fetch(`/api/connectors/${id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(updates),
    });
    if (!res.ok) throw new Error('Failed to update connector');
    return res.json();
  },

  async deleteConnector(id: string): Promise<void> {
    const res = await fetch(`/api/connectors/${id}`, { method: 'DELETE' });
    if (!res.ok) throw new Error('Failed to delete connector');
  },

  // Queries
  async getQueries(): Promise<Query[]> {
    const res = await fetch('/api/queries');
    if (!res.ok) throw new Error('Failed to fetch queries');
    return res.json();
  },

  async getQuery(id: string): Promise<Query> {
    const res = await fetch(`/api/queries/${id}`);
    if (!res.ok) throw new Error('Failed to fetch query');
    return res.json();
  },

  async getQueriesByConnector(connectorId: string): Promise<Query[]> {
    const res = await fetch(`/api/queries/connector/${connectorId}`);
    if (!res.ok) throw new Error('Failed to fetch queries');
    return res.json();
  },

  async createQuery(query: Omit<Query, 'id' | 'createdAt' | 'updatedAt' | 'active'>): Promise<Query> {
    const res = await fetch('/api/queries', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(query),
    });
    if (!res.ok) throw new Error('Failed to create query');
    return res.json();
  },

  async updateQuery(id: string, updates: Partial<Query>): Promise<Query> {
    const res = await fetch(`/api/queries/${id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(updates),
    });
    if (!res.ok) throw new Error('Failed to update query');
    return res.json();
  },

  async deleteQuery(id: string): Promise<void> {
    const res = await fetch(`/api/queries/${id}`, { method: 'DELETE' });
    if (!res.ok) throw new Error('Failed to delete query');
  },

  async runQuery(id: string): Promise<{ query: Query; result: QueryResult }> {
    const res = await fetch(`/api/queries/${id}/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
    });
    if (!res.ok) throw new Error('Failed to run query');
    return res.json();
  },

  async getQueryResults(id: string): Promise<QueryResult[]> {
    const res = await fetch(`/api/queries/${id}/results`);
    if (!res.ok) throw new Error('Failed to fetch query results');
    return res.json();
  },

  async getLatestQueryResult(id: string): Promise<QueryResult> {
    const res = await fetch(`/api/queries/${id}/results/latest`);
    if (!res.ok) throw new Error('Failed to fetch latest query result');
    return res.json();
  },
};

// Zustand store for local state management
interface AppState {
  connectors: Connector[];
  queries: Query[];
  
  setConnectors: (connectors: Connector[]) => void;
  setQueries: (queries: Query[]) => void;
  addConnector: (connector: Connector) => void;
  addQuery: (query: Query) => void;
  removeConnector: (id: string) => void;
  removeQuery: (id: string) => void;
}

export const useAppStore = create<AppState>((set) => ({
  connectors: [],
  queries: [],
  
  setConnectors: (connectors) => set({ connectors }),
  setQueries: (queries) => set({ queries }),
  addConnector: (connector) => set((state) => ({ 
    connectors: [connector, ...state.connectors] 
  })),
  addQuery: (query) => set((state) => ({ 
    queries: [query, ...state.queries] 
  })),
  removeConnector: (id) => set((state) => ({ 
    connectors: state.connectors.filter(c => c.id !== id) 
  })),
  removeQuery: (id) => set((state) => ({ 
    queries: state.queries.filter(q => q.id !== id) 
  })),
}));
