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
  dataPath?: string | null;
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

export interface QueryConfig {
  query_id: string;
  alias?: string;
  join_column?: string;
}

export interface AnalysisConfig {
  basic_statistics?: boolean;
  exploratory?: boolean;
  inferential_tests?: Array<{ x: string; y: string; test: string }>;
  time_series?: { time_column: string; target_column: string; freq?: string };
  linear_regression?: { features: string[]; target: string; test_size?: number };
  random_forest?: { features: string[]; target: string; n_estimators?: number; max_depth?: number };
  multivariate?: { features: string[]; n_components?: number };
  predictive?: { features: string[]; target: string; model_type: 'linear' | 'forest' };
}

export interface AnalysisPlan {
  plan_id: string;
  plan_name: string;
  description?: string;
  queries: QueryConfig[];
  analysis_plan: AnalysisConfig;
  tags?: string[];
  active?: boolean;
  created_at?: string;
  updated_at?: string;
  last_run_at?: string | null;
  last_run_status?: string | null;
  last_run_error?: string | null;
}

export interface AnalysisPlanResult {
  success: boolean;
  plan_id: string;
  plan_name: string;
  record_count: number;
  columns: string[];
  analysis: Record<string, any>;
  data_sample: any[];
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

  async testConnector(id: string): Promise<{ success: boolean; status?: number; statusText?: string; responseTime: number; url: string; message?: string; error?: string }> {
    const res = await fetch(`/api/connectors/${id}/test`, { method: 'POST' });
    if (!res.ok) throw new Error('Failed to test connector');
    return res.json();
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

  async runQuery(id: string, parameterOverrides?: Record<string, any>, options?: { saveToResults?: boolean }): Promise<{ query: Query; result: QueryResult }> {
    const body: any = { saveToResults: options?.saveToResults !== false };
    if (parameterOverrides && Object.keys(parameterOverrides).length > 0) {
      body.parameterOverrides = parameterOverrides;
    }
    const res = await fetch(`/api/queries/${id}/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
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

  async getQueryColumns(queryId: string): Promise<{ queryId: string; columns: string[] }> {
    const res = await fetch(`/api/queries/${queryId}/columns`);
    if (!res.ok) throw new Error('Failed to fetch query columns');
    return res.json();
  },

  // Analysis Plans
  async getAnalysisPlans(): Promise<AnalysisPlan[]> {
    const res = await fetch('/api/analysis-plans');
    if (!res.ok) throw new Error('Failed to fetch analysis plans');
    return res.json();
  },

  async getAnalysisPlan(planId: string): Promise<AnalysisPlan> {
    const res = await fetch(`/api/analysis-plans/${planId}`);
    if (!res.ok) throw new Error('Failed to fetch analysis plan');
    return res.json();
  },

  async createAnalysisPlan(plan: Omit<AnalysisPlan, 'created_at' | 'updated_at' | 'last_run_at' | 'last_run_status'>): Promise<AnalysisPlan> {
    const res = await fetch('/api/analysis-plans', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(plan),
    });
    if (!res.ok) throw new Error('Failed to create analysis plan');
    return res.json();
  },

  async updateAnalysisPlan(planId: string, updates: Partial<AnalysisPlan>): Promise<AnalysisPlan> {
    const res = await fetch(`/api/analysis-plans/${planId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(updates),
    });
    if (!res.ok) throw new Error('Failed to update analysis plan');
    return res.json();
  },

  async deleteAnalysisPlan(planId: string): Promise<void> {
    const res = await fetch(`/api/analysis-plans/${planId}`, { method: 'DELETE' });
    if (!res.ok) throw new Error('Failed to delete analysis plan');
  },

  async getJoinedColumns(queries: QueryConfig[]): Promise<{ columns: string[]; recordCount: number; sample: any[] }> {
    const res = await fetch('/api/analysis-plans/joined-columns', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ queries }),
    });
    if (!res.ok) throw new Error('Failed to fetch joined columns');
    return res.json();
  },

  async validateAnalysisPlan(plan: Partial<AnalysisPlan>): Promise<{ success: boolean; validation: { valid: boolean; errors: string[] }; available_columns: string[] }> {
    const res = await fetch('/api/analysis-plans/validate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(plan),
    });
    if (!res.ok) throw new Error('Failed to validate analysis plan');
    return res.json();
  },

  async executeAnalysisPlan(planId: string): Promise<AnalysisPlanResult> {
    const res = await fetch(`/api/analysis-plans/${planId}/execute`, { method: 'POST' });
    if (!res.ok) throw new Error('Failed to execute analysis plan');
    return res.json();
  },

  async previewAnalysisPlan(planId: string): Promise<{ success: boolean; plan_id: string; columns: string[]; record_count: number; sample: any[] }> {
    const res = await fetch(`/api/analysis-plans/${planId}/preview`, { method: 'POST' });
    if (!res.ok) throw new Error('Failed to preview analysis plan');
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
