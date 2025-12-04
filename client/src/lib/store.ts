import { create } from 'zustand';
import { nanoid } from 'nanoid';

// Types
export type ConnectorType = 'REST' | 'GRAPHQL';

export interface ConnectorField {
  id: string;
  key: string;
  value: string;
  description?: string;
}

export interface Connector {
  id: string;
  name: string;
  baseUrl: string;
  type: ConnectorType;
  description: string;
  headers: ConnectorField[];
  authType: 'None' | 'Bearer' | 'ApiKey';
  authKey?: string; // Key name for API Key
  icon?: string;
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
  description?: string;
  queryId: string; // User defined ID
  endpoint: string;
  method: 'GET' | 'POST' | 'PUT' | 'DELETE';
  params: QueryParam[];
  lastRun?: string;
  status?: 'idle' | 'loading' | 'success' | 'error';
  result?: any;
}

interface AppState {
  connectors: Connector[];
  queries: Query[];
  
  addConnector: (connector: Omit<Connector, 'id'>) => void;
  deleteConnector: (id: string) => void;
  
  addQuery: (query: Omit<Query, 'id'>) => void;
  updateQuery: (id: string, query: Partial<Query>) => void;
  runQuery: (id: string) => Promise<void>;
}

// Mock Data for Initial State
const INITIAL_CONNECTORS: Connector[] = [
  {
    id: 'conn_1',
    name: 'US Census Bureau',
    baseUrl: 'https://api.census.gov/data',
    type: 'REST',
    description: 'Demographic data from the US Census Bureau',
    headers: [],
    authType: 'None',
  },
  {
    id: 'conn_2',
    name: 'FBI Crime Data',
    baseUrl: 'https://api.usa.gov/crime/fbi/sapi',
    type: 'REST',
    description: 'Uniform Crime Reporting (UCR) Program data',
    headers: [{ id: 'h1', key: 'Accept', value: 'application/json' }],
    authType: 'ApiKey',
    authKey: 'api_key'
  }
];

const INITIAL_QUERIES: Query[] = [
  {
    id: 'q_1',
    connectorId: 'conn_1',
    name: '2020 Population by State',
    description: 'Fetches population data for all states from the 2020 Census PL file.',
    queryId: 'census_pop_2020',
    endpoint: '/2020/dec/pl',
    method: 'GET',
    params: [
      { id: 'p1', key: 'get', value: 'NAME,P1_001N', enabled: true },
      { id: 'p2', key: 'for', value: 'state:*', enabled: true }
    ],
    status: 'success',
    lastRun: new Date().toISOString(),
    result: [
      ["NAME", "P1_001N", "state"],
      ["Alabama", "5024279", "01"],
      ["Alaska", "733391", "02"],
      ["Arizona", "7151502", "04"],
      ["Arkansas", "3011524", "05"],
      ["California", "39538223", "06"]
    ]
  }
];

export const useAppStore = create<AppState>((set, get) => ({
  connectors: INITIAL_CONNECTORS,
  queries: INITIAL_QUERIES,

  addConnector: (connector) => set((state) => ({
    connectors: [...state.connectors, { ...connector, id: nanoid() }]
  })),

  deleteConnector: (id) => set((state) => ({
    connectors: state.connectors.filter((c) => c.id !== id),
    queries: state.queries.filter((q) => q.connectorId !== id) // Cascade delete
  })),

  addQuery: (query) => set((state) => ({
    queries: [...state.queries, { ...query, id: nanoid(), status: 'idle' }]
  })),

  updateQuery: (id, updates) => set((state) => ({
    queries: state.queries.map((q) => q.id === id ? { ...q, ...updates } : q)
  })),

  runQuery: async (id) => {
    const query = get().queries.find(q => q.id === id);
    if (!query) return;

    set(state => ({
      queries: state.queries.map(q => q.id === id ? { ...q, status: 'loading' } : q)
    }));

    // Simulate API call
    await new Promise(resolve => setTimeout(resolve, 1500));

    // Mock results based on query name or randomness
    const mockResult = generateMockData(query.name);

    set(state => ({
      queries: state.queries.map(q => 
        q.id === id ? { 
          ...q, 
          status: 'success', 
          lastRun: new Date().toISOString(),
          result: mockResult 
        } : q
      )
    }));
  }
}));

function generateMockData(context: string) {
  // Simple mock data generator based on context keywords
  if (context.toLowerCase().includes('population') || context.toLowerCase().includes('census')) {
    return [
      ["State", "Population", "Growth"],
      ["California", 39538223, "-0.8%"],
      ["Texas", 29145505, "+1.2%"],
      ["Florida", 21538187, "+1.0%"],
      ["New York", 20201249, "-0.5%"],
      ["Pennsylvania", 13002700, "+0.1%"]
    ];
  }
  
  if (context.toLowerCase().includes('crime')) {
    return {
      year: 2023,
      total_offenses: 12450,
      breakdown: [
        { type: "Violent Crime", count: 450, trend: "down" },
        { type: "Property Crime", count: 12000, trend: "stable" }
      ]
    };
  }

  return {
    timestamp: new Date().toISOString(),
    status: "OK",
    data: Array.from({ length: 5 }, (_, i) => ({
      id: i + 1,
      value: Math.floor(Math.random() * 1000),
      metric: `Metric ${String.fromCharCode(65 + i)}`
    }))
  };
}
