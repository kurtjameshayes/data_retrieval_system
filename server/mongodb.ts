import mongoose from 'mongoose';

if (!process.env.MONGODB_URI) {
  throw new Error(
    "MONGODB_URI must be set. Did you forget to add your MongoDB connection string?",
  );
}

// Connect to specific database: data_retrieval_system
mongoose.connect(process.env.MONGODB_URI, {
  dbName: 'data_retrieval_system',
})
  .then(() => console.log('Connected to MongoDB - data_retrieval_system'))
  .catch((err) => console.error('MongoDB connection error:', err));

// Connector Schema - maps to connector_config collection
const connectorSchema = new mongoose.Schema({
  name: { type: String, required: true },
  baseUrl: { type: String, required: true },
  type: { type: String, enum: ['REST', 'GRAPHQL'], required: true },
  description: { type: String, default: null },
  headers: { type: Array, default: [] },
  authType: { type: String, enum: ['None', 'Bearer', 'ApiKey'], required: true },
  authKey: { type: String, default: null },
  createdAt: { type: Date, default: Date.now },
});

// Query Schema - maps to stored_queries collection
const querySchema = new mongoose.Schema({
  connectorId: { type: mongoose.Schema.Types.ObjectId, ref: 'ConnectorConfig', required: true },
  name: { type: String, required: true },
  description: { type: String, default: null },
  notes: { type: String, default: null },
  tags: { type: [String], default: [] },
  queryId: { type: String, required: true, unique: true },
  endpoint: { type: String, required: true },
  method: { type: String, enum: ['GET', 'POST', 'PUT', 'DELETE'], required: true },
  params: { type: Array, default: [] },
  lastRun: { type: Date, default: null },
  status: { type: String, enum: ['idle', 'loading', 'success', 'error'], default: 'idle' },
  createdAt: { type: Date, default: Date.now },
});

// Query Results Schema - maps to query_results collection
const queryResultSchema = new mongoose.Schema({
  queryId: { type: mongoose.Schema.Types.ObjectId, ref: 'StoredQuery', required: true },
  result: { type: mongoose.Schema.Types.Mixed, required: true },
  executedAt: { type: Date, default: Date.now },
  status: { type: String, enum: ['success', 'error'], required: true },
  error: { type: String, default: null },
});

// Use specific collection names
export const ConnectorModel = mongoose.model('ConnectorConfig', connectorSchema, 'connector_config');
export const QueryModel = mongoose.model('StoredQuery', querySchema, 'stored_queries');
export const QueryResultModel = mongoose.model('QueryResult', queryResultSchema, 'query_results');
