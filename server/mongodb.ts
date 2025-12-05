import mongoose from 'mongoose';

if (!process.env.MONGODB_URI) {
  throw new Error(
    "MONGODB_URI must be set. Did you forget to add your MongoDB connection string?",
  );
}

mongoose.connect(process.env.MONGODB_URI, {
  dbName: 'data_retrieval_system',
})
  .then(() => console.log('Connected to MongoDB - data_retrieval_system'))
  .catch((err) => console.error('MongoDB connection error:', err));

// Connector Schema - maps to connector_configs collection
const connectorSchema = new mongoose.Schema({
  source_id: { type: String, required: true, unique: true },
  source_name: { type: String, required: true },
  connector_type: { type: String, required: true },
  url: { type: String, required: true },
  api_key: { type: String, default: null },
  format: { type: String, default: 'JSON' },
  active: { type: Boolean, default: true },
  description: { type: String, default: null },
  documentation: { type: String, default: null },
  notes: { type: String, default: null },
  max_retries: { type: Number, default: 3 },
  retry_delay: { type: Number, default: 1 },
  data_path: { type: String, default: null },
  created_at: { type: Date, default: Date.now },
  updated_at: { type: Date, default: Date.now },
});

// Query Schema - maps to stored_queries collection
const querySchema = new mongoose.Schema({
  query_id: { type: String, required: true, unique: true },
  query_name: { type: String, required: true },
  connector_id: { type: String, required: true },
  description: { type: String, default: null },
  parameters: { type: mongoose.Schema.Types.Mixed, default: {} },
  tags: { type: [String], default: [] },
  notes: { type: mongoose.Schema.Types.Mixed, default: null },
  active: { type: Boolean, default: true },
  created_at: { type: Date, default: Date.now },
  updated_at: { type: Date, default: Date.now },
});

// Query Results Schema - maps to query_results collection
const queryResultSchema = new mongoose.Schema({
  query_id: { type: String, required: true },
  result: { type: mongoose.Schema.Types.Mixed, required: true },
  executed_at: { type: Date, default: Date.now },
  status: { type: String, enum: ['success', 'error'], required: true },
  error: { type: String, default: null },
});

// Use specific collection names
export const ConnectorModel = mongoose.model('ConnectorConfig', connectorSchema, 'connector_configs');
export const QueryModel = mongoose.model('StoredQuery', querySchema, 'stored_queries');
export const QueryResultModel = mongoose.model('QueryResult', queryResultSchema, 'query_results');
