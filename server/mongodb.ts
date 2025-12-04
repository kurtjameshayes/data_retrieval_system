import mongoose from 'mongoose';

if (!process.env.MONGODB_URI) {
  throw new Error(
    "MONGODB_URI must be set. Did you forget to add your MongoDB connection string?",
  );
}

mongoose.connect(process.env.MONGODB_URI)
  .then(() => console.log('Connected to MongoDB'))
  .catch((err) => console.error('MongoDB connection error:', err));

// Connector Schema
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

// Query Schema
const querySchema = new mongoose.Schema({
  connectorId: { type: mongoose.Schema.Types.ObjectId, ref: 'Connector', required: true },
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
  result: { type: mongoose.Schema.Types.Mixed, default: null },
  createdAt: { type: Date, default: Date.now },
});

export const ConnectorModel = mongoose.model('Connector', connectorSchema);
export const QueryModel = mongoose.model('Query', querySchema);
