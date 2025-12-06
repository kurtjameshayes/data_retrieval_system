# DataNexus - Data Retrieval and Analysis System

## Overview

DataNexus is a comprehensive data retrieval and analysis platform that combines a Python-based query engine with a React TypeScript frontend. The system provides a unified interface for connecting to multiple external data sources (USDA NASS, US Census, FBI Crime Data), executing queries with caching, and performing advanced data analysis. The architecture supports both REST API access and interactive web-based query building with real-time execution and visualization.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture (React + TypeScript)

**Framework & Build Tools**
- React 18 with TypeScript for type-safe component development
- Vite as the build tool and dev server for fast HMR
- Wouter for lightweight client-side routing
- TailwindCSS v4 with custom design tokens for styling

**Design Rationale**: Vite was chosen over Create React App for significantly faster build times and better development experience. Wouter provides routing with minimal bundle size compared to React Router.

**UI Component System**
- shadcn/ui components built on Radix UI primitives
- "New York" style variant with neutral base color
- Custom theme system with CSS variables for consistent theming
- Lucide icons for consistent iconography

**Design Rationale**: shadcn/ui provides accessible, customizable components without runtime dependency overhead. Radix UI primitives ensure ARIA compliance and keyboard navigation.

**State Management**
- Zustand for global application state (connectors, queries)
- TanStack Query (React Query) for server state management with automatic caching
- Local component state with React hooks for UI interactions

**Design Rationale**: Zustand provides lightweight global state without boilerplate. TanStack Query handles server synchronization, caching, and refetching automatically, eliminating manual cache invalidation logic.

**Key Features**
- Visual connector builder for API configuration
- Query builder with parameter management and tag organization
- Real-time query execution with loading states
- Result visualization and analysis dashboard
- Responsive layout with persistent sidebar navigation
- Analysis Plans for configurable multi-query joins and ML-based analysis workflows

### Backend Architecture (Node.js + Express)

**Server Framework**
- Express.js for HTTP server and API routing
- TypeScript for type safety across the stack
- ESBuild for production bundling with selective dependency bundling

**Design Rationale**: Express provides mature middleware ecosystem. ESBuild bundles critical dependencies to reduce cold start times in deployment environments while externalizing less frequently used packages.

**API Layer**
- RESTful endpoints for connector and query CRUD operations
- Subprocess execution model for Python query engine integration
- JSON request/response format with Zod validation

**Design Rationale**: Subprocess model allows the Node.js backend to leverage the existing Python query engine without rewriting business logic. Zod provides runtime type validation for API inputs.

**Python Integration**
- `run_query.py` wrapper script executed via Node.js child_process
- Environment variable injection for database URI
- JSON-based communication between Node and Python processes

**Design Rationale**: This hybrid approach leverages Python's data processing libraries while maintaining a JavaScript frontend. The subprocess model provides process isolation and error containment.

### Python Query Engine Architecture

**Core Components**
- `QueryEngine`: Orchestrates query execution with caching and optimization
- `ConnectorManager`: Manages connector lifecycle and dynamic loading
- `CacheManager`: Handles MongoDB-based result caching with TTL
- `DataAnalysisEngine`: Provides statistical and ML-based analysis capabilities

**Design Rationale**: Separation of concerns with distinct managers for connectivity, caching, and analysis. Each component can be tested and evolved independently.

**Connector System**
- Abstract `BaseConnector` interface for consistent connector implementation
- Plugin-based architecture with dynamic class loading
- Built-in connectors: USDA NASS, US Census, FBI Crime Data, Local File
- Retry logic with exponential backoff for API resilience

**Design Rationale**: Abstract base class ensures all connectors implement required methods (connect, disconnect, query, validate). Dynamic loading enables adding connectors without modifying core code.

**Query Storage & Execution**
- Stored queries in MongoDB with metadata (tags, descriptions, parameters)
- Query result caching with configurable TTL
- Support for query composition and parameter overrides

**Design Rationale**: MongoDB provides flexible schema for varying query structures. Caching reduces API calls and improves response times for repeated queries.

**Analysis Engine**
- Basic statistics (mean, median, correlation, distribution)
- Exploratory analysis (data types, missing values, unique counts)
- Inferential statistics (t-tests, ANOVA, correlation tests)
- Time series analysis (decomposition, trend detection)
- Multivariate analysis (PCA, clustering)
- Predictive modeling (linear regression, random forests)

**Design Rationale**: Pandas for data manipulation, scikit-learn for ML algorithms, scipy for statistical tests. This stack provides comprehensive analysis without custom implementations.

### Data Storage Solutions

**PostgreSQL (Neon Serverless)**
- Primary relational database for application data
- Drizzle ORM for type-safe database operations
- Schema: `connectors` and `queries` tables with JSON columns for flexibility

**Design Rationale**: PostgreSQL provides ACID guarantees and robust querying. Neon's serverless model eliminates connection pool management. Drizzle provides type-safe queries without runtime overhead.

**MongoDB**
- Stores connector configurations (connector_configs collection)
- Caches query results (query_results collection with TTL indexes)
- Stores reusable queries (stored_queries collection)
- Stores analysis plans (analysis_plans collection) for multi-query join and ML workflows

**Design Rationale**: MongoDB's flexible schema suits varying connector configurations and query parameters. TTL indexes automatically expire cached results without manual cleanup.

### Analysis Plans System

**Purpose**: Enable configurable multi-query joins and ML-based analysis workflows

**Key Components**:
- `AnalysisPlan` model (python_src/models/analysis_plan.py): MongoDB model for plan storage
- `execute_analysis_plan.py`: Executes plans, joins queries, runs DataAnalysisEngine.run_suite()
- `manage_analysis_plan.py`: CRUD operations for analysis plans
- `/api/analysis-plans/*` endpoints: REST API with column validation
- AnalysisPlans.tsx: React UI with dynamic column selection

**Column Validation**: Before saving plans, the system:
1. Executes the referenced queries to get actual column names
2. Joins the query outputs to determine available columns after join
3. Validates that all target/feature columns in analysis_config exist
4. Returns detailed errors if validation fails

**Analysis Config Options**:
- `basic_statistics`: Mean, median, correlation matrix
- `exploratory`: Data types, distributions, missing values
- `linear_regression`: Features list and target column
- `random_forest`: Features list, target column, n_estimators
- `multivariate`: PCA with feature list and n_components
- `predictive`: Unified interface for linear or forest models

**Dual Database Strategy**
- PostgreSQL for structured application data (UI-driven queries)
- MongoDB for Python engine data (connector configs, cache, stored queries)

**Design Rationale**: This architecture bridges the Node.js frontend (PostgreSQL) and Python backend (MongoDB). Each technology uses its native database driver for optimal performance.

### Authentication & Session Management

**Current State**: No authentication implemented
**Session Management**: Express session middleware configured with connect-pg-simple

**Design Rationale**: Authentication is prepared but not enforced, allowing rapid development and testing. Session storage in PostgreSQL ensures sessions persist across server restarts.

## External Dependencies

### Data Sources & APIs

**USDA NASS QuickStats API**
- URL: https://quickstats.nass.usda.gov/api
- Purpose: Agricultural statistics (crop production, livestock, prices)
- Authentication: API key required
- Rate Limits: Enforced by USDA

**US Census Bureau API**
- URL: https://api.census.gov/data
- Purpose: Demographic and economic data (ACS, Decennial Census)
- Authentication: API key optional (recommended for higher limits)
- Rate Limits: Higher limits with API key

**FBI Crime Data Explorer API**
- URL: https://api.usa.gov/crime/fbi/sapi
- Purpose: National and state crime statistics
- Authentication: API key required
- Rate Limits: Standard government API limits

### Cloud Services

**Neon Serverless PostgreSQL**
- Connection via @neondatabase/serverless with WebSocket support
- Automatic connection pooling and scaling
- DATABASE_URL environment variable for configuration

**MongoDB Atlas**
- Cloud-hosted MongoDB cluster
- Connection URI with credentials in MONGODB_URI environment variable
- Database: data_retrieval_system

### Third-Party Libraries

**Frontend**
- @tanstack/react-query: Server state management and caching
- recharts: Data visualization and charting
- react-hook-form + zod: Form handling with validation
- @radix-ui/*: Headless UI primitives for accessibility

**Backend**
- drizzle-orm: Type-safe PostgreSQL ORM
- mongoose: MongoDB ODM for Python data models
- express: HTTP server framework
- zod: Runtime schema validation

**Python**
- pandas: Data manipulation and analysis
- scikit-learn: Machine learning algorithms
- scipy: Scientific computing and statistics
- requests: HTTP client for API calls
- pymongo: MongoDB driver
- flask: Python API server (secondary to Node.js)

**Development Tools**
- Vite plugins: Runtime error overlay, cartographer, dev banner
- ESBuild: Production bundling
- TypeScript: Type checking across frontend and backend
- Drizzle Kit: Database migrations

### Environment Configuration

**Required Environment Variables**
- `DATABASE_URL`: PostgreSQL connection string (Neon)
- `MONGODB_URI`: MongoDB connection string (Atlas)
- `NODE_ENV`: Environment mode (development/production)

**Optional Configuration**
- `API_HOST`, `API_PORT`: Server binding configuration
- `CACHE_TTL`: Query result cache duration
- `MAX_RETRIES`: API retry attempts