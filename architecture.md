# NovaTrack Pipeline Architecture

## Overview

This document provides an in-depth look at the architecture of the NovaTrack Analytics data pipeline, including design decisions, data flow, and system components.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        External Sources                          │
│                    (REST API - Telemetry Data)                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ HTTPS/JSON
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Ingestion Layer                              │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  API Client (Python)                                      │   │
│  │  - Authentication & Authorization                         │   │
│  │  - Retry Logic & Error Handling                          │   │
│  │  - Rate Limiting                                         │   │
│  │  - Data Validation                                       │   │
│  └──────────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ Validated Data
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Storage Layer (Raw)                          │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  PostgreSQL - Raw Schema                                  │   │
│  │  - telemetry_events table                                │   │
│  │  - JSONB payload storage                                 │   │
│  │  - Indexes for performance                              │   │
│  └──────────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ SQL Reads
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Transformation Layer (dbt)                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Staging Models                                           │   │
│  │  - stg_telemetry__raw.sql                               │   │
│  │  - Data cleaning & standardization                      │   │
│  └──────────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Marts Models                                             │   │
│  │  - fct_telemetry_metrics.sql (Fact table)              │   │
│  │  - dim_devices.sql (Dimension table)                   │   │
│  └──────────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ Materialized Tables
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                 Storage Layer (Analytics)                        │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  PostgreSQL - Analytics Schema                            │   │
│  │  - Fact tables                                           │   │
│  │  - Dimension tables                                      │   │
│  │  - Optimized for analytical queries                     │   │
│  └──────────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              Orchestration Layer (Airflow)                       │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  DAG: novatrack_telemetry_pipeline                       │   │
│  │  - Task scheduling & dependency management               │   │
│  │  - Error handling & retries                             │   │
│  │  - Monitoring & alerting                                │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Design Principles

### 1. ELT Over ETL
- **Extract & Load First**: Raw data is loaded into the database as-is
- **Transform in Database**: Transformations happen using SQL (dbt)
- **Benefits**:
  - Preserve raw data for reprocessing
  - Leverage database optimization
  - Separate concerns (ingestion vs transformation)

### 2. Idempotency
- All pipeline operations can be run multiple times safely
- Upsert operations prevent duplicates
- dbt models are deterministic

### 3. Incremental Loading
- Support for full and incremental loads
- Date-based partitioning for efficient queries
- Configurable batch sizes

### 4. Separation of Concerns
- **Ingestion**: API client and database loader
- **Transformation**: dbt models
- **Orchestration**: Airflow DAGs
- **Deployment**: Docker containers

### 5. Observability
- Structured logging (JSON format)
- Comprehensive error handling
- Data quality checks at each stage

## Data Flow

### 1. Extraction Phase
```python
# API Client fetches data
1. Authenticate with API
2. Request telemetry data for date range
3. Handle pagination
4. Validate response schema
5. Enrich with metadata
```

### 2. Loading Phase
```python
# Database Loader stores raw data
1. Connect to PostgreSQL
2. Prepare records (extract to JSON payload)
3. Bulk insert/upsert into telemetry_events
4. Create indexes
5. Commit transaction
```

### 3. Transformation Phase
```sql
-- dbt executes models in order

-- Staging: Clean and standardize
stg_telemetry__raw.sql
  ↓
-- Marts: Business logic
fct_telemetry_metrics.sql (fact)
dim_devices.sql (dimension)
```

## Component Details

### API Client (`src/ingestion/api_client.py`)

**Responsibilities:**
- REST API communication
- Authentication
- Retry logic with exponential backoff
- Response validation
- Error handling

**Key Features:**
```python
- Retry Strategy: 3 attempts with backoff
- Timeout: 30 seconds (configurable)
- Rate Limiting: 0.5s between requests
- Pagination: Automatic page handling
```

### Database Loader (`src/ingestion/loader.py`)

**Responsibilities:**
- Database connection management
- Bulk insert operations
- Upsert logic (conflict handling)
- Transaction management

**Key Features:**
```python
- Batch Size: 1000 records (configurable)
- Connection Pooling: psycopg2
- Conflict Resolution: ON CONFLICT DO UPDATE
- JSONB Storage: Flexible schema
```

### dbt Models

#### Staging Layer
**Purpose:** Clean and standardize raw data

```sql
-- stg_telemetry__raw.sql
- Extract fields from JSON payload
- Convert data types
- Filter invalid records
- Apply business rules
```

#### Marts Layer
**Purpose:** Create analytical datasets

```sql
-- fct_telemetry_metrics.sql (Fact)
- Daily aggregations by device/event
- Metrics: counts, averages, min/max
- Time-based partitioning

-- dim_devices.sql (Dimension)
- Device attributes (SCD Type 1)
- Latest state per device
- Activity metrics
```

### Airflow DAG

**Schedule:** Daily at 2 AM UTC

**Tasks:**
1. `extract_telemetry_data` - Fetch from API
2. `load_to_database` - Insert into PostgreSQL
3. `run_dbt_models` - Execute transformations
4. `run_dbt_tests` - Validate data quality
5. `data_quality_checks` - Additional validations
6. `send_notification` - Alert on completion

**SLA:** 2 hours

## Database Schema

### Raw Layer (`public` schema)

```sql
CREATE TABLE telemetry_events (
    event_id VARCHAR(255) PRIMARY KEY,
    device_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    payload JSONB,                    -- Flexible JSON storage
    source_api VARCHAR(100),
    ingestion_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ
);

-- Indexes
CREATE INDEX idx_telemetry_device_id ON telemetry_events(device_id);
CREATE INDEX idx_telemetry_timestamp ON telemetry_events(timestamp);
CREATE INDEX idx_telemetry_payload ON telemetry_events USING GIN(payload);
```

### Analytics Layer (`analytics` schema)

```sql
-- Fact table: Daily metrics
CREATE TABLE analytics.fct_telemetry_metrics (
    metric_id VARCHAR(255) PRIMARY KEY,  -- Surrogate key
    device_id VARCHAR(100) NOT NULL,
    metric_date DATE NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_count INTEGER,
    unique_sessions INTEGER,
    unique_users INTEGER,
    avg_duration_ms NUMERIC,
    ...
);

-- Dimension table: Devices
CREATE TABLE analytics.dim_devices (
    device_id VARCHAR(100) PRIMARY KEY,
    device_model VARCHAR(100),
    os_version VARCHAR(50),
    country VARCHAR(50),
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    total_events INTEGER,
    ...
);
```

## Scalability Considerations

### Current Capacity
- **Volume:** ~1M events/day
- **Latency:** < 2 hours end-to-end
- **Storage:** PostgreSQL with partitioning

### Scaling Strategies

**Horizontal Scaling:**
- Multiple Airflow workers
- Database read replicas
- API client parallelization

**Vertical Scaling:**
- Larger database instance
- Increased Airflow resources
- More CPU/memory for dbt

**Optimization:**
- Table partitioning by date
- Materialized views for hot queries
- Index optimization
- Connection pooling

## Security

### Data in Transit
- HTTPS for API communication
- TLS for database connections

### Data at Rest
- Database encryption (configurable)
- Secrets management (environment variables)
- No plaintext credentials

### Access Control
- Database role-based access
- Airflow authentication
- API key management

## Monitoring

### Metrics Tracked
- Pipeline execution time
- Record counts at each stage
- Error rates
- Data quality scores

### Alerting
- Pipeline failures
- SLA violations
- Data quality issues
- Resource utilization

### Logging
- Structured JSON logs
- Centralized log aggregation (ready for ELK/Splunk)
- Log retention policy

## Disaster Recovery

### Backup Strategy
- Daily database backups
- Point-in-time recovery capability
- Backup retention: 30 days

### Recovery Procedures
1. Database restore from backup
2. Replay pipeline from last checkpoint
3. Validate data integrity
4. Resume normal operations

### Data Lineage
- Track data transformations
- Audit trail of changes
- Version control for code

## Future Enhancements

### Short Term
- Real-time streaming (Kafka)
- Advanced monitoring (Prometheus/Grafana)
- Data catalog integration

### Long Term
- Multi-region deployment
- Machine learning features
- Data mesh architecture
- Self-service analytics

## References

- [dbt Documentation](https://docs.getdbt.com/)
- [Apache Airflow](https://airflow.apache.org/)
- [PostgreSQL Best Practices](https://www.postgresql.org/docs/)