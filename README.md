# NovaTrack Analytics - Data Engineering Pipeline

## 📋 Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Pipeline Components](#pipeline-components)
- [CI/CD Pipeline](#cicd-pipeline)
- [Database Schema](#database-schema)
- [Configuration](#configuration)
- [Development](#development)
- [Testing](#testing)
- [Deployment](#deployment)
- [Monitoring & Logging](#monitoring--logging)
- [Future Improvements](#future-improvements)
- [Troubleshooting](#troubleshooting)

## 🎯 Overview

NovaTrack Analytics data pipeline is a production-ready ELT (Extract, Load, Transform) system that:
- Ingests telemetry data from REST APIs
- Loads raw data into PostgreSQL database
- Transforms data using dbt for analytical consumption
- Orchestrates workflows with Apache Airflow
- Implements CI/CD with GitHub Actions
- Containerizes all components with Docker

**Pipeline Type:** ELT (Extract, Load, Transform)  
**Orchestration:** Apache Airflow  
**Transformation:** dbt (data build tool)  
**Storage:** PostgreSQL  
**Deployment:** Docker + Docker Hub

## 🏗️ Architecture

```
┌─────────────────┐
│   REST API      │
│  (Telemetry)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Data Ingestion │
│   (Python)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   PostgreSQL    │
│  (Raw Layer)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│      dbt        │
│ (Transformation)│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   PostgreSQL    │
│ (Analytics Layer)│
└─────────────────┘

Orchestration: Apache Airflow
CI/CD: GitHub Actions → Docker Hub
```

## ✨ Features

### Core Features
- ✅ REST API data ingestion with error handling and retry logic
- ✅ PostgreSQL for reliable data storage
- ✅ dbt for SQL-based transformations
- ✅ Apache Airflow for workflow orchestration
- ✅ Docker containerization for portability
- ✅ GitHub Actions CI/CD pipeline
- ✅ Automated testing (unit, integration)
- ✅ Type checking with mypy
- ✅ Code linting with flake8, black, isort
- ✅ Comprehensive logging and monitoring

### Data Quality
- Schema validation
- Data quality checks in dbt
- Idempotent pipeline execution
- Incremental loading support

## 📦 Prerequisites

- Docker & Docker Compose (v20.10+)
- Python 3.11+
- Git
- GitHub account (for CI/CD)
- Docker Hub account (for image registry)

## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/your-org/novatrack-pipeline.git
cd novatrack-pipeline
```

### 2. Environment Setup
```bash
# Copy environment template
cp .env.example .env

# Update .env with your configurations
# Required: API_URL, DB credentials
```

### 3. Start the Pipeline
```bash
# Build and start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f
```

### 4. Access Airflow UI
```
URL: http://localhost:8080
Username: admin
Password: admin
```

### 5. Trigger Pipeline
```bash
# Via Airflow UI: Enable and trigger DAG "novatrack_telemetry_pipeline"

# Or via CLI:
docker-compose exec airflow-webserver airflow dags trigger novatrack_telemetry_pipeline
```

## 📁 Project Structure

```
novatrack-pipeline/
├── .github/
│   └── workflows/
│       └── ci-cd.yml           # GitHub Actions workflow
├── airflow/
│   ├── dags/
│   │   └── telemetry_pipeline.py  # Airflow DAG definition
│   ├── plugins/                # Custom Airflow plugins
│   └── logs/                   # Airflow logs
├── dbt_project/
│   ├── models/
│   │   ├── staging/            # Staging models
│   │   ├── marts/              # Business logic models
│   │   └── schema.yml          # Model documentation
│   ├── tests/                  # dbt tests
│   ├── macros/                 # Reusable SQL
│   ├── dbt_project.yml         # dbt configuration
│   └── profiles.yml            # Connection profiles
├── src/
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── api_client.py       # REST API client
│   │   ├── loader.py           # Database loader
│   │   └── config.py           # Configuration management
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── logger.py           # Logging utilities
│   │   └── db.py               # Database utilities
│   └── __init__.py
├── tests/
│   ├── unit/                   # Unit tests
│   ├── integration/            # Integration tests
│   └── conftest.py             # Pytest configuration
├── sql/
│   └── init.sql                # Database initialization
├── docker/
│   ├── Dockerfile.ingestion    # Ingestion service
│   ├── Dockerfile.airflow      # Airflow services
│   └── Dockerfile.dbt          # dbt service
├── docker-compose.yml          # Multi-service orchestration
├── requirements.txt            # Python dependencies
├── requirements-dev.txt        # Development dependencies
├── setup.py                    # Package setup
├── pyproject.toml              # Tool configuration
├── .env.example                # Environment template
├── .gitignore
├── .dockerignore
└── README.md
```

## 🔧 Pipeline Components

### 1. Data Ingestion (Extract & Load)

**File:** `src/ingestion/api_client.py`

Fetches telemetry data from REST API endpoints:
- Handles authentication
- Implements retry logic with exponential backoff
- Validates response schemas
- Supports pagination
- Error handling and logging

**File:** `src/ingestion/loader.py`

Loads raw data into PostgreSQL:
- Bulk insert operations
- Upsert logic (handles duplicates)
- Transaction management
- Data validation

### 2. Data Transformation (dbt)

**Location:** `dbt_project/models/`

**Staging Layer (`staging/`):**
- `stg_telemetry__raw.sql` - Clean and standardize raw data
- Basic data type conversions
- Filter invalid records

**Marts Layer (`marts/`):**
- `fct_telemetry_metrics.sql` - Fact table with metrics
- `dim_devices.sql` - Device dimension
- Aggregated views for reporting

**Tests:**
- Uniqueness constraints
- Not null checks
- Referential integrity
- Custom data quality tests

### 3. Orchestration (Apache Airflow)

**File:** `airflow/dags/telemetry_pipeline.py`

DAG configuration:
- **Schedule:** Daily at 2 AM UTC
- **Retries:** 3 with exponential backoff
- **SLA:** 2 hours
- **Dependencies:** Extract → Load → Transform

Tasks:
1. `extract_telemetry_data` - Fetch from API
2. `load_to_database` - Insert into PostgreSQL
3. `run_dbt_models` - Execute transformations
4. `data_quality_checks` - Validation
5. `send_notification` - Alert on completion/failure

## 🔄 CI/CD Pipeline

**File:** `.github/workflows/ci-cd.yml`

### Workflow Stages

#### 1. Code Quality Checks
- **Type Checking:** mypy
- **Linting:** flake8
- **Formatting:** black, isort
- **Complexity:** radon

#### 2. Testing
- Unit tests with pytest
- Integration tests
- Coverage reporting (minimum 80%)

#### 3. dbt Validation
- Model compilation
- Test execution
- Documentation generation

#### 4. Docker Build
- Multi-stage builds for optimization
- Layer caching
- Security scanning with Trivy

#### 5. Deployment
- Push to Docker Hub
- Tag with git commit SHA and version
- Deploy to staging (on main branch)

### Triggers
- **Push to main:** Full pipeline + deployment
- **Pull Request:** Quality checks + tests
- **Tag push (v*):** Production deployment

## 💾 Database Schema

### Raw Layer (`public` schema)

```sql
-- Telemetry events table
CREATE TABLE telemetry_events (
    event_id VARCHAR(255) PRIMARY KEY,
    device_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    payload JSONB,
    source_api VARCHAR(100),
    ingestion_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_telemetry_device_id ON telemetry_events(device_id);
CREATE INDEX idx_telemetry_timestamp ON telemetry_events(timestamp);
CREATE INDEX idx_telemetry_event_type ON telemetry_events(event_type);
CREATE INDEX idx_telemetry_payload ON telemetry_events USING GIN(payload);
```

### Analytics Layer (`analytics` schema)

Created by dbt models:

```sql
-- Fact table: Daily metrics
CREATE TABLE analytics.fct_telemetry_metrics AS
SELECT
    device_id,
    DATE(timestamp) as metric_date,
    event_type,
    COUNT(*) as event_count,
    MIN(timestamp) as first_event_time,
    MAX(timestamp) as last_event_time,
    ...
FROM staging.stg_telemetry__raw
GROUP BY 1, 2, 3;

-- Dimension: Devices
CREATE TABLE analytics.dim_devices AS
SELECT DISTINCT
    device_id,
    FIRST_VALUE(payload->>'device_model') as device_model,
    FIRST_VALUE(payload->>'os_version') as os_version,
    ...
FROM staging.stg_telemetry__raw
GROUP BY device_id;
```

## ⚙️ Configuration

### Environment Variables (.env)

```bash
# Database Configuration
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=novatrack
POSTGRES_USER=novatrack_user
POSTGRES_PASSWORD=secure_password

# API Configuration
API_URL=https://api.example.com/telemetry
API_KEY=your_api_key_here
API_TIMEOUT=30
API_RETRY_ATTEMPTS=3

# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CORE__FERNET_KEY=your_fernet_key_here

# dbt Configuration
DBT_PROFILES_DIR=/usr/app/dbt_project
DBT_TARGET=dev

# Logging
LOG_LEVEL=INFO
```

## 🛠️ Development

### Local Development Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Install pre-commit hooks
pre-commit install

# Run tests
pytest tests/ -v --cov=src

# Run linting
black src/ tests/
isort src/ tests/
flake8 src/ tests/
mypy src/
```

### Code Style

This project follows:
- **PEP 8** style guide
- **Black** for code formatting (line length: 100)
- **isort** for import sorting
- **Type hints** for all functions
- **Docstrings** in Google style

### Pre-commit Hooks

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/psf/black
    hooks:
      - id: black
  - repo: https://github.com/pycqa/isort
    hooks:
      - id: isort
  - repo: https://github.com/pycqa/flake8
    hooks:
      - id: flake8
```

## 🧪 Testing

### Run All Tests
```bash
pytest tests/ -v --cov=src --cov-report=html
```

### Unit Tests
```bash
pytest tests/unit/ -v
```

### Integration Tests
```bash
# Requires running database
docker-compose up -d postgres
pytest tests/integration/ -v
```

### dbt Tests
```bash
cd dbt_project
dbt test
```

## 🚢 Deployment

### Docker Hub Deployment

```bash
# Login to Docker Hub
docker login

# Build images
docker-compose build

# Tag images
docker tag novatrack-ingestion:latest your-org/novatrack-ingestion:v1.0.0
docker tag novatrack-airflow:latest your-org/novatrack-airflow:v1.0.0

# Push to registry
docker push your-org/novatrack-ingestion:v1.0.0
docker push your-org/novatrack-airflow:v1.0.0
```

### Production Deployment

```bash
# Pull latest images
docker pull your-org/novatrack-ingestion:latest
docker pull your-org/novatrack-airflow:latest

# Deploy with production config
docker-compose -f docker-compose.prod.yml up -d

# Health check
curl http://localhost:8080/health
```

## 📊 Monitoring & Logging

### Airflow Monitoring
- **UI:** http://localhost:8080
- **Metrics:** Task duration, success rate, retries
- **Alerts:** Email/Slack on failures

### Application Logging
- **Format:** JSON structured logs
- **Levels:** DEBUG, INFO, WARNING, ERROR, CRITICAL
- **Storage:** Docker volumes, rotated daily
- **Aggregation:** Can integrate with ELK stack

### Database Monitoring
```sql
-- Check recent ingestion
SELECT COUNT(*), MAX(ingestion_timestamp)
FROM telemetry_events
WHERE ingestion_timestamp > NOW() - INTERVAL '1 hour';

-- Data quality metrics
SELECT event_type, COUNT(*), AVG(processing_time)
FROM analytics.fct_telemetry_metrics
WHERE metric_date = CURRENT_DATE
GROUP BY event_type;
```

## 🔮 Future Improvements

### Short-term (1-3 months)
- [ ] Add data lineage tracking
- [ ] Implement change data capture (CDC)
- [ ] Add more comprehensive integration tests
- [ ] Set up Prometheus + Grafana monitoring
- [ ] Implement data versioning with dbt snapshots
- [ ] Add data quality dashboard

### Medium-term (3-6 months)
- [ ] Migrate to cloud infrastructure (AWS/GCP/Azure)
- [ ] Implement real-time streaming with Kafka
- [ ] Add ML feature store integration
- [ ] Implement data catalog (DataHub/Amundsen)
- [ ] Add automated schema evolution
- [ ] Implement cost optimization strategies

### Long-term (6-12 months)
- [ ] Multi-region deployment
- [ ] Advanced data governance with Apache Atlas
- [ ] Automated anomaly detection
- [ ] Self-service analytics platform
- [ ] Data mesh architecture
- [ ] Advanced security (column-level encryption, masking)

## 🐛 Troubleshooting

### Common Issues

#### Pipeline Not Running
```bash
# Check Airflow scheduler
docker-compose logs airflow-scheduler

# Check DAG errors
docker-compose exec airflow-webserver airflow dags list-import-errors

# Restart services
docker-compose restart
```

#### Database Connection Issues
```bash
# Test connection
docker-compose exec postgres psql -U novatrack_user -d novatrack

# Check network
docker network ls
docker network inspect novatrack-pipeline_default
```

#### dbt Model Failures
```bash
# Run specific model
docker-compose exec dbt dbt run --select model_name

# Debug with verbose logging
docker-compose exec dbt dbt run --select model_name --debug

# Check compiled SQL
cat dbt_project/target/compiled/...
```

#### CI/CD Pipeline Failures
- Check GitHub Actions logs
- Verify secrets are set correctly
- Ensure Docker Hub credentials are valid
- Review test coverage requirements

### Getting Help

- **Issues:** GitHub Issues tracker
- **Documentation:** `/docs` folder
- **Team Chat:** Slack #data-engineering
- **Email:** data-engineering@novatrack.com

## 📝 License

Copyright © 2024 NovaTrack Analytics. All rights reserved.

## 👥 Contributors

- Data Engineering Team @ NovaTrack Analytics
- For contributions, see CONTRIBUTING.md

---

**Last Updated:** February 2026  
**Version:** 1.0.0  
**Maintained by:** NovaTrack Data Engineering Team
