# Weather Data Playground

A comprehensive end-to-end data engineering project demonstrating ETL pipelines, data quality checks, data lake architecture, warehouse modeling, monitoring, and business intelligence dashboards.

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Project Phases](#project-phases)
- [Phase 1: Build ETL + Airflow + Docker](#phase-1-build-etl--airflow--docker)
- [Phase 2: Data Quality Checks](#phase-2-data-quality-checks)
- [Phase 3: Data Lake Architecture](#phase-3-data-lake-architecture)
- [Phase 4: Warehouse Models](#phase-4-warehouse-models)
- [Phase 5: Monitoring & Alerting](#phase-5-monitoring--alerting)
- [Phase 6: BI Dashboard](#phase-6-bi-dashboard)

---

## Project Overview

**Weather Data Playground** is a hands-on learning environment covering essential data engineering concepts:

- **ETL Pipelines** with Apache Airflow
- **Data Quality** validation and checks
- **Data Lake** bronze/silver/gold architecture
- **Data Warehouse** star schema modeling
- **Alerting & Monitoring** for production reliability
- **BI Dashboards** for analytics visualization

---

## Architecture

```
┌─────────────────┐
│  Weather API    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────────┐
│   Airflow ETL   │─────▶│   MinIO (S3)     │
│   (Docker)      │      │   Data Lake      │
└────────┬────────┘      └──────────────────┘
         │                Bronze │ Silver │ Gold
         │                       │        │
         ▼                       ▼        ▼
┌─────────────────┐      ┌──────────────────┐
│   PostgreSQL    │◀─────│  Data Quality    │
│   Warehouse     │      │  Checks          │
└────────┬────────┘      └──────────────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────────┐
│   Metabase      │      │  Email Alerts    │
│   Dashboard     │      │  (SMTP)          │
└─────────────────┘      └──────────────────┘
```

---

## Project Phases

- **[Phase 1]** Build ETL + Airflow + Docker
- **[Phase 2]** Data Quality Checks
- **[Phase 3]** Data Lake Architecture
- **[Phase 4]** Warehouse Models (SQL)
- **[Phase 5]** Monitoring & Alerting
- **[Phase 6]** BI Dashboard (Metabase)

---

## Phase 1: Build ETL + Airflow + Docker

### Docker Setup

#### Essential Docker Commands

```bash
# Check running containers
docker ps

# Execute commands inside PostgreSQL container
docker exec -it weather-etl-postgres-1 psql -U airflow -d airflow

# Restart services
docker compose down
docker compose up -d
```

#### Command Breakdown

- `docker exec` = execute a command inside a running container (not your device)
- `-it` = input and terminal (interactive mode)
- `weather-etl-postgres-1` = container name (check with `docker ps`)
- `psql` = runs PostgreSQL client (like MySQL)
- `-U airflow` = username to log in
- `-d airflow` = database to access

### Folder Structure

```
weather-etl/
├── config/
├── dags/
│   ├── __pycache__/
│   ├── sql/
│   │   └── warehouse/
│   │       ├── 01_create_schema.sql
│   │       ├── 02_dim_date.sql
│   │       ├── 03_fact_weather_hourly.sql
│   │       └── 04_fact_weather_daily.sql
│   ├── utils/
│   ├── warehouse_dag.py
│   └── weather_etl_dag.py
├── logs/
├── plugins/
├── sql/
├── .env
├── .gitignore
├── docker-compose.yaml
├── Dockerfile
├── README.md
└── requirements.txt
```

---

## Phase 2: Data Quality Checks

### Define Data Quality Rules

Critical validation rules to implement:

1. **Row count must be > 0** (ensure data exists)
2. **No NULL values** in important columns (timestamp, temperature, humidity)
3. **Temperature range is realistic** (e.g., -50°C to 60°C)
4. **Humidity must be in valid range** (0% to 100%)

### Implementation Steps

1. Create a Data Quality Task in your DAG
2. Add the DQC task to the DAG workflow
3. Restart Airflow and verify
4. Test with bad data to ensure DQC works

### Testing Data Quality

#### Insert Bad Data (for testing)

```bash
docker exec -it weather-etl-postgres-1 psql -U airflow -d airflow -c \
  "INSERT INTO weather_data (timestamp, temp, humidity) VALUES (NULL, 999, -5);"
```

#### Clean Bad Data

```bash
docker exec -it weather-etl-postgres-1 psql -U airflow -d airflow -c \
  "DELETE FROM weather_data WHERE temperature > 200 OR humidity < -1 OR timestamp IS NULL;"
```

---

## Phase 3: Data Lake Architecture

### MinIO Setup (S3-Compatible Storage)

Add MinIO service to `docker-compose.yaml`:

```yaml
services:
  minio:
    image: minio/minio:latest
    container_name: minio
    ports:
      - "9000:9000"     # S3 API
      - "9001:9001"     # MinIO Console (UI)
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data
    restart: always
```

**Important:** Place MinIO service after `redis:` or before `airflow-webserver:`. Ensure proper indentation — align `minio:` with `postgres:` and `redis:`.

Add MinIO volume at the bottom of `docker-compose.yaml`:

```yaml
volumes:
  postgres-db-volume:
  minio-data:
```

### Data Lake Layers

Access MinIO Console at: **http://localhost:9001/**

Create three buckets following the **Medallion Architecture**:

1. **Bronze Layer** (Raw Data)
   - Raw ingested data
   - No transformations
   - Append-only

2. **Silver Layer** (Cleaned Data)
   - Validated and cleaned
   - Deduplicated
   - Schema enforced

3. **Gold Layer** (Aggregations)
   - Business-ready datasets
   - Aggregated metrics
   - Analytics-optimized

---

## Phase 4: Warehouse Models

### Star Schema Design

We use a **Star Schema** (simple & effective for analytics):

```
       dim_date
           |
           |
    fact_weather_hourly
           |
           |
    fact_weather_daily
```

### Database Objects

#### 1. Schema

```sql
CREATE SCHEMA IF NOT EXISTS warehouse;
```

#### 2. Dimension Table: `dim_date`

Date dimension for time-based analysis.

#### 3. Fact Table: `fact_weather_hourly`

Hourly weather measurements.

#### 4. Fact Table: `fact_weather_daily`

Daily aggregated weather data.

### SQL Files Structure

```
dags/
├── weather_etl_dag.py
├── warehouse_dag.py
└── sql/
    └── warehouse/
        ├── 01_create_schema.sql
        ├── 02_dim_date.sql
        ├── 03_fact_weather_hourly.sql
        └── 04_fact_weather_daily.sql
```

### Incremental Load Strategy

**Important:** Implement incremental loading to:
- Avoid reprocessing all historical data
- Improve performance
- Reduce resource consumption

### Verify SQL Files

```bash
docker exec -it airflow-webserver ls /opt/airflow/sql/warehouse
```

Expected output:
```
01_create_schema.sql
02_dim_date.sql
03_fact_weather_hourly.sql
04_fact_weather_daily.sql
```

---

## Setting Up Airflow Connection

### Option 1: Create Connection via UI (Recommended)

**Step-by-step:**

1. Open Airflow UI: **http://localhost:8080/**
2. Navigate to: **Admin → Connections → + (Add)**
3. Fill in exactly:

| Field       | Value             |
|-------------|-------------------|
| Conn Id     | `postgres_default`|
| Conn Type   | Postgres          |
| Host        | `postgres`        |
| Schema      | `airflow`         |
| Login       | `airflow`         |
| Password    | `airflow`         |
| Port        | `5432`            |

4. Click **Save**

---

## Verify Warehouse Build

### Step A: Open psql Inside Container

```bash
docker exec -it weather-etl-postgres-1 psql -U airflow -d airflow
```

### Step B: List Schemas

```sql
\dn
```

### Step C: List Warehouse Tables

```sql
\dt warehouse.*
```

### Step D: Check Table Structures

```sql
\d warehouse.fact_weather_hourly
```

### Step E: Verify Data Loaded

```sql
SELECT * FROM warehouse.fact_weather_hourly LIMIT 10;
SELECT * FROM warehouse.fact_weather_daily LIMIT 10;
```

---

## Phase 5: Monitoring & Alerting

### 1. DAG Failure Alerts (Must-Have)

#### A. Add Global Failure Callback

Create `utils/alerting.py` with email notification logic.

#### B. Attach It to Your DAG

```python
default_args = {
    'on_failure_callback': send_failure_email,
    # ... other args
}
```

### 2. SLA Monitoring (Silent Killer Detector)

Detects tasks that run too long even if they don't fail.

```python
'sla': timedelta(hours=1)  # Alert if task exceeds 1 hour
```

### 3. Data Quality Checks (Critical)

#### A. Row Count Check

```python
def check_row_count(**context):
    # Query warehouse tables
    # Ensure row count > 0
```

#### B. Freshness Check

```python
def check_data_freshness(**context):
    # Verify yesterday's data exists
    # Alert if missing
```

### 4. Retries + Backoff

```python
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
}
```

---

## Airflow UI Monitoring

### Daily Checklist

Monitor these in Airflow UI:

- **DAG Graph View** - Visual workflow status
- **Task Duration** - Performance trends
- **SLA Misses** - Time violations
- **Retry Count** - Stability indicators

### Red Flags!

Watch for:
- Increasing retries over time
- SLA misses becoming frequent
- Tasks "green but slow"
- Warehouse tables not growing

---

## Email Alerting Setup (Gmail SMTP)

### Step 1: Create Gmail App Password

**Required** if you use Gmail:

1. Go to **Google Account → Security**
2. Enable **2-Step Verification**
3. Go to **App passwords**
4. Create: App: **Mail**, Device: **Other → Airflow**
5. Copy the 16-character password
   - Example: `hgyj gjli jvye dbfm`

### Step 2: Create SMTP Connection in Airflow UI

1. Open: **Admin → Connections → +**
2. Fill exactly:

| Field          | Value                 |
|----------------|-----------------------|
| Connection Id  | `smtp_default`        |
| Connection Type| Email                 |
| Host           | `smtp.gmail.com`      |
| Port           | `587`                 |
| Login          | `yourgmail@gmail.com` |
| Password       | `your_app_password`   |
| Extra          | (leave empty)         |

3. Click **Save**

### Step 3: Configure Airflow to Use SMTP

Add to `docker-compose.yml` under `airflow-webserver` and `airflow-scheduler`:

```yaml
environment:
  AIRFLOW__EMAIL__EMAIL_BACKEND: airflow.utils.email.send_email_smtp
  AIRFLOW__SMTP__SMTP_HOST: smtp.gmail.com
  AIRFLOW__SMTP__SMTP_PORT: 587
  AIRFLOW__SMTP__SMTP_STARTTLS: 'true'
  AIRFLOW__SMTP__SMTP_SSL: 'false'
  AIRFLOW__SMTP__SMTP_MAIL_FROM: yourgmail@gmail.com
```

### Step 4: Restart Services

```bash
docker compose down
docker compose up -d
```

---

## Phase 6: BI Dashboard

### Phase 6.1: Add Metabase (Docker)

Add to `docker-compose.yml`:

```yaml
services:
  metabase:
    image: metabase/metabase:latest
    container_name: metabase
    ports:
      - "3000:3000"
    environment:
      MB_DB_FILE: /metabase-data/metabase.db
    volumes:
      - metabase-data:/metabase-data
    restart: always
```

Add volume:

```yaml
volumes:
  metabase-data:
```

Restart services:

```bash
docker compose down
docker compose up -d
```

### Phase 6.2: Connect Metabase to PostgreSQL

1. Open Metabase: **http://localhost:3000/**
2. Complete initial setup
3. Add database connection:

| Field        | Value       |
|--------------|-------------|
| Database type| PostgreSQL  |
| Host         | `postgres`  |
| Port         | `5432`      |
| Database     | `airflow`   |
| Username     | `airflow`   |
| Password     | `airflow`   |
| Schema       | `warehouse` |

This provides **read-only analytics access**.

### Phase 6.3: Semantic Layer (Very Important)

Configure in Metabase UI:

1. **Mark timestamp as Date/Time**
2. **Set relationships:**
   - `fact_weather_hourly` → `dim_date`
   - `fact_weather_daily` → `dim_date`

3. **Rename columns** for non-technical users:
   - `temperature` → "Temperature (°C)"
   - `humidity` → "Humidity (%)"
   - `timestamp` → "Date/Time"

**Why this matters:** Makes dashboards accessible to business users.

### Phase 6.4: Dashboard Creation

Build these **portfolio-worthy dashboards**:

#### Dashboard 1: Weather Overview
- Daily avg temperature (line chart)
- Daily avg humidity (line chart)
- Grouped by date

#### Dashboard 2: Hourly Trends
- Hourly temperature line chart
- Hourly humidity line chart
- Date range filter

#### Dashboard 3: Correlation Analysis
- Scatter plot: temperature vs humidity
- Identify patterns and relationships

### Phase 6.5: Validation

**Critical:** Cross-check BI numbers vs SQL queries to ensure accuracy.

```sql
-- Verify dashboard data matches warehouse
SELECT 
    DATE(timestamp) as date,
    AVG(temperature) as avg_temp,
    AVG(humidity) as avg_humidity
FROM warehouse.fact_weather_daily
GROUP BY DATE(timestamp)
ORDER BY date DESC
LIMIT 7;
```

Ensure dashboard freshness matches Airflow run schedule.

---

## Example Analytics Queries

### Daily Temperature Trends

```sql
SELECT 
    d.date,
    AVG(f.temperature) as avg_temperature,
    MIN(f.temperature) as min_temperature,
    MAX(f.temperature) as max_temperature
FROM warehouse.fact_weather_daily f
JOIN warehouse.dim_date d ON f.date_id = d.date_id
WHERE d.date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY d.date
ORDER BY d.date;
```

### Hourly Patterns

```sql
SELECT 
    EXTRACT(HOUR FROM timestamp) as hour,
    AVG(temperature) as avg_temp,
    AVG(humidity) as avg_humidity
FROM warehouse.fact_weather_hourly
WHERE timestamp >= CURRENT_DATE - INTERVAL '1 day'
GROUP BY EXTRACT(HOUR FROM timestamp)
ORDER BY hour;
```

### Temperature vs Humidity Correlation

```sql
SELECT 
    temperature,
    humidity,
    COUNT(*) as frequency
FROM warehouse.fact_weather_hourly
GROUP BY temperature, humidity
ORDER BY frequency DESC;
```

---

## Key Takeaways

### What Makes This Project Strong

1. **End-to-End Pipeline**: From data ingestion to BI visualization
2. **Production Practices**: 
   - Data quality checks
   - Monitoring and alerting
   - Incremental loading
   - Star schema design
3. **Modern Stack**: Airflow, Docker, MinIO, PostgreSQL, Metabase
4. **Scalable Architecture**: Bronze/Silver/Gold data lake layers

---

## Troubleshooting

### Common Issues

**Issue:** Airflow can't connect to PostgreSQL  
**Solution:** Create `postgres_default` connection in Airflow UI (Admin → Connections)

**Issue:** SQL files not found  
**Solution:** Move SQL files to `dags/sql/warehouse/` directory

**Issue:** Email alerts not working  
**Solution:** Use Gmail App Password (not regular password) and verify SMTP connection

**Issue:** MinIO buckets not accessible  
**Solution:** Check MinIO is running on port 9001 and credentials are correct

**Issue:** Metabase shows old data  
**Solution:** Verify Airflow DAG runs successfully and warehouse tables are updating