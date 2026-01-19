# Project Spec: StreamFlow Phase 2 - Enterprise Analytics Pipeline (Simplified)

## 1. Executive Summary

**StreamFlow** is an end-to-end data analytics platform that ingests real-time e-commerce events via **Apache Kafka**, processes them through **PySpark** batch jobs orchestrated by **Apache Airflow**, loads the results into **Snowflake** for transformation through the **Medallion Architecture**, and delivers actionable business insights via **Power BI Desktop** dashboards with DAX measures.

---

## 2. Architecture Overview

### Complete Data Flow (Phase 1 + Phase 2)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              PHASE 1 (RETAINED)                                 │
│  ┌─────────────┐    ┌─────────┐    ┌──────────────┐    ┌───────────────────┐   │
│  │ Producers   │ →  │ Kafka   │ →  │ Batch        │ →  │ Landing Zone      │   │
│  │ (Faker)     │    │ Topics  │    │ Consumer     │    │ (JSON files)      │   │
│  └─────────────┘    └─────────┘    └──────────────┘    └─────────┬─────────┘   │
│                                                                   ↓             │
│                                     ┌───────────────┐    ┌───────────────────┐ │
│                                     │ Gold Zone     │ ←  │ Spark ETL         │ │
│                                     │ (CSV files)   │    │ (DataFrames)      │ │
│                                     └───────┬───────┘    └───────────────────┘ │
│                                             ↓            Orchestrated by       │
│                                       AIRFLOW DAG       Airflow                │
└─────────────────────────────────────────────────────────────────────────────────┘
                                              ↓
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         PHASE 2 (ENTERPRISE ANALYTICS)                          │
│  ┌────────────────────────────────────────────────────────────────────────────┐ │
│  │                         SNOWFLAKE (STREAMFLOW_DW)                          │ │
│  │  ┌──────────────┐    ┌──────────────┐    ┌───────────────────────────────┐ │ │
│  │  │ BRONZE       │ →  │ SILVER       │ →  │ GOLD                          │ │ │
│  │  │ raw_* tables │    │ stg_* tables │    │ dim_*, fact_*, agg_* tables   │ │ │
│  │  │ (from CSV)   │    │ (Task refresh│    │ (star schema, Task refresh)   │ │ │
│  │  └──────────────┘    └──────────────┘    └───────────────────────────────┘ │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
│                                              ↓                                  │
│  ┌────────────────────────────────────────────────────────────────────────────┐ │
│  │                         POWER BI DESKTOP                                   │ │
│  │  DirectQuery → Gold Tables → DAX Measures → 5 Dashboard Pages              │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Functional Modules

### Module A: Snowflake Data Warehouse Layer

**Objective:** Load CSV files from Phase 1 Gold Zone into Snowflake and transform through Medallion layers using native SQL and Tasks.

**Key Deliverables:**

- **Airflow DAG Extension** (`dag_streamflow_warehouse.py`):
  - Task to upload CSV files to Snowflake internal stage
  - Task to execute COPY INTO for Bronze tables
  - Error handling and retry logic

> [!WARNING]
> **Remove Consumer from DAG**: In Phase 2, the Kafka batch consumer runs as a standalone Python script (like the producers), NOT as an Airflow task. Remove any consumer-triggering tasks from your DAG.

- **Medallion Architecture Tables:**

| Layer | Schema | Tables | Refresh Strategy |
|-------|--------|--------|------------------|
| **Bronze** | BRONZE | `raw_user_events`, `raw_transactions`, `raw_products`, `raw_customers` | COPY INTO from stage |
| **Silver** | SILVER | `stg_user_events`, `stg_transactions`, `stg_transaction_items`, `stg_products`, `stg_customers` | Task: `refresh_silver` |
| **Gold** | GOLD | `dim_customer`, `dim_product`, `dim_date`, `fact_transactions`, `fact_user_activity`, `agg_daily_revenue` | Task: `refresh_gold` |

---

### Module B: Dimension Data Files

**Objective:** Provide static dimension data for products and customers that enables consistent joins with event streams.

#### Static Dimension Files

| File | Records | Purpose |
|------|---------|---------|
| `data/products.json` | 2,000 products | Product catalog for `dim_product` |
| `data/customers.json` | 1,000 customers | Customer profiles for `dim_customer` |

> [!NOTE]
> Event producers (`user_events_producer.py`, `transaction_events_producer.py`) load IDs from these files automatically. This ensures all `product_id` and `user_id` values in events match the dimension data for reliable joins.

**Product Fields:**
`product_id`, `product_name`, `description`, `category`, `subcategory`, `brand`, `manufacturer`, `msrp`, `cost_price`, `created_date`, `is_active`

**Customer Fields:**
`user_id`, `email`, `first_name`, `last_name`, `registration_date`, `account_type`, `date_of_birth`, `loyalty_points`, `state`

---

### Module C: Power BI Desktop Analytics

**Objective:** Build interactive dashboards showcasing trainee skills to managers using DirectQuery and DAX.

**Key Deliverables:**

- **Snowflake Connection:**
  - DirectQuery mode from GOLD schema
  - Snowflake ODBC driver configuration
  - Connection parameters documented

- **Data Model:**
  - Star schema relationships in Power BI
  - Proper cardinality (1:many for dims to facts)
  - Date table relationship for time intelligence

- **DAX Measures (8-10 measures):**

| Category | Measures | DAX Pattern |
|----------|----------|-------------|
| **Aggregations** | Total Revenue, Unique Users | `SUMX`, `DISTINCTCOUNT` |
| **Ratios** | Conversion Rate, Avg Order Value | `DIVIDE` |
| **Time Intelligence** | YoY Growth, MTD Revenue | `SAMEPERIODLASTYEAR`, `TOTALMTD` |
| **Context Modifiers** | Running Total, % of Total | `CALCULATE` + `ALL` |
| **Iterators** | Top N Products | `RANKX` |

- **Dashboard Pages:**

| Page | Focus | Key Visuals |
|------|-------|-------------|
| **Executive Summary** | KPIs | Cards, trend sparklines, period comparisons |
| **User Engagement** | Behavior | Events by type, device breakdown, activity trends |
| **Sales Performance** | Revenue | Category revenue bar, payment method pie, daily trend |
| **Product Analytics** | Products | Top products table, category treemap, cart analysis |
| **Funnel Analysis** | Conversion | Funnel (page_view → add_to_cart → purchase) |

---

## 4. Snowflake Schema Reference

```
STREAMFLOW_DW (Database)
├── BRONZE (Schema) - Raw ingested data
│   ├── CSV_STAGE (Internal Stage)
│   ├── CSV_FORMAT (File Format)
│   ├── raw_user_events
│   ├── raw_transactions
│   ├── raw_products
│   └── raw_customers
│
├── SILVER (Schema) - Cleaned, typed, flattened
│   ├── stg_user_events
│   ├── stg_transactions
│   ├── stg_transaction_items (flattened from nested products)
│   ├── stg_products
│   └── stg_customers
│
└── GOLD (Schema) - Analytics-ready star schema
    ├── dim_customer
    ├── dim_product
    ├── dim_date
    ├── fact_transactions
    ├── fact_user_activity
    └── agg_daily_revenue
```

---

## 5. Provided Starter Kit

Assets provided in the `assets/` directory:

| Asset | Description |
|-------|-------------|
| `user_events_producer.py` | Event producer (loads IDs from JSON) |
| `transaction_events_producer.py` | Transaction producer (loads IDs from JSON) |
| `data/products.json` | Static product catalog (2,000 products) |
| `data/customers.json` | Static customer profiles (1,000 customers with region) |

> [!NOTE]
> **No Docker Changes Required**: Phase 2 uses the same `docker-compose.yml` and `Dockerfile.airflow` from Phase 1. All Phase 2 components (Snowflake loading scripts, SQL files, Power BI) run outside Docker.

> [!IMPORTANT]
> **Trainees Build From Scratch:**
>
> - Airflow DAG for Snowflake loading (Week 4 skills)
> - Kafka batch consumer (Week 3-4 skills)
> - All Snowflake SQL: stages, Bronze/Silver/Gold tables, Tasks (Week 5 skills)
> - Power BI dashboard with DAX measures (Week 6 skills)

---

## 6. Trainee Deliverables

What trainees must build and demonstrate:

| Deliverable | Description |
|-------------|-------------|
| Kafka Batch Consumer | Python script consuming from topics to landing zone |
| Snowflake SQL Scripts | Stages, Bronze/Silver/Gold tables, stored procedures |
| Snowflake Task Chain | Automated refresh pipeline |
| Airflow DAG | Orchestrates Snowflake loading |
| Snowflake Loader Script | Python script using snowflake-connector-python |
| Power BI Dashboard | 5 pages with 8-10 DAX measures |
| Demo Presentation | End-to-end walkthrough for managers |

---

## 7. Technical Constraints

| Constraint | Strategy |
|------------|----------|
| **Snowflake Trial** | Reproducible setup scripts, X-SMALL warehouse, 30-day account |
| **Power BI Desktop** | DirectQuery mode, Windows required for Desktop |
| **Data Volume** | 50k+ events via bulk mode |
| **Credentials** | Environment variables, never committed to repo |
| **Phase 1 Dependency** | Requires Phase 1 stack running for CSV output |

## 8. Related Documentation

- [Phase 1 Specification](https://github.com/120925-Data-Engineering/trainer-code/blob/main/Project-Specs/Project-1/Stream_Analytics_Platform.md)
- [Group Assignments](https://github.com/120925-Data-Engineering/trainer-code/blob/main/Project-Specs/Project-2/Groups.md)
