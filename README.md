# Happy Family Homestay: End-to-End Data Pipeline & Executive Performance Dashboard

[![Python](https://img.shields.io/badge/Python-387011?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-000000?style=for-the-badge&logo=delta-lake&logoColor=white)](https://delta.io/)
[![SQL](https://img.shields.io/badge/SQL-003B57?style=for-the-badge&logo=sqlite&logoColor=white)](https://en.wikipedia.org/wiki/SQL)
[![Google Sheets API](https://img.shields.io/badge/Google%20Sheets%20API-34A853?style=for-the-badge&logo=googlesheets&logoColor=white)](https://developers.google.com/sheets/api)

An end-to-end data engineering and analytics solution that converts a family-run homestay from manual operations into a measurable, data-driven hospitality business.

## Executive Summary

### Business context

**Happy Family Homestay** previously operated without digital record-keeping. Bookings, guest details, and financial transactions were managed manually through paper notes, phone calls, and WhatsApp messages.

### Business challenge

- No centralized operational data capture for guest check-ins, sources, and room availability.
- Limited visibility into occupancy, average daily rate, and revenue per available room.
- Manual and error-prone monthly tax reporting.
- Poor marketing attribution and weak decision-making based on incomplete guest-source data.

### Objective

Design and deploy a scalable data platform that captures reservations, transforms raw data into clean relational tables, models star-schema analytics, and delivers executive KPI dashboards.

## End-to-End Architecture

The pipeline follows a Medallion Architecture on Databricks with Delta Lake for reliable ingestion, transformation, and analytics.

```text
Google Forms -> Google Sheets API -> Databricks Volume -> Bronze -> Silver -> Gold -> Executive Dashboard
```

| Layer | Purpose | Key Outputs |
| --- | --- | --- |
| Bronze | Raw ingestion and merge logic | Reconciled reservation data |
| Silver | Cleaning, parsing, and standardization | Normalized guest and stay records |
| Gold | Dimensional modeling for analytics | Bookings, calendar, and daily stay facts |
| BI Layer | Executive reporting | KPI dashboards and operational insights |

## Technical Deep Dives

### 1. Automated data ingestion

- A standardized Google Form was used to collect guest check-in data consistently.
- Reservation responses were ingested into a Databricks Volume via PySpark and SQL using `read_files` with schema inference and header evaluation.
- Idempotent merge logic in the Bronze layer prevents duplicate rows during re-runs.

### 2. Silver layer transformation and parsing

- Multi-room package strings such as `1 Kamar, 2 Rumah` were parsed into separate fields like `kamar_count`, `rumah_count`, and `paviliun_count` using `REGEXP_EXTRACT`.
- Deterministic booking IDs were generated using MD5 hashing to create stable identifiers such as `BK-<hash>`.
- Revenue and reporting logic included date normalization, null handling, phone-number standardization, and dynamic pricing tier calculations.

```sql
CONCAT('BK-', SUBSTRING(MD5(CONCAT(timestamp, TRIM(nama_lengkap), nomor_hp)), 1, 8)) AS id
```

### 3. Gold layer star schema modeling

The Gold layer was designed for OLAP-style reporting and fast aggregation.

- `gold.dim_booker`: customer dimension that separates PII from analytics and supports customer lifetime value tracking.
- `gold.dim_calendar`: date spine dimension that preserves zero-occupancy days for accurate reporting.
- `gold.fact_booking`: booking-level fact table storing revenue and acquisition-channel attribution.
- `gold.fact_daily_stay`: daily stay fact table for precise occupancy and room-utilization analysis.

## Key Business Metrics

| Metric | Definition | Strategic Value |
| --- | --- | --- |
| Occupancy Rate | Rooms booked divided by available room capacity across the period | Measures capacity utilization |
| Average Daily Rate (ADR) | Total daily revenue divided by booked rooms | Indicates pricing effectiveness |
| RevPAR | Total daily revenue divided by total available room capacity | Measures overall property efficiency |
| Average Length of Stay (ALOS) | Total stay nights divided by total reservations | Helps plan housekeeping and minimum-stay decisions |
| Channel Attribution | Booking count by acquisition source | Identifies highest-converting channels |
| Tax & Compliance | Monthly revenue and room-night summaries | Speeds up audit and tax reporting |

## Dashboard Showcase

Replace the placeholders below with actual dashboard exports.

| Executive Overview | Channel & Capacity Performance |
| --- | --- |
| Placeholder | Placeholder |

## Project Setup & Execution

### Prerequisites

- Databricks Workspace (Community or Enterprise)
- Unity Catalog or Hive Metastore configured
- Python 3.9+ and PySpark

### Environment setup

1. Clone the repository:

```bash
git clone https://github.com/your-username/happy-family-homestay-analytics.git
cd happy-family-homestay-analytics
```

2. Run the SQL scripts in sequence:

```sql
-- Step 1: Ingest raw CSV into Bronze
RUN SCRIPT ./notebooks/01_bronze_ingestion.sql;

-- Step 2: Clean and parse data into Silver
RUN SCRIPT ./notebooks/02_silver_clean_up.sql;

-- Step 3: Build the Gold star schema
RUN SCRIPT ./notebooks/03_gold_star_schema.sql;
```

## Contact & Links

- Author: Faris Riski
- LinkedIn: [farisriski86](https://www.linkedin.com/in/farisriski86/)
- Email: [farisriski86@gmail.com](mailto:farisriski86@gmail.com)
