# Real-Time Heart Rate Data Engineering & Analytics Platform

*Microsoft Fabric | Eventstream | PySpark | Lakehouse | Data Pipeline | Direct Lake | Power BI*

A hands-on **Data Engineering project built on Microsoft Fabric** to ingest,
validate, transform, and visualize synthetic wearable IoT telemetry.

The project demonstrates an end-to-end data engineering workflow covering
streaming ingestion, Lakehouse architecture, data-quality validation,
Dead-Letter Queue (DLQ) handling, deduplication, analytical transformations,
orchestration, semantic modeling, Power BI, testing, and GitHub version
control.

---

## 1. Business Problem

Wearable IoT systems generate continuous telemetry such as heart rate,
blood oxygen saturation (SpO₂), and battery level.

In real-world IoT environments, incoming telemetry can contain data-quality
problems such as:

- Invalid or out-of-range sensor values
- Missing fields
- Duplicate events
- Late-arriving events
- Network-related delivery variations

If invalid telemetry is allowed to flow directly into analytical datasets,
it can produce misleading results.

This project demonstrates how a data engineering platform can:

- Ingest streaming telemetry
- Preserve the raw incoming data
- Validate and classify data-quality issues
- Route invalid records to a Dead-Letter Queue
- Deduplicate valid records
- Create trusted Silver data
- Produce Gold analytical datasets
- Orchestrate processing
- Provide Power BI monitoring and analytics

The project uses **synthetic wearable telemetry** and is intended as a
data-engineering and telemetry-analytics demonstration rather than a
clinical or medical decision-making system.

---

## 2. Project Objective

The objective is to build an end-to-end Microsoft Fabric data engineering
solution for synthetic wearable telemetry.

The main objectives are:

1. **Simulate Wearable Telemetry**
   Generate synthetic heart rate, SpO₂, battery, timestamp, and device data
   using a Python simulator. The simulator also intentionally generates
   invalid, duplicate, missing, and late events.

2. **Streaming Ingestion**
   Send telemetry from the Python simulator to Microsoft Fabric Eventstream
   through a custom application endpoint.

3. **Raw Data Preservation**
   Store incoming telemetry in a Fabric Lakehouse Bronze table before
   validation and transformation.

4. **Data Quality & Isolation**
   Use PySpark to apply validation rules, classify invalid records, route
   rejected events to a Dead-Letter Queue, and deduplicate valid telemetry
   into the Silver layer.

5. **Analytical Transformation**
   Create Gold datasets for five-minute device-level metrics, daily summaries,
   device-level summaries, event-level telemetry, and data-quality metrics.

6. **Orchestration**
   Use Microsoft Fabric Data Pipeline to orchestrate the PySpark processing
   notebook.

7. **Business Intelligence**
   Expose the Gold datasets through a Direct Lake semantic model and build
   Power BI dashboards for fleet overview, data quality, and device
   monitoring.

---

## 3. Architecture Overview

```text
                    +-----------------------------------+
                    | Python Wearable Telemetry         |
                    | Simulator                         |
                    +-----------------+-----------------+
                                      |
                                      | Custom Application Endpoint
                                      v
                    +-----------------+-----------------+
                    | Microsoft Fabric Eventstream      |
                    +-----------------+-----------------+
                                      |
                                      | Streaming Ingestion
                                      v
                    +-----------------+-----------------+
                    | Fabric Lakehouse - Bronze         |
                    | dbo.bronze_telemetry_raw          |
                    +-----------------+-----------------+
                                      |
                                      | PySpark Validation
                                      v
                         +------------+------------+
                         | Data Quality Processing |
                         +------------+------------+
                              /               \
                         [Valid]             [Invalid]
                            |                   |
                            v                   v
             +-------------------------+   +----------------------+
             | Silver                  |   | DLQ                  |
             | silver_telemetry        |   | telemetry_dlq        |
             +------------+------------+   +----------------------+
                          |
                          | PySpark Aggregation
                          v
             +--------------------------------------+
             | Gold Analytical Layer                 |
             |                                      |
             | gold_device_5min_vitals              |
             | gold_device_daily_summary            |
             | gold_device_summary                  |
             | gold_device_telemetry                |
             | gold_data_quality_summary            |
             +-------------------+------------------+
                                 |
                                 | Fabric Data Pipeline
                                 v
                    +------------+-------------+
                    | Direct Lake Semantic     |
                    | Model                   |
                    | SM_WEARABLE_ANALYTICS    |
                    +------------+-------------+
                                 |
                                 v
                    +------------+-------------+
                    | Power BI Report          |
                    |                          |
                    | Fleet Overview           |
                    | Data Quality             |
                    | Device Monitoring        |
                    +--------------------------+
```

---

## 4. Technology Stack & Component Roles

| Technology | Role | Description |
|---|---|---|
| **Python 3.10+** | Producer / Simulator | Generates synthetic wearable telemetry and controlled data-quality anomalies. |
| **Microsoft Fabric Eventstream** | Streaming Ingestion | Receives telemetry from the Python simulator through a custom application endpoint and routes events to the Lakehouse. |
| **Microsoft Fabric Lakehouse** | Storage | Stores Bronze, Silver, DLQ, and Gold Delta-based analytical tables. |
| **PySpark / Fabric Notebook** | Processing | Performs timestamp conversion, validation, data-quality classification, deduplication, and aggregations. |
| **DLQ** | Error Handling | Preserves invalid records together with rejection reasons for investigation and reconciliation. |
| **Silver Layer** | Trusted Data | Stores validated and deduplicated telemetry. |
| **Gold Layer** | Analytics | Stores analysis-ready device and data-quality datasets. |
| **Fabric Data Pipeline** | Orchestration | Executes the PySpark processing workflow and provides pipeline monitoring. |
| **Direct Lake Semantic Model** | Semantic Layer | Exposes the Gold analytical datasets for Power BI consumption. |
| **Power BI** | Visualization | Provides fleet overview, data-quality analysis, and device-level telemetry trends. |
| **Git / GitHub** | Version Control | Stores source code, tests, documentation, and configuration templates. |

---
## 5. Repository Structure

```text
wearable-iot-platform/
│
├── simulator/
│   ├── __init__.py
│   ├── config.py
│   ├── device_state.py
│   ├── anomaly_generator.py
│   └── producer.py
│
├── tests/
│   └── test_simulator.py
│
├── config/
│   └── .gitkeep
│
├── notebooks/
│   └── .gitkeep
│
├── .env.example
├── .gitignore
├── requirements.txt
│
├── README.md
├── PROJECT_DEEP_DIVE.md
├── ARCHITECTURE.md
├── DATA_CONTRACT.md
└── DEVELOPMENT_GUIDE.md
```

Generated local telemetry such as:

```text
data/telemetry.jsonl
```

and local development environments such as:

```text
.venv/
.env
```

are excluded from version control through `.gitignore`.

The `notebooks/` directory is currently a repository placeholder; the
implemented Fabric notebook is maintained in the Microsoft Fabric workspace.

---

## 6. Running the Simulator

### Setup Environment

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux/macOS
source .venv/bin/activate

pip install -r requirements.txt
```

### Configuration

1. Copy `.env.example` to `.env`.

```bash
cp .env.example .env
```

2. For Fabric mode, configure the local environment with the Fabric
   Eventstream connection details.

```env
FABRIC_EVENTHUB_CONNECTION_STRING=Endpoint=sb://...
FABRIC_EVENTHUB_NAME=your_eventhub_name
```

> [!WARNING]
> Never commit `.env` to version control. The file contains sensitive
> connection information and is excluded through `.gitignore`.

### Run in Local Mode

Local mode generates synthetic events and writes them as JSON Lines:

```text
data/telemetry.jsonl
```

Example:

```bash
python -m simulator.producer --mode local --duration 30 --devices 10 --eps 1
```

### Run in Fabric Mode

Fabric mode sends telemetry directly to the Microsoft Fabric Eventstream
custom application endpoint using the Azure Event Hubs-compatible Python SDK.

```bash
python -m simulator.producer --mode fabric --duration 30 --devices 10 --eps 1
```

---

## 7. Example Generated Event

```json
{
  "event_id": "c33b9340-97f3-4d43-9df2-51c6c59b2a64",
  "device_id": "WATCH-001",
  "event_timestamp": "2026-09-18T15:20:05.123Z",
  "heart_rate_bpm": 72,
  "spo2": 98.5,
  "battery_level": 84,
  "ingestion_timestamp": "2026-09-18T15:20:05.123Z"
}
```

---

## 8. Data Quality Rules

PySpark applies validation rules including:

- Required-field validation
- Heart-rate range validation
- SpO₂ range validation
- Battery-level validation
- Late-event validation

Invalid records are routed to:

```text
telemetry_dlq
```

Valid records are deduplicated using:

```text
device_id
event_id
event_timestamp
```

and written to:

```text
silver_telemetry
```

---

## 9. Gold Analytical Tables

The project creates the following Gold datasets:

| Gold Table | Purpose |
|---|---|
| `gold_device_5min_vitals` | Five-minute device-level vital-sign metrics |
| `gold_device_daily_summary` | Daily device-level telemetry summary |
| `gold_device_summary` | Device-level telemetry and data-quality summary |
| `gold_device_telemetry` | Event-level trusted telemetry for trend analysis |
| `gold_data_quality_summary` | Pipeline-level data-quality metrics |

---

## 10. Power BI Report

The Power BI report contains three pages:

### Fleet Overview

Provides device-level comparisons for:

- Average heart rate
- Average SpO₂
- Minimum battery
- Data-quality metrics

### Data Quality

Provides:

- Total events
- Valid events
- Invalid events
- Invalid rate
- Invalid events by device

### Device Monitoring

Provides selectable device-level trends for:

- Heart rate
- SpO₂
- Battery level

---

## 11. Testing

The Python simulator includes automated tests using `pytest`.

Example:

```bash
python -m pytest
```

The pipeline was also validated using a successful Fabric integration test.

One representative test run processed:

```text
91 total events
87 valid events
4 invalid events
86 Silver records after duplicate handling
4.4% invalid rate
```

The numbers above represent a synthetic test run and can vary between
simulations.

---

## 12. Security

Secrets are kept outside source code using local environment configuration.

The repository includes:

```text
.env.example
```

but does not include the active:

```text
.env
```

The `.env` file is excluded using `.gitignore`.

Production environments would require stronger secret-management approaches
such as managed identity or a dedicated secret-management service.

---

## 13. Project Scope and Limitations

This is a portfolio-scale data engineering project using synthetic telemetry.

It demonstrates:

- Streaming ingestion
- Lakehouse architecture
- Data-quality validation
- DLQ handling
- Deduplication
- PySpark transformations
- Analytical data modeling
- Fabric orchestration
- Direct Lake
- Power BI
- Testing
- Git/GitHub

It does not claim to implement:

- Real patient data
- Clinical decision support
- Production healthcare compliance
- Production-scale performance testing
- Full stateful streaming processing
- Enterprise CI/CD
- Managed identity
- Automated DLQ remediation

---

## 14. Documentation

For the detailed technical explanation, architecture decisions, troubleshooting
history, limitations, and interview preparation, see:

```text
PROJECT_DEEP_DIVE.md
```