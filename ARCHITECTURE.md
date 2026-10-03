# System Architecture: Real-Time Heart Rate Data Engineering & Analytics Platform

This document describes the architectural design, data flow, and component responsibilities of the Real-Time Heart Rate Data Engineering & Analytics Platform built on Microsoft Fabric.

---

## 1. High-Level Architecture Diagram

```
+-----------------------------+
|  Python Wearable Simulator  |
|  - Synthetic Biometrics     |
|  - Anomaly Generation       |
+--------------+--------------+
               |
               | (HTTPS REST / Custom App Ingestion)
               v
+-----------------------------+
| Microsoft Fabric Eventstream|
| - Low-latency Streaming     |
| - Routing to OneLake        |
+--------------+--------------+
               |
               | (Direct Ingestion / Delta append)
               v
+-----------------------------+
|    Bronze Lakehouse Table   |
|    - Raw telemetry JSON     |
|    - Append-only audit log  |
+--------------+--------------+
               |
               | (Fabric PySpark Notebook: Data Quality & Cleansing)
               v
     +---------+---------+
     | Schema Validation |
     +----+---------+----+
          |         |
 [Valid]  |         | [Invalid]
          v         v
+--------------+   +-----------------------------+
| Silver Table |   | Dead Letter Queue (DLQ)     |
| - Typed      |   | - Corrupt / out-of-range    |
| - Deduplicated   | - Error reason flagged      |
+-------+------+   +-----------------------------+
        |
        | (Fabric PySpark Notebook: Hourly Aggregations)
        v
+--------------+
|  Gold Table  |
|  - Aggregated|
|  - KPIs      |
+-------+------+
        |
        | (Direct Lake Mode)
        v
+--------------+
|   Power BI   |
|   Dashboard  |
+--------------+
```

*Orchestration for transformation notebooks is scheduled and monitored using **Fabric Data Pipeline**.*

---

## 2. Component Details

### 2.1. Python Telemetry Simulator
- **Purpose:** Acts as the IoT device fleet edge source, generating synthetic biometric readings.
- **Characteristics:**
  - Emulates multiple distinct wearable devices (`device_id`).
  - Emits vital statistics: Heart Rate (`heart_rate_bpm`), Blood Oxygen (`spo2`), and Battery Level (`battery_level`).
  - Configurable simulation parameters (tick interval, total devices, rate of anomalies).
  - Generates both healthy telemetry and realistic edge cases (missing fields, extreme out-of-range values, simulated network duplicates) to test downstream resilience.
- **Protocol:** Pushes events directly to the Microsoft Fabric Eventstream endpoint via standard HTTPS POST requests.

---

### 2.2. Microsoft Fabric Eventstream
- **Purpose:** Cloud-native streaming ingestion engine in Microsoft Fabric.
- **Characteristics:**
  - Provides an ingestion endpoint (Custom App source) requiring no self-managed Kafka broker infrastructure.
  - Scales elastically to handle high-throughput streaming events.
  - Seamlessly streams inbound records into OneLake as the destination with zero code.

---

### 2.3. Bronze Lakehouse (Landing & Raw Storage)
- **Purpose:** Append-only landing zone for raw telemetry.
- **Characteristics:**
  - Stores incoming events in Delta format with original payloads intact.
  - Adds an `ingestion_timestamp` to capture exact arrival time.
  - Unmodified source of truth: No records are discarded at this stage, preserving full data auditability and replay capabilities.

---

### 2.4. PySpark Processing Engine
- **Purpose:** Distributed compute framework running within Microsoft Fabric Notebooks.
- **Characteristics:**
  - Reads raw micro-batches or scheduled incremental windows from the Bronze table.
  - Performs schema validation against the defined Data Contract.
  - Handles type casting, timestamp parsing, and duplicate detection.
  - Splits records deterministically into Valid (Silver) and Invalid (DLQ) targets.

---

### 2.5. Silver Lakehouse (Cleaned & Conformed)
- **Purpose:** Reliable, query-ready storage for verified health events.
- **Characteristics:**
  - Enforces strict data types: integers for heart rate, floats for SpO2, timestamps for event times.
  - **Deduplication:** Filters out redundant records caused by network retries using composite keys (`event_id` or `device_id` + `event_timestamp`).
  - Serves as the trusted foundation for analytical aggregations and feature engineering.

---

### 2.6. Dead Letter Queue (DLQ Table)
- **Purpose:** Isolation layer for defective, non-compliant, or corrupted records.
- **Characteristics:**
  - Stores events that fail contract rules (e.g., negative heart rate, $SpO_2 > 100\%$, unparseable timestamps, missing `event_id`).
  - Appends diagnostic metadata: `error_reason`, `validation_failure_timestamp`, and `original_payload`.
  - Prevents bad data from corrupting reports while enabling data engineers to inspect, debug, and remediate errors.

---

### 2.7. Gold Lakehouse (Curated & Aggregated Analytics)
- **Purpose:** High-performance analytical mart optimized for business intelligence.
- **Characteristics:**
  - Pre-calculates business metrics and clinical indicators across defined time windows (e.g., 5-minute rolling averages, hourly summaries).
  - Typical metrics: Average/Min/Max heart rate per device, count of critical low $SpO_2$ incidents, device battery drain trends.
  - Highly compressed and partitioned for rapid query responses.

---

### 2.8. Fabric Data Pipeline (Orchestration)
- **Purpose:** Managed workflow orchestrator for end-to-end batch processing.
- **Characteristics:**
  - Coordinates notebook execution sequence:
    1. Trigger Bronze -> Silver/DLQ validation notebook.
    2. Upon success, trigger Silver -> Gold aggregation notebook.
  - Handles retries, logging, execution alerts, and pipeline scheduling without needing external orchestration tools.

---

### 2.9. Power BI (Visualization & Monitoring)
- **Purpose:** Near-real-time business intelligence and clinical monitoring interface.
- **Characteristics:**
  - Connects directly to the Gold Lakehouse Delta tables using **Direct Lake** mode for blazing query speeds with zero data duplication.
  - Provides dashboards for:
    - Patient Vitals Monitoring: Real-time trends, tachycardia/bradycardia thresholds.
    - IoT Fleet Health: Active device counts, battery degradation alerts, transmission latency.
    - Data Quality Scorecard: Rate of valid vs. DLQ quarantined records over time.
