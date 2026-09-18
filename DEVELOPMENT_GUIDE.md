# Development Guide: Wearable Health IoT Platform

A step-by-step roadmap for building the Wearable Health IoT Telemetry & Monitoring Platform on Microsoft Fabric. This guide breaks the project down into 13 beginner-friendly stages.

---

## Roadmap Overview

- [x] **Stage 1: Project Foundation**
- [x] **Stage 2: Python Simulator**
- [ ] **Stage 3: Local Simulator Testing**
- [ ] **Stage 4: Fabric Workspace and Lakehouse**
- [ ] **Stage 5: Fabric Eventstream**
- [ ] **Stage 6: Eventstream to Bronze**
- [ ] **Stage 7: PySpark Bronze to Silver**
- [ ] **Stage 8: DLQ / Data Quality**
- [ ] **Stage 9: Deduplication and Idempotency**
- [ ] **Stage 10: Silver to Gold**
- [ ] **Stage 11: Fabric Data Pipeline**
- [ ] **Stage 12: Power BI**
- [ ] **Stage 13: GitHub and Final Documentation**

---

### Stage 1: Project Foundation (Current Stage)
- **Goal:** Initialize Git repository, define core documentation, establish data contract, and set up project folder structure.
- **Tasks:**
  - Initialize Git repository and create `.gitignore`.
  - Scaffolding directories: `simulator/`, `config/`, `tests/`, `notebooks/`.
  - Define `README.md`, `ARCHITECTURE.md`, `DATA_CONTRACT.md`, and `DEVELOPMENT_GUIDE.md`.
  - Establish initial dependencies in `requirements.txt` and template `.env.example`.

---

### Stage 2: Python Simulator
- **Goal:** Build the telemetry producer script in Python to simulate wearable devices emitting sensor data.
- **Tasks:**
  - Implement a configurable device model (Device ID, normal heart rate, $SpO_2$, battery decay).
  - Implement an event generator outputting records strictly adhering to `DATA_CONTRACT.md`.
  - Add support for controlled injection of anomalies (corrupted readings, duplicate IDs) for downstream validation testing.
  - Implement HTTP POST transmission capability targeting an Eventstream custom endpoint.

---

### Stage 3: Local Simulator Testing
- **Goal:** Validate the simulator locally without needing cloud resources.
- **Tasks:**
  - Create unit tests with `pytest` in `tests/` verifying JSON output schema, range compliance, and anomaly behavior.
  - Test dry-run mode (output to console or local file).
  - Ensure zero runtime errors during sustained generation runs.

---

### Stage 4: Fabric Workspace and Lakehouse
- **Goal:** Set up Microsoft Fabric cloud infrastructure.
- **Tasks:**
  - Create a dedicated Microsoft Fabric workspace (e.g., `Wearable-Health-IoT-Platform`).
  - Create the primary Lakehouse (e.g., `health_iot_lakehouse`).
  - Familiarize with Fabric OneLake file explorer and Lakehouse SQL analytics endpoints.

---

### Stage 5: Fabric Eventstream
- **Goal:** Provision and configure the real-time event streaming ingress in Fabric.
- **Tasks:**
  - Create a new Eventstream item in the Fabric workspace.
  - Configure a **Custom App** source to obtain the ingestion endpoint URL and connection keys.
  - Add the endpoint to local `.env`.

---

### Stage 6: Eventstream to Bronze
- **Goal:** Connect Eventstream to Lakehouse to land raw data.
- **Tasks:**
  - Add the Lakehouse as an Eventstream destination targeting a `bronze_telemetry_raw` Delta table.
  - Run the Python simulator and verify live records appearing in the Bronze Lakehouse table.
  - Confirm raw payload integrity and schema auto-creation.

---

### Stage 7: PySpark Bronze to Silver
- **Goal:** Develop the first transformation layer to clean, type, and filter valid telemetry.
- **Tasks:**
  - Create a PySpark notebook in Fabric: `01_bronze_to_silver_and_dlq`.
  - Read new micro-batches from `bronze_telemetry_raw`.
  - Parse and type-cast fields (timestamps, integers, floats).
  - Filter clean records matching all data contract rules into `silver_telemetry_cleaned`.

---

### Stage 8: DLQ / Data Quality
- **Goal:** Implement Dead Letter Queue routing for quarantined records.
- **Tasks:**
  - Extract invalid records (out-of-range HR, missing values, corrupted formats).
  - Append metadata column `error_reason` detailing why the event failed.
  - Write invalid records into `dlq_telemetry_rejected` Delta table for inspection.

---

### Stage 9: Deduplication and Idempotency
- **Goal:** Ensure exactly-once semantics in the Silver layer despite network retries.
- **Tasks:**
  - Implement PySpark deduplication logic based on `event_id` and `(device_id, event_timestamp)`.
  - Use Delta Lake `MERGE INTO` (upsert) to ensure reprocessing the same batch produces identical, non-duplicated state in Silver.

---

### Stage 10: Silver to Gold
- **Goal:** Compute aggregated clinical and operational metrics for downstream consumers.
- **Tasks:**
  - Create a PySpark notebook: `02_silver_to_gold_aggregations`.
  - Aggregate metrics:
    - 5-minute and hourly average, min, max heart rate and $SpO_2$ per device.
    - Anomaly counts (e.g., tachycardia occurrences where HR > 120, low SpO2 < 92%).
    - Device health status (battery level, active transmission status).
  - Write aggregates into curated Delta tables: `gold_device_vitals_summary` and `gold_fleet_health`.

---

### Stage 11: Fabric Data Pipeline
- **Goal:** Orchestrate and automate the end-to-end data processing lifecycle.
- **Tasks:**
  - Create a Fabric Data Pipeline: `orchestrate_telemetry_processing`.
  - Add sequential notebook activities: Bronze-to-Silver/DLQ followed by Silver-to-Gold.
  - Configure error handling, failure alerts, and scheduled trigger cadence (e.g., every 15 minutes).

---

### Stage 12: Power BI
- **Goal:** Build real-time and analytical health monitoring dashboards.
- **Tasks:**
  - Connect Power BI to the Gold Lakehouse tables using **Direct Lake** mode.
  - Design key visuals:
    - Patient Vitals Monitoring: Real-time trends, gauge charts, high-risk alert cards.
    - Device Fleet Management: Low-battery warnings, active devices count.
    - Data Quality Monitor: Daily DLQ error rates and common rejection causes.

---

### Stage 13: GitHub and Final Documentation
- **Goal:** Finalize code repository, documentation, and project artifacts for showcase.
- **Tasks:**
  - Export Fabric notebooks and pipeline definitions into the `notebooks/` directory.
  - Add dashboard screenshots and operational instructions to `README.md`.
  - Commit all files to GitHub with clear, conventional commit messages.
