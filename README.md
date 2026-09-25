# Wearable Health IoT Telemetry & Monitoring Platform

A hands-on Data Engineering project built on **Microsoft Fabric** to ingest, process, validate, and visualize streaming health telemetry from wearable IoT devices.

---

## 1. Business Problem

Modern healthcare, clinical trials, and remote patient monitoring depend on real-time biometric telemetry gathered from smart wearables. Critical vitals such as heart rate and blood oxygen saturation ($SpO_2$) require prompt tracking to identify emergencies (e.g., severe tachycardia, bradycardia, or hypoxia).

However, real-world IoT environments introduce frequent data engineering challenges:
- **Flawed & Corrupt Data:** Sensor degradation, physical disconnections, or software glitches emit anomalous or out-of-range readings (e.g., negative heart rates, null timestamps).
- **Network Glitches & Duplicates:** Intermittent connectivity causes duplicate packet transmissions and out-of-order deliveries.
- **Data Contamination:** Ingesting unvalidated telemetry directly into reporting layers skews analytics and endangers operational decisions.

Organizations need an automated, scalable data platform capable of continuously ingesting high-volume device telemetry, isolating corrupt events into a quarantine holding area (Dead Letter Queue), deduplicating records, and serving clean, aggregated metrics for medical and operational dashboards.

---

## 2. Project Objective

The primary objective of this project is to build an end-to-end, production-grade cloud data solution using **Microsoft Fabric** and the **Medallion Architecture**:
1. **Simulate Real-Time Telemetry:** Stream synthetic wearable sensor data (heart rate, $SpO_2$, battery levels) using an extensible Python simulator.
2. **Real-Time Ingestion:** Ingest streaming events directly into **Microsoft Fabric Eventstream** via custom application endpoints.
3. **Raw Data Preservation (Bronze):** Persist raw streaming payloads into a Fabric Lakehouse Bronze layer in Delta format.
4. **Data Quality & Isolation (Silver & DLQ):** Use PySpark notebooks to enforce strict data contracts. Validated records are cleansed and deduplicated into the Silver table, while invalid/corrupt records are routed to a Dead Letter Queue (DLQ).
5. **Analytical Aggregations (Gold):** Compute hourly/daily patient biometric summaries and device health metrics into the Gold layer.
6. **Orchestration:** Automate and schedule batch transformation runs with Fabric Data Pipelines.
7. **Business Intelligence:** Present near-real-time executive and clinical metrics through interactive Power BI dashboards.

---

## 3. Architecture Overview

```
                      +-----------------------------------+
                      | Python Wearable Telemetry         |
                      | Simulator                         |
                      +-----------------+-----------------+
                                        |
                                        | (HTTPS / Custom Endpoint)
                                        v
                      +-----------------+-----------------+
                      | Microsoft Fabric Eventstream      |
                      +-----------------+-----------------+
                                        |
                                        | (Streaming Ingestion)
                                        v
                      +-----------------+-----------------+
                      | Fabric Lakehouse - Bronze         |
                      | (Raw Delta Lake Storage)          |
                      +-----------------+-----------------+
                                        |
                                        | (PySpark Processing & Validation)
                                        v
                       +----------------+---------------+
                       |   Data Quality & Validation    |
                       +----------------+---------------+
                                       / \
                                      /   \
                           [Valid]   /     \   [Invalid]
                                    v       v
+------------------------------------+     +----------------------------------+
| Fabric Lakehouse - Silver          |     | Fabric Lakehouse - DLQ           |
| (Cleaned, Deduplicated Delta Table)|     | (Quarantined Records + Error Log)|
+------------------+-----------------+     +----------------------------------+
                   |
                   | (PySpark Aggregations)
                   v
+------------------+-----------------+
| Fabric Lakehouse - Gold            |
| (Aggregated Biometrics & KPIs)     |
+------------------+-----------------+
                   |
                   | (Direct Lake / Import)
                   v
+------------------+-----------------+
| Power BI Dashboard                 |
| (Monitoring & Vitals Reporting)    |
+------------------------------------+
```

*Orchestration across Bronze -> Silver/DLQ -> Gold is managed by **Fabric Data Pipeline**.*

---

## 4. Technology Stack & Component Roles

| Technology | Layer / Role | Description |
| :--- | :--- | :--- |
| **Python 3.10+** | Producer / Simulator | Generates realistic, synthetic wearable telemetry streams, simulating normal sensor activity, network jitter, and occasional anomalies. |
| **Microsoft Fabric Eventstream** | Ingestion & Streaming Broker | Ingests real-time events from the Python simulator via a custom application endpoint and streams raw events into OneLake. |
| **Microsoft Fabric Lakehouse (Bronze)** | Raw Storage | Serves as the landing zone (Bronze Delta table), preserving raw payloads with ingestion timestamps for full auditability. |
| **PySpark (Fabric Notebooks)** | Compute & Transformation | Executes distributed data processing, data contract validation, duplicate handling, schema casting, and analytical aggregations. |
| **Fabric Lakehouse (Silver)** | Cleaned Storage | Stores cleansed, typed, and deduplicated telemetry records conforming to strict validation rules. |
| **Fabric Lakehouse (DLQ)** | Error Quarantine | Holds invalid or malformed events along with rejection error reasons for debugging and data reconciliation. |
| **Fabric Lakehouse (Gold)** | Analytical Storage | Houses business-ready, pre-aggregated dimensional tables (e.g., hourly patient heart rate stats, low battery alerts). |
| **Fabric Data Pipeline** | Orchestration | Coordinates the scheduled execution and dependency management of Lakehouse notebooks. |
| **Power BI** | Visualization | Delivers interactive dashboards tracking patient vitals, telemetry volume, and device fleet health via Direct Lake mode. |
| **Git / GitHub** | Version Control | Provides source control, version history, and CI/CD collaboration for all code, queries, and configuration files. |

---

## 5. Repository Structure

```
wearable-iot-platform/
│
├── simulator/             # Python wearable telemetry simulation scripts
├── config/                # Environment and simulation configuration
├── tests/                 # Unit and data validation tests
├── notebooks/             # PySpark notebooks for Fabric Lakehouse
├── .env.example           # Template for environment configuration
├── .gitignore             # Git ignore configuration
├── requirements.txt       # Python dependencies for simulation and tests
├── README.md              # Project overview and architecture summary
├── ARCHITECTURE.md        # Detailed architectural design and data flow
├── DATA_CONTRACT.md       # Telemetry schema specification and constraints
└── DEVELOPMENT_GUIDE.md   # Step-by-step 13-stage project roadmap
```

---

## 6. Running the Simulator

### Setup Environment
```bash
python -m venv .venv
# On Windows
.venv\Scripts\activate
# On Linux/macOS
source .venv/bin/activate

pip install -r requirements.txt
```

### Configuration
1. Copy `.env.example` to `.env`
```bash
cp .env.example .env
```
2. For Fabric Mode, update `.env` with your Microsoft Fabric Eventstream details:
```env
FABRIC_EVENTHUB_CONNECTION_STRING=Endpoint=sb://...
FABRIC_EVENTHUB_NAME=your_eventhub_name
```
> [!WARNING]
> Never commit your `.env` file to version control. It contains sensitive credentials. Ensure it remains in `.gitignore`.

### Run in Local Mode
Local mode generates events and appends them as JSON Lines to `data/telemetry.jsonl`.
```bash
python -m simulator.producer --mode local --duration 30 --devices 10 --eps 1
```

### Run in Fabric Mode
Fabric mode sends events directly to Microsoft Fabric Eventstream using the Azure Event Hubs SDK.
```bash
python -m simulator.producer --mode fabric --duration 30 --devices 10 --eps 1
```

### Example Generated Event
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
