# Data Contract: Wearable Telemetry Stream

This document defines the schema specification, data types, validation rules, and contract guidelines for all wearable IoT telemetry payloads transmitted to Microsoft Fabric.

---

## 1. Telemetry Schema Specification

| Field Name | Data Type | Nullable | Description | Validation & Constraint Rules |
| :--- | :--- | :--- | :--- | :--- |
| `event_id` | String (UUID v4) | No | Unique identifier for the individual telemetry reading. | Must be a non-empty, valid UUID format string. |
| `device_id` | String | No | Unique device identifier (e.g., `DEV-1001`). | Must match format pattern `^DEV-[0-9]{4,6}$`. |
| `event_timestamp` | String (ISO 8601) | No | UTC timestamp when the measurement was taken on the device. | Must be valid ISO 8601 UTC format (`YYYY-MM-DDTHH:MM:SS.sssZ`). Cannot be more than 1 hour in the future. |
| `heart_rate_bpm` | Integer | No | Measured heart rate in beats per minute. | Normal clinical range: `30 <= heart_rate_bpm <= 240`. Readings outside this range are rejected to DLQ. |
| `spo2` | Float | No | Blood oxygen saturation percentage. | Range: `70.0 <= spo2 <= 100.0`. Readings outside this range are rejected to DLQ. |
| `battery_level` | Integer | No | Remaining wearable battery percentage. | Range: `0 <= battery_level <= 100`. |
| `ingestion_timestamp` | String (ISO 8601) | Yes | UTC timestamp added when payload lands in Bronze Lakehouse. | Null at initial device generation; assigned automatically by ingestion layer. |

---

## 2. Validation & Quality Rules

1. **Mandatory Fields:**
   - Payloads missing `event_id`, `device_id`, `event_timestamp`, `heart_rate_bpm`, `spo2`, or `battery_level` fail validation immediately.
2. **Value Range Checks:**
   - **Heart Rate (`heart_rate_bpm`):** Must be between 30 and 240.
   - **Blood Oxygen (`spo2`):** Must be between 70.0% and 100.0%.
   - **Battery (`battery_level`):** Must be an integer between 0% and 100%.
3. **Deduplication Key:**
   - Composite key: `(device_id, event_timestamp)` and `event_id`.
   - If a duplicate `event_id` is received, downstream deduplication retains the first ingested instance and discards repeats.
4. **Dead Letter Queue (DLQ) Routing:**
   - Any message failing schema parsing, field presence, or range validation is routed to the DLQ table with the specific failure reason recorded.

---

## 3. Valid JSON Example

```json
{
  "event_id": "a4b73e52-19c2-4b2a-9f56-8e2b5e7d1001",
  "device_id": "DEV-1001",
  "event_timestamp": "2026-09-18T09:15:30.125Z",
  "heart_rate_bpm": 76,
  "spo2": 98.5,
  "battery_level": 87,
  "ingestion_timestamp": null
}
```
