# Architecture Decision: Vietnamese Ride-hailing CDC → Lakehouse (Decree 13 Compliant)

**Author:** Tong Van Tien  
**Date:** 2026-08-18  
**Topic:** C. Vietnamese Ride-hailing CDC → Lakehouse (Decree 13 compliant)  
**Status:** Design Review Draft

---

## 1. Problem Statement (≤ 200 words)

Một công ty ride-hailing Việt Nam cần xây dựng Lakehouse để phân tích dữ liệu từ hệ thống Oracle production. Yêu cầu chính:

- **Scale:** 100 triệu chuyến/năm, 30K writes/giây peak
- **CDC Pipeline:** Oracle DB → Debezium CDC → Lakehouse (Delta)
- **Compliance:** Tuân thủ **Nghị định 13/2023/NĐ-CP** về bảo vệ dữ liệu cá nhân (PII: phone, ID, GPS)
- **SLA Analytics:** Dashboard refresh < 60s từ source commit; ad-hoc query p95 < 1s
- **Challenge:** Late-arriving events phổ biến (mất mạng ở vùng sâu); PII phải được tokenize tại Bronze layer

**Tại sao khó:** High-throughput CDC + strict PII governance + near-real-time SLA + late-data handling đòi hỏi thiết kế cẩn thận về ingestion ordering, deduplication, và security audit trail.

---

## 2. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           BRONZE LAYER (Raw CDC)                                │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                         │
│  │  passengers │    │   drivers   │    │   trips     │                         │
│  │  _bronze    │    │  _bronze    │    │  _bronze    │                         │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘                         │
│         │                   │                   │                                │
│    PII Tokenized       PII Tokenized       PII Tokenized                        │
│    (Hash + Salt)       (Hash + Salt)       (Hash + Salt)                        │
└─────────┼───────────────────┼───────────────────┼───────────────────────────────┘
          │                   │                   │
          ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          SILVER LAYER (Curated)                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                         │
│  │passengers   │    │  drivers    │    │   trips     │                         │
│  _silver      │    │  _silver    │    │  _silver    │                         │
│  (SCD Type 2) │    │ (SCD Type 2)│    │ (SCD Type 2)│                         │
│  │  +audit_log│    │  +audit_log │    │ +audit_log  │                         │
│  └─────────────┘    └─────────────┘    └─────────────┘                         │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐        │
│  │                    pii_audit_table (Immutable Log)                   │        │
│  │  requester_id | table_name | pii_field | action | timestamp         │        │
│  └─────────────────────────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           GOLD LAYER (Analytics)                                │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                         │
│  │trip_summary │    │ driver_stats│    │ passenger   │                         │
│  │  _gold      │    │   _gold     │    │ _metrics    │                         │
│  └─────────────┘    └─────────────┘    └─────────────┘                         │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐        │
│  │               lineage_catalog (OpenLineage Events)                  │        │
│  └─────────────────────────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                         INGESTION PATH                                          │
│                                                                                  │
│  Oracle DB ──► Debezium ──► Kafka ──► Spark Structured Streaming ──► Bronze    │
│                 (CDC)          Topics    (Upsert + Tokenize)          Tables    │
│                                                                                  │
│  Late Data: Kafka retention 7 days + MERGE with late-arrival handling           │
│             (WHEN MATCHED AND source.ts > target.ts)                             │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                         QUERY PATH                                               │
│                                                                                  │
│  Dashboard ──► Spark Thrift Server ──► Gold Tables ──► 60s refresh             │
│  Ad-hoc     ──► Databricks SQL     ──► Silver Tables ──► p95 < 1s             │
│                                                                                  │
│  PII Access ──► Token Detokenization Service (authorized only)                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Key Decisions with Rejected Alternatives

### Decision 1: Table Format — **Delta Lake** (chọn) vs Iceberg vs Hudi

**Chọn: Delta Lake**

**Lý do chọn:**
- Native CDC support qua Delta Change Data Feed (CDF) — capture changed rows tự động
- MERGE statement hỗ trợ late-data handling: `WHEN MATCHED AND source.ts > target.ts THEN UPDATE`
- Time travel với `VERSION AS OF` cho rollback và audit
- Active development từ Databricks, enterprise support tốt

**Loại Iceberg vì:**
- Iceberg tốt cho multi-engine (Spark, Flink, Trino), nhưng MERGE semantics phức tạp hơn
- Iceberg's time travel cần thêm configuration cho retention
- Ecosystem tooling cho Iceberg CDC chưa mature bằng Delta

**Loại Hudi vì:**
- Hudi designed cho incremental processing, nhưng COW (Copy-on-Write) có write amplification
- MoR (Merge-on-Read) tạo complex file structure
- CDC semantics không native như Delta

---

### Decision 2: CDC Tool — **Debezium** (chọn) vs Oracle GoldenGate vs JDBC Polling

**Chọn: Debezium + Kafka**

**Lý do chọn:**
- Open-source, no license cost
- Captures CDC events với before/after images — đủ cho SCD Type 2
- Native Kafka connector, scales horizontally
- Supports Oracle CDC qua LogMiner hoặc XStream

**Loại Oracle GoldenGate vì:**
- License cost $50K+/year — không fit cho startup
- Complex setup và management
- vendor lock-in

**Loại JDBC Polling vì:**
- Cannot capture DELETE events
- High latency (polling interval)
- Cannot guarantee ordering guarantees

---

### Decision 3: PII Tokenization — **Hash + Salt at Bronze** (chọn) vs Encryption vs Proxy Hash

**Chọn: Deterministic Hash với per-tenant Salt tại Bronze Layer**

**Lý do chọn:**
- Enables joins across tables (same person = same hash)
- Irreversible — no decryption key leak possible
- Bronze là checkpoint đầu tiên, đảm bảo PII không bao giờ được query raw

**Loại Encryption (AES-256) vì:**
- Encryption reversible nếu key leak
- Không enable cross-table joins (same PII = different ciphertext)
- Compliance risk: key management phức tạp

**Loại Proxy Hash (tokenization service) vì:**
- Single point of failure
- Latency overhead cho every PII access
- Không fit với 30K writes/sec requirement

---

### Decision 4: SCD Type 2 Implementation — **MERGE-based** (chọn) vs History Table vs Slowly Changing Dimension

**Chọn: MERGE-based SCD Type 2 với `EFFECTIVE_FROM` và `EFFECTIVE_TO` columns**

**Lý do chọn:**
- Delta MERGE handles late-arriving data với condition: `WHEN MATCHED AND source.ts > target.ts`
- Single table query, no join needed
- Preserves full history với `EFFECTIVE_TO = '9999-12-31'` cho current record

**Code pattern:**
```sql
MERGE INTO drivers_silver tgt
USING (SELECT * FROM drivers_bronze WHERE _processing_ts = (SELECT MAX(_processing_ts) FROM drivers_bronze)) src
ON tgt.driver_id = src.driver_id AND tgt.is_current = true
WHEN MATCHED AND src._cdc_ts > tgt.effective_from THEN
  UPDATE SET tgt.effective_to = src._cdc_ts, tgt.is_current = false
WHEN NOT MATCHED THEN
  INSERT (driver_id, name_hash, phone_hash, is_current, effective_from, effective_to)
  VALUES (src.driver_id, src.name_hash, src.phone_hash, true, src._cdc_ts, '9999-12-31')
```

**Loại History Table pattern vì:**
- Separate history table = double storage + complex queries
- JOIN needed for point-in-time analysis

**Loại Kimball SCD vì:**
- Type 1 (overwrite) không preserve history
- Type 3 (add new column) limited use cases

---

### Decision 5: Catalog — **Unity Catalog** (chọn) vs Hive Metastore vs Polaris

**Chọn: Databricks Unity Catalog**

**Lý do chọn:**
- Native Delta format support
- Column-level security (grant access to non-PII columns only)
- Built-in audit logging cho PII access
- Integration với Delta sharing cho external consumers

**Loại Hive Metastore vì:**
- No column-level security
- No built-in audit trail
- Legacy technology

**Loại Apache Polaris vì:**
- Good choice cho multi-vendor setup, nhưng overkill cho single Databricks deployment
- Unity Catalog already integrated, less operational overhead

---

### Decision 6: Late-Data Handling — **Kafka Retention 7 days + MERGE Condition** (chọn) vs Idempotent Writes vs Windowed Processing

**Chọn: 7-day Kafka retention + MERGE condition `source.ts > target.ts`**

**Lý do chọn:**
- Guarantees ordering với Kafka partitions by trip_id
- MERGE với timestamp comparison handles out-of-order events
- No complex windowing logic

**Loại Idempotent Writes vì:**
- Overwrites data — loses historical changes
- Cannot handle true late-arriving corrections

**Loại Windowed Processing (Flink/Spark Streaming) vì:**
- Watermark-based windows có lag inherent
- Memory pressure với large windows
- Complex recovery from failures

---

### Decision 7: Storage Tiering — **S3 Standard (7d) → S3 IA (8-90d) → Glacier (91-365d)** (chọn)

**Chọn: Lifecycle policy theo retention requirements**

**Lý do chọn:**
- 7-day retention for Bronze: hot data, frequent access for re-processing
- 8-90 day: Silver/Gold aggregates, moderate query frequency
- 91-365 day: regulatory compliance, rare queries
- Estimated savings: 60% vs all-S3 Standard

**Loại all-S3 Standard vì:**
- Cost ~$23/TB/month vs $4.50/TB/month cho IA
- Over-provisioned for cold data

**Loại Glacier Immediate vì:**
- Glacier retrieval fees expensive if queries > occasional
- p95 < 2s requirement for 90-day data means hot storage needed

---

## 4. Failure Modes

### Failure Mode 1: Late-Arriving Data Corruption (tie to: time travel, MERGE)

**Scenario:** Mạng drop ở vùng sâu khiến trip events đến 48 giờ muộn. MERGE xử lý không đúng thứ tự, tạo incorrect historical record.

**Detection:**
- Monitor `late_arrival_delta_seconds` metric — alert nếu > 1 hour
- Delta Lake time travel: `SELECT * FROM trips_silver VERSION AS OF <commit> WHERE trip_id = X`
- Compare expected sequence vs actual sequence

**Rollback:**
```python
# Restore to clean state using time travel
spark.sql("""
  CREATE TABLE trips_silver_restore AS
  SELECT * FROM trips_silver VERSION AS OF <last_known_good_version>
""")
spark.sql("RENAME TABLE trips_silver TO trips_silver_corrupted")
spark.sql("RENAME TABLE trips_silver_restore TO trips_silver")
```

---

### Failure Mode 2: PII Tokenization Failure (tie to: security, lineage)

**Scenario:** Tokenization function throws exception, raw PII written to Bronze table. Compliance violation — Decree 13 breach.

**Detection:**
- Schema enforcement: Bronze tables require PII fields to be HASH format (regex check)
- Automated PII scanner on Bronze landing (AWS Macie equivalent)
- Alert on any raw PII pattern match in Bronze

**Rollback:**
```python
# Delete contaminated records using time travel
spark.sql("DELETE FROM passengers_bronze WHERE _processing_ts > '2024-01-01' AND name NOT LIKE '%#HASH%'")

# Re-process from Debezium offset (Kafka replay)
# Reset Kafka consumer group to last known good offset
```

**Prevention:**
- Tokenization UDF wrapped in try-catch, failures fail the batch
- Pre-commit hooks: validate all PII fields are hashed before write

---

### Failure Mode 3: CDC Offset Loss / Duplicate Events (tie to: Delta CDF, exactly-once semantics)

**Scenario:** Debezium crashes mid-batch, offset not committed. Duplicates written to Bronze → Silver cascade duplicates.

**Detection:**
- Monitor `deduplication_ratio` metric — spike indicates duplicates
- Daily reconciliation: `SELECT trip_id, COUNT(*) FROM trips_bronze GROUP BY trip_id HAVING COUNT(*) > 1`
- Compare Delta CDF change feed vs expected event count

**Rollback:**
```python
# Remove duplicates using deduplication
spark.sql("""
  CREATE TABLE trips_bronze_dedup AS
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY _cdc_ts DESC) as rn
    FROM trips_bronze
  ) WHERE rn = 1
""")
spark.sql("DELETE FROM trips_bronze WHERE _cdc_ts > '2024-01-01'")
spark.sql("INSERT INTO trips_bronze SELECT * FROM trips_bronze_dedup")
```

**Prevention:**
- Enable Delta write commit tokens for exactly-once semantics
- Kafka enable idempotent producer
- Bronze table: `ALTER TABLE trips_bronze SET TBLPROPERTIES ('delta.appendOnly' = 'false')`

---

## 5. Cost Back-of-Envelope

### Storage Costs (Monthly)

Assumptions:
- 100M trips/year = 8.3M trips/month
- Average trip record: 2 KB raw → 500 bytes compressed
- Bronze retention: 7 days = 58M records
- Silver retention: 90 days = 750M records  
- Gold retention: 365 days = 3.3B records

| Layer | Records | Size (compressed) | S3 Tier | $/TB-mo | Monthly Cost |
|-------|---------|-------------------|---------|---------|--------------|
| Bronze (7d) | 58M | 30 GB | Standard | $23 | $0.69 |
| Silver (90d) | 750M | 375 GB | IA | $12.50 | $4.69 |
| Gold (365d) | 3.3B | 1.65 TB | IA + Glacier | $4.50 | $7.43 |
| **Total** | | **~2 TB** | | | **~$12.81/mo** |

Note: Includes 3x replication factor implicit in S3 durability.

### Compute Costs (Monthly)

Assumptions:
- CDC ingestion: 30K writes/sec peak, avg 10K writes/sec
- Spark Structured Streaming: 4x r5.4xlarge (16 vCPU, 128 GB RAM) = $1.20/hr
- 24/7 operation

| Component | Instance | Hours/mo | $/hr | Monthly Cost |
|-----------|----------|----------|------|--------------|
| Ingestion Cluster | 4x r5.4xlarge | 730 | $1.20 | $3,504 |
| Analytics Cluster | 2x r5.2xlarge (on-demand) | 200 | $0.60 | $240 |
| **Total Compute** | | | | **~$3,744/mo** |

### Total Estimated Monthly Cost

| Category | Cost |
|----------|------|
| Storage | $13/mo |
| Compute | $3,744/mo |
| **Total** | **~$3,757/mo** |

**Note:** This is for the Lakehouse tier. Production Oracle and Kafka costs separate. Budget is well within typical enterprise analytics infrastructure spend for this scale.

---

## 6. What I Would Build First (One-Week MVP Slice)

### MVP Scope: End-to-End Trip Ingestion with PII Tokenization

**Goal:** Prove the ingestion pipeline and PII governance work correctly before building analytics.

**Day 1-2: Infrastructure Setup**
- [ ] Deploy Oracle → Debezium connector (existing team handles Oracle)
- [ ] Setup Kafka topics: `passengers`, `drivers`, `trips`
- [ ] Create Bronze tables with tokenization UDF

```python
from pyspark.sql.functions import udf, sha2, concat_ws
from pyspark.sql.types import StringType

SALT = "vn-ridehailing-2024-prod-salt"  # From secrets manager

@udf(StringType())
def tokenize_pii(value: str) -> str:
    if value is None:
        return None
    return sha2(concat_ws("|", value, SALT), 256)

# Apply to Bronze landing
bronze_df = kafka_df.select(
    col("key.trip_id").alias("trip_id"),
    tokenize_pii("value.phone_number").alias("phone_hash"),
    tokenize_pii("value.passenger_name").alias("name_hash"),
    col("value.trip_amount").alias("amount"),
    col("timestamp").alias("_cdc_ts"),
    current_timestamp().alias("_processing_ts")
)
bronze_df.write.format("delta").mode("append").saveAsTable("trips_bronze")
```

**Day 3-4: Silver Layer with SCD Type 2**
- [ ] Implement MERGE-based SCD Type 2
- [ ] Test late-arriving data handling
- [ ] Verify audit table captures all PII access

**Day 5: Testing & Documentation**
- [ ] Load test: 30K writes/sec for 1 hour
- [ ] Verify PII cannot be read raw from Silver/Gold
- [ ] Document rollback procedures
- [ ] Present to team: "This is the ingestion path — analytics layer next sprint"

**Deliverable:** A working pipeline that ingests trips with tokenized PII, maintains history, and passes PII compliance audit.

---

## 7. References & Concepts Applied

| Day 18 Concept | How Applied |
|---------------|-------------|
| **Medallion Architecture** | Bronze → Silver → Gold with clear data transformation stages |
| **ACID Transactions** | Delta Lake MERGE ensures exactly-once processing |
| **Time Travel** | VERSION AS OF for rollback and audit |
| **Change Data Feed** | Delta CDF captures changed rows for Silver propagation |
| **Catalog & Lineage** | Unity Catalog + OpenLineage for governance |
| **FinOps Tiering** | S3 lifecycle: Standard → IA → Glacier |
| **Schema Evolution** | Late-arriving schema changes handled via Delta schema enforcement |
| **Deletion Vectors** | Soft delete for compliance with 365-day retention |

---

## 8. Open Questions / Future Work

1. **Decree 13 specific:** Need legal review on exact PII fields in scope (phone, ID, GPS — what about device fingerprint, IP address?)
2. **Multi-region:** If expansion to other ASEAN countries, need to consider data residency laws
3. **Real-time analytics:** Current design is micro-batch (5 min). If p95 < 1s required, consider Flink for real-time Silver layer
4. **Machine learning:** Feature store integration for demand prediction — future phase

---

*Document version: 1.0 — Ready for design review*
