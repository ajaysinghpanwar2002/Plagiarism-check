# Plagiarism Detection Service

This repository powers a **plagiarism detection system** built for a **multilingual content platform**. It efficiently detects near-duplicate stories across millions of documents using **SimHash** and **Locality-Sensitive Hashing (LSH)**. The system is optimized for scale and cost-efficiency while operating on minimal resources.

---

## 📈 Scale of Data

* **Total Stories in S3**: \~15.8 million

* **Per Language Breakdown**:

  | Language  | Story Count |
  | --------- | ----------- |
  | Hindi     | 4,169,351   |
  | Bengali   | 1,852,634   |
  | Marathi   | 1,851,468   |
  | Tamil     | 1,817,685   |
  | Malayalam | 2,055,712   |
  | Telugu    | 1,536,833   |
  | Gujarati  | 1,124,591   |
  | Odia      | 405,674     |
  | Punjabi   | 270,427     |
  | English   | 712,591     |

* **New Documents per Day**: \~10,000

---

## ⚙️ Resources

* **EC2 Instance (Daily Processing)**:

  * vCPUs: 2
  * Memory: 8 GiB (4 GiB per vCPU)
* **Redis DB**: Used for SimHash storage and fast LSH-based similarity lookups

---

## 🏗️ System Architecture

```
Data Ingestion (Athena + S3)
           ↓
     Processing Core (Go)
           ↓
     State Cache (Redis)
           ↓
      Outputs (Flagged IDs, Monitoring)
```

### 1. Data Ingestion

* **Two Modes**:

  * **Initial SimHash DB Build** (\~15M stories)
  * **Daily Incremental Processing** (\~10K new stories)
* **Strategy**: Efficient parallel processing using a **Producer-Consumer Model**

### 2. Processing Core

* Written in **Go**
* Responsibilities:

  * Download story HTML from S3
  * Convert HTML to plain text
  * Generate 128-bit SimHash
  * Batch insert into Redis

### 3. Redis State Cache

* SimHashes stored for efficient lookup and comparison
* Uses **Locality-Sensitive Hashing (LSH)** over SimHash
* Memory-efficient (\~240 MB for 15M SimHashes)

### 4. Outputs

* **Flagged IDs**: Sent for manual review
* **Monitoring**: Metrics exported to a **Grafana dashboard**

---

## 🔄 Processing Pipeline (Producer-Consumer Model)

* **ID Producer (1 Goroutine)**:
  Queries Athena and pushes story IDs to a channel.

* **Worker Pool (10–50 Goroutines)**:
  Each worker:

  * Downloads story from S3
  * Parses and extracts text
  * Generates SimHash
  * Batches for Redis
  * Cleans up local files

* **Redis Writer (1 Goroutine)**:
  Pulls batches from worker channel and inserts into Redis using pipelining.

---

## 🧠 Locality-Sensitive Hashing (LSH)

### 128-bit SimHash Bucketing

1. **Split Hash**: 8 bands of 16 bits each
   Example:

   ```
   [16b][16b][16b][16b][16b][16b][16b][16b]
   ```

2. **Store Buckets in Redis**:
   For each band, store the `pratilipi_id` in a Redis Set with the key:

   ```
   <LANG>:<BAND_INDEX>:<BAND_VALUE>
   ```

   Example (Hindi, ID = 12345):

   ```
   SADD HINDI:0:A8D3 12345
   SADD HINDI:1:FF01 12345
   ...
   ```

3. **Store Full Hash**:

   ```
   HSET simhashes:<LANG> <PRATILIPI_ID> <128_BIT_HASH_AS_HEX>
   ```

---

## 🔍 Duplicate Detection (Daily Flow)

1. Generate SimHash for new story
2. Split into 8 bands
3. Query Redis:

   ```
   SUNION HINDI:0:<band_0> HINDI:1:<band_1> ... HINDI:7:<band_7>
   ```
4. Fetch full SimHashes of candidate IDs
5. Calculate **Hamming Distance** with the new SimHash
6. If distance ≤ 3 → **Flag as near-duplicate**

---

## 📊 Monitoring

* Real-time metrics tracked via **Grafana**
* Includes:

  * Total stories processed
  * Daily flag rate
  * Latency per stage
  * Memory usage

---

## ✅ Summary

| Component        | Description                                  |
| ---------------- | -------------------------------------------- |
| SimHash          | Compact fingerprint of a story (128-bit)     |
| Redis Sets       | Buckets for LSH band-wise indexing           |
| Redis Hashes     | Full SimHash storage for final comparison    |
| Hamming Distance | Used to determine near-duplicate threshold   |
| Parallel Design  | High-throughput, low-memory efficient system |

---

