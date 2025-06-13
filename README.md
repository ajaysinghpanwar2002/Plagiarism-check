# Plagiarism Detection System

## Overview

A Go-based plagiarism detection system that uses SimHash algorithm and Locality-Sensitive Hashing (LSH) to identify duplicate content in stories. The system processes approximately 10,000 new documents daily from a corpus of 15+ million existing stories across multiple languages.

## System Architecture

### Core Components

1. **Athena Client Interface** - Fetches Pratilipi IDs for stories published/updated in the last 24 hours
2. **S3 Client Interface** - Downloads story content from S3 buckets
3. **Content Parser** - Converts HTML content to plain text
4. **SimHash Generator** - Creates 128-bit SimHash fingerprints
5. **Redis Client** - Stores and retrieves SimHashes using LSH bucketing
6. **HTTP Layer** - Sends flagged stories to CMS for manual review
7. **Monitoring Stack** - Prometheus metrics with Grafana dashboards
8. **Notification System** - Slack webhook integration for failure alerts

### Producer-Consumer Architecture

```
[Athena] → [ID Producer] → [ID Channel] → [Worker Pool] → [Batch Channel] → [Redis Writer]
    ↓           (1)            ↓           (10-50)          ↓              (1)
[S3 Bucket] ←─────────────────────────────┘               └─────────────→ [Redis LSH]
```

**Flow Description:**
- **ID Producer (1 Goroutine)**: Fetches Pratilipi IDs from Athena and feeds them into a buffered channel
- **Worker Pool (10-50 Goroutines)**: Processes IDs concurrently by downloading from S3, parsing content, generating SimHash, and preparing Redis batches
- **Redis Writer (1 Goroutine)**: Handles bulk writes to Redis to prevent connection overload

## Locality-Sensitive Hashing (LSH) Implementation

### Bucketing Strategy

The 128-bit SimHash is divided into 8 bands of 16 bits each for efficient similarity detection:

```
128-bit SimHash: [16 bits][16 bits][16 bits][16 bits][16 bits][16 bits][16 bits][16 bits]
                    Band 0   Band 1   Band 2   Band 3   Band 4   Band 5   Band 6   Band 7
```

### Redis Storage Structure

**Bucket Storage (Redis Sets):**
```
Key Pattern: {LANGUAGE}:{BAND_INDEX}:{BAND_VALUE}
Example: SADD HINDI:0:A8D3 12345
         SADD HINDI:1:FF01 12345
```

**Full Hash Storage (Redis Hash):**
```
Key Pattern: simhashes:{LANGUAGE}
Example: HSET simhashes:HINDI 12345 <full_128_bit_hash_hex>
```

### Duplicate Detection Process

```
New Story → Generate SimHash → Split into 8 Bands → Query LSH Buckets
    ↓                                                        ↓
Flag for Review ← Hamming Distance < Threshold ← Get Candidate Hashes
```

1. Generate 128-bit SimHash for new story
2. Split into 8 bands of 16 bits each
3. Query Redis using `SUNION` on corresponding bucket keys
4. Retrieve full hashes for candidate matches
5. Calculate Hamming distance between new hash and candidates
6. Flag as potential plagiarism if distance ≤ threshold

## Daily Processing Workflow

### Data Processing Pipeline

```
[Athena Query] → [Content Removal] → [S3 Download] → [HTML Parsing] → [SimHash Generation]
      ↓               ↓                   ↓              ↓                ↓
[24h Stories]   [Updated Content]   [Story Content]  [Plain Text]   [128-bit Hash]
      ↓               ↓                                                    ↓
[New: ~10k]     [Updated: ~15k]                                    [LSH Comparison]
                                                                          ↓
                                                               [Store or Flag for Review]
```

### Processing Steps

1. **Data Identification**
   - Fetch newly published stories (last 24 hours) from Athena
   - Identify stories with updated content or state changes

2. **Content Cleanup**
   - Remove SimHashes for updated/unpublished stories from Redis

3. **Content Processing**
   - Download story content from S3 (~25k stories total)
   - Parse HTML content to extract plain text

4. **Similarity Detection**
   - Generate SimHash for new stories
   - Compare against existing hashes using LSH buckets
   - Flag potential plagiarism cases

5. **Storage and Review**
   - Store unique SimHashes in Redis
   - Send flagged stories to CMS for manual review

## Technical Specifications

### Data Scale

| Language   | Story Count |
|------------|-------------|
| Hindi      | 4,169,351   |
| Bengali    | 1,852,634   |
| Marathi    | 1,851,468   |
| Tamil      | 1,817,685   |
| Telugu     | 1,536,833   |
| Malayalam  | 2,055,712   |
| Gujarati   | 1,124,591   |
| English    | 712,591     |
| Odia       | 405,674     |
| Punjabi    | 270,427     |
| **Total**  | **15,796,966** |

**Daily Volume:** ~10,000 new documents across all languages

### Infrastructure Requirements

**Development Environment:**
- EC2 Instance: 2 vCPUs, 8 GiB RAM
- Single instance deployment for daily processing

### Memory Footprint Estimation

**Redis Memory Usage:**
- Full Hashes Storage: ~400 MB
- LSH Buckets Storage: ~1.6 GB + overhead
- **Total Estimated: 2.5 - 3.5 GB**

### Performance Estimates

**Daily Job Execution Time:**
- Athena Query: ~1.5 minutes
- S3 Download & Processing: ~4 minutes
- **Total Estimated: 5-6 minutes**

**Per Document Processing:**
- S3 Download: 50-100 ms
- HTML Parsing & SimHash: <5 ms
- Redis Operations: 15-20 ms
- **Total Average: 70-125 ms per document**

## Configuration and Flexibility

### Configurable Parameters

- **Hamming Distance Threshold**: Per-language configuration for plagiarism detection sensitivity
- **Worker Pool Size**: Adjustable based on system resources
- **Batch Sizes**: Configurable for Redis bulk operations
- **Retry Policies**: Configurable retry attempts and backoff strategies

### Error Handling and Reliability

- **Checkpointing**: Track last processed Pratilipi ID and timestamp in Redis
- **Fail-Fast Strategy**: Immediate failure detection with retry mechanisms
- **Stateless Design**: No persistent application state
- **Comprehensive Logging**: Detailed error logging and metrics collection

## Monitoring and Observability

### Metrics Collection

- **Prometheus Integration**: Custom metrics for processing rates, error counts, and performance
- **Grafana Dashboard**: Detailed visualization of system performance and health
- **Slack Notifications**: Webhook-based failure alerts

### Key Metrics

- Documents processed per minute
- SimHash generation rate
- Redis operation latency
- Plagiarism detection accuracy
- System resource utilization

## Future Enhancements

### Planned Features

1. **Algorithm Flexibility**
   - Support for additional plagiarism detection algorithms
   - Algorithm selection based on content characteristics

2. **Advanced Validation**
   - Multi-algorithm verification before CMS submission
   - Confidence scoring for plagiarism detection

3. **Content Analysis**
   - Enhanced preprocessing for different content types
   - Language-specific optimizations

## Development Tasks

### Core Implementation

- [X] Containerize application using Docker
- [X] Implement Athena client interface for Pratilipi ID fetching
- [X] Develop S3 client for story content download
- [X] Create HTML to text parser
- [X] Implement 128-bit SimHash algorithm in Go
- [X] Build Producer-Consumer model with goroutines
- [X] Develop Redis client with LSH bucketing support
- [X] Implement Hamming distance calculation for 128-bit SimHashes
- [ ] Create HTTP layer for CMS integration
- [X] Set up Prometheus metrics and Grafana dashboards
- [ ] Implement Slack webhook notifications

### Additional Tasks

- [ ] Performance benchmarking and optimization
- [X] One-time SimHash generation for existing 15M stories
- [X] Sanity check implementation for existing story corpus
- [ ] Comprehensive error handling and retry mechanisms
- [X] Find Optimal goroutine pool size, Channel size, redis write size based on system resources
- [ ] Pipe in new Algorithm for false positive reductions
