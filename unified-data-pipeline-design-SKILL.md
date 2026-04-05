---
name: unified-data-pipeline-design
description: >
  Generate a complete, production-grade data pipeline design document that unifies multiple upstream datasets
  for ML, analytics, and reporting use cases. Use this skill whenever a user asks to design, architect, or
  document a data pipeline — especially when they mention upstream data sources, ETL, Spark, Airflow, S3,
  data unification, data modeling, or need a comprehensive design doc for data engineering work. Also trigger
  when users say things like "design a pipeline", "data architecture document", "unified dataset",
  "pipeline for ML", "data engineering design", or reference ingesting/merging multiple data sources.
  Output is a downloadable Markdown document covering all critical aspects from architecture to runbook.

---

# Unified Data Pipeline Design Skill

## Purpose

You are a senior data engineer. Generate a **complete, practical, slightly casual** design document that unifies
multiple upstream datasets into a single reliable dataset serving ML engineers, data analysts, and reporting users.

The output **must be a downloadable Markdown file** saved to `/mnt/user-data/outputs/` and presented with
`present_files`.

---

## Inputs to Gather

Before writing, extract or ask for:
1. **Upstream data description** — What datasets exist? Formats, owners, update frequency?
2. **Data constraints** — PII/PHI? HIPAA? GDPR? Other compliance?
3. **Scale expectations** — Row counts, data size, growth rate, latency requirements?

If the user hasn't provided these, ask before generating. If they've provided partial info, proceed with
reasonable assumptions and call them out clearly in the document.

---

## Document Structure

Generate all 27 sections below. Be thorough but avoid repetition. Use code blocks for schemas,
SQL/PySpark snippets, and configs. Keep tone clear, practical, and slightly casual.

### 1. High-Level Overview
- What the pipeline does end-to-end in plain language
- Key data flow diagram (ASCII or described)
- Who benefits and how

### 2. Volume & Scale Considerations
- Estimated data size today and projected growth (1yr / 3yr)
- Partitioning strategy
- Horizontal scaling approach (Spark cluster sizing, parallelism)

### 3. Users & Access Patterns
- Consumer personas: ML engineers, analysts, reporting/BI
- Read patterns: batch vs interactive, full scan vs filtered
- Expected query concurrency

### 4. Upstream Data Sources
- Table for each source: name, owner, format, update frequency, row count estimate
- Known data quality issues
- Recommended POC contacts
- Data contract definition per source (schema, SLA, freshness guarantee)

### 5. Data Model Design
- Proposed unified model: fact/dimension, wide table, or hybrid — justify the choice
- Key join logic and entity resolution approach
- Traceability fields (`source_system`, `ingested_at`, `pipeline_version`)

### 6. Schema Evolution Strategy
- How to handle upstream schema changes (additive vs breaking)
- Compatibility rules: backward / forward
- Process for schema change review and approval

### 7. Common Data Challenges
- **Backfills**: strategy for historical loads, parallelism, cost controls
- **Late-arriving data**: watermark definition, grace window (e.g., 3-day window)
- **Idempotency**: how reruns produce the same result (partition overwrite pattern)

### 8. Output Schema
- Final table DDL or schema definition (with types)
- Partition columns and rationale
- Versioning fields

### 9. Design Approaches
Present **3 distinct approaches** with a pros/cons table each, then select and justify one:
- Approach A: [e.g., Daily Batch Spark Job]
- Approach B: [e.g., Incremental/CDC with Delta Lake]
- Approach C: [e.g., Streaming with Spark Structured Streaming]
- **Selected approach + rationale**

### 10. Pipeline Architecture
Describe the full stack:
- **Airflow** — DAG structure, task dependencies, scheduling
- **Spark** — ETL job breakdown (extract / transform / load phases)
- **Amazon S3** — bucket layout (`raw/`, `curated/`, `published/`)
- Include an ASCII architecture diagram

### 11. Pipeline Implementation
- High-level PySpark code template (extract, transform, write)
- Daily run vs backfill run invocation
- Idempotency pattern (partition overwrite, deduplication key)

```python
# Example skeleton — flesh out per actual schema
def run_pipeline(execution_date: str, is_backfill: bool = False):
    spark = SparkSession.builder.appName("unified_pipeline").getOrCreate()
    df = extract(spark, execution_date)
    df = transform(df)
    load(df, execution_date, overwrite=True)
```

### 12. Data Quality Strategy
- **Hard checks** (fail pipeline): row count = 0, critical nulls, referential integrity
- **Soft checks** (alert only): anomaly detection, statistical drift, unexpected nulls
- Framework recommendation (e.g., Great Expectations, dbt tests, custom Spark checks)

### 13. Data Validation
- **Reactive**: post-load checks that gate downstream consumption
- **Proactive**: upstream source monitoring before ingestion begins
- Sample validation logic

### 14. SLA / SLO Definitions
| Metric | Target | Alert Threshold |
|--------|--------|----------------|
| Freshness | < N hours | > N+1 hours |
| Completeness | > 99.5% | < 99% |
| Pipeline latency | < N hours | > N+2 hours |
| Recovery (P1) | < 4 hours | — |

### 15. PII / PHI Handling
- Fields requiring masking / tokenization / encryption
- Masking strategy per field type (hash, redact, tokenize)
- HIPAA applicability and controls (if applicable)
- Audit trail for access to sensitive fields

### 16. Access Control & Governance
- RBAC matrix: roles × dataset tiers
- Dataset tiers: `raw` (restricted) / `curated` (team) / `published` (broad)
- IAM policies and row/column-level security
- Audit logging setup

### 17. Versioning Strategy
- Partition-based versioning (e.g., `dt=YYYY-MM-DD/v=2`)
- No in-place overwrites of published data
- Time travel support (Delta Lake or S3 versioning)
- Rollback procedure

### 18. Data Accessibility
- Hive metastore / Glue catalog registration
- Query layer: Athena, Presto, Spark SQL
- ML access pattern: direct S3 read vs table API
- Recommended file format (Parquet + Snappy by default)

### 19. Observability & Monitoring
- Key metrics: job duration, rows processed, error rate, data freshness
- Logging: structured logs with `run_id`, `execution_date`, `source`
- Alerting: PagerDuty / Slack / email thresholds
- Dashboard recommendation

### 20. Cost Optimization
- Partition pruning to minimize S3 scans
- Target file size: 128–512 MB per Parquet file
- Compression: Snappy for hot data, ZSTD for cold
- Spot/preemptible instances for backfill jobs
- Lifecycle policies for raw tier (e.g., transition to Glacier after 90 days)

### 21. Metadata & Cataloging
- Catalog: AWS Glue or Hive Metastore
- Column-level metadata: description, PII flag, owner, last updated
- Lineage tracking: upstream source → transformation → output table
- Recommended tool (e.g., DataHub, Apache Atlas, Marquez)

### 22. Testing Strategy
- **Unit tests**: transformation functions in isolation (pytest)
- **Integration tests**: end-to-end mini pipeline with fixture data
- **Data validation tests**: schema conformance, value range, referential integrity
- Test coverage targets

### 23. CI/CD & Deployment
- Environments: `dev` → `staging` → `prod`
- Deployment tool: Terraform / CDK / manual
- Airflow DAG deployment process
- Spark job artifact versioning (Docker image or wheel file)
- Rollback procedure for bad deploys

### 24. Failure Handling & Runbook
- Retry policy: N retries, exponential backoff
- Alerting on failure: who gets paged and when
- **Runbook**:
  1. Check Airflow logs for failed task
  2. Identify failure point (extract / transform / load)
  3. Fix root cause
  4. Clear and re-run affected DAG task
  5. Validate output quality post-rerun
  6. Notify downstream consumers if data was delayed

### 25. Analytics & Visualization
- Key use cases enabled by this dataset
- Recommended BI tools (e.g., Tableau, Looker, Superset)
- Suggested starter dashboards or reports
- ML feature engineering opportunities

### 26. Timeline Estimate
| Phase | Tasks | Estimated Duration |
|-------|-------|--------------------|
| Discovery & Design | Data contracts, schema design, architecture review | 1–2 weeks |
| Core Pipeline Build | Spark ETL, Airflow DAG, S3 layout | 2–3 weeks |
| Data Quality & Validation | Checks, alerting, runbooks | 1 week |
| Testing & Staging | Integration tests, UAT | 1 week |
| Production Rollout | Deploy, monitor, handoff | 1 week |
| **Total** | | **~6–8 weeks** |

*Adjust based on actual data complexity and team size.*

### 27. Naming Conventions
- **Tables**: `{domain}_{entity}_{grain}` (e.g., `ecomm_orders_daily`)
- **Columns**: `snake_case`; booleans prefixed `is_` or `has_`; timestamps suffixed `_at` or `_dt`
- **Partitions**: `dt=YYYY-MM-DD` (date), `region=us-east-1` (geo)
- **S3 paths**: `s3://bucket/tier/domain/table/dt=YYYY-MM-DD/`
- **Airflow DAGs**: `{domain}_{pipeline_name}_{frequency}` (e.g., `ecomm_orders_daily`)
- **Spark jobs**: match DAG name with `_job` suffix

---

## Output Requirements

1. Save the document as a `.md` file to `/mnt/user-data/outputs/pipeline_design_<short_name>.md`
2. Call `present_files` with the output path
3. Include a brief summary in chat (2–3 sentences max) — the document speaks for itself
