## Architecture Overview

```
Manufacturing Data Sources
   │
   │ (CSV / JSON / Operational Logs)
   ▼
Amazon S3 (Data Lake - Raw Zone)
   │
   │ AWS Glue Crawlers
   ▼
AWS Glue ETL (PySpark Transformations)
   │
   │ Cleaned + Aggregated Data
   ▼
Amazon S3 (Processed Zone)
   │
   │ COPY / Glue Connector
   ▼
Amazon Redshift Data Warehouse
   │
   │ Star Schema (Facts + Dimensions)
   ▼
Power BI Dashboards
   │
   ▼
Plant → Line → Machine Drilldowns
```
