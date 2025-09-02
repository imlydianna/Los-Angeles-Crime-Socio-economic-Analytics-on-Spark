# Los-Angeles Crime Socio Economic Big Data Analytics on Spark

Distributed analytics of Los Angeles crime and socio-economic datasets using Apache Spark on a Kubernetes cluster with HDFS, supporting multiple Spark APIs: RDDs, DataFrames, Spark SQL, and UDFs. Focus: scalable processing, performance evaluation, and reproducible pipelines.

## Objectives
- Preprocess large, heterogeneous datasets (CSV/TXT → Parquet).
- Implement queries across RDD, DataFrame, SQL, and UDF APIs.
- Benchmark runtime, shuffle overhead, and scaling efficiency.
- Assess impact of data formats (CSV vs Parquet) and cluster resources (executors, cores, memory).
- Verify and analyze join strategies chosen by the Catalyst Optimizer.
  
## Experimental Setup
- Cluster: Spark deployed on Kubernetes (cluster mode) with per-user namespaces.
- Storage: HDFS for raw inputs, Parquet outputs, and event logs.
- Preprocessing: Raw CSV/TXT → Parquet (compressed, columnar) for faster reads and predicate pushdown.
- Monitoring: Spark History Server configured via Docker Compose to inspect DAGs, stages, shuffle operations, and job metrics.

Tunable parameters: executors, cores, memory, input format, API choice.

Docker support: jobs can run inside prebuilt Spark Docker images for local testing, reproducibility, and consistent cluster deployment.
  
## Implementation 

### Running Spark Jobs on Kubernetes
All jobs were submitted to the CSLab Kubernetes cluster using ```spark-submit ```. Key configuration parameters included master URL, deploy mode, container image, and event logging.

Example Run:
```bash
spark-submit \
  --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
  --deploy-mode cluster \
  --name Query1 \
  --conf spark.kubernetes.namespace=<username>-priv \
  --conf spark.kubernetes.container.image=apache/spark \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/<username>/logs \
  hdfs://hdfs-namenode:9000/user/<username>/code/Query_1/DfQ1.py
```

### Monitoring with Spark History Server
- Event logs stored in: ```hdfs://hdfs-namenode:9000/user/<username>/logs/ ```
- Local web UI: http://localhost:18080

This setup allowed inspection of execution DAGs, stage durations, shuffle operations, and optimizer-selected join strategies.

### Queries Implemented

1. Victim Age Analysis for Aggravated Assaults: Analyzed and ranked victim age groups in aggravated assault cases.

2. Annual Police Precinct Performance: Determined the 3 highest-performing precincts each year based on the proportion of cases successfully closed.

3. Average Income by ZIP Code: Computed the average income per capita by joining census and income datasets.

4. Spatial Analysis of Gun-Related Crimes: Measured the frequency of gun-related incidents and calculated the average distance to the nearest police station using the Haversine formula. Applied cross joins for spatial computation, serving as a scalability benchmark.


### Workflow
1. Upload raw datasets to HDFS.
2. Preprocess → convert to Parquet for optimized queries.
3. Submit queries via spark-submit (Kubernetes / Docker).
4. Analyze results via Spark History Server: stages, shuffle, skew, executor metrics, join strategies.




















- Executors per job
- Cores & memory per executor
- Input format (CSV vs Parquet)
- Spark API selection (RDD, DataFrame, SQL/UDF)

## Repository Structure
```bash.
.
├── Preprocessing/
│   └── prepare_data.py        # ETL: raw CSV/TXT → Parquet (columnar, compressed)
│
├── Query_1/                   # Age distribution of victims
│   ├── RddQ1.py
│   ├── DfQ1.py
│   └── DfUdfQ1.py
│
├── Query_2/                   # Police precinct ranking (closure rates)
│   ├── RddQ2.py
│   ├── DfQ2.py
│   └── SqlQ2.py
│
├── Query_3/                   # Income per capita by ZIP (CSV vs Parquet benchmarks)
│   ├── RddQ3.py
│   ├── DfQ3_csv.py
│   └── DfQ3_parquet.py
│
├── Query_4/                   # Gun-related crimes vs nearest police stations (geospatial)
│   └── DfQ4.py
│
└── README.md
```

## Workflow
1. Data ingestion → Upload raw files to HDFS.
2. Preprocessing → Convert to Parquet for efficient distributed queries.
3. Execution → Submit queries with spark-submit on Kubernetes.
4. Performance analysis → Collect metrics from History Server:
   - Stage breakdown, shuffle read/write, task skew
   - Executor CPU/memory utilization
   - Optimizer-selected join strategies

## Example Run
```bash.
spark-submit \
  --master k8s://https://<K8S_API_SERVER>:6443 \
  --deploy-mode cluster \
  --name Query3-Parquet \
  --conf spark.kubernetes.namespace=<user>-priv \
  --conf spark.kubernetes.container.image=apache/spark \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/<user>/logs \
  hdfs://hdfs-namenode:9000/user/<user>/code/Query_3/DfQ3_parquet.py
```
