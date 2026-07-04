# 🚔 Real-Time Crime Analytics & Alert System

A full **Lambda Architecture** big data pipeline built on the **City of Chicago Public Safety dataset** (5.7 GB, 7M+ records). Combines Apache Spark batch processing, Kafka + Apache Storm real-time streaming, and a live Streamlit dashboard — all containerised with Docker Compose.

---

## 🏗️ Architecture

```
                        ┌─────────────────────────────────────┐
                        │           LAMBDA ARCHITECTURE        │
                        └─────────────────────────────────────┘

  Chicago Crime CSV  ──▶  Kafka Producer  ──▶  Kafka Topic: crime_events
  (7M+ records)                                        │
                                                       ▼
                                            ┌──────────────────────┐
  ┌─────────────────────────────┐           │   Storm Topology      │
  │        BATCH LAYER          │           │  (Speed Layer)        │
  │                             │           │                       │
  │  PySpark                    │           │  KafkaSpout           │
  │  ├── Crime Trend Analysis   │           │      ↓                │
  │  ├── Arrest Rate Analysis   │           │  ParseBolt            │
  │  ├── Violence Analysis      │           │      ↓                │
  │  ├── Sex Offender Proximity │           │  DistrictBolt         │
  │  ├── Cross-Dataset Correlat.│           │      ↓                │
  │  └── K-Means Hotspot (k=10) │           │  WindowBolt (5-min)   │
  └────────────┬────────────────┘           │      ↓                │
               │                            │  AnomalyBolt          │
               ▼                            │      ↓                │
        PostgreSQL                          │  AlertBolt            │
        (structured results)                └───────┬──────────────┘
               │                                    │
               │                            PostgreSQL + MongoDB
               │                            (alerts + raw JSON)
               │                                    │
               └──────────────┬─────────────────────┘
                               ▼
                    ┌─────────────────────┐
                    │  Streamlit Dashboard │
                    │  (Serving Layer)     │
                    └─────────────────────┘
```

---

## ✨ Features

### Batch Layer (PySpark)
- **Crime Trend Analysis** — yearly, monthly, hourly, and day-of-week crime counts
- **Arrest Rate Efficiency** — arrest rates by crime type, district, and community area
- **Violence & Gunshot Analysis** — filtered violent crime statistics
- **Sex Offender Proximity** — cross-dataset spatial analysis
- **Geospatial K-Means Clustering** — clusters 7M+ GPS coordinates into 10 high-risk zones using `pyspark.ml.clustering.KMeans`
- All results persisted to **PostgreSQL** via JDBC

### Speed Layer (Kafka + Storm)
- **Kafka Producer** — streams crime CSV row-by-row as JSON at configurable rate
- **Storm Topology**: `ParseBolt → DistrictBolt → WindowBolt → AnomalyBolt → AlertBolt`
- **WindowBolt** — 5-minute sliding window tracks incident counts per district
- **AnomalyBolt** — triggers `MEDIUM`/`HIGH` severity alerts when counts exceed threshold
- **AlertBolt** — persists alerts to both PostgreSQL and MongoDB

### Serving Layer
- **PostgreSQL** — structured batch results + alerts
- **MongoDB** — raw alert JSON documents
- **Streamlit Dashboard** — live monitoring interface for law enforcement

### Infrastructure
- Fully containerised with **Docker Compose**: Kafka, Zookeeper, Storm (Nimbus + Supervisor + UI), PostgreSQL, MongoDB

---

## 📁 Project Structure

```
crime-analytics/
├── config/
│   └── config.yaml               # All config: Kafka, Storm, Postgres, MongoDB, paths
├── data/
│   └── download_data.py          # Dataset downloader (Chicago Public Safety API)
├── db/
│   ├── init.sql                  # PostgreSQL schema (tables, indexes)
│   └── mongo_setup.py            # MongoDB collection + index setup
├── kafka/
│   └── producer.py               # Kafka crime event streamer
├── spark/
│   ├── run_batch.py              # Entry point: runs all batch jobs
│   ├── analytics/
│   │   └── batch_analysis.py     # All 5 Spark analytics functions
│   ├── ml/
│   │   └── hotspots.py           # K-Means geospatial hotspot detection
│   ├── preprocessing/
│   │   └── preprocessing.py      # Dataset loaders + schema enforcement
│   └── schemas/
│       └── schemas.py            # Spark schema definitions for all datasets
├── storm/
│   ├── consumer.py               # Kafka consumer → topology runner
│   ├── bolts/
│   │   └── bolts.py              # ParseBolt, DistrictBolt, WindowBolt, AnomalyBolt, AlertBolt
│   └── topology/
│       └── topology.py           # CrimeTopology — wires all bolts together
├── dashboard/
│   └── app.py                    # Streamlit dashboard
├── docker-compose.yml            # Full infrastructure stack
├── requirements.txt
├── FINAL_REPORT.md               # Full project report
└── .gitignore
```

---

## 🚀 Setup & How to Run

### Prerequisites
- **Docker Desktop** (for infrastructure)
- **Python 3.10+**
- **Java 11+** (for PySpark)

### 1. Clone the repo
```bash
git clone https://github.com/Abdullah-07s/crime-analytics.git
cd crime-analytics
```

### 2. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 3. Download the dataset
```bash
python data/download_data.py
```
This downloads the Chicago Crimes, Arrests, Violence, Sex Offenders, and Police Stations CSVs into `data/`.

### 4. Start the infrastructure
```bash
cd docker
docker-compose up -d
```
Starts: Kafka + Zookeeper, Storm (Nimbus, Supervisor, UI), PostgreSQL, MongoDB.

Wait ~30 seconds for all services to be healthy.

### 5. Run the Batch Layer
```bash
python spark/run_batch.py
```
Runs all Spark analytics and K-Means clustering. Results written to PostgreSQL.

### 6. Run the Speed Layer
In two separate terminals:

**Terminal 1 — Kafka Producer:**
```bash
python kafka/producer.py
```

**Terminal 2 — Storm Consumer:**
```bash
python storm/consumer.py
```

### 7. Launch the Dashboard
```bash
streamlit run dashboard/app.py
```
Open `http://localhost:8501` in your browser.

---

## ⚙️ Configuration

All settings are in `config/config.yaml`:

```yaml
kafka:
  broker: "localhost:9092"
  topic: "crime_events"
  producer_rate: 100        # rows/second

storm:
  window_size_minutes: 5    # sliding window duration
  slide_interval_minutes: 1
  anomaly_threshold: 50     # incidents/window to trigger alert

postgres:
  host: localhost
  port: 5432
  database: crime_db

paths:
  crime_csv: "Crimes.csv"
```

---

## 🧠 Technical Highlights

### K-Means Hotspot Detection
```python
assembler = VectorAssembler(inputCols=["Latitude", "Longitude"], outputCol="features")
kmeans = KMeans(k=10, seed=42, maxIter=20)
model = kmeans.fit(assembler.transform(df))
# → identifies 10 highest-risk geographic zones from 7M+ GPS points
```

### Storm Sliding Window
```python
class WindowBolt:
    # Maintains deque per district, evicts events older than window_size_sec
    # Emits (district, count, timestamp) at each slide interval
    # AnomalyBolt triggers alert if count > threshold
```

### Storm Topology Pipeline
```
ParseBolt     → validates fields (CASE NUMBER, DISTRICT, DATE, PRIMARY TYPE)
DistrictBolt  → routes tuples by police district
WindowBolt    → 5-min sliding window count per district
AnomalyBolt   → MEDIUM alert if count > threshold, HIGH if count > 2× threshold
AlertBolt     → persists to PostgreSQL + MongoDB
```

---

## 🛠️ Tech Stack

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Apache Storm](https://img.shields.io/badge/Apache_Storm-6DB33F?style=flat-square&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=flat-square&logo=postgresql&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-4EA94B?style=flat-square&logo=mongodb&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)

---

## 📊 Dataset

**City of Chicago Public Safety Open Data**
- Crimes: 7M+ records, 25+ police districts, 2001–present
- File size: ~5.7 GB
- Source: [Chicago Data Portal](https://data.cityofchicago.org/)

---

## 📄 License

Educational use only.
