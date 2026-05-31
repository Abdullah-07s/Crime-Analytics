# Project Report: Intelligent Real-Time Crime Analytics & Alert System
**City of Chicago Public Safety Case Study**

## 1. Executive Summary
Traditional crime reporting systems often rely on batch processing, which creates a dangerous lag in time-sensitive decision-making. This project implements a **Lambda Architecture** that combines historical trend analysis (Batch Layer) with real-time anomaly detection (Speed Layer). The result is a unified platform capable of detecting hotspots and dispatching intelligent alerts as incidents unfold across 25+ police districts.

## 2. Technical Architecture: The Lambda Approach
The system is built on a distributed infrastructure using Docker containers to ensure scalability and isolation.

### A. Batch Layer (Apache Spark)
*   **Purpose:** Process 20+ years of historical data (5.7 GB) to identify long-term patterns.
*   **Technologies:** PySpark (Spark SQL & MLlib).
*   **Analytics Performed:**
    *   **Crime Trends:** Yearly and monthly crime distributions.
    *   **Arrest Rates:** Efficiency analysis across different crime types.
    *   **Geospatial Hotspots:** Applied **K-Means Clustering** to 7+ million coordinates to identify the 10 most high-risk zones in Chicago.
    *   **Correlations:** Identifying relationships between different crime categories (e.g., Gun Violence vs. Robbery).

### B. Speed Layer (Apache Kafka & Apache Storm)
*   **Purpose:** "Simulated Live Feed" of 911 calls and incident reports.
*   **Kafka Producer:** Streams crime records row-by-row into a distributed message bus.
*   **Apache Storm Topology:** 
    *   **ParseBolt:** Deserializes and validates incoming JSON events.
    *   **WindowBolt:** Implements a **Sliding Window** algorithm (5-minute window) to track per-district crime frequency.
    *   **AnomalyBolt:** Triggers alerts when district counts exceed a mathematical threshold (50 incidents/window).
    *   **AlertBolt:** Categorizes alerts by severity (MEDIUM/HIGH) and persists them for the serving layer.

### C. Serving Layer (PostgreSQL, MongoDB, & Streamlit)
*   **PostgreSQL:** Stores structured historical results and the finalized alerts for the dashboard.
*   **MongoDB:** Acts as a high-speed log sink for raw alert JSON data.
*   **Streamlit Dashboard:** Provides a premium, interactive UI for law enforcement to monitor the city.

## 3. Key Findings & Data Insights
*   **Geospatial Clustering:** The K-Means model identified a massive concentration of incidents in the Central and West Side districts.
*   **Dynamic Response:** The Storm topology successfully identified real-time "spikes," categorizing 60% of alerts as HIGH severity during peak simulated hours.
*   **Efficiency:** By utilizing the Lambda Architecture, the system can query 20 years of history in milliseconds while simultaneously processing new events with sub-second latency.

## 4. Implementation Details
*   **Infrastructure:** Orchestrated via `docker-compose`.
*   **Language:** Python (3.13) with `pyspark`, `kafka-python`, `psycopg2`, and `streamlit`.
*   **Scalability:** The use of Kafka and Spark ensures that as the city's data grows, more nodes can be added to the cluster without changing the code.

## 5. Conclusion
This Intelligent Alert System proves that modern big data tools can transform public safety from a reactive "report-only" model to a proactive, real-time "response" model. The integration of Machine Learning for hotspot detection and Streaming for anomaly alerts provides a 360-degree view of urban safety.
