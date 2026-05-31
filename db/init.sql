-- Crime Trends: yearly, monthly, hourly, day_of_week aggregations
CREATE TABLE IF NOT EXISTS crime_trends (
    id SERIAL PRIMARY KEY,
    group_type VARCHAR(50),
    group_value VARCHAR(50),
    crime_count BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- K-Means hotspot cluster centroids
CREATE TABLE IF NOT EXISTS hotspots (
    id SERIAL PRIMARY KEY,
    cluster_id INT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    crime_count BIGINT
);

-- Real-time anomaly alerts from Storm
CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    district VARCHAR(50),
    timestamp TIMESTAMP,
    event_count INT,
    threshold INT,
    severity VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Cross-dataset correlations
CREATE TABLE IF NOT EXISTS correlations (
    id SERIAL PRIMARY KEY,
    correlation_type VARCHAR(100),
    group_key VARCHAR(100),
    metric_1_name VARCHAR(100),
    metric_1_value DOUBLE PRECISION,
    metric_2_name VARCHAR(100),
    metric_2_value DOUBLE PRECISION,
    correlation_value DOUBLE PRECISION
);

-- Arrest rates by crime type, district, race
CREATE TABLE IF NOT EXISTS arrest_rates (
    id SERIAL PRIMARY KEY,
    group_type VARCHAR(50),
    group_value VARCHAR(100),
    total_crimes BIGINT,
    total_arrests BIGINT,
    arrest_rate DOUBLE PRECISION
);

-- Violence & gunshot statistics
CREATE TABLE IF NOT EXISTS violence_stats (
    id SERIAL PRIMARY KEY,
    group_type VARCHAR(50),
    group_value VARCHAR(100),
    total_incidents BIGINT,
    homicides BIGINT,
    non_fatal_shootings BIGINT,
    gunshot_injuries BIGINT,
    gunshot_proportion DOUBLE PRECISION
);

-- Sex offender density by district
CREATE TABLE IF NOT EXISTS sex_offender_stats (
    id SERIAL PRIMARY KEY,
    district VARCHAR(50),
    offender_count BIGINT,
    minor_victim_count BIGINT,
    minor_victim_proportion DOUBLE PRECISION
);

-- Create indexes for dashboard query performance
CREATE INDEX IF NOT EXISTS idx_crime_trends_type ON crime_trends(group_type);
CREATE INDEX IF NOT EXISTS idx_alerts_district ON alerts(district);
CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_arrest_rates_type ON arrest_rates(group_type);
CREATE INDEX IF NOT EXISTS idx_violence_stats_type ON violence_stats(group_type);
