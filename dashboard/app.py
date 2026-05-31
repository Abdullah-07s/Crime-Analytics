"""
Streamlit Dashboard — Real-Time Crime Analytics Dashboard
Displays alerts, crime trends, hotspots, arrest rates, violence stats, and correlations.

Usage:
    streamlit run dashboard/app.py
"""
import streamlit as st
import pandas as pd
import psycopg2
import yaml
import os
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient

# Load config
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# ─── Page Config ───
st.set_page_config(
    page_title="Chicago Crime Analytics",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── Custom CSS ───
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(90deg, #FF4B4B, #FF8C00);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0;
    }
    .sub-header {
        color: #888;
        font-size: 1.1rem;
        margin-top: -10px;
    }
    .metric-card {
        background: linear-gradient(135deg, #1e1e2e, #2d2d44);
        border-radius: 12px;
        padding: 20px;
        border: 1px solid #333;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_pg_connection():
    """Get PostgreSQL connection."""
    return psycopg2.connect(
        host=config['postgres']['host'],
        port=config['postgres']['port'],
        database=config['postgres']['database'],
        user=config['postgres']['user'],
        password=config['postgres']['password']
    )


@st.cache_resource
def get_mongo_db():
    """Get MongoDB database."""
    client = MongoClient(config['mongodb']['uri'])
    return client[config['mongodb']['database']]


def safe_read_sql(query, conn):
    """Safely read SQL query, return empty DataFrame on error."""
    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        st.warning(f"Query failed: {e}")
        return pd.DataFrame()


# ─── Header ───
st.markdown('<p class="main-header">🚨 Real-Time Crime Analytics Dashboard</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">City of Chicago Public Safety — Lambda Architecture</p>', unsafe_allow_html=True)
st.divider()

# ─── Sidebar ───
st.sidebar.title("🔧 Controls")
auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)
if auto_refresh:
    import time
    time.sleep(30)
    st.rerun()

section = st.sidebar.radio("Navigate", [
    "📊 Overview",
    "🚨 Live Alerts",
    "📈 Crime Trends",
    "🔫 Arrest Rates",
    "💥 Violence Stats",
    "🗺️ Hotspot Map",
    "🔗 Correlations"
])

try:
    conn = get_pg_connection()
except Exception as e:
    st.error(f"Cannot connect to PostgreSQL: {e}")
    st.info("Make sure Docker services are running: `docker-compose up -d`")
    st.stop()


# ════════════════════════════════════════════════
# 📊 OVERVIEW
# ════════════════════════════════════════════════
if section == "📊 Overview":
    col1, col2, col3, col4 = st.columns(4)

    alerts_df = safe_read_sql("SELECT COUNT(*) as cnt FROM alerts", conn)
    trends_df = safe_read_sql("SELECT SUM(crime_count) as total FROM crime_trends WHERE group_type='year'", conn)
    hotspots_df = safe_read_sql("SELECT COUNT(*) as cnt FROM hotspots", conn)
    violence_df = safe_read_sql("SELECT SUM(total_incidents) as total FROM violence_stats WHERE group_type='district'", conn)

    with col1:
        cnt = int(alerts_df['cnt'].iloc[0]) if not alerts_df.empty and alerts_df['cnt'].iloc[0] else 0
        st.metric("🚨 Alerts Triggered", f"{cnt:,}")
    with col2:
        total = int(trends_df['total'].iloc[0]) if not trends_df.empty and trends_df['total'].iloc[0] else 0
        st.metric("📋 Total Crime Records", f"{total:,}")
    with col3:
        cnt = int(hotspots_df['cnt'].iloc[0]) if not hotspots_df.empty and hotspots_df['cnt'].iloc[0] else 0
        st.metric("🗺️ Hotspot Clusters", f"{cnt}")
    with col4:
        total = int(violence_df['total'].iloc[0]) if not violence_df.empty and violence_df['total'].iloc[0] else 0
        st.metric("💥 Violence Incidents", f"{total:,}")

    st.divider()

    # Recent alerts preview
    st.subheader("Recent Alerts")
    recent = safe_read_sql("SELECT * FROM alerts ORDER BY timestamp DESC LIMIT 10", conn)
    if not recent.empty:
        st.dataframe(recent, use_container_width=True)
    else:
        st.info("No alerts yet. Start the Kafka producer and Storm consumer.")


# ════════════════════════════════════════════════
# 🚨 LIVE ALERTS
# ════════════════════════════════════════════════
elif section == "🚨 Live Alerts":
    st.subheader("🚨 Real-Time Anomaly Alerts")

    # From PostgreSQL
    st.markdown("#### PostgreSQL Alerts")
    alerts_df = safe_read_sql(
        "SELECT * FROM alerts ORDER BY timestamp DESC LIMIT 50", conn
    )
    if not alerts_df.empty:
        # Severity distribution
        col1, col2 = st.columns([2, 1])
        with col1:
            st.dataframe(alerts_df, use_container_width=True)
        with col2:
            severity_counts = alerts_df['severity'].value_counts()
            fig = px.pie(values=severity_counts.values, names=severity_counts.index,
                         title="Alert Severity Distribution",
                         color_discrete_map={"HIGH": "#FF4B4B", "MEDIUM": "#FF8C00"})
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No alerts yet.")

    # From MongoDB
    st.markdown("#### MongoDB Alert Logs")
    try:
        db = get_mongo_db()
        mongo_alerts = list(db[config['mongodb']['alert_collection']].find(
            {}, {"_id": 0}
        ).sort("timestamp", -1).limit(20))
        if mongo_alerts:
            st.dataframe(pd.DataFrame(mongo_alerts), use_container_width=True)
        else:
            st.info("No MongoDB alerts yet.")
    except Exception as e:
        st.warning(f"MongoDB unavailable: {e}")


# ════════════════════════════════════════════════
# 📈 CRIME TRENDS
# ════════════════════════════════════════════════
elif section == "📈 Crime Trends":
    st.subheader("📈 Crime Trend Analysis")

    trends_df = safe_read_sql("SELECT * FROM crime_trends", conn)
    if not trends_df.empty:
        tab1, tab2, tab3, tab4 = st.tabs(["Yearly", "Monthly", "Day of Week", "Hourly"])

        with tab1:
            yearly = trends_df[trends_df['group_type'] == 'year'].sort_values('group_value')
            if not yearly.empty:
                fig = px.bar(yearly, x='group_value', y='crime_count',
                             title="Crimes by Year", labels={'group_value': 'Year', 'crime_count': 'Count'},
                             color='crime_count', color_continuous_scale='Reds')
                st.plotly_chart(fig, use_container_width=True)

        with tab2:
            monthly = trends_df[trends_df['group_type'] == 'month'].sort_values('group_value', key=lambda x: x.astype(int))
            if not monthly.empty:
                month_names = {str(i): name for i, name in enumerate(
                    ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                     'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'], 0)}
                monthly['month_name'] = monthly['group_value'].map(month_names)
                fig = px.line(monthly, x='month_name', y='crime_count',
                              title="Crimes by Month", markers=True)
                st.plotly_chart(fig, use_container_width=True)

        with tab3:
            dow = trends_df[trends_df['group_type'] == 'day_of_week'].sort_values('group_value')
            if not dow.empty:
                day_names = {'1': 'Sun', '2': 'Mon', '3': 'Tue', '4': 'Wed',
                             '5': 'Thu', '6': 'Fri', '7': 'Sat'}
                dow['day_name'] = dow['group_value'].map(day_names)
                fig = px.bar(dow, x='day_name', y='crime_count',
                             title="Crimes by Day of Week", color='crime_count',
                             color_continuous_scale='Blues')
                st.plotly_chart(fig, use_container_width=True)

        with tab4:
            hourly = trends_df[trends_df['group_type'] == 'hour'].sort_values('group_value', key=lambda x: x.astype(int))
            if not hourly.empty:
                fig = px.area(hourly, x='group_value', y='crime_count',
                              title="Crimes by Hour of Day",
                              labels={'group_value': 'Hour', 'crime_count': 'Count'})
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No trend data. Run the Spark batch analytics first.")


# ════════════════════════════════════════════════
# 🔫 ARREST RATES
# ════════════════════════════════════════════════
elif section == "🔫 Arrest Rates":
    st.subheader("🔫 Arrest Rate Analysis")

    arrest_df = safe_read_sql("SELECT * FROM arrest_rates", conn)
    if not arrest_df.empty:
        tab1, tab2, tab3 = st.tabs(["By Crime Type", "By District", "By Race"])

        with tab1:
            by_type = arrest_df[arrest_df['group_type'] == 'crime_type'] \
                .sort_values('arrest_rate', ascending=False).head(15)
            if not by_type.empty:
                fig = px.bar(by_type, x='group_value', y='arrest_rate',
                             title="Top 15 Crime Types by Arrest Rate",
                             color='arrest_rate', color_continuous_scale='Greens')
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)

        with tab2:
            by_dist = arrest_df[arrest_df['group_type'] == 'district'].sort_values('group_value')
            if not by_dist.empty:
                fig = px.bar(by_dist, x='group_value', y='arrest_rate',
                             title="Arrest Rate by District", color='total_crimes',
                             color_continuous_scale='Viridis')
                st.plotly_chart(fig, use_container_width=True)

        with tab3:
            by_race = arrest_df[arrest_df['group_type'] == 'race'].sort_values('total_arrests', ascending=False)
            if not by_race.empty:
                fig = px.bar(by_race, x='group_value', y='total_arrests',
                             title="Arrests by Race", color='group_value')
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No arrest rate data. Run the Spark batch analytics first.")


# ════════════════════════════════════════════════
# 💥 VIOLENCE STATS
# ════════════════════════════════════════════════
elif section == "💥 Violence Stats":
    st.subheader("💥 Violence & Gunshot Analysis")

    violence_df = safe_read_sql("SELECT * FROM violence_stats", conn)
    if not violence_df.empty:
        tab1, tab2, tab3 = st.tabs(["By Month", "By District", "Top Community Areas"])

        with tab1:
            by_month = violence_df[violence_df['group_type'] == 'month'].sort_values('group_value')
            if not by_month.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(x=by_month['group_value'], y=by_month['homicides'],
                                     name='Homicides', marker_color='#FF4B4B'))
                fig.add_trace(go.Bar(x=by_month['group_value'], y=by_month['non_fatal_shootings'],
                                     name='Non-Fatal Shootings', marker_color='#FF8C00'))
                fig.update_layout(title="Homicides vs Non-Fatal Shootings by Month",
                                  barmode='group')
                st.plotly_chart(fig, use_container_width=True)

        with tab2:
            by_dist = violence_df[violence_df['group_type'] == 'district'].sort_values('total_incidents', ascending=False)
            if not by_dist.empty:
                fig = px.bar(by_dist, x='group_value', y='total_incidents',
                             title="Violence Incidents by District",
                             color='gunshot_proportion', color_continuous_scale='YlOrRd')
                st.plotly_chart(fig, use_container_width=True)

        with tab3:
            by_area = violence_df[violence_df['group_type'] == 'community_area'] \
                .sort_values('total_incidents', ascending=False).head(20)
            if not by_area.empty:
                fig = px.bar(by_area, x='group_value', y='total_incidents',
                             title="Top 20 Community Areas by Violence",
                             color='gunshot_proportion', color_continuous_scale='Reds')
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No violence data. Run the Spark batch analytics first.")


# ════════════════════════════════════════════════
# 🗺️ HOTSPOT MAP
# ════════════════════════════════════════════════
elif section == "🗺️ Hotspot Map":
    st.subheader("🗺️ Crime Hotspot Map (K-Means Clusters)")

    hotspots_df = safe_read_sql("SELECT * FROM hotspots", conn)
    if not hotspots_df.empty:
        col1, col2 = st.columns([3, 1])

        with col1:
            fig = px.scatter_mapbox(
                hotspots_df,
                lat="latitude", lon="longitude",
                size="crime_count",
                color="crime_count",
                color_continuous_scale="YlOrRd",
                size_max=30,
                zoom=10,
                mapbox_style="carto-darkmatter",
                title="Crime Hotspot Clusters",
                hover_data=["cluster_id", "crime_count"]
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("#### Cluster Details")
            st.dataframe(
                hotspots_df[['cluster_id', 'latitude', 'longitude', 'crime_count']]
                .sort_values('crime_count', ascending=False),
                use_container_width=True
            )
    else:
        st.info("No hotspot data. Run the Spark batch analytics first.")


# ════════════════════════════════════════════════
# 🔗 CORRELATIONS
# ════════════════════════════════════════════════
elif section == "🔗 Correlations":
    st.subheader("🔗 Cross-Dataset Correlations")

    corr_df = safe_read_sql("SELECT * FROM correlations", conn)
    if not corr_df.empty:
        corr_types = corr_df['correlation_type'].unique()

        for ct in corr_types:
            st.markdown(f"#### {ct.replace('_', ' ').title()}")
            subset = corr_df[corr_df['correlation_type'] == ct]

            fig = px.scatter(
                subset, x='metric_1_value', y='metric_2_value',
                hover_data=['group_key'],
                labels={
                    'metric_1_value': subset['metric_1_name'].iloc[0],
                    'metric_2_value': subset['metric_2_name'].iloc[0]
                },
                title=f"{subset['metric_1_name'].iloc[0]} vs {subset['metric_2_name'].iloc[0]}",
                trendline="ols"
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(subset, use_container_width=True)
    else:
        st.info("No correlation data. Run the Spark batch analytics first.")


# ─── Footer ───
st.divider()
st.markdown(
    "<div style='text-align:center; color:#666; font-size:0.85rem;'>"
    "Chicago Crime Analytics Dashboard • Lambda Architecture • "
    "Spark + Kafka + Storm + PostgreSQL + MongoDB"
    "</div>",
    unsafe_allow_html=True
)
