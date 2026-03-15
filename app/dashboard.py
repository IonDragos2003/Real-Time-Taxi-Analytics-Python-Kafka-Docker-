"""
Streamlit live dashboard — consumes the FastAPI endpoints and displays
real-time analytics from the taxi pipeline.

Run with:
    streamlit run app/dashboard.py
"""

import time
import requests
import streamlit as st
import pandas as pd

API_BASE = "http://localhost:8000"

st.set_page_config(
    page_title="Taxi Analytics Dashboard",
    page_icon="🚕",
    layout="wide",
)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🚕 Taxi Analytics")
    st.markdown("Real-time stats from the Kafka pipeline.")
    st.divider()
    refresh = st.slider("Refresh interval (s)", min_value=1, max_value=30, value=5)
    st.caption(f"Auto-refreshing every {refresh}s")
    st.divider()
    st.markdown("**Pipeline**")
    st.markdown("- Producer → `taxi_rides.raw`")
    st.markdown("- Processor → `taxi_aggregates`")
    st.markdown("- FastAPI → `localhost:8000`")


# ── Data fetchers ─────────────────────────────────────────────────────────────

def fetch(endpoint: str):
    """Fetch JSON from the API. Returns (data, error_string)."""
    try:
        r = requests.get(f"{API_BASE}{endpoint}", timeout=3)
        r.raise_for_status()
        return r.json(), None
    except requests.exceptions.ConnectionError:
        return None, "API not reachable — is the FastAPI server running? (`python3 -m app.main --api`)"
    except Exception as e:
        return None, str(e)


# ── Session state for throughput tracking ─────────────────────────────────────
if "prev_cleaned" not in st.session_state:
    st.session_state.prev_cleaned = 0
    st.session_state.prev_ts = time.time()
    st.session_state.msgs_per_sec = 0.0


# ── Fetch all data up front ───────────────────────────────────────────────────
health_data, health_err = fetch("/health")
summary_data, summary_err = fetch("/stats/summary")
rides_data, rides_err = fetch("/stats/active-rides")
fare_data, fare_err = fetch("/stats/avg-fare")

# Update throughput
if health_data:
    now = time.time()
    elapsed = now - st.session_state.prev_ts
    delta = health_data["total_cleaned"] - st.session_state.prev_cleaned
    if elapsed > 0 and delta >= 0:
        st.session_state.msgs_per_sec = round(delta / elapsed, 1)
    st.session_state.prev_cleaned = health_data["total_cleaned"]
    st.session_state.prev_ts = now


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_metrics, tab_analytics, tab_health = st.tabs(["📊 Live Metrics", "🗺 Analytics", "🩺 Health"])


# ══ Tab 1: Live Metrics ═══════════════════════════════════════════════════════
with tab_metrics:
    st.header("Live Metrics")

    if health_err:
        st.error(health_err)
    elif health_data:
        total = health_data["total_cleaned"] + health_data["total_dlq"]

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Total Processed", f"{total:,}")
        col2.metric("Cleaned", f"{health_data['total_cleaned']:,}")
        col3.metric("DLQ", f"{health_data['total_dlq']:,}")
        col4.metric("Zones Tracked", health_data["zones_tracked"])
        col5.metric("Throughput (msg/s)", st.session_state.msgs_per_sec)

        st.divider()

        if total > 0:
            st.subheader("Message breakdown")
            df_breakdown = pd.DataFrame(
                {
                    "Category": ["Cleaned", "DLQ"],
                    "Count": [health_data["total_cleaned"], health_data["total_dlq"]],
                }
            )
            st.bar_chart(df_breakdown.set_index("Category"))
        else:
            st.info("No messages yet — run the producer and processor first.")
    else:
        st.info("Waiting for data...")


# ══ Tab 2: Analytics ══════════════════════════════════════════════════════════
with tab_analytics:
    st.header("Zone Analytics")

    if summary_err:
        st.error(summary_err)
    elif summary_data:
        top_rides = summary_data.get("top_zones_by_active_rides", [])
        top_fare = summary_data.get("top_zones_by_avg_fare", [])
        total_zones = summary_data.get("total_zones_tracked", 0)

        st.caption(f"Tracking {total_zones} zones total. Charts show top 10.")
        st.divider()

        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("Active Rides — last 5 min (Top 10 Zones)")
            if top_rides:
                df_rides = (
                    pd.DataFrame(top_rides)[["zone", "active_rides_5min"]]
                    .sort_values("active_rides_5min", ascending=True)
                )
                st.bar_chart(df_rides.set_index("zone"), horizontal=True)
            else:
                st.info("No active rides data yet.")

        with col_right:
            st.subheader("Avg Fare — last 15 min (Top 10 Zones)")
            if top_fare:
                df_fare = (
                    pd.DataFrame(top_fare)[["zone", "avg_fare_15min"]]
                    .sort_values("avg_fare_15min", ascending=True)
                )
                st.bar_chart(df_fare.set_index("zone"), horizontal=True)
            else:
                st.info("No avg fare data yet.")

        st.divider()
        st.subheader("All Zones — Full Table")

        if rides_err:
            st.error(rides_err)
        elif rides_data and fare_data:
            # Merge rides and fare dicts into one table
            rows = []
            all_zones = set(rides_data.keys()) | set(fare_data.keys())
            for zone in sorted(all_zones, key=lambda z: int(z) if z.isdigit() else 0):
                ride_info = rides_data.get(zone, {})
                fare_info = fare_data.get(zone, {})
                rows.append(
                    {
                        "Zone": zone,
                        "Active Rides (5m)": ride_info.get("active_rides_5min", 0),
                        "Avg Fare (15m) $": fare_info.get("avg_fare_15min", 0.0),
                        "Last Updated": ride_info.get("computed_at", "—")[:19].replace("T", " "),
                    }
                )
            df_all = pd.DataFrame(rows)
            st.dataframe(
                df_all,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Avg Fare (15m) $": st.column_config.NumberColumn(format="$%.2f"),
                    "Active Rides (5m)": st.column_config.NumberColumn(),
                },
            )
        else:
            st.info("No zone data yet.")
    else:
        st.info("Waiting for data...")


# ══ Tab 3: Health ═════════════════════════════════════════════════════════════
with tab_health:
    st.header("Pipeline Health")

    if health_err:
        st.error(health_err)
    elif health_data:
        total = health_data["total_cleaned"] + health_data["total_dlq"]
        dlq_pct = health_data["dlq_rate_pct"]

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Messages", f"{total:,}")
            st.metric("Valid (Cleaned)", f"{health_data['total_cleaned']:,}")
            st.metric("Rejected (DLQ)", f"{health_data['total_dlq']:,}")

        with col2:
            color = "normal" if dlq_pct < 5 else ("off" if dlq_pct < 20 else "inverse")
            st.metric("DLQ Rate", f"{dlq_pct}%")
            if dlq_pct == 0:
                st.success("DLQ rate is 0% — pipeline is clean.")
            elif dlq_pct < 5:
                st.warning(f"DLQ rate is {dlq_pct}% — within acceptable range.")
            else:
                st.error(f"DLQ rate is {dlq_pct}% — check your data or validation logic.")

        with col3:
            st.metric("Zones Tracked", health_data["zones_tracked"])
            st.metric("Throughput (msg/s)", st.session_state.msgs_per_sec)

        st.divider()
        st.subheader("API status")

        checks = [
            ("/health", health_data is not None),
            ("/stats/summary", summary_data is not None),
            ("/stats/active-rides", rides_data is not None),
            ("/stats/avg-fare", fare_data is not None),
        ]
        for endpoint, ok in checks:
            icon = "✅" if ok else "❌"
            st.markdown(f"{icon} `{endpoint}`")
    else:
        st.info("Waiting for data...")


# ── Auto-refresh ──────────────────────────────────────────────────────────────
st.caption(f"Last updated: {time.strftime('%H:%M:%S')} · refreshing in {refresh}s")
time.sleep(refresh)
st.rerun()
