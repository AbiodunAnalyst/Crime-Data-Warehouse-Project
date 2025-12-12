import streamlit as st
import pandas as pd
import plotly.express as px

# ------------------ PAGE CONFIG ------------------ #
st.set_page_config(
    page_title="Crime Data Warehouse – Business Insight Dashboards",
    page_icon="🚓",
    layout="wide",
)

# ------------------ DEFAULT FILE PATHS ------------------ #
DEFAULT_CC_PATH = "fact_crime_count.csv"
DEFAULT_CN_PATH = "fact_crime_num.csv"
DEFAULT_OT_PATH = "fact_occuring_time.csv"
DEFAULT_RES_PATH = "fact_resolution.csv"


# ------------------ HELPERS ------------------ #
@st.cache_data
def load_csv(path_or_file):
    df = pd.read_csv(path_or_file)
    # Standardise year/month & year_month
    if {"year", "month_number"}.issubset(df.columns):
        df["year"] = df["year"].astype(int)
        df["month_number"] = df["month_number"].astype(int)
        df["year_month"] = (
            df["year"].astype(str)
            + "-"
            + df["month_number"].astype(str).str.zfill(2)
        )
    else:
        df["year_month"] = "Unknown"
    return df


def sidebar_file_uploader(label, default_path, key):
    """Upload or fall back to default CSV."""
    file = st.sidebar.file_uploader(label, type=["csv"], key=key)
    if file is not None:
        st.sidebar.success("Using uploaded file ✅")
        return load_csv(file)
    else:
        try:
            st.sidebar.info(f"Using default file: `{default_path}`")
            return load_csv(default_path)
        except Exception as e:
            st.sidebar.error(f"Could not load `{default_path}`. Upload a CSV instead.\n\n{e}")
            return None


def add_basic_filters(df, prefix=""):
    """Return filtered df & show filters in sidebar for year, month, lsoa, crime type."""
    if df is None or df.empty:
        return df

    st.sidebar.markdown(f"**Filters – {prefix}**")

    df_f = df.copy()

    # Year filter
    if "year" in df_f.columns:
        years = sorted(df_f["year"].dropna().unique().tolist())
        selected_years = st.sidebar.multiselect(
            f"{prefix}Year", years, default=years
        )
        if selected_years:
            df_f = df_f[df_f["year"].isin(selected_years)]

    # Month filter
    if "month_number" in df_f.columns:
        months = sorted(df_f["month_number"].dropna().unique().tolist())
        selected_months = st.sidebar.multiselect(
            f"{prefix}Month (number)", months, default=months
        )
        if selected_months:
            df_f = df_f[df_f["month_number"].isin(selected_months)]

    # LSOA filter
    if "lsoa_name" in df_f.columns:
        lsoas = sorted(df_f["lsoa_name"].dropna().unique().tolist())
        selected_lsoas = st.sidebar.multiselect(
            f"{prefix}LSOA Name", lsoas, default=[]
        )
        if selected_lsoas:
            df_f = df_f[df_f["lsoa_name"].isin(selected_lsoas)]

    # Crime type filter
    if "crime_type" in df_f.columns:
        cts = sorted(df_f["crime_type"].dropna().unique().tolist())
        selected_ct = st.sidebar.multiselect(
            f"{prefix}Crime Type", cts, default=[]
        )
        if selected_ct:
            df_f = df_f[df_f["crime_type"].isin(selected_ct)]

    return df_f


def kpi_metric(col, label, value, fmt="{:,}"):
    col.metric(label, fmt.format(value))


# ------------------ SIDEBAR: DATA SOURCES ------------------ #
st.sidebar.header("📁 Data Sources")

cc_df = sidebar_file_uploader("Crime Count fact_crime_count.csv", DEFAULT_CC_PATH, key="cc")
cn_df = sidebar_file_uploader("Crime Volume & Strength fact_crime_num.csv", DEFAULT_CN_PATH, key="cn")
ot_df = sidebar_file_uploader("Crime Time Pattern fact_occuring_time.csv", DEFAULT_OT_PATH, key="ot")
res_df = sidebar_file_uploader("Resolution fact_resolution.csv", DEFAULT_RES_PATH, key="res")

st.sidebar.markdown("---")
st.sidebar.caption("Tip: Upload new CSVs to refresh the dashboards.")


# ------------------ MAIN TABS ------------------ #
tab_cc, tab_cn, tab_ot, tab_res, tab_data = st.tabs(
    [
        "📊 Crime Count",
        "👮 Crime Volume & Police Strength",
        "⏰ Crime Time Patterns",
        "✅ Resolution & Outcomes",
        "📂 Data Explorer",
    ]
)

# ============================================================
#  TAB 1 – CRIME COUNT (fact_crime_count)
# ============================================================
with tab_cc:
    st.title("📊 Crime Count Dashboard")

    if cc_df is None or cc_df.empty:
        st.warning("No data available for `fact_crime_count`. Check file or upload in sidebar.")
    else:
        filtered_cc = add_basic_filters(cc_df, prefix="[Count] ")

        if filtered_cc.empty:
            st.warning("No data after filters. Adjust filters in the sidebar.")
        else:
            total_crimes = int(filtered_cc["number_of_crime"].sum())
            total_lsoas = filtered_cc["lsoa_id"].nunique()
            total_locations = filtered_cc["location_id"].nunique()
            total_crime_types = filtered_cc["crime_type_id"].nunique()

            c1, c2, c3, c4 = st.columns(4)
            kpi_metric(c1, "Total Crimes", total_crimes)
            kpi_metric(c2, "Unique LSOAs", total_lsoas)
            kpi_metric(c3, "Unique Locations", total_locations)
            kpi_metric(c4, "Crime Types", total_crime_types)

            st.markdown("---")

            # Crimes over time
            col1, col2 = st.columns((2, 1))

            with col1:
                st.subheader("Crimes Over Time (Monthly)")
                ts = (
                    filtered_cc.groupby("year_month", as_index=False)["number_of_crime"]
                    .sum()
                    .sort_values("year_month")
                )
                fig_ts = px.bar(
                    ts,
                    x="year_month",
                    y="number_of_crime",
                    labels={"year_month": "Year-Month", "number_of_crime": "Number of Crimes"},
                )
                fig_ts.update_layout(margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig_ts, use_container_width=True, key="cc_timeseries")

            with col2:
                st.subheader("Top Crime Types")
                top_types = (
                    filtered_cc.groupby("crime_type", as_index=False)["number_of_crime"]
                    .sum()
                    .sort_values("number_of_crime", ascending=False)
                    .head(10)
                )
                fig_top_types = px.bar(
                    top_types,
                    x="number_of_crime",
                    y="crime_type",
                    orientation="h",
                    labels={"number_of_crime": "Number of Crimes", "crime_type": "Crime Type"},
                )
                fig_top_types.update_layout(
                    margin=dict(l=0, r=0, t=30, b=0),
                    yaxis={"categoryorder": "total ascending"},
                )
                st.plotly_chart(fig_top_types, use_container_width=True, key="cc_toptypes")

            st.markdown("---")

            col3, col4 = st.columns((1.4, 1.6))

            with col3:
                st.subheader("Crimes by LSOA")
                lsoa_sum = (
                    filtered_cc.groupby("lsoa_name", as_index=False)["number_of_crime"]
                    .sum()
                    .sort_values("number_of_crime", ascending=False)
                    .head(15)
                )
                fig_lsoa = px.bar(
                    lsoa_sum,
                    x="number_of_crime",
                    y="lsoa_name",
                    orientation="h",
                    labels={"number_of_crime": "Number of Crimes", "lsoa_name": "LSOA Name"},
                )
                fig_lsoa.update_layout(
                    margin=dict(l=0, r=0, t=30, b=0),
                    yaxis={"categoryorder": "total ascending"},
                )
                st.plotly_chart(fig_lsoa, use_container_width=True, key="cc_lsoa")

            with col4:
                st.subheader("Crime Locations Map")
                map_df = (
                    filtered_cc.groupby(
                        ["location", "longitude", "latitude"], as_index=False
                    )["number_of_crime"]
                    .sum()
                    .dropna(subset=["longitude", "latitude"])
                )
                if not map_df.empty:
                    fig_map = px.scatter_mapbox(
                        map_df,
                        lat="latitude",
                        lon="longitude",
                        size="number_of_crime",
                        hover_name="location",
                        hover_data={"number_of_crime": True, "latitude": False, "longitude": False},
                        zoom=9,
                    )
                    fig_map.update_layout(
                        mapbox_style="open-street-map",
                        margin=dict(l=0, r=0, t=0, b=0),
                    )
                    st.plotly_chart(fig_map, use_container_width=True, key="cc_map")
                else:
                    st.info("No valid coordinates to display on the map for current filters.")

# ============================================================
#  TAB 2 – CRIME VOLUME & POLICE STRENGTH (fact_crime_num)
# ============================================================
with tab_cn:
    st.title("👮 Crime Volume & Police Strength Dashboard")

    if cn_df is None or cn_df.empty:
        st.warning("No data available for `fact_crime_num`. Check file or upload in sidebar.")
    else:
        filtered_cn = add_basic_filters(cn_df, prefix="[Num] ")

        if filtered_cn.empty:
            st.warning("No data after filters. Adjust filters in the sidebar.")
        else:
            total_crimes = int(filtered_cn["number_of_crime"].sum())
            avg_officer = float(filtered_cn["police_officer_strength"].mean())
            avg_staff = float(filtered_cn["police_staff_strength"].mean())
            avg_pcso = float(filtered_cn["pcso_strength"].mean())

            c1, c2, c3, c4 = st.columns(4)
            kpi_metric(c1, "Total Crimes", total_crimes)
            kpi_metric(c2, "Avg Officer Strength", round(avg_officer, 1), "{:,.1f}")
            kpi_metric(c3, "Avg Staff Strength", round(avg_staff, 1), "{:,.1f}")
            kpi_metric(c4, "Avg PCSO Strength", round(avg_pcso, 1), "{:,.1f}")

            st.markdown("---")

            col1, col2 = st.columns((2, 1))

            with col1:
                st.subheader("Crimes Over Time vs Officer Strength")
                ts = (
                    filtered_cn.groupby("year_month", as_index=False)
                    .agg(
                        number_of_crime=("number_of_crime", "sum"),
                        police_officer_strength=("police_officer_strength", "mean"),
                    )
                    .sort_values("year_month")
                )
                fig_ts = px.bar(
                    ts,
                    x="year_month",
                    y="number_of_crime",
                    labels={"year_month": "Year-Month", "number_of_crime": "Number of Crimes"},
                )
                fig_ts.update_layout(margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig_ts, use_container_width=True, key="cn_timeseries")

            with col2:
                st.subheader("Crime vs Officer Strength (Scatter)")
                fig_scatter = px.scatter(
                    filtered_cn,
                    x="police_officer_strength",
                    y="number_of_crime",
                    color="crime_type",
                    hover_data=["lsoa_name", "location", "year_month"],
                    labels={
                        "police_officer_strength": "Police Officer Strength",
                        "number_of_crime": "Number of Crimes",
                    },
                )
                fig_scatter.update_layout(margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig_scatter, use_container_width=True, key="cn_scatter")

            st.markdown("---")

            st.subheader("Crime by LSOA and Police Strength")
            lsoa_strength = (
                filtered_cn.groupby("lsoa_name", as_index=False)
                .agg(
                    number_of_crime=("number_of_crime", "sum"),
                    police_officer_strength=("police_officer_strength", "mean"),
                )
                .sort_values("number_of_crime", ascending=False)
                .head(15)
            )

            fig_lsoa_strength = px.scatter(
                lsoa_strength,
                x="police_officer_strength",
                y="number_of_crime",
                text="lsoa_name",
                labels={
                    "police_officer_strength": "Avg Officer Strength",
                    "number_of_crime": "Number of Crimes",
                },
            )
            fig_lsoa_strength.update_traces(textposition="top center")
            fig_lsoa_strength.update_layout(margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig_lsoa_strength, use_container_width=True, key="cn_lsoa_strength")

# ============================================================
#  TAB 3 – CRIME TIME PATTERNS (fact_occuring_time)
# ============================================================
with tab_ot:
    st.title("⏰ Crime Time Patterns Dashboard")

    if ot_df is None or ot_df.empty:
        st.warning("No data available for `fact_occuring_time`. Check file or upload in sidebar.")
    else:
        filtered_ot = add_basic_filters(ot_df, prefix="[Time] ")

        if filtered_ot.empty:
            st.warning("No data after filters. Adjust filters in the sidebar.")
        else:
            total_crimes = int(filtered_ot["number_of_crime_occuring"].sum())
            total_lsoas = filtered_ot["lsoa_id"].nunique()
            total_locations = filtered_ot["location_id"].nunique()
            total_crime_types = filtered_ot["crime_type_id"].nunique()

            c1, c2, c3, c4 = st.columns(4)
            kpi_metric(c1, "Total Crimes (Time Fact)", total_crimes)
            kpi_metric(c2, "Unique LSOAs", total_lsoas)
            kpi_metric(c3, "Unique Locations", total_locations)
            kpi_metric(c4, "Crime Types", total_crime_types)

            st.markdown("---")

            # If day_of_week exists, show weekly pattern
            if "day_of_week" in filtered_ot.columns:
                st.subheader("Crimes by Day of Week")
                dow_sum = (
                    filtered_ot.groupby("day_of_week", as_index=False)["number_of_crime_occuring"]
                    .sum()
                )
                # Optional: order days if you use Mon–Sun codes.
                fig_dow = px.bar(
                    dow_sum,
                    x="day_of_week",
                    y="number_of_crime_occuring",
                    labels={
                        "day_of_week": "Day of Week",
                        "number_of_crime_occuring": "Number of Crimes",
                    },
                )
                fig_dow.update_layout(margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig_dow, use_container_width=True, key="ot_dow")
            else:
                st.info("No `day_of_week` column in fact_occuring_time. Showing monthly trend instead.")
                ts = (
                    filtered_ot.groupby("year_month", as_index=False)["number_of_crime_occuring"]
                    .sum()
                    .sort_values("year_month")
                )
                fig_ts = px.bar(
                    ts,
                    x="year_month",
                    y="number_of_crime_occuring",
                    labels={
                        "year_month": "Year-Month",
                        "number_of_crime_occuring": "Number of Crimes",
                    },
                )
                fig_ts.update_layout(margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig_ts, use_container_width=True, key="ot_timeseries")

            st.markdown("---")

            st.subheader("Top Crime Types (Time Fact)")
            ct_sum = (
                filtered_ot.groupby("crime_type", as_index=False)["number_of_crime_occuring"]
                .sum()
                .sort_values("number_of_crime_occuring", ascending=False)
                .head(10)
            )
            fig_ct = px.bar(
                ct_sum,
                x="number_of_crime_occuring",
                y="crime_type",
                orientation="h",
                labels={
                    "number_of_crime_occuring": "Number of Crimes",
                    "crime_type": "Crime Type",
                },
            )
            fig_ct.update_layout(
                margin=dict(l=0, r=0, t=30, b=0),
                yaxis={"categoryorder": "total ascending"},
            )
            st.plotly_chart(fig_ct, use_container_width=True, key="ot_toptypes")

# ============================================================
#  TAB 4 – RESOLUTION & OUTCOMES (fact_resolution)
# ============================================================
with tab_res:
    st.title("✅ Resolution & Outcomes Dashboard")

    if res_df is None or res_df.empty:
        st.warning("No data available for `fact_resolution`. Check file or upload in sidebar.")
    else:
        filtered_res = add_basic_filters(res_df, prefix="[Res] ")

        if filtered_res.empty:
            st.warning("No data after filters. Adjust filters in the sidebar.")
        else:
            total_resolutions = int(filtered_res["number_of_resolution"].sum())
            total_outcomes = filtered_res["outcome_id"].nunique()
            total_crime_types = filtered_res["crime_type_id"].nunique()
            total_lsoas = filtered_res["lsoa_id"].nunique()

            c1, c2, c3, c4 = st.columns(4)
            kpi_metric(c1, "Total Resolutions", total_resolutions)
            kpi_metric(c2, "Distinct Outcomes", total_outcomes)
            kpi_metric(c3, "Crime Types Involved", total_crime_types)
            kpi_metric(c4, "LSOAs Affected", total_lsoas)

            st.markdown("---")

            col1, col2 = st.columns((2, 1))

            with col1:
                st.subheader("Resolutions Over Time (Monthly)")
                ts = (
                    filtered_res.groupby("year_month", as_index=False)["number_of_resolution"]
                    .sum()
                    .sort_values("year_month")
                )
                fig_ts = px.bar(
                    ts,
                    x="year_month",
                    y="number_of_resolution",
                    labels={
                        "year_month": "Year-Month",
                        "number_of_resolution": "Number of Resolutions",
                    },
                )
                fig_ts.update_layout(margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig_ts, use_container_width=True, key="res_timeseries")

            with col2:
                st.subheader("Top Outcomes")
                outcome_sum = (
                    filtered_res.groupby("last_outcome_category", as_index=False)["number_of_resolution"]
                    .sum()
                    .sort_values("number_of_resolution", ascending=False)
                    .head(10)
                )
                fig_outcome = px.bar(
                    outcome_sum,
                    x="number_of_resolution",
                    y="last_outcome_category",
                    orientation="h",
                    labels={
                        "number_of_resolution": "Number of Resolutions",
                        "last_outcome_category": "Outcome",
                    },
                )
                fig_outcome.update_layout(
                    margin=dict(l=0, r=0, t=30, b=0),
                    yaxis={"categoryorder": "total ascending"},
                )
                st.plotly_chart(fig_outcome, use_container_width=True, key="res_top_outcomes")

            st.markdown("---")

            st.subheader("Crime Types by Outcome")
            ct_outcome = (
                filtered_res.groupby(["crime_type", "last_outcome_category"], as_index=False)
                ["number_of_resolution"]
                .sum()
            )
            fig_ct_out = px.treemap(
                ct_outcome,
                path=["crime_type", "last_outcome_category"],
                values="number_of_resolution",
            )
            fig_ct_out.update_layout(margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig_ct_out, use_container_width=True, key="res_tree")

# ============================================================
#  TAB 5 – DATA EXPLORER
# ============================================================
with tab_data:
    st.title("📂 Data Explorer")

    st.markdown("### Choose a dataset to explore")
    dataset_name = st.selectbox(
        "Dataset",
        [
            "fact_crime_count",
            "fact_crime_num",
            "fact_occuring_time",
            "fact_resolution",
        ],
    )

    df_map = {
        "fact_crime_count": cc_df,
        "fact_crime_num": cn_df,
        "fact_occuring_time": ot_df,
        "fact_resolution": res_df,
    }

    df_selected = df_map.get(dataset_name)

    if df_selected is None or df_selected.empty:
        st.warning(f"No data loaded for `{dataset_name}`.")
    else:
        st.write(f"Rows: **{len(df_selected):,}**, Columns: **{len(df_selected.columns)}**")

        with st.expander("Preview data (first 300 rows)", expanded=True):
            st.dataframe(df_selected.head(300))

        st.markdown("### Column Summary")
        col_summary = pd.DataFrame({
            "column": df_selected.columns,
            "dtype": df_selected.dtypes.astype(str),
            "n_unique": [df_selected[c].nunique() for c in df_selected.columns],
            "n_missing": [df_selected[c].isna().sum() for c in df_selected.columns],
        })
        st.dataframe(col_summary)
