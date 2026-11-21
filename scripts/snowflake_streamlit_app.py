"""Paste into Snowsight streamlit_app.py. No extra packages (pandas + altair ship with Streamlit)."""

import altair as alt
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Purchase orders", layout="wide")
session = get_active_session()

st.markdown(
    """
    <style>
      .block-container { padding-top: 1.2rem; }
      [data-testid="stMetricValue"] { font-variant-numeric: tabular-nums; }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    df = session.sql(sql).to_pandas()
    df.columns = [c.lower() for c in df.columns]
    return df


def money(series: pd.Series) -> str:
    return f"${series.sum():,.0f}"


def hbar(frame: pd.DataFrame, value: str, label: str, title: str, color: str) -> None:
    chart = (
        alt.Chart(frame)
        .mark_bar(cornerRadiusEnd=3, color=color)
        .encode(
            x=alt.X(f"{value}:Q", title=None, scale=alt.Scale(zero=True), axis=alt.Axis(format="$,.0f")),
            y=alt.Y(f"{label}:N", sort="-x", title=None),
            tooltip=[
                alt.Tooltip(label, title=""),
                alt.Tooltip(value, title="Amount", format="$,.0f"),
            ],
        )
        .properties(height=max(36 * len(frame), 160), title=title)
        .configure_view(strokeWidth=0)
        .configure_axis(labelLimit=220, grid=False)
        .configure_title(anchor="start", fontSize=16)
    )
    st.altair_chart(chart, use_container_width=True)


st.title("Purchase orders")
st.caption("MART · line grain, cost-centre allocations, goods receipts")

lines = query(
    """
    SELECT
        l.order_number,
        l.line_number,
        l.line_status,
        l.derived_line_fulfilment,
        l.category_code,
        s.supplier_name,
        l.active_net_amount,
        l.cancelled_net_amount,
        l.net_received_amount
    FROM PURCHASE_ORDER_DB.MART.FCT_PURCHASE_ORDER_LINES AS l
    LEFT JOIN PURCHASE_ORDER_DB.MART.DIM_SUPPLIER AS s
        ON l.supplier_code = s.supplier_code
    """
)
alloc = query(
    """
    SELECT
        COALESCE(cc.cost_center_name, a.cost_center_code) AS cost_center_name,
        a.allocated_active_net_amount
    FROM PURCHASE_ORDER_DB.MART.FCT_PO_COST_ALLOCATIONS AS a
    LEFT JOIN PURCHASE_ORDER_DB.MART.DIM_COST_CENTER AS cc
        ON a.cost_center_code = cc.cost_center_code
    """
)
receipts = query(
    """
    SELECT order_number, line_number, is_reversal, received_qty, received_net_amount
    FROM PURCHASE_ORDER_DB.MART.FCT_GOODS_RECEIPTS
    """
)

if lines.empty:
    st.warning("MART is empty. Run merge + dbt_build first.")
    st.stop()

for frame in (lines, alloc, receipts):
    for col in frame.columns:
        if "amount" in col or col.endswith("_qty"):
            frame[col] = pd.to_numeric(frame[col], errors="coerce")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Orders", f"{lines['order_number'].nunique():,}")
c2.metric("Lines", f"{len(lines):,}")
c3.metric("Active net", money(lines["active_net_amount"]))
c4.metric("Received net", money(lines["net_received_amount"]))

by_supplier = (
    lines.groupby("supplier_name", dropna=False)["active_net_amount"]
    .sum()
    .reset_index()
    .sort_values("active_net_amount", ascending=False)
)
by_cc = (
    alloc.groupby("cost_center_name", dropna=False)["allocated_active_net_amount"]
    .sum()
    .reset_index()
    .sort_values("allocated_active_net_amount", ascending=False)
)

left, right = st.columns(2)
with left:
    hbar(by_supplier, "active_net_amount", "supplier_name", "Active net by supplier", "#4C78A8")
with right:
    hbar(by_cc, "allocated_active_net_amount", "cost_center_name", "Allocated active net by cost centre", "#72B7B2")

fulfil = (
    lines["derived_line_fulfilment"]
    .value_counts()
    .rename_axis("fulfilment")
    .reset_index(name="lines")
)
receipts = receipts.copy()
receipts["kind"] = receipts["is_reversal"].map(
    {True: "reversal", False: "receipt", "TRUE": "reversal", "FALSE": "receipt"}
)
kinds = receipts.groupby("kind", dropna=False)["received_net_amount"].sum().reset_index()

left, right = st.columns(2)
with left:
    chart = (
        alt.Chart(fulfil)
        .mark_bar(cornerRadiusEnd=3, color="#F58518")
        .encode(
            x=alt.X("lines:Q", title="Lines", scale=alt.Scale(zero=True)),
            y=alt.Y(
                "fulfilment:N",
                sort="-x",
                title=None,
                axis=alt.Axis(labelOverlap=False, labelLimit=200),
            ),
            tooltip=["fulfilment", "lines"],
        )
        .properties(height=max(48 * len(fulfil), 180), title="Line fulfilment")
        .configure_view(strokeWidth=0)
        .configure_axis(grid=False)
        .configure_title(anchor="start", fontSize=16)
    )
    st.altair_chart(chart, use_container_width=True)
with right:
    chart = (
        alt.Chart(kinds)
        .mark_bar(cornerRadiusEnd=3, color="#E45756")
        .encode(
            x=alt.X("received_net_amount:Q", title=None, scale=alt.Scale(zero=True), axis=alt.Axis(format="$,.0f")),
            y=alt.Y("kind:N", sort="-x", title=None),
            tooltip=[alt.Tooltip("kind"), alt.Tooltip("received_net_amount", format="$,.0f")],
        )
        .properties(height=180, title="Receipts vs reversals")
        .configure_view(strokeWidth=0)
        .configure_axis(grid=False)
        .configure_title(anchor="start", fontSize=16)
    )
    st.altair_chart(chart, use_container_width=True)

st.divider()
st.subheader("Lines")

def _opts(series):
    return sorted(series.dropna().astype(str).unique())

f1, f2, f3, f4, f5 = st.columns(5)
order = f1.selectbox("Order", ["All", *_opts(lines["order_number"])])
status = f2.multiselect("Line status", _opts(lines["line_status"]))
fulfilment_f = f3.multiselect("Fulfilment", _opts(lines["derived_line_fulfilment"]))
category = f4.multiselect("Category", _opts(lines["category_code"]))
supplier = f5.multiselect("Supplier", _opts(lines["supplier_name"]))

detail = lines.copy()
detail["order_number"] = detail["order_number"].astype(str)
if order != "All":
    detail = detail[detail["order_number"] == order]
if status:
    detail = detail[detail["line_status"].astype(str).isin(status)]
if fulfilment_f:
    detail = detail[detail["derived_line_fulfilment"].astype(str).isin(fulfilment_f)]
if category:
    detail = detail[detail["category_code"].astype(str).isin(category)]
if supplier:
    detail = detail[detail["supplier_name"].astype(str).isin(supplier)]

st.caption(
    f"{len(detail):,} lines · active net ${detail['active_net_amount'].sum():,.0f}"
    f" · received ${detail['net_received_amount'].sum():,.0f}"
)
st.dataframe(
    detail.sort_values(["order_number", "line_number"]),
    use_container_width=True,
    hide_index=True,
    column_config={
        "active_net_amount": st.column_config.NumberColumn("Active net", format="$%.2f"),
        "cancelled_net_amount": st.column_config.NumberColumn("Cancelled net", format="$%.2f"),
        "net_received_amount": st.column_config.NumberColumn("Received net", format="$%.2f"),
    },
)
