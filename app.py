"""
SAP P2P Analytics — Interactive Dashboard
==========================================
Author  : S M Misbahul Haque
Roll No : 23052832
Branch  : CSE

Run: python app.py  →  Open: http://127.0.0.1:8050
"""
import os, sys, sqlite3
import pandas as pd

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
sys.path.insert(0, SCRIPTS_DIR)

from etl_pipeline import run_pipeline, DB_PATH
from analytics    import run_analytics

import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import plotly.graph_objects as go

run_pipeline()
df, monthly, kpis, _ = run_analytics()

C_PRIMARY = "#1B4F72"
C_BG      = "#F7F9FC"
C_CARD    = "#FFFFFF"
C_TEXT    = "#2C3E50"

app = dash.Dash(__name__, title="SAP P2P Analytics Dashboard")

def kpi_card(label, value, color="#1B4F72"):
    return html.Div([
        html.P(label, style={"margin":"0","fontSize":"12px","color":"#7F8C8D","fontWeight":"600"}),
        html.H3(str(value), style={"margin":"4px 0 0","color":color,"fontSize":"21px"}),
    ], style={"background":C_CARD,"borderRadius":"10px","padding":"14px 18px",
              "boxShadow":"0 2px 8px rgba(0,0,0,0.08)","flex":"1","minWidth":"140px",
              "borderTop":f"4px solid {color}"})

def fmt_inr(v):
    if v >= 1e7: return f"Rs {v/1e7:.1f}Cr"
    if v >= 1e5: return f"Rs {v/1e5:.1f}L"
    return f"Rs {v:,.0f}"

kpi_row = html.Div([
    kpi_card("Total POs",          kpis["Total POs"],                          "#1B4F72"),
    kpi_card("Total Spend",        fmt_inr(kpis["Total PO Value"]),             "#27AE60"),
    kpi_card("Total Invoiced",     fmt_inr(kpis["Total Invoice Amount"]),       "#8E44AD"),
    kpi_card("Tax Paid",           fmt_inr(kpis["Total Tax Paid"]),             "#E67E22"),
    kpi_card("Unique Vendors",     kpis["Unique Vendors"],                      "#2980B9"),
    kpi_card("Avg Cycle Time",     f"{kpis['Avg Cycle Time (Days)']} days",    "#E74C3C"),
], style={"display":"flex","gap":"12px","flexWrap":"wrap","marginBottom":"22px"})

app.layout = html.Div([
    html.Div([
        html.Div([
            html.H1("SAP P2P Procurement Analytics Dashboard",
                    style={"margin":"0","color":"white","fontSize":"22px"}),
            html.P("Procure-to-Pay | SAP MM | Data Engineering Pipeline",
                   style={"margin":"4px 0 0","color":"#BDC3C7","fontSize":"12px"}),
        ]),
        html.Div([
            html.P("S M Misbahul Haque  |  Roll: 23052832  |  CSE",
                   style={"color":"#BDC3C7","margin":"0","fontSize":"12px","textAlign":"right"}),
        ])
    ], style={"background":f"linear-gradient(135deg,{C_PRIMARY},#0D2B4E)",
              "padding":"18px 28px","display":"flex","justifyContent":"space-between",
              "alignItems":"center","borderRadius":"0 0 12px 12px","marginBottom":"24px",
              "boxShadow":"0 4px 12px rgba(27,79,114,0.3)"}),

    html.Div([
        kpi_row,
        html.Div([
            html.Div([
                html.Label("Filter by Region:", style={"fontWeight":"600","fontSize":"12px"}),
                dcc.Dropdown(id="region-filter",
                    options=[{"label":"All Regions","value":"ALL"}]+
                            [{"label":r,"value":r} for r in sorted(df["Region"].unique())],
                    value="ALL", clearable=False, style={"fontSize":"12px"})
            ], style={"flex":"1","minWidth":"180px"}),
            html.Div([
                html.Label("Filter by Category:", style={"fontWeight":"600","fontSize":"12px"}),
                dcc.Dropdown(id="category-filter",
                    options=[{"label":"All Categories","value":"ALL"}]+
                            [{"label":c,"value":c} for c in sorted(df["Category"].unique())],
                    value="ALL", clearable=False, style={"fontSize":"12px"})
            ], style={"flex":"1","minWidth":"180px"}),
        ], style={"display":"flex","gap":"18px","marginBottom":"22px",
                  "background":C_CARD,"padding":"14px 18px","borderRadius":"10px",
                  "boxShadow":"0 2px 8px rgba(0,0,0,0.06)"}),

        html.Div([
            html.Div([dcc.Graph(id="chart-monthly")],
                     style={"flex":"2","background":C_CARD,"borderRadius":"10px",
                            "padding":"10px","boxShadow":"0 2px 8px rgba(0,0,0,0.06)"}),
            html.Div([dcc.Graph(id="chart-category")],
                     style={"flex":"1","background":C_CARD,"borderRadius":"10px",
                            "padding":"10px","boxShadow":"0 2px 8px rgba(0,0,0,0.06)"}),
        ], style={"display":"flex","gap":"14px","marginBottom":"14px"}),

        html.Div([
            html.Div([dcc.Graph(id="chart-vendors")],
                     style={"flex":"1","background":C_CARD,"borderRadius":"10px",
                            "padding":"10px","boxShadow":"0 2px 8px rgba(0,0,0,0.06)"}),
            html.Div([dcc.Graph(id="chart-region")],
                     style={"flex":"1","background":C_CARD,"borderRadius":"10px",
                            "padding":"10px","boxShadow":"0 2px 8px rgba(0,0,0,0.06)"}),
            html.Div([dcc.Graph(id="chart-buyer")],
                     style={"flex":"1","background":C_CARD,"borderRadius":"10px",
                            "padding":"10px","boxShadow":"0 2px 8px rgba(0,0,0,0.06)"}),
        ], style={"display":"flex","gap":"14px","marginBottom":"14px"}),

        html.Div([
            html.H3("Recent Purchase Orders",
                    style={"margin":"0 0 10px","color":C_TEXT,"fontSize":"15px"}),
            html.Div(id="po-table")
        ], style={"background":C_CARD,"borderRadius":"10px","padding":"18px",
                  "boxShadow":"0 2px 8px rgba(0,0,0,0.06)"}),
    ], style={"padding":"0 28px 28px"}),
], style={"background":C_BG,"minHeight":"100vh","fontFamily":"Arial,sans-serif"})


@app.callback(
    Output("chart-monthly",  "figure"),
    Output("chart-category", "figure"),
    Output("chart-vendors",  "figure"),
    Output("chart-region",   "figure"),
    Output("chart-buyer",    "figure"),
    Output("po-table",       "children"),
    Input("region-filter",   "value"),
    Input("category-filter", "value"),
)
def update(region, category):
    fdf = df.copy()
    if region   != "ALL": fdf = fdf[fdf["Region"]  == region]
    if category != "ALL": fdf = fdf[fdf["Category"]== category]

    # Monthly spend
    mf = fdf.groupby("Month").agg(PO=("PO_Value","sum"), Inv=("Invoice_Amount","sum")).reset_index()
    fig_m = go.Figure()
    fig_m.add_trace(go.Bar(x=mf["Month"], y=mf["PO"]/1e5,  name="PO Value",      marker_color="#1B4F72"))
    fig_m.add_trace(go.Bar(x=mf["Month"], y=mf["Inv"]/1e5, name="Invoice Amount", marker_color="#27AE60"))
    fig_m.update_layout(title="Monthly Procurement Spend", barmode="group",
                        yaxis_title="Amount (Lakhs Rs)", plot_bgcolor=C_BG,
                        paper_bgcolor=C_CARD, margin=dict(t=40,b=40,l=40,r=20))

    # Category pie
    cf = fdf.groupby("Category")["PO_Value"].sum().reset_index()
    fig_c = px.pie(cf, values="PO_Value", names="Category",
                   title="Spend by Category",
                   color_discrete_sequence=px.colors.qualitative.Bold)
    fig_c.update_layout(paper_bgcolor=C_CARD, margin=dict(t=40,b=20,l=20,r=20))

    # Top vendors
    vf = fdf.groupby("Vendor_Name")["PO_Value"].sum().nlargest(8).reset_index()
    fig_v = px.bar(vf, x="PO_Value", y="Vendor_Name", orientation="h",
                   title="Top Vendors by Spend", color="PO_Value",
                   color_continuous_scale="Blues")
    fig_v.update_layout(paper_bgcolor=C_CARD, plot_bgcolor=C_BG,
                        margin=dict(t=40,b=40,l=10,r=20),
                        showlegend=False, coloraxis_showscale=False)

    # Regional spend
    rf = fdf.groupby("Region")["PO_Value"].sum().reset_index()
    fig_r = px.bar(rf, x="Region", y="PO_Value", title="Spend by Region",
                   color="Region", color_discrete_sequence=px.colors.qualitative.Pastel)
    fig_r.update_layout(paper_bgcolor=C_CARD, plot_bgcolor=C_BG,
                        margin=dict(t=40,b=40,l=40,r=20), showlegend=False)

    # Buyer scatter
    bf = fdf.groupby("Buyer").agg(Spend=("PO_Value","sum"), POs=("PO_Number","count")).reset_index()
    fig_b = px.scatter(bf, x="POs", y="Spend", text="Buyer",
                       title="Buyer: POs vs Spend", size="Spend",
                       color="Spend", color_continuous_scale="Viridis")
    fig_b.update_traces(textposition="top center")
    fig_b.update_layout(paper_bgcolor=C_CARD, plot_bgcolor=C_BG,
                        margin=dict(t=40,b=40,l=40,r=20), coloraxis_showscale=False)

    # PO table
    tbl = fdf.sort_values("PO_Date", ascending=False).head(10)[
        ["PO_Number","Vendor_Name","Material_Name","PO_Value","PO_Status","PO_Date"]
    ].copy()
    tbl["PO_Value"] = tbl["PO_Value"].apply(lambda x: f"Rs {x:,.0f}")
    tbl["PO_Date"]  = pd.to_datetime(tbl["PO_Date"]).dt.strftime("%d %b %Y")
    tbl.columns     = ["PO No","Vendor","Material","PO Value","Status","Date"]

    hs = {"background":C_PRIMARY,"color":"white","padding":"9px 12px","fontSize":"11px","fontWeight":"700"}
    re = {"background":"#F0F4FF","padding":"8px 12px","fontSize":"11px"}
    ro = {"background":"white",  "padding":"8px 12px","fontSize":"11px"}
    table = html.Table([
        html.Thead(html.Tr([html.Th(c, style=hs) for c in tbl.columns])),
        html.Tbody([html.Tr([html.Td(tbl.iloc[i][c], style=re if i%2==0 else ro)
                             for c in tbl.columns]) for i in range(len(tbl))])
    ], style={"width":"100%","borderCollapse":"collapse"})

    return fig_m, fig_c, fig_v, fig_r, fig_b, table


if __name__ == "__main__":
    print("\n" + "="*60)
    print("   SAP P2P Analytics Dashboard")
    print("   Author : S M Misbahul Haque | Roll: 23052832 | CSE")
    print("   URL    : http://127.0.0.1:8050")
    print("="*60 + "\n")
    app.run(debug=True)
