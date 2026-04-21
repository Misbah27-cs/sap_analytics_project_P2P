"""
SAP P2P Analytics - Analytics & Visualization Engine
Author  : S M Misbahul Haque | Roll: 23052832 | CSE
"""
import os, sqlite3
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH    = os.path.join(BASE_DIR, "data", "p2p_analytics.db")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

COLORS = ["#1B4F72","#2980B9","#27AE60","#E67E22","#8E44AD",
          "#E74C3C","#F39C12","#1ABC9C","#D35400","#2C3E50"]
BG   = "#F7F9FC"
GRID = "#DDE3EC"

def load_data():
    with sqlite3.connect(DB_PATH) as conn:
        df      = pd.read_sql("SELECT * FROM fact_purchases", conn)
        monthly = pd.read_sql("SELECT * FROM agg_monthly_purchases", conn)
    df["PO_Date"] = pd.to_datetime(df["PO_Date"])
    return df, monthly

def kpi_summary(df):
    return {
        "Total POs"           : len(df),
        "Total PO Value"      : df["PO_Value"].sum(),
        "Total Invoice Amount": df["Invoice_Amount"].sum(),
        "Total Tax Paid"      : df["Tax_Amount"].sum(),
        "Unique Vendors"      : df["Vendor_ID"].nunique(),
        "Avg Cycle Time (Days)": round(df["Total_Cycle_Days"].mean(), 1),
        "Avg PO to GR (Days)" : round(df["PO_to_GR_Days"].mean(), 1),
    }

def chart_monthly_spend(monthly):
    fig, ax = plt.subplots(figsize=(11, 5), facecolor=BG)
    ax.set_facecolor(BG)
    x = np.arange(len(monthly))
    pov = monthly["Total_PO_Value"] / 1e5
    inv = monthly["Total_Invoice"]  / 1e5
    ax.bar(x-0.2, pov, 0.38, label="PO Value",      color=COLORS[0], alpha=0.88)
    ax.bar(x+0.2, inv, 0.38, label="Invoice Amount", color=COLORS[2], alpha=0.88)
    ax.plot(x, pov, "o--", color=COLORS[0], lw=1.5)
    ax.set_xticks(x)
    ax.set_xticklabels(monthly["Month"], rotation=30, ha="right", fontsize=9)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_: f"Rs{v:.0f}L"))
    ax.set_title("Monthly Procurement Spend (PO Value vs Invoice Amount)", fontsize=13, fontweight="bold", pad=10)
    ax.set_ylabel("Amount (Lakhs Rs)", fontsize=10)
    ax.legend(fontsize=10)
    ax.grid(axis="y", color=GRID, linestyle="--", linewidth=0.7)
    ax.spines[["top","right"]].set_visible(False)
    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "chart1_monthly_spend.png")
    plt.savefig(path, dpi=150, bbox_inches="tight"); plt.close()
    print(f"  [OK] {path}"); return path

def chart_top_vendors(df):
    top = df.groupby("Vendor_Name")["PO_Value"].sum().sort_values(ascending=True).tail(10) / 1e5
    fig, ax = plt.subplots(figsize=(10, 6), facecolor=BG)
    ax.set_facecolor(BG)
    bars = ax.barh(top.index, top.values, color=COLORS[:10], height=0.6, edgecolor="white")
    for bar, val in zip(bars, top.values):
        ax.text(bar.get_width()+0.3, bar.get_y()+bar.get_height()/2,
                f"Rs{val:.1f}L", va="center", fontsize=9, color="#2C3E50")
    ax.set_title("Top Vendors by Total PO Value", fontsize=13, fontweight="bold", pad=10)
    ax.set_xlabel("PO Value (Lakhs Rs)", fontsize=10)
    ax.grid(axis="x", color=GRID, linestyle="--", linewidth=0.7)
    ax.spines[["top","right"]].set_visible(False)
    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "chart2_top_vendors.png")
    plt.savefig(path, dpi=150, bbox_inches="tight"); plt.close()
    print(f"  [OK] {path}"); return path

def chart_category_pie(df):
    cat = df.groupby("Category")["PO_Value"].sum()
    fig, ax = plt.subplots(figsize=(7, 7), facecolor=BG)
    wedges, texts, autotexts = ax.pie(
        cat.values, labels=cat.index, autopct="%1.1f%%",
        colors=COLORS[:len(cat)], startangle=140,
        wedgeprops=dict(edgecolor="white", linewidth=1.5),
        textprops=dict(fontsize=11))
    for at in autotexts: at.set_fontsize(10); at.set_fontweight("bold")
    ax.set_title("Spend Distribution by Category", fontsize=13, fontweight="bold", pad=16)
    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "chart3_category_pie.png")
    plt.savefig(path, dpi=150, bbox_inches="tight"); plt.close()
    print(f"  [OK] {path}"); return path

def chart_buyer_performance(df):
    buyer = df.groupby("Buyer").agg(Spend=("PO_Value","sum"), POs=("PO_Number","count")).sort_values("Spend", ascending=False)
    buyer["Spend_L"] = buyer["Spend"] / 1e5
    fig, ax1 = plt.subplots(figsize=(9, 5), facecolor=BG)
    ax1.set_facecolor(BG)
    ax2 = ax1.twinx()
    x = np.arange(len(buyer))
    ax1.bar(x, buyer["Spend_L"], color=COLORS[0], alpha=0.85, width=0.5, label="Spend (L)")
    ax2.plot(x, buyer["POs"], "D--", color=COLORS[3], lw=2, ms=8, label="PO Count")
    ax1.set_xticks(x); ax1.set_xticklabels(buyer.index, fontsize=10)
    ax1.set_ylabel("Spend (Lakhs Rs)", fontsize=10, color=COLORS[0])
    ax2.set_ylabel("PO Count",         fontsize=10, color=COLORS[3])
    ax1.set_title("Buyer Performance — Spend vs PO Count", fontsize=13, fontweight="bold", pad=10)
    ax1.grid(axis="y", color=GRID, linestyle="--", linewidth=0.7)
    ax1.spines[["top","right"]].set_visible(False)
    ax2.spines[["top","left"]].set_visible(False)
    lines1,lbl1 = ax1.get_legend_handles_labels()
    lines2,lbl2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1+lines2, lbl1+lbl2, fontsize=9)
    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "chart4_buyer_performance.png")
    plt.savefig(path, dpi=150, bbox_inches="tight"); plt.close()
    print(f"  [OK] {path}"); return path

def chart_regional_spend(df):
    reg = df.groupby("Region")["PO_Value"].sum().sort_values(ascending=False) / 1e5
    fig, ax = plt.subplots(figsize=(8, 5), facecolor=BG)
    ax.set_facecolor(BG)
    bars = ax.bar(reg.index, reg.values, color=COLORS[:len(reg)], width=0.5, edgecolor="white")
    for bar, val in zip(bars, reg.values):
        ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.5,
                f"Rs{val:.1f}L", ha="center", fontsize=10, fontweight="bold", color="#2C3E50")
    ax.set_title("Procurement Spend by Region", fontsize=13, fontweight="bold", pad=10)
    ax.set_ylabel("PO Value (Lakhs Rs)", fontsize=10)
    ax.grid(axis="y", color=GRID, linestyle="--", linewidth=0.7)
    ax.spines[["top","right"]].set_visible(False)
    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "chart5_regional_spend.png")
    plt.savefig(path, dpi=150, bbox_inches="tight"); plt.close()
    print(f"  [OK] {path}"); return path

def chart_spend_tier(df):
    tier = df["Spend_Tier"].value_counts()
    fig, ax = plt.subplots(figsize=(7, 5), facecolor=BG)
    ax.set_facecolor(BG)
    bars = ax.bar(tier.index, tier.values, color=[COLORS[3],COLORS[1],COLORS[2]], width=0.4, edgecolor="white")
    for bar, val in zip(bars, tier.values):
        ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.3,
                str(val), ha="center", fontsize=11, fontweight="bold")
    ax.set_title("PO Distribution by Spend Tier", fontsize=13, fontweight="bold", pad=10)
    ax.set_ylabel("Number of POs", fontsize=10)
    ax.grid(axis="y", color=GRID, linestyle="--", linewidth=0.7)
    ax.spines[["top","right"]].set_visible(False)
    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "chart6_spend_tier.png")
    plt.savefig(path, dpi=150, bbox_inches="tight"); plt.close()
    print(f"  [OK] {path}"); return path

def run_analytics():
    print("\n" + "="*60)
    print("   SAP P2P Analytics — Generating Charts & KPIs")
    print("="*60)
    df, monthly = load_data()
    kpis = kpi_summary(df)
    print("\nKPI Summary:")
    for k, v in kpis.items():
        print(f"   {k:<28}: {v}")
    print("\nGenerating Charts...")
    paths = {
        "monthly_spend"    : chart_monthly_spend(monthly),
        "top_vendors"      : chart_top_vendors(df),
        "category_pie"     : chart_category_pie(df),
        "buyer_performance": chart_buyer_performance(df),
        "regional_spend"   : chart_regional_spend(df),
        "spend_tier"       : chart_spend_tier(df),
    }
    print("\n" + "="*60)
    print("   All analytics complete ✔")
    print("="*60 + "\n")
    return df, monthly, kpis, paths

if __name__ == "__main__":
    run_analytics()
