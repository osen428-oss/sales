from flask import Flask, render_template, request, redirect, url_for
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
import sqlite3
import json

app = Flask(__name__)

BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)

DB_PATH = BASE_DIR / "sales.db"

REQUIRED_COLS = {"date", "order_id", "product", "quantity", "unit_price"}


# -------------------------
# DB
# -------------------------
def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db_connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                original_filename TEXT NOT NULL,
                kpi_json TEXT NOT NULL,
                top_products_json TEXT NOT NULL,
                daily_json TEXT NOT NULL,
                chart_file TEXT NOT NULL
            )
        """)
        conn.commit()


# -------------------------
# Data
# -------------------------
def load_and_clean(csv_file) -> pd.DataFrame:
    df = pd.read_csv(csv_file)

    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"Eksik sütun(lar): {', '.join(sorted(missing))}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    df = df.dropna(subset=["date", "order_id", "product", "quantity", "unit_price"])
    df = df[df["quantity"] > 0]
    df = df[df["unit_price"] >= 0]

    df["revenue"] = df["quantity"] * df["unit_price"]
    return df


def make_daily_revenue_chart(df: pd.DataFrame) -> str:
    daily = (
        df.groupby(df["date"].dt.date)["revenue"]
        .sum()
        .reset_index(name="revenue")
        .rename(columns={"date": "day"})
        .sort_values("day")
    )

    fig = plt.figure()
    plt.plot(daily["day"], daily["revenue"])
    plt.xlabel("Date")
    plt.ylabel("Revenue")
    plt.xticks(rotation=45)
    plt.tight_layout()

    filename = f"daily_revenue_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    outpath = STATIC_DIR / filename
    fig.savefig(outpath, dpi=150)
    plt.close(fig)

    return filename


def summarize(df: pd.DataFrame) -> dict:
    total_revenue = float(df["revenue"].sum())
    total_orders = int(df["order_id"].nunique())
    total_items = int(df["quantity"].sum())
    aov = float(total_revenue / total_orders) if total_orders else 0.0

    top_products = (
        df.groupby("product")
        .agg(
            revenue=("revenue", "sum"),
            units=("quantity", "sum"),
            orders=("order_id", "nunique"),
        )
        .sort_values(["revenue", "units"], ascending=False)
        .head(10)
        .reset_index()
    )

    daily = (
        df.groupby(df["date"].dt.date)
        .agg(
            revenue=("revenue", "sum"),
            orders=("order_id", "nunique"),
            units=("quantity", "sum"),
        )
        .reset_index()
        .rename(columns={"date": "day"})
        .sort_values("day", ascending=False)
        .head(14)
    )

    # ✅ JSON için: date objesini string'e çevir
    daily["day"] = daily["day"].astype(str)

    return {
        "kpi": {
            "total_revenue": total_revenue,
            "total_orders": total_orders,
            "total_items": total_items,
            "aov": aov,
        },
        "top_products": top_products,
        "daily": daily,
    }



# -------------------------
# Routes
# -------------------------
@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


@app.route("/report", methods=["POST"])
def report_create():
    file = request.files.get("file")
    if not file or file.filename.strip() == "":
        return render_template("index.html", error="CSV seçmedin.")

    try:
        df = load_and_clean(file)
        if df.empty:
            return render_template("index.html", error="Veri boş veya tamamen hatalı.")

        summary = summarize(df)
        chart_file = make_daily_revenue_chart(df)

        # DB'ye kaydet
        created_at = datetime.now().isoformat(timespec="seconds")
        original_filename = file.filename

        top_products_records = summary["top_products"].to_dict(orient="records")
        daily_records = summary["daily"].to_dict(orient="records")

        with db_connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO reports (created_at, original_filename, kpi_json, top_products_json, daily_json, chart_file)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at,
                    original_filename,
                    json.dumps(summary["kpi"], ensure_ascii=False),
                    json.dumps(top_products_records, ensure_ascii=False),
                    json.dumps(daily_records, ensure_ascii=False),
                    chart_file,
                ),
            )
            conn.commit()
            report_id = cur.lastrowid

        return redirect(url_for("report_view", report_id=report_id))

    except Exception as e:
        return render_template("index.html", error=f"Hata: {e}")


@app.route("/report/<int:report_id>", methods=["GET"])
def report_view(report_id: int):
    with db_connect() as conn:
        row = conn.execute("SELECT * FROM reports WHERE id = ?", (report_id,)).fetchone()

    if not row:
        return "Report not found", 404

    kpi = json.loads(row["kpi_json"])
    top_products = json.loads(row["top_products_json"])
    daily = json.loads(row["daily_json"])

    return render_template(
        "report.html",
        kpi=kpi,
        top_products=top_products,
        daily=daily,
        preview=[],  # artık preview zorunlu değil; istersen ekleriz
        chart_file=row["chart_file"],
        meta={
            "id": row["id"],
            "created_at": row["created_at"],
            "original_filename": row["original_filename"],
        },
    )


@app.route("/history", methods=["GET"])
def history():
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT id, created_at, original_filename FROM reports ORDER BY id DESC LIMIT 50"
        ).fetchall()

    reports = [dict(r) for r in rows]
    return render_template("history.html", reports=reports)


if __name__ == "__main__":
    init_db()
    app.run(debug=True)
