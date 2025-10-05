# trend_viewer.py — versione pulita e stabile (senza font)

import os
import io
import re
import pandas as pd
import streamlit as st

# Google Drive API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Matplotlib (tabella stile "sonofacorner")
import matplotlib
import matplotlib.pyplot as plt

# ────────────────────────── CONFIG STREAMLIT ──────────────────────────
st.set_page_config(page_title="Trend Deep-Dive", layout="wide")

PAGE_BG   = "#F7F5F2"   # beige chiaro
HEADER_BG = "#EFECE6"   # header tabella
ROW_EVEN  = "#FBFAF7"   # zebra rows
GRID_COL  = "#C6C6C6"   # linee orizzontali
TEXT_COL  = "#1E1E1E"

# CSS semplice globale (senza font custom)
st.markdown(
    f"""
    <style>
    html, body, [class*="stApp"] {{
        background-color: {PAGE_BG};
        color: {TEXT_COL};
        font-family: system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial, sans-serif;
    }}
    .block-container {{ max-width: 1600px; padding-top: 1rem; }}
    </style>
    """,
    unsafe_allow_html=True
)

# Colori matplotlib allineati allo sfondo
matplotlib.rcParams.update({
    "figure.facecolor": PAGE_BG,
    "axes.facecolor": PAGE_BG,
    "font.size": 10,
})

st.title("Trend Deep-Dive")

# ────────────────────────── SPLIT DATE ──────────────────────────
def _parse_split(val):
    if val is None:
        return None
    s = str(val).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return pd.to_datetime(s).normalize()
    return pd.to_datetime(s, dayfirst=True, errors="coerce").normalize()

def load_split_date():
    # 1) query param ?split=YYYY-MM-DD (o dd/mm/yyyy)
    dt = _parse_split(st.query_params.get("split"))
    if dt is not None:
        return dt
    # 2) config.yaml accanto al file
    cfg_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    try:
        import yaml
        if os.path.exists(cfg_path):
            with open(cfg_path, "r", encoding="utf-8") as f:
                y = yaml.safe_load(f) or {}
                dt = _parse_split(y.get("split_date"))
                if dt is not None:
                    return dt
    except Exception:
        pass
    # 3) default
    return pd.Timestamp(2022, 8, 1)

SPLIT_DATE = load_split_date()

# ────────────────────────── PARAMETRI URL ──────────────────────────
trend = st.query_params.get("trend")
if not trend:
    st.warning("⚠️ Nessun trend passato nell’URL. Usa ?trend=CODICE_TREND.")
    st.stop()

base_trend = trend[:-1]  # parquet è senza l'ultima cifra
st.caption(f"Trend selezionato: **{trend}** • Split date: **{SPLIT_DATE.date()}**")

# ────────────────────────── GOOGLE DRIVE ──────────────────────────
try:
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["google_service_account"],
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
except KeyError:
    st.error("Manca la sezione `google_service_account` nei secrets Streamlit.")
    st.stop()
except Exception as e:
    st.exception(e)
    st.stop()

try:
    drive = build("drive", "v3", credentials=creds)
    resp = drive.files().list(
        q=f"name='{base_trend}.parquet'",
        fields="files(id,name)",
    ).execute()
    files = resp.get("files", [])
    if not files:
        st.error(f"Nessun file **{base_trend}.parquet** trovato su Drive.")
        st.stop()

    buf = io.BytesIO()
    req = drive.files().get_media(fileId=files[0]["id"])
    down = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = down.next_chunk()
    buf.seek(0)

    df = pd.read_parquet(buf)
except Exception as e:
    st.error("Errore nel download/lettura del parquet da Drive.")
    st.exception(e)
    st.stop()

# ────────────────────────── PREPARAZIONE DATI ──────────────────────────
date_str = df.get("Date", pd.Series("", index=df.index)).astype(str).fillna("")
time_str = df.get("Time", pd.Series("", index=df.index)).astype(str).fillna("")
dt = pd.to_datetime((date_str + " " + time_str).str.strip(), dayfirst=True, errors="coerce")
df["__dt__"] = dt
df["__date__"] = df["__dt__"].dt.normalize()

df = df[df["__date__"] >= SPLIT_DATE].sort_values("__dt__").reset_index(drop=True)
if df.empty:
    st.info("Nessun evento dopo la split_date.")
    st.stop()

wanted = [
    "Date","Time","HomeTeam","AwayTeam","FAV_odds","P>2.5",
    "FAV_goal","SFAV_goal","FAV_goal_1T","SFAV_goal_1T",
    "p_t","mu_t","Bet1","Odds1","NetProfit1","Bet2","Odds2","NetProfit2"
]
cols = [c for c in wanted if c in df.columns]
tbl = df[cols].copy()
tbl["Date"] = df["__date__"].dt.strftime("%d/%m/%Y")

for c in ["p_t","mu_t","P>2.5"]:
    if c in tbl.columns:
        v = pd.to_numeric(tbl[c], errors="coerce")
        if v.dropna().between(0, 1).all():
            tbl[c] = (v * 100).round(1).astype(str) + "%"

for c in ["FAV_odds","Odds1","Odds2","NetProfit1","NetProfit2"]:
    if c in tbl.columns:
        tbl[c] = pd.to_numeric(tbl[c], errors="coerce").round(2)

tbl = tbl.fillna("")

# ────────────────────────── TABELLA MATPLOTLIB ──────────────────────────
def draw_mpl_table(dataframe: pd.DataFrame, max_rows: int = 150):
    data = dataframe.head(max_rows)
    ncol, nrow = data.shape[1], data.shape[0]

    fig_w = min(24, 6 + 0.9 * ncol)
    base_row_h = 0.34
    header_h   = base_row_h * 1.1
    fig_h = min(30, 1.0 + header_h + base_row_h * nrow)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=150)
    fig.patch.set_facecolor(PAGE_BG)
    ax.set_facecolor(PAGE_BG)
    ax.axis("off")

    cell_text = data.astype(str).values.tolist()
    col_labels = list(data.columns)

    table = ax.table(
        cellText=cell_text,
        colLabels=col_labels,
        cellLoc="center",
        colLoc="center",
        loc="upper left",
        colColours=[HEADER_BG] * ncol,
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)

    for i in range(ncol):
        table.auto_set_column_width(i)

    # rimuovo i bordi verticali → disegno solo le orizzontali
    for (r, c), cell in table.get_celld().items():
        cell.set_linewidth(0.0)
        if r == 0:
            cell.set_height(header_h)
            cell.get_text().set_color(TEXT_COL)
            cell.get_text().set_fontweight("bold")
        else:
            cell.set_height(base_row_h)
            if r % 2 == 0:
                cell.set_facecolor(ROW_EVEN)

    # linee orizzontali puntinate
    # y di ogni riga del corpo (prendo dalla colonna 0)
    body_y = sorted({cell.xy[1] for (r, c), cell in table.get_celld().items() if r > 0 and c == 0}, reverse=True)
    # y sotto l’header
    y_header = next((cell.xy[1] for (r, c), cell in table.get_celld().items() if r == 0 and c == 0), None)

    if y_header is not None:
        ax.hlines(y_header, xmin=0, xmax=1, colors=GRID_COL, linewidth=1.0, linestyles="solid")
    for y in body_y:
        ax.hlines(y, xmin=0, xmax=1, colors=GRID_COL, linewidth=0.8, linestyles=(0, (3, 3)))

    plt.tight_layout()
    return fig

st.subheader("Partite (dopo split_date)")
fig_table = draw_mpl_table(tbl)
st.pyplot(fig_table, use_container_width=True) 

# ────────────────────────── NETPROFIT CUMULATO ──────────────────────────
np1 = pd.to_numeric(df.get("NetProfit1", 0), errors="coerce").fillna(0.0)
np2 = pd.to_numeric(df.get("NetProfit2", 0), errors="coerce").fillna(0.0)

by_day = (
    pd.DataFrame({"Date": df["__date__"], "NetProfit1": np1, "NetProfit2": np2})
    .groupby("Date", as_index=False).sum()
    .sort_values("Date")
)
by_day["Cum_NetProfit1"] = by_day["NetProfit1"].cumsum()
by_day["Cum_NetProfit2"] = by_day["NetProfit2"].cumsum()

st.subheader("NetProfit cumulato")
st.line_chart(by_day.set_index("Date")[["Cum_NetProfit1","Cum_NetProfit2"]], use_container_width=True)
