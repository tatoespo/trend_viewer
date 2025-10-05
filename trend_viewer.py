# trend_viewer.py

# ============== FONT & CSS GLOBALI (usa i .ttf locali) ==============
from base64 import b64encode

PAGE_BG   = "#F7F5F2"   # beige chiaro
HEADER_BG = "#EFECE6"
ROW_EVEN  = "#FBFAF7"
GRID_COL  = "#C6C6C6"
TEXT_COL  = "#1E1E1E"

# 1) registra i TTF per Matplotlib e scopri il "family name" reale
font_dir = os.path.join(os.path.dirname(__file__), "fonts")
ttf_candidates = [
    "Inter-Regular.ttf",  # se hai rinominato i file così
    "Inter-Medium.ttf",
    "Inter-Bold.ttf",
    # nomi alternativi (se non hai .ttf con i nomi sopra, lasciali pure)
    "Inter_18pt-Regular.ttf",
    "Inter_18pt-Medium.ttf",
    "Inter_18pt-Bold.ttf",
]

found_ttf = []
for fname in ttf_candidates:
    fpath = os.path.join(font_dir, fname)
    if os.path.exists(fpath):
        try:
            fm.fontManager.addfont(fpath)
            found_ttf.append(fpath)
        except Exception:
            pass

# family name da usare in Matplotlib (leggo da un .ttf reale)
mpl_family = None
for fpath in found_ttf:
    try:
        fam = fm.FontProperties(fname=fpath).get_name()
        if fam:  # prendo il primo valido
            mpl_family = fam
            break
    except Exception:
        pass

if mpl_family:
    matplotlib.rcParams["font.family"] = mpl_family
else:
    matplotlib.rcParams["font.family"] = "DejaVu Sans"

matplotlib.rcParams.update({
    "figure.facecolor": PAGE_BG,
    "axes.facecolor": PAGE_BG,
})

# 2) inietto @font-face in CSS (base64) così TUTTA la pagina usa Inter
def _read_bytes(p):
    with open(p, "rb") as f:
        return f.read()

css_faces = []
# mappa: (path parziale da trovare, peso CSS, stile)
css_plan = [
    (("Inter-Regular.ttf","Inter_18pt-Regular.ttf"), 400, "normal"),
    (("Inter-Medium.ttf","Inter_18pt-Medium.ttf"),   500, "normal"),
    (("Inter-Bold.ttf","Inter_18pt-Bold.ttf"),       700, "normal"),
]

for names, weight, style in css_plan:
    real = None
    for n in names:
        p = os.path.join(font_dir, n)
        if os.path.exists(p):
            real = p
            break
    if not real:
        continue
    raw = _read_bytes(real)
    uri = "data:font/ttf;base64," + b64encode(raw).decode("ascii")
    css_faces.append(
        f"""@font-face {{
              font-family: 'Inter';
              src: url('{uri}') format('truetype');
              font-weight: {weight};
              font-style: {style};
              font-display: swap;
           }}"""
    )

st.markdown(
    f"""
    <style>
    {'\n'.join(css_faces)}
    html, body, [class*="stApp"] {{
        background-color: {PAGE_BG};
        color: {TEXT_COL};
        font-family: 'Inter', system-ui, -apple-system, Segoe UI, Roboto, 'Helvetica Neue', Arial, sans-serif;
    }}
    .block-container {{ max-width: 1600px; padding-top: 1rem; }}
    </style>
    """,
    unsafe_allow_html=True
)

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
import matplotlib.font_manager as fm

# ============== IMPOSTAZIONI STREAMLIT ==============
st.set_page_config(page_title="Trend Deep-Dive", layout="wide")

PAGE_BG   = "#F7F5F2"   # beige chiaro
HEADER_BG = "#EFECE6"   # header tabella
ROW_EVEN  = "#FBFAF7"   # zebra rows
GRID_COL  = "#C6C6C6"   # linee orizzontali
TEXT_COL  = "#1E1E1E"

# CSS globale (pagina intera + fallback font Inter lato HTML)
st.markdown(
    f"""
    <style>
    html, body, [class*="stApp"] {{
        background-color: {PAGE_BG};
        color: {TEXT_COL};
        font-family: "Inter", system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial, sans-serif;
    }}
    /* contenitore più largo */
    .block-container {{ max-width: 1600px; padding-top: 1rem; }}
    </style>
    """,
    unsafe_allow_html=True
)

st.title("Trend Deep-Dive")

# ============== FONT MATPLOTLIB (Inter se presente) ==============
try:
    font_dir = os.path.join(os.path.dirname(__file__), "fonts")
    any_added = False
    for fname in ["Inter-Regular.ttf", "Inter-Medium.ttf", "Inter-Bold.ttf"]:
        fpath = os.path.join(font_dir, fname)
        if os.path.exists(fpath):
            fm.fontManager.addfont(fpath)
            any_added = True
    if any_added:
        matplotlib.rcParams["font.family"] = "Inter"
    else:
        matplotlib.rcParams["font.family"] = "DejaVu Sans"
except Exception:
    matplotlib.rcParams["font.family"] = "DejaVu Sans"

matplotlib.rcParams.update({
    "figure.facecolor": PAGE_BG,
    "axes.facecolor": PAGE_BG,
})

# ============== SPLIT DATE (URL -> config.yaml -> default) ==============
def _parse_split(val):
    if val is None:
        return None
    s = str(val).strip()
    if re.fullmatch(r"\d{{4}}-\d{{2}}-\d{{2}}", s):
        return pd.to_datetime(s).normalize()
    return pd.to_datetime(s, dayfirst=True, errors="coerce").normalize()

def load_split_date():
    # 1) Query param ?split=YYYY-MM-DD (o dd/mm/yyyy)
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

# ============== PARAMETRI URL (trend) ==============
trend = st.query_params.get("trend")
if not trend:
    st.warning("⚠️ Nessun trend passato nell’URL. Usa ?trend=CODICE_TREND.")
    st.stop()

base_trend = trend[:-1]  # parquet è senza l'ultima cifra

st.caption(f"Trend selezionato: **{trend}** • Split date: **{SPLIT_DATE.date()}**")

# ============== GOOGLE DRIVE (service account) ==============
try:
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["google_service_account"],
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
except KeyError:
    st.error("Manca la sezione `google_service_account` nei secrets Streamlit.")
    st.stop()

drive = build("drive", "v3", credentials=creds)

# ============== DOWNLOAD PARQUET ==============
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

# ============== DATE/TIME + FILTRO ==============
date_str = df.get("Date", pd.Series("", index=df.index)).astype(str).fillna("")
time_str = df.get("Time", pd.Series("", index=df.index)).astype(str).fillna("")
dt = pd.to_datetime((date_str + " " + time_str).str.strip(), dayfirst=True, errors="coerce")
df["__dt__"] = dt
df["__date__"] = df["__dt__"].dt.normalize()

df = df[df["__date__"] >= SPLIT_DATE].sort_values("__dt__").reset_index(drop=True)
if df.empty:
    st.info("Nessun evento dopo la split_date.")
    st.stop()

# ============== PREPARA TABELLA (NO HTML) ==============
wanted = [
    "Date","Time","HomeTeam","AwayTeam","FAV_odds","P>2.5",
    "FAV_goal","SFAV_goal","FAV_goal_1T","SFAV_goal_1T",
    "p_t","mu_t","Bet1","Odds1","NetProfit1","Bet2","Odds2","NetProfit2"
]
cols = [c for c in wanted if c in df.columns]
tbl = df[cols].copy()

# Data in dd/mm/YYYY
tbl["Date"] = df["__date__"].dt.strftime("%d/%m/%Y")

# Percentuali (se 0..1)
for c in ["p_t","mu_t","P>2.5"]:
    if c in tbl.columns:
        v = pd.to_numeric(tbl[c], errors="coerce")
        if v.dropna().between(0,1).all():
            tbl[c] = (v*100).round(1).astype(str) + "%"

# Quote/NetProfit -> 2 decimali
for c in ["FAV_odds","Odds1","Odds2","NetProfit1","NetProfit2"]:
    if c in tbl.columns:
        tbl[c] = pd.to_numeric(tbl[c], errors="coerce").round(2)

tbl = tbl.fillna("")

# ============== TABELLA MATPLOTLIB (solo orizzontali) ==============
def draw_mpl_table(dataframe: pd.DataFrame, max_rows: int = 150):
    data = dataframe.head(max_rows)
    ncol, nrow = data.shape[1], data.shape[0]

    # dimensioni dinamiche (Streamlit farà stretch)
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
        cellLoc='center',
        colLoc='center',
        loc='upper left',
        colColours=[HEADER_BG]*ncol
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)

    for i in range(ncol):
        table.auto_set_column_width(i)

    y_under_header = None
    body_y = []
    numeric_cols = {
        "FAV_odds","P>2.5","FAV_goal","SFAV_goal","FAV_goal_1T","SFAV_goal_1T",
        "p_t","mu_t","Bet1","Odds1","NetProfit1","Bet2","Odds2","NetProfit2"
    }

    # Stile celle + raccolta Y per disegnare linee orizzontali
    for (row, col), cell in table.get_celld().items():
        # niente bordi delle celle (evita verticali)
        cell.set_linewidth(0.0)

        if row == 0:
            cell.set_height(header_h)
            cell.get_text().set_color(TEXT_COL)
            cell.get_text().set_fontweight('bold')
            y_under_header = cell.xy[1]
        else:
            cell.set_height(base_row_h)
            if row % 2 == 0:
                cell.set_facecolor(ROW_EVEN)
            if col == 0:
                body_y.append(cell.xy[1])

        # allineamento numerico a destra
        if col_labels[col] in numeric_cols:
            cell._text.set_ha('right')

    # linee orizzontali (header solida, corpo tratteggiato)
    try:
        if y_under_header is not None:
            ax.hlines(y_under_header, xmin=0, xmax=1, colors=GRID_COL, linewidth=1.0, linestyles='solid')
        for y in sorted(set(body_y), reverse=True):
            ax.hlines(y, xmin=0, xmax=1, colors=GRID_COL, linewidth=0.8, linestyles=(0,(3,3)))
    except Exception:
        pass

    plt.tight_layout()
    return fig

st.subheader("Partite (dopo split_date)")
fig_table = draw_mpl_table(tbl)
st.pyplot(fig_table, width="stretch")  # nuovo parametro (sostituisce use_container_width)

# ============== NETPROFIT CUMULATO (interattivo) ==============
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
st.line_chart(
    by_day.set_index("Date")[["Cum_NetProfit1","Cum_NetProfit2"]],
    width="stretch"  # nuovo parametro
)
