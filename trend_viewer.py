import os
import io
import re
import pandas as pd
import streamlit as st

# Google Drive API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Matplotlib
import matplotlib
import matplotlib.pyplot as plt
from cycler import cycler

# ============== CONFIG GENERALE ==============
st.set_page_config(page_title="Trend Deep-Dive", layout="wide")

# --- Colori UI/Tabella ---
PAGE_BG   = "#EFE9E6"   # richiesta: figure/axes facecolor = efe9e6
HEADER_BG = "#E7E1DC"
ROW_EVEN  = "#F7F2EF"
GRID_COL  = "#C9C3BE"
TEXT_COL  = "#1E1E1E"

# Limiti tabella per evitare immagini enormi
MAX_ROWS_DISPLAY = 80
FIG_MAX_W_IN     = 18
FIG_MAX_H_IN     = 10
FIG_DPI          = 110

# ---------- CSS globale (allineamento e spaziature) ----------
st.markdown(
    f"""
    <style>
    /* colore di sfondo coerente con i plot */
    html, body, [class*="stApp"] {{
        background-color: {PAGE_BG};
        color: {TEXT_COL};
    }}

    /* contenitore centrale più largo e con padding sinistro ridotto */
    .block-container {{
        max-width: 1600px;
        padding-top: 0.6rem;
        padding-left: 1.0rem; /* allinea meglio i contenuti a sinistra */
        padding-right: 1.0rem;
    }}

    /* compattiamo i titoli */
    h1, h2, h3 {{
        margin-top: 0.2rem;
        margin-bottom: 0.5rem;
    }}

    /* avvicina la figura Matplotlib al sottotitolo */
    div[data-testid="stPyplot"] {{
        margin-top: -8px;
    }}

    /* assicura che l'immagine della figura non sia centrata ma "a filo" a sinistra */
    div[data-testid="stPyplot"] img {{
        display: block;
        margin-left: 0 !important;
    }}
    </style>
    """,
    unsafe_allow_html=True
)

st.title("Trend Deep-Dive")

# ============== STILE MATPLOTLIB (RICHIESTO) ==============
matplotlib.rcParams.update({
    "figure.facecolor": "#efe9e6",
    "axes.facecolor":   "#efe9e6",
    "axes.prop_cycle":  cycler(color=["#287271", "#495371"]),
    "axes.spines.top":  False,
    "axes.spines.right": False,
    # dettagli utili (opzionali) per più pulizia
    "axes.grid": True,
    "grid.color": "#d7d2cd",
    "grid.linewidth": 0.6,
    "grid.alpha": 0.7,
})

# ============== SPLIT DATE ==============
def _parse_split(val):
    if val is None:
        return None
    s = str(val).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return pd.to_datetime(s, format="%Y-%m-%d", errors="coerce").normalize()
    return pd.to_datetime(s, dayfirst=True, errors="coerce").normalize()

def load_split_date():
    dt = _parse_split(st.query_params.get("split"))
    if dt is not None:
        return dt
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
    return pd.Timestamp(2022, 8, 1)

SPLIT_DATE = load_split_date()

# ============== PARAMETRI URL ==============
trend = st.query_params.get("trend")
if not trend:
    st.warning("⚠️ Nessun trend passato nell’URL. Usa ?trend=CODICE_TREND.")
    st.stop()

base_trend = trend[:-1]
st.caption(f"Trend selezionato: **{trend}** • Split date: **{SPLIT_DATE.date()}**")

# ============== GOOGLE DRIVE ==============
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

# ============== PREPARA DATI ==============
date_str = df.get("Date", pd.Series("", index=df.index)).astype(str)
time_str = df.get("Time", pd.Series("", index=df.index)).astype(str)
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
        if v.dropna().between(0,1).all():
            tbl[c] = (v*100).round(1).astype(str) + "%"

for c in ["FAV_odds","Odds1","Odds2","NetProfit1","NetProfit2"]:
    if c in tbl.columns:
        tbl[c] = pd.to_numeric(tbl[c], errors="coerce").round(2)

tbl = tbl.fillna("")

# ============== TABELLA MATPLOTLIB ==============
def draw_mpl_table(dataframe: pd.DataFrame, max_rows: int = MAX_ROWS_DISPLAY):
    """
    Tabella leggibile e allineata: font più grande, righe più alte,
    header bold, zebra rows e linee guida leggere.
    """
    data = dataframe.head(max_rows)
    ncol, nrow = data.shape[1], data.shape[0]

    # dimensioni figura calibrate su righe alte
    base_row_h = 0.40   # (più alte) prima 0.28
    header_h   = base_row_h * 1.30
    fig_w = min(FIG_MAX_W_IN, 6 + 0.74 * ncol)
    fig_h = min(FIG_MAX_H_IN, 0.7 + header_h + base_row_h * nrow)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=FIG_DPI)
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

    # font più grande + più spazio verticale tra le righe
    table.auto_set_font_size(False)
    table.set_fontsize(12)   # caratteri più grandi
    table.scale(1.0, 1.30)   # righe più alte

    y_under_header = None
    body_y = []
    numeric_cols = {
        "FAV_odds","P>2.5","FAV_goal","SFAV_goal","FAV_goal_1T","SFAV_goal_1T",
        "p_t","mu_t","Bet1","Odds1","NetProfit1","Bet2","Odds2","NetProfit2"
    }

    for (row, col), cell in table.get_celld().items():
        # togli i bordi neri default per look più pulito
        cell.set_linewidth(0.0)
        if row == 0:  # header
            cell.get_text().set_color(TEXT_COL)
            cell.get_text().set_fontweight('bold')
            y_under_header = cell.xy[1]
        else:
            # zebra
            if row % 2 == 0:
                cell.set_facecolor(ROW_EVEN)
            if col == 0:
                body_y.append(cell.xy[1])
        # numerici allineati a destra
        if col < len(col_labels) and col_labels[col] in numeric_cols:
            cell._text.set_ha('right')

    # linee guida orizzontali molto leggere
    try:
        if y_under_header is not None:
            ax.hlines(y_under_header, xmin=0, xmax=1, colors=GRID_COL, linewidth=1.0)
        for y in sorted(set(body_y), reverse=True):
            ax.hlines(y, xmin=0, xmax=1, colors=GRID_COL, linewidth=0.7, linestyles=(0,(3,3)))
    except Exception:
        pass

    fig.tight_layout()
    return fig

st.subheader("Partite (dopo split_date)")
fig_table = draw_mpl_table(tbl)
# usa tutta la larghezza della colonna e resta a sinistra
st.pyplot(fig_table, use_container_width=True)

# ============== NETPROFIT CUMULATO ==============
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
    use_container_width=True
)
