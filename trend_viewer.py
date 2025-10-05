import streamlit as st
import pandas as pd
import io, os, re

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Matplotlib
import matplotlib
import matplotlib.pyplot as plt
from matplotlib import font_manager

# ─────────────────────────────────────────────────────────────
# Streamlit: pagina wide + sfondo uniforme (NO emoji)
# ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="Trend Deep-Dive", layout="wide")
st.markdown("""
<style>
/* sfondo caldo su tutta la pagina */
[data-testid="stAppViewContainer"] { background: #f7f6f3; }
[data-testid="stHeader"] { background: #f7f6f3; }
.block-container { max-width: 100%; padding-top: 1rem; }
</style>
""", unsafe_allow_html=True)

st.title("Trend Deep-Dive")

# ─────────────────────────────────────────────────────────────
# Font Matplotlib: usa Inter se presente, altrimenti DejaVu Sans
# ─────────────────────────────────────────────────────────────
PAGE_BG   = "#f7f6f3"   # sfondo pagina / figura
HEADER_BG = "#f0ede7"   # intestazione tabella
ROW_EVEN  = "#fbfaf7"   # zebra rows
GRID_COL  = "#cfcac2"   # colore linee orizzontali
TEXT_COL  = "#2b2b2b"

try:
    font_path = os.path.join(os.path.dirname(__file__), "fonts", "Inter-Regular.ttf")
    if os.path.exists(font_path):
        font_manager.fontManager.addfont(font_path)
        matplotlib.rcParams["font.family"] = "Inter"
    else:
        matplotlib.rcParams["font.family"] = "DejaVu Sans"
except Exception:
    matplotlib.rcParams["font.family"] = "DejaVu Sans"

matplotlib.rcParams.update({
    "axes.facecolor":   PAGE_BG,
    "figure.facecolor": PAGE_BG,
})

# ─────────────────────────────────────────────────────────────
# split_date robusto (+ override via ?split=YYYY-MM-DD)
# ─────────────────────────────────────────────────────────────
def _parse_split(val):
    if val is None: return None
    s = str(val).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return pd.to_datetime(s).normalize()
    return pd.to_datetime(s, dayfirst=True, errors="coerce").normalize()

def load_split_date():
    dt = _parse_split(st.query_params.get("split"))
    if dt is not None: return dt
    cfg_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    try:
        import yaml
        if os.path.exists(cfg_path):
            with open(cfg_path, "r", encoding="utf-8") as f:
                dt = _parse_split((yaml.safe_load(f) or {}).get("split_date"))
                if dt is not None: return dt
    except Exception:
        pass
    return pd.Timestamp(2022, 8, 1)

SPLIT_DATE = load_split_date()

# ─────────────────────────────────────────────────────────────
# Google Drive (secrets Streamlit)
# ─────────────────────────────────────────────────────────────
creds = service_account.Credentials.from_service_account_info(
    st.secrets["google_service_account"],
    scopes=["https://www.googleapis.com/auth/drive.readonly"]
)
drive = build("drive", "v3", credentials=creds)

# ─────────────────────────────────────────────────────────────
# Parametri URL & file parquet
# ─────────────────────────────────────────────────────────────
trend = st.query_params.get("trend")
if not trend:
    st.warning("⚠️ Nessun trend passato nell’URL."); st.stop()

base_trend = trend[:-1]
st.caption(f"Trend selezionato: **{trend}** • Split date: **{SPLIT_DATE.date()}**")

res = drive.files().list(q=f"name='{base_trend}.parquet'", fields="files(id,name)").execute()
files = res.get("files", [])
if not files:
    st.error(f"Nessun file {base_trend}.parquet trovato."); st.stop()

buf = io.BytesIO()
req = drive.files().get_media(fileId=files[0]["id"])
down = MediaIoBaseDownload(buf, req)
done = False
while not done:
    _, done = down.next_chunk()
buf.seek(0)
df = pd.read_parquet(buf)

# ─────────────────────────────────────────────────────────────
# Date/Time + filtro (solo data) + ordinamento
# ─────────────────────────────────────────────────────────────
date_str = df.get("Date", pd.Series("", index=df.index)).astype(str).fillna("")
time_str = df.get("Time", pd.Series("", index=df.index)).astype(str).fillna("")
dt = pd.to_datetime((date_str + " " + time_str).str.strip(), dayfirst=True, errors="coerce")
df["__dt__"] = dt
df["__date__"] = df["__dt__"].dt.normalize()
df = df[df["__date__"] >= SPLIT_DATE].sort_values("__dt__").reset_index(drop=True)
if df.empty:
    st.info("Nessun evento dopo la split_date."); st.stop()

# ─────────────────────────────────────────────────────────────
# Colonne & formattazioni (NO HTML)
# ─────────────────────────────────────────────────────────────
wanted = [
    "Date","Time","HomeTeam","AwayTeam","FAV_odds","P>2.5",
    "FAV_goal","SFAV_goal","FAV_goal_1T","SFAV_goal_1T",
    "p_t","mu_t","Bet1","Odds1","NetProfit1","Bet2","Odds2","NetProfit2"
]
cols = [c for c in wanted if c in df.columns]
disp = df[cols].copy()
disp["Date"] = df["__date__"].dt.strftime("%d/%m/%Y")

for c in ["p_t","mu_t","P>2.5"]:
    if c in disp.columns:
        vals = pd.to_numeric(disp[c], errors="coerce")
        if vals.dropna().between(0,1).all():
            disp[c] = (vals*100).round(1).astype(str) + "%"

for c in ["FAV_odds","Odds1","Odds2","NetProfit1","NetProfit2"]:
    if c in disp.columns:
        disp[c] = pd.to_numeric(disp[c], errors="coerce").round(2)

disp = disp.fillna("")

# ─────────────────────────────────────────────────────────────
# Tabella Matplotlib: SOLO orizzontali (corpo tratteggiate), header normale
# ─────────────────────────────────────────────────────────────
def matplotlib_table(df_show: pd.DataFrame, max_rows: int = 150):
    data = df_show.head(max_rows)
    ncol = data.shape[1]
    nrow = data.shape[0]

    # dimensioni dinamiche (Streamlit la scalerà a container width)
    fig_w = min(24, 6 + 0.9 * ncol)
    base_row_h = 0.34  # altezza righe uniforme
    header_h   = base_row_h * 1.1
    fig_h = min(30, 1.0 + header_h + base_row_h * nrow)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=150)
    fig.patch.set_facecolor(PAGE_BG)
    ax.set_facecolor(PAGE_BG)
    ax.axis("off")

    cell_text = data.astype(str).values.tolist()
    col_labels = list(data.columns)

    # crea tabella SENZA bordi (poi disegniamo noi SOLO orizzontali)
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

    # larghezze auto
    for i in range(ncol):
        table.auto_set_column_width(i)

    # togli bordi a tutte le celle e imposta altezze uniformi
    for (row, col), cell in table.get_celld().items():
        cell.set_linewidth(0.0)
        if row == 0:
            cell.set_height(header_h)
            cell.get_text().set_color(TEXT_COL)
            cell.get_text().set_fontweight('bold')
        else:
            cell.set_height(base_row_h)
            if row % 2 == 0:
                cell.set_facecolor(ROW_EVEN)

        # allineamento numerico a destra per certe colonne
        numeric_cols = {"FAV_odds","P>2.5","FAV_goal","SFAV_goal","FAV_goal_1T","SFAV_goal_1T",
                        "p_t","mu_t","Bet1","Odds1","NetProfit1","Bet2","Odds2","NetProfit2"}
        if col_labels[col] in numeric_cols:
            cell._text.set_ha('right')

    # disegna orizzontali: solida sotto header, tratteggiate per il corpo
    # usiamo la geometria delle celle per ricavare le y
    y_lines = []

    # y top = top dell'header
    header_cell = table[0, 0]
    y_top = header_cell.xy[1] + header_cell.get_height()
    y_lines.append((y_top, "header"))  # top (opzionale)

    # linea sotto header
    y_under_header = header_cell.xy[1]
    y_lines.append((y_under_header, "solid"))

    # righe del corpo
    for r in range(1, nrow+1):
        cell = table[r, 0]
        y = cell.xy[1]
        y_lines.append((y, "dashed"))

    # disegno linee
    for y, kind in y_lines:
        if kind == "solid":
            ax.hlines(y, xmin=0, xmax=1, colors=GRID_COL, linewidth=1.0, linestyles='solid')
        elif kind == "dashed":
            ax.hlines(y, xmin=0, xmax=1, colors=GRID_COL, linewidth=0.8, linestyles=(0,(3,3)))

    plt.tight_layout()
    return fig

st.subheader("Partite (dopo split_date)")
fig_table = matplotlib_table(disp)
st.pyplot(fig_table, use_container_width=True)

# ─────────────────────────────────────────────────────────────
# NetProfit cumulato — INTERATTIVO, stessa larghezza container
# ─────────────────────────────────────────────────────────────
np1 = pd.to_numeric(df.get("NetProfit1",0), errors="coerce").fillna(0.0)
np2 = pd.to_numeric(df.get("NetProfit2",0), errors="coerce").fillna(0.0)
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
