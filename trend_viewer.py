import streamlit as st
import pandas as pd
import io, os, re

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Matplotlib
import matplotlib
import matplotlib.pyplot as plt

# ─────────────────────────────────────────────────────────────
# Streamlit: pagina wide + sfondo pagina uniforme (NO emoji)
# ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="Trend Deep-Dive", layout="wide")
st.markdown("""
<style>
/* sfondo caldo uniformato a tutta la pagina */
[data-testid="stAppViewContainer"] { background: #f7f6f3; }
[data-testid="stHeader"] { background: #f7f6f3; }
</style>
""", unsafe_allow_html=True)

st.title("Trend Deep-Dive")

# ─────────────────────────────────────────────────────────────
# Matplotlib: tema "sonofacorner-like" (colori) + font standard
# ─────────────────────────────────────────────────────────────
# Nota: usiamo "DejaVu Sans" (default Matplotlib), identico e disponibile ovunque.
matplotlib.rcParams.update({
    "font.family": "DejaVu Sans",
    "axes.facecolor": "#f7f6f3",
    "figure.facecolor": "#f7f6f3",
})

PAGE_BG   = "#f7f6f3"   # sfondo pagina / figura
HEADER_BG = "#f0ede7"   # intestazione tabella
ROW_EVEN  = "#fbfaf7"   # zebra rows
GRID_COL  = "#e2dfd9"   # bordo celle
TEXT_COL  = "#2b2b2b"

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
    # 1) URL
    dt = _parse_split(st.query_params.get("split"))
    if dt is not None: return dt
    # 2) config.yaml
    cfg_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    try:
        import yaml
        if os.path.exists(cfg_path):
            with open(cfg_path, "r", encoding="utf-8") as f:
                dt = _parse_split((yaml.safe_load(f) or {}).get("split_date"))
                if dt is not None: return dt
    except Exception:
        pass
    # 3) default
    return pd.Timestamp(2022, 8, 1)

SPLIT_DATE = load_split_date()

# ─────────────────────────────────────────────────────────────
# Google Drive auth (secrets Streamlit)
# ─────────────────────────────────────────────────────────────
creds = service_account.Credentials.from_service_account_info(
    st.secrets["google_service_account"],
    scopes=["https://www.googleapis.com/auth/drive.readonly"]
)
drive = build("drive", "v3", credentials=creds)

# ─────────────────────────────────────────────────────────────
# Parametri URL
# ─────────────────────────────────────────────────────────────
trend = st.query_params.get("trend")
if not trend:
    st.warning("⚠️ Nessun trend passato nell’URL.")
    st.stop()
base_trend = trend[:-1]
st.caption(f"Trend selezionato: **{trend}** • Split date: **{SPLIT_DATE.date()}**")

# ─────────────────────────────────────────────────────────────
# Download parquet
# ─────────────────────────────────────────────────────────────
res = drive.files().list(q=f"name='{base_trend}.parquet'", fields="files(id,name)").execute()
files = res.get("files", [])
if not files:
    st.error(f"Nessun file {base_trend}.parquet trovato.")
    st.stop()

buf = io.BytesIO()
req = drive.files().get_media(fileId=files[0]["id"])
down = MediaIoBaseDownload(buf, req)
done = False
while not done:
    _, done = down.next_chunk()
buf.seek(0)
df = pd.read_parquet(buf)

# ─────────────────────────────────────────────────────────────
# Parsing date/time + filtro data (solo data) + ordinamento
# ─────────────────────────────────────────────────────────────
date_str = df.get("Date", pd.Series("", index=df.index)).astype(str).fillna("")
time_str = df.get("Time", pd.Series("", index=df.index)).astype(str).fillna("")
dt = pd.to_datetime((date_str + " " + time_str).str.strip(), dayfirst=True, errors="coerce")
df["__dt__"] = dt
df["__date__"] = df["__dt__"].dt.normalize()
df = df[df["__date__"] >= SPLIT_DATE].sort_values("__dt__").reset_index(drop=True)

if df.empty:
    st.info("Nessun evento dopo la split_date.")
    st.stop()

# ─────────────────────────────────────────────────────────────
# Colonne da mostrare + formattazioni (NO HTML!)
# ─────────────────────────────────────────────────────────────
wanted = [
    "Date","Time","HomeTeam","AwayTeam","FAV_odds","P>2.5",
    "FAV_goal","SFAV_goal","FAV_goal_1T","SFAV_goal_1T",
    "p_t","mu_t","Bet1","Odds1","NetProfit1","Bet2","Odds2","NetProfit2"
]
cols = [c for c in wanted if c in df.columns]
disp = df[cols].copy()

# Date in dd/mm/YYYY (senza 00:00:00)
disp["Date"] = df["__date__"].dt.strftime("%d/%m/%Y")

# Percentuali: se sono 0..1 -> %
for c in ["p_t","mu_t","P>2.5"]:
    if c in disp.columns:
        vals = pd.to_numeric(disp[c], errors="coerce")
        if vals.dropna().between(0,1).all():
            disp[c] = (vals*100).round(1).astype(str) + "%"

# Quote / NetProfit: 2 decimali
for c in ["FAV_odds","Odds1","Odds2","NetProfit1","NetProfit2"]:
    if c in disp.columns:
        v = pd.to_numeric(disp[c], errors="coerce")
        disp[c] = v.round(2)

# NaN -> stringa vuota per la tabella
disp = disp.fillna("")

# ─────────────────────────────────────────────────────────────
# Funzione: tabella Matplotlib stile sonofacorner
# ─────────────────────────────────────────────────────────────
def matplotlib_table(df_show: pd.DataFrame, title: str | None = None, max_rows: int = 120):
    data = df_show.head(max_rows)  # per sicurezza

    # dimensioni in base a colonne/righe
    ncol = data.shape[1]
    nrow = data.shape[0]
    fig_w = min(22, 6 + 0.85 * ncol)         # larghezza dinamica
    fig_h = min(30, 1.2 + 0.35 * nrow)       # altezza dinamica

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=150)
    fig.patch.set_facecolor(PAGE_BG)
    ax.set_facecolor(PAGE_BG)
    ax.axis('off')

    cell_text = data.astype(str).values.tolist()
    col_labels = list(data.columns)

    # crea tabella
    table = ax.table(
        cellText=cell_text,
        colLabels=col_labels,
        cellLoc='center',
        colLoc='center',
        loc='upper left',
        colColours=[HEADER_BG]*ncol
    )

    # stile base
    table.auto_set_font_size(False)
    table.set_fontsize(9)

    # larghezze automatiche
    for i in range(ncol):
        table.auto_set_column_width(i)

    # zebra rows, bordi e allineamenti numerici a destra
    numeric_cols = {"FAV_odds","P>2.5","FAV_goal","SFAV_goal","FAV_goal_1T","SFAV_goal_1T",
                    "p_t","mu_t","Bet1","Odds1","NetProfit1","Bet2","Odds2","NetProfit2"}

    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor(GRID_COL)
        cell.set_linewidth(0.7)

        if row == 0:
            cell.set_height(0.55)  # header più alto
            cell.get_text().set_color(TEXT_COL)
            cell.get_text().set_fontweight('bold')
        else:
            if row % 2 == 0:
                cell.set_facecolor(ROW_EVEN)

            # allinea a destra le colonne numeriche
            if col_labels[col] in numeric_cols:
                cell._text.set_ha('right')

    if title:
        ax.set_title(title, fontsize=14, fontweight="bold", color=TEXT_COL, pad=12)

    plt.tight_layout()
    return fig

# ─────────────────────────────────────────────────────────────
# Render tabella (Matplotlib ONLY)
# ─────────────────────────────────────────────────────────────
st.subheader("Partite (dopo split_date)")
fig_table = matplotlib_table(disp, title=None)
st.pyplot(fig_table, use_container_width=True)

# ─────────────────────────────────────────────────────────────
# NetProfit cumulato (Matplotlib)
# ─────────────────────────────────────────────────────────────
np1 = pd.to_numeric(df.get("NetProfit1",0), errors="coerce").fillna(0.0)
np2 = pd.to_numeric(df.get("NetProfit2",0), errors="coerce").fillna(0.0)
by_day = pd.DataFrame({"Date": df["__date__"], "NetProfit1": np1, "NetProfit2": np2}) \
           .groupby("Date", as_index=False).sum()
by_day["Cum_NetProfit1"] = by_day["NetProfit1"].cumsum()
by_day["Cum_NetProfit2"] = by_day["NetProfit2"].cumsum()

fig_w = 12
fig_h = 4.6
fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=150)
fig.patch.set_facecolor(PAGE_BG); ax.set_facecolor(PAGE_BG)
ax.plot(by_day["Date"], by_day["Cum_NetProfit1"], label="Cum_NetProfit1")
ax.plot(by_day["Date"], by_day["Cum_NetProfit2"], label="Cum_NetProfit2")
ax.grid(True, color=GRID_COL, linewidth=0.7)
ax.spines[:].set_visible(False)
ax.tick_params(axis='x', rotation=0)
ax.legend(frameon=False)
ax.set_title("NetProfit cumulato", color=TEXT_COL)
st.pyplot(fig, use_container_width=True)
