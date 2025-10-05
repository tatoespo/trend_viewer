import streamlit as st
import pandas as pd
import io, os, re

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ──────────────────────────────────────────────────────────────────────────────
# PAGE THEME (wide + stile "sonofacorner")
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Trend Deep-Dive", layout="wide")

st.markdown("""
<style>
/* ===== THEME ispirato a sonofacorner ===== */
:root{
  --page-bg:   #f7f6f3; /* fondo caldo (no bianco puro) */
  --card-brd:  #e2dfd9;
  --head-bg:   #f0ede7;
  --row-even:  #fbfaf7;
  --row-hover: #efece6;
  --text:      #2b2b2b;
  --title:     #3b3b3b;
  --grid:      #e6e2db;
  --radius:    12px;
}

/* Font: Lora (titoli) + Inter (testo) */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&family=Lora:wght@700&display=swap');
html, body, [class*="css"] { background: var(--page-bg); color: var(--text); }
h1,h2 { font-family: 'Lora', serif; color: var(--title); }
p,div,table { font-family: 'Inter', system-ui, -apple-system, sans-serif; }
.block-container{ padding-top: 1.1rem; max-width: 100%; }

.small-note{ color:#6c757d; font-size:.92rem; margin-top:-.25rem; }

/* WRAPPER TABELLA: mai tagliare l’ultima colonna */
.table-wrap{
  border:1px solid var(--card-brd);
  border-radius: var(--radius);
  background: #fff;
  overflow-x:auto; overflow-y:hidden;
}

/* TABELLA stile sonofacorner */
table.sf { width:100%; border-collapse: collapse; white-space:nowrap; font-size:.92rem; }
.sf thead th{
  background: var(--head-bg);
  color: var(--title);
  font-weight: 600; font-size: .80rem;
  padding: 10px 8px; border-bottom:1px solid var(--grid);
  text-transform: uppercase; letter-spacing:.02em; text-align:center;
}
.sf tbody td{
  padding: 8px 8px; border-bottom: 1px solid var(--grid);
}
.sf tbody tr:nth-child(even){ background: var(--row-even); }
.sf tbody tr:hover{ background: var(--row-hover); }
.sf td.num{ text-align:right; font-variant-numeric: tabular-nums; }
.sf td.team{ font-weight:600; }
</style>
""", unsafe_allow_html=True)

st.title("Trend Deep-Dive")

# ──────────────────────────────────────────────────────────────────────────────
# split_date robusto (+ override via ?split=YYYY-MM-DD)
# ──────────────────────────────────────────────────────────────────────────────
def _parse_split(val):
    if val is None: return None
    s = str(val).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return pd.to_datetime(s).normalize()
    return pd.to_datetime(s, dayfirst=True, errors="coerce").normalize()

def load_split_date():
    # 1) query param
    qp = st.query_params.get("split")
    dt = _parse_split(qp)
    if dt is not None: return dt
    # 2) config.yaml
    cfg = os.path.join(os.path.dirname(__file__), "config.yaml")
    try:
        import yaml
        if os.path.exists(cfg):
            with open(cfg,"r",encoding="utf-8") as f:
                dt = _parse_split((yaml.safe_load(f) or {}).get("split_date"))
                if dt is not None: return dt
    except Exception:
        pass
    # 3) default
    return pd.Timestamp(2022,8,1)

SPLIT_DATE = load_split_date()

# ──────────────────────────────────────────────────────────────────────────────
# Google Drive auth (secrets)
# ──────────────────────────────────────────────────────────────────────────────
creds = service_account.Credentials.from_service_account_info(
    st.secrets["google_service_account"],
    scopes=["https://www.googleapis.com/auth/drive.readonly"]
)
drive = build("drive","v3",credentials=creds)

# ──────────────────────────────────────────────────────────────────────────────
# Parametri URL
# ──────────────────────────────────────────────────────────────────────────────
trend = st.query_params.get("trend")
if not trend:
    st.warning("⚠️ Nessun trend passato nell’URL."); st.stop()
base_trend = trend[:-1]
st.markdown(
    f'<div class="small-note">Trend selezionato: <b>{trend}</b> • Split date: <b>{SPLIT_DATE.date()}</b></div>',
    unsafe_allow_html=True
)

# Optional: scelta stile tabella
mode = st.radio("Stile tabella", ["Interattiva (HTML)", "Screenshot (Matplotlib)"], horizontal=True)

# ──────────────────────────────────────────────────────────────────────────────
# Scarica parquet
# ──────────────────────────────────────────────────────────────────────────────
res = drive.files().list(q=f"name='{base_trend}.parquet'", fields="files(id,name)").execute()
files = res.get("files",[])
if not files: st.error(f"Nessun file {base_trend}.parquet"); st.stop()

buf = io.BytesIO()
req = drive.files().get_media(fileId=files[0]["id"])
down = MediaIoBaseDownload(buf, req)
done = False
while not done: _, done = down.next_chunk()
buf.seek(0)
df = pd.read_parquet(buf)

# ──────────────────────────────────────────────────────────────────────────────
# Date/Time + filtro (solo data) + ordinamento
# ──────────────────────────────────────────────────────────────────────────────
date_str = df.get("Date", pd.Series("", index=df.index)).astype(str).fillna("")
time_str = df.get("Time", pd.Series("", index=df.index)).astype(str).fillna("")
dt = pd.to_datetime((date_str+" "+time_str).str.strip(), dayfirst=True, errors="coerce")
df["__dt__"] = dt
df["__date__"] = df["__dt__"].dt.normalize()
df = df[df["__date__"] >= SPLIT_DATE].sort_values("__dt__").reset_index(drop=True)
if df.empty: st.info("Nessun evento dopo la split_date."); st.stop()

# ──────────────────────────────────────────────────────────────────────────────
# Prepara tabella
# ──────────────────────────────────────────────────────────────────────────────
wanted = [
  "Date","Time","HomeTeam","AwayTeam","FAV_odds","P>2.5",
  "FAV_goal","SFAV_goal","FAV_goal_1T","SFAV_goal_1T",
  "p_t","mu_t","Bet1","Odds1","NetProfit1","Bet2","Odds2","NetProfit2"
]
cols = [c for c in wanted if c in df.columns]
tbl = df[cols].copy()

# formati
tbl["Date"] = df["__date__"].dt.strftime("%d/%m/%Y")
for c in ["p_t","mu_t","P>2.5"]:
    if c in tbl.columns:
        v = pd.to_numeric(tbl[c], errors="coerce")
        if v.dropna().between(0,1).all():
            tbl[c] = (v*100).round(1).astype(str)+"%"
for c in ["FAV_odds","Odds1","Odds2","NetProfit1","NetProfit2"]:
    if c in tbl.columns: tbl[c] = pd.to_numeric(tbl[c], errors="coerce").round(2)
for c in ["HomeTeam","AwayTeam"]:
    if c in tbl.columns: tbl[c] = tbl[c].apply(lambda x: f'<span class="team">{x}</span>')

num_cols = {"FAV_odds","P>2.5","p_t","mu_t","Odds1","Odds2","NetProfit1","NetProfit2",
            "FAV_goal","SFAV_goal","FAV_goal_1T","SFAV_goal_1T"}
def _td(col, val):
    cls = "num" if col in num_cols else ""
    return f'<td class="{cls}">{"" if pd.isna(val) else val}</td>'

thead = "".join([f"<th>{c}</th>" for c in tbl.columns])
rows_html = []
for _, r in tbl.iterrows():
    tds = "".join([_td(c, r[c]) for c in tbl.columns])
    rows_html.append(f"<tr>{tds}</tr>")

# ──────────────────────────────────────────────────────────────────────────────
# TAB: Interattiva (HTML) o Screenshot (Matplotlib)
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("📋 Partite (dopo split_date)")

if mode == "Interattiva (HTML)":
    html_table = f"""
    <div class="table-wrap">
      <table class="sf">
        <thead><tr>{thead}</tr></thead>
        <tbody>{''.join(rows_html)}</tbody>
      </table>
    </div>
    """
    st.markdown(html_table, unsafe_allow_html=True)

else:
    # Render “fedele” con Matplotlib (come nel tutorial)
    import matplotlib.pyplot as plt
    def matplotlib_table(df_show, max_rows=40, title=None):
        data = df_show.head(max_rows)
        page_bg = "#f7f6f3"; header_bg = "#f0ede7"; grid = "#e2dfd9"; row_even = "#fbfaf7"
        fig_h = 1.1 + 0.35*len(data)
        fig, ax = plt.subplots(figsize=(min(18, 0.18*len(data.columns)+8), fig_h), dpi=150)
        fig.patch.set_facecolor(page_bg); ax.axis('off')
        cell_text = data.astype(str).values.tolist()
        col_labels = list(data.columns)
        the_table = ax.table(cellText=cell_text, colLabels=col_labels,
                             cellLoc='center', loc='upper left',
                             colColours=[header_bg]*len(col_labels))
        the_table.auto_set_font_size(False); the_table.set_fontsize(9)
        for i,_ in enumerate(col_labels): the_table.auto_set_column_width(col=i)
        for (row, col), cell in the_table.get_celld().items():
            cell.set_edgecolor(grid); cell.set_linewidth(0.7)
            if row==0: cell.set_height(0.5)
            elif row%2==0: cell.set_facecolor(row_even)
        if title: ax.set_title(title, fontsize=14, fontweight="bold", pad=12)
        plt.tight_layout(); return fig

    fig = matplotlib_table(tbl, max_rows=50, title=None)
    st.pyplot(fig, use_container_width=True)

# ──────────────────────────────────────────────────────────────────────────────
# NetProfit cumulati distinti per data
# ──────────────────────────────────────────────────────────────────────────────
np1 = pd.to_numeric(df.get("NetProfit1",0), errors="coerce").fillna(0.0)
np2 = pd.to_numeric(df.get("NetProfit2",0), errors="coerce").fillna(0.0)
by_day = pd.DataFrame({"Date": df["__date__"], "NetProfit1": np1, "NetProfit2": np2}) \
           .groupby("Date", as_index=False).sum()
by_day["Cum_NetProfit1"] = by_day["NetProfit1"].cumsum()
by_day["Cum_NetProfit2"] = by_day["NetProfit2"].cumsum()

st.subheader("💰 NetProfit cumulato")
st.line_chart(by_day.set_index("Date")[["Cum_NetProfit1","Cum_NetProfit2"]])
