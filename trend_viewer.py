import streamlit as st
import pandas as pd
import io, os, re

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ──────────────────────────────────────────────────────────────────────────────
# PAGE THEME (wide + stile diretta.it)
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Trend Deep-Dive", layout="wide")
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');

:root{--font:'Inter',Roboto,'Segoe UI',system-ui,-apple-system,sans-serif}
html, body, [class*="css"]{font-family:var(--font)}
/* usa tutta la larghezza pagina */
.block-container{padding-top:1.2rem; max-width:100%}
h1{font-weight:800; letter-spacing:-.2px; margin-bottom:.25rem}
h2{font-weight:700; margin:1rem 0 .5rem}
.small-note{color:#6c757d; font-size:.92rem}

/* contenitore tabella: nessun taglio: se serve, sola barra orizzontale */
.score-wrap{
  border:1px solid #e6e8eb; border-radius:12px; background:#fff;
  padding:0; overflow-x:auto; overflow-y:hidden; /* ← FIX colonna finale */
}

/* tabella compatta stile diretta */
table.score{ border-collapse:collapse; width:100%; font-size:.90rem; white-space:nowrap }
.score thead th{
  background:#f6f7f8; color:#141414; font-weight:600;
  padding:8px 6px; border-bottom:1px solid #e6e8eb; text-transform:uppercase;
  font-size:.76rem; letter-spacing:.03em; text-align:center;
}
.score tbody td{ padding:6px 6px; border-bottom:1px solid #f1f3f5 }
.score tbody tr:nth-child(even){ background:#fbfcfe }
.score tbody tr:hover{ background:#eef3ff }
.score .num{ text-align:right; font-variant-numeric:tabular-nums }
.score .team{ font-weight:600 }
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
    qp = st.query_params.get("split")
    dt = _parse_split(qp)
    if dt is not None: return dt
    cfg_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    try:
        import yaml
        if os.path.exists(cfg_path):
            with open(cfg_path,"r",encoding="utf-8") as f:
                dt = _parse_split((yaml.safe_load(f) or {}).get("split_date"))
                if dt is not None: return dt
    except Exception:
        pass
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
# Parametri
# ──────────────────────────────────────────────────────────────────────────────
trend = st.query_params.get("trend")
if not trend:
    st.warning("⚠️ Nessun trend passato nell’URL."); st.stop()
base_trend = trend[:-1]

st.markdown(
    f'<div class="small-note">Trend selezionato: <b>{trend}</b> • Split date: <b>{SPLIT_DATE.date()}</b></div>',
    unsafe_allow_html=True
)

# ──────────────────────────────────────────────────────────────────────────────
# Scarica il parquet
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
# Date/Time + filtro (solo data, no ora) + ordinamento crescente
# ──────────────────────────────────────────────────────────────────────────────
date_str = df.get("Date", pd.Series("", index=df.index)).astype(str).fillna("")
time_str = df.get("Time", pd.Series("", index=df.index)).astype(str).fillna("")
dt = pd.to_datetime((date_str+" "+time_str).str.strip(), dayfirst=True, errors="coerce")
df["__dt__"] = dt
df["__date__"] = df["__dt__"].dt.normalize()
df = df[df["__date__"] >= SPLIT_DATE].sort_values("__dt__").reset_index(drop=True)
if df.empty: st.info("Nessun evento dopo la split_date."); st.stop()

# ──────────────────────────────────────────────────────────────────────────────
# Tabella HTML stile diretta.it (nessun taglio, ultima colonna visibile)
# ──────────────────────────────────────────────────────────────────────────────
wanted = [
  "Date","Time","HomeTeam","AwayTeam","FAV_odds","P>2.5",
  "FAV_goal","SFAV_goal","FAV_goal_1T","SFAV_goal_1T",
  "p_t","mu_t","Bet1","Odds1","NetProfit1","Bet2","Odds2","NetProfit2"
]
cols = [c for c in wanted if c in df.columns]
tbl = df[cols].copy()

# formati compatti
tbl["Date"] = df["__date__"].dt.strftime("%d/%m/%Y")
for c in ["p_t","mu_t","P>2.5"]:
    if c in tbl.columns:
        v = pd.to_numeric(tbl[c], errors="coerce")
        if v.dropna().between(0,1).all(): tbl[c] = (v*100).round(2).astype(str)+"%"
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
rows = []
for _, r in tbl.iterrows():
    tds = "".join([_td(c, r[c]) for c in tbl.columns])
    rows.append(f"<tr>{tds}</tr>")
html_table = f"""
<div class="score-wrap">
  <table class="score">
    <thead><tr>{thead}</tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</div>
"""
st.subheader("📋 Partite (dopo split_date)")
st.markdown(html_table, unsafe_allow_html=True)

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

