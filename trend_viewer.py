import streamlit as st
import pandas as pd
import io, os, re

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ------------------------------------------------------------
# PAGE THEME (stile diretta.it)
# ------------------------------------------------------------
st.set_page_config(page_title="Trend Deep-Dive", layout="wide")
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
:root{--font: 'Inter', Roboto, 'Segoe UI', system-ui, -apple-system, sans-serif;}
html, body, [class*="css"]{font-family:var(--font)}
.block-container{padding-top:1.2rem; max-width:1400px}
h1{font-weight:800; letter-spacing:-.2px; margin-bottom:.25rem}
h2{font-weight:700; margin:.9rem 0 .45rem}
.small-note{color:#6c757d; font-size:.92rem}

.score-wrap{border:1px solid #e6e8eb; border-radius:12px; overflow:hidden; background:#fff}
table.score{width:100%; border-collapse:collapse; font-size:.93rem}
.score thead th{
  background:#f6f7f8; color:#141414; font-weight:600;
  padding:10px 8px; border-bottom:1px solid #e6e8eb; text-transform:uppercase;
  font-size:.78rem; letter-spacing:.03em;
}
.score tbody td{padding:9px 8px; border-bottom:1px solid #f1f3f5}
.score tbody tr:nth-child(even){background:#fbfcfe}
.score tbody tr:hover{background:#eef3ff}
.score .num{ text-align:right; font-variant-numeric:tabular-nums; }
.score .team{ font-weight:600; }
</style>
""", unsafe_allow_html=True)

st.title("Trend Deep-Dive")

# ------------------------------------------------------------
# Split date – robust parser + override via URL
# ------------------------------------------------------------
def _parse_split(any_val):
    """Ritorna pd.Timestamp normalizzato (00:00) da diversi formati."""
    if any_val is None:
        return None
    s = str(any_val).strip()
    # se ISO YYYY-MM-DD, prendo diretto (nessuna ambiguità)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return pd.to_datetime(s).normalize()
    # altrimenti uso dayfirst=True per 1/08/2022, 01-08-2022, ecc.
    return pd.to_datetime(s, dayfirst=True, errors="coerce").normalize()

def load_split_date():
    # 1) query param
    qp = st.query_params.get("split")
    dt = _parse_split(qp)
    if dt is not None:
        return dt
    # 2) config.yaml nella stessa cartella (chiave: split_date)
    cfg_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    try:
        import yaml
        if os.path.exists(cfg_path):
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
                dt = _parse_split(cfg.get("split_date"))
                if dt is not None:
                    return dt
    except Exception:
        pass
    # 3) default
    return pd.Timestamp(2022, 8, 1)  # 2022-08-01

SPLIT_DATE = load_split_date()

# ------------------------------------------------------------
# Credenziali Google (Streamlit Secrets)
# ------------------------------------------------------------
creds_dict = st.secrets["google_service_account"]
creds = service_account.Credentials.from_service_account_info(
    creds_dict, scopes=["https://www.googleapis.com/auth/drive.readonly"]
)
drive = build("drive", "v3", credentials=creds)

# ------------------------------------------------------------
# Parametri
# ------------------------------------------------------------
trend = st.query_params.get("trend")
if not trend:
    st.warning("⚠️ Nessun trend passato nell’URL.")
    st.stop()

base_trend = trend[:-1]  # nome file parquet senza ultima cifra
st.markdown(
    f'<div class="small-note">Trend selezionato: <b>{trend}</b> • Split date: <b>{SPLIT_DATE.date()}</b></div>',
    unsafe_allow_html=True
)

# ------------------------------------------------------------
# Scarica parquet da Drive
# ------------------------------------------------------------
query = f"name='{base_trend}.parquet'"
res = drive.files().list(q=query, fields="files(id,name)").execute()
files = res.get("files", [])
if not files:
    st.error(f"Nessun file trovato: {base_trend}.parquet")
    st.stop()

fid = files[0]["id"]
buf = io.BytesIO()
req = drive.files().get_media(fileId=fid)
down = MediaIoBaseDownload(buf, req)
done = False
while not done:
    _, done = down.next_chunk()
buf.seek(0)

df = pd.read_parquet(buf)

# ------------------------------------------------------------
# Date/Time + filtro dopo split_date (solo data, no ora)
# ------------------------------------------------------------
date_str = df.get("Date", pd.Series("", index=df.index)).astype(str).fillna("")
time_str = df.get("Time", pd.Series("", index=df.index)).astype(str).fillna("")
dt = pd.to_datetime((date_str + " " + time_str).str.strip(), dayfirst=True, errors="coerce")
df["__dt__"] = dt
df["__date__"] = df["__dt__"].dt.normalize()

df = df[df["__date__"] >= SPLIT_DATE].sort_values("__dt__").reset_index(drop=True)
if df.empty:
    st.info("Nessun evento dopo la split_date.")
    st.stop()

# ------------------------------------------------------------
# Tabella "diretta.it" (HTML, senza scroll)
# ------------------------------------------------------------
wanted = [
    "Date","Time","HomeTeam","AwayTeam","FAV_odds","P>2.5",
    "FAV_goal","SFAV_goal","FAV_goal_1T","SFAV_goal_1T",
    "p_t","mu_t","Bet1","Odds1","NetProfit1","Bet2","Odds2","NetProfit2"
]
cols = [c for c in wanted if c in df.columns]
tbl = df[cols].copy()

# sistemazione formati
# - mostra Date come dd/mm/yyyy (senza 00:00:00)
tbl["Date"] = df["__date__"].dt.strftime("%d/%m/%Y")
# - percentuali piacevoli
for c in ["p_t","mu_t","P>2.5"]:
    if c in tbl.columns:
        vals = pd.to_numeric(tbl[c], errors="coerce")
        if vals.dropna().between(0,1).all():
            tbl[c] = (vals*100).round(2).astype(str) + "%"
# - quote e netprofit con 2 decimali
for c in ["FAV_odds","Odds1","Odds2","NetProfit1","NetProfit2"]:
    if c in tbl.columns:
        tbl[c] = pd.to_numeric(tbl[c], errors="coerce").round(2)

# rende alcune colonne più “forti”
for c in ["HomeTeam","AwayTeam"]:
    if c in tbl.columns:
        tbl[c] = tbl[c].apply(lambda x: f'<span class="team">{x}</span>')

# allineamento right per numeriche
num_cols = {"FAV_odds","P>2.5","p_t","mu_t","Odds1","Odds2","NetProfit1","NetProfit2",
            "FAV_goal","SFAV_goal","FAV_goal_1T","SFAV_goal_1T"}
def _fmt_cell(col, val):
    cls = "num" if col in num_cols else ""
    return f'<td class="{cls}">{val if pd.notna(val) else ""}</td>'

# costruzione HTML (senza indice)
headers = "".join([f"<th>{c}</th>" for c in tbl.columns])
rows = []
for _, r in tbl.iterrows():
    tds = "".join([_fmt_cell(c, r[c]) for c in tbl.columns])
    rows.append(f"<tr>{tds}</tr>")
table_html = f"""
<div class="score-wrap">
  <table class="score">
    <thead><tr>{headers}</tr></thead>
    <tbody>
      {''.join(rows)}
    </tbody>
  </table>
</div>
"""
st.subheader("📋 Partite (dopo split_date)")
st.markdown(table_html, unsafe_allow_html=True)

# ------------------------------------------------------------
# NetProfit cumulati distinti su asse Date
# ------------------------------------------------------------
np1 = pd.to_numeric(df.get("NetProfit1", 0), errors="coerce").fillna(0.0)
np2 = pd.to_numeric(df.get("NetProfit2", 0), errors="coerce").fillna(0.0)
by_day = pd.DataFrame({"Date": df["__date__"], "NetProfit1": np1, "NetProfit2": np2}) \
           .groupby("Date", as_index=False).sum()
by_day["Cum_NetProfit1"] = by_day["NetProfit1"].cumsum()
by_day["Cum_NetProfit2"] = by_day["NetProfit2"].cumsum()

st.subheader("💰 NetProfit cumulato")
st.line_chart(by_day.set_index("Date")[["Cum_NetProfit1", "Cum_NetProfit2"]])
