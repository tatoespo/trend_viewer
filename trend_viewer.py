import streamlit as st
import pandas as pd
import io, os

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# -----------------------------------------------------------------------------
# PAGE SETUP + THEME (font & stile alla "diretta.it"/"sofascore")
# -----------------------------------------------------------------------------
st.set_page_config(page_title="Trend Deep-Dive", layout="wide")
st.markdown("""
<style>
/* Google Font (Inter) */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');

:root { --base-font: 'Inter', Roboto, 'Segoe UI', system-ui, -apple-system, sans-serif; }
html, body, [class*="css"] { font-family: var(--base-font); }

/* Titoli */
h1 { font-weight: 800; letter-spacing: -0.2px; margin-bottom: .25rem; }
h2 { font-weight: 700; margin: 1.2rem 0 .6rem; }

/* Card-look per contenuti */
.block-container { padding-top: 1.5rem; }
section.main > div { max-width: 1400px; }

/* Tabella stile diretta.it */
table { border-collapse: collapse; border-radius: 12px; overflow: hidden; }
thead th {
  background: #f6f7f8;
  color: #141414;
  font-weight: 600;
  padding: 10px 8px !important;
  border-bottom: 1px solid #e6e8eb;
}
tbody td { padding: 8px 8px !important; }
tbody tr:nth-child(even) { background: #fafbfc; }
tbody tr:hover { background: #eef2f6; }

/* Didascalie */
.small-note { color:#6c757d; font-size: 0.92rem; margin-bottom:.25rem; }
</style>
""", unsafe_allow_html=True)

st.title("Trend Deep-Dive")

# -----------------------------------------------------------------------------
# CONFIG: split_date
# - prova a leggere app/config.yaml (chiave: split_date)
# - se non c'è o manca PyYAML → default "2022-08-01"
# Accetta formati "2022-08-01" o "1/08/2022"
# -----------------------------------------------------------------------------
def load_split_date():
    default_str = "2022-08-01"
    split_str = default_str
    cfg_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    try:
        import yaml  # facoltativo
        if os.path.exists(cfg_path):
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
                split_str = str(cfg.get("split_date", default_str))
    except Exception:
        split_str = default_str

    # parsing robusto (dayfirst=True per date tipo 1/08/2022)
    dt = pd.to_datetime(split_str, dayfirst=True, errors="coerce")
    if pd.isna(dt):  # fallback
        dt = pd.to_datetime(default_str)
    return dt.normalize()

SPLIT_DATE = load_split_date()

# -----------------------------------------------------------------------------
# CREDENZIALI: dai Secrets di Streamlit Cloud
# -----------------------------------------------------------------------------
creds_dict = st.secrets["google_service_account"]
creds = service_account.Credentials.from_service_account_info(
    creds_dict,
    scopes=["https://www.googleapis.com/auth/drive.readonly"]
)
drive = build("drive", "v3", credentials=creds)

# -----------------------------------------------------------------------------
# Parametro trend nell'URL
# -----------------------------------------------------------------------------
trend = st.query_params.get("trend")
if not trend:
    st.warning("⚠️ Nessun trend passato nell’URL.")
    st.stop()

base_trend = trend[:-1]  # nome file parquet senza l'ultima cifra
st.markdown(
    f'<div class="small-note">Trend selezionato: <b>{trend}</b> • Split date: <b>{SPLIT_DATE.date().isoformat()}</b></div>',
    unsafe_allow_html=True
)

# -----------------------------------------------------------------------------
# Recupera il parquet da Google Drive
# -----------------------------------------------------------------------------
query = f"name='{base_trend}.parquet'"
res = drive.files().list(q=query, fields="files(id,name)").execute()
files = res.get("files", [])
if not files:
    st.error(f"Nessun file trovato: {base_trend}.parquet")
    st.stop()

file_id = files[0]["id"]
fh = io.BytesIO()
req = drive.files().get_media(fileId=file_id)
down = MediaIoBaseDownload(fh, req)
done = False
while not done:
    _, done = down.next_chunk()
fh.seek(0)

df = pd.read_parquet(fh)

# -----------------------------------------------------------------------------
# Parsing Date/Time e FILTRO dopo split_date (solo la data conta)
# -----------------------------------------------------------------------------
# crea una colonna datetime robusta a formati gg/mm/aaaa + Time
date_str = df.get("Date", pd.Series("", index=df.index)).astype(str).fillna("")
time_str = df.get("Time", pd.Series("", index=df.index)).astype(str).fillna("")
dt = pd.to_datetime((date_str + " " + time_str).str.strip(), dayfirst=True, errors="coerce")
df["__dt__"] = dt

# filtro: tieni SOLO righe con data >= split_date (ignora l’ora)
mask = df["__dt__"].dt.date >= SPLIT_DATE.date()
df = df.loc[mask].sort_values("__dt__").reset_index(drop=True)

if df.empty:
    st.info("Nessun evento dopo la split_date.")
    st.stop()

# -----------------------------------------------------------------------------
# Tabella (no scrolling), colonne richieste
# -----------------------------------------------------------------------------
wanted_cols = [
    "Date","Time","HomeTeam","AwayTeam","FAV_odds","P>2.5",
    "FAV_goal","SFAV_goal","FAV_goal_1T","SFAV_goal_1T",
    "p_t","mu_t","Bet1","Odds1","NetProfit1","Bet2","Odds2","NetProfit2"
]
present_cols = [c for c in wanted_cols if c in df.columns]
table = df[present_cols].copy()

# formattazioni leggere
for c in ["FAV_odds","Odds1","Odds2"]:
    if c in table.columns:
        table[c] = pd.to_numeric(table[c], errors="coerce").round(2)

for c in ["p_t","mu_t","P>2.5"]:
    if c in table.columns:
        vals = pd.to_numeric(table[c], errors="coerce")
        # se i valori sono tra 0 e 1 li mostro come percentuali
        if vals.dropna().between(0,1).all():
            table[c] = (vals * 100).round(2).astype(str) + "%"

st.subheader("📋 Partite (dopo split_date)")
st.table(table)  # blocco unico come una pagina, niente frame scrollabile

# -----------------------------------------------------------------------------
# NetProfit cumulati (DISTINTI) su asse Date
# -----------------------------------------------------------------------------
np1 = pd.to_numeric(df.get("NetProfit1", 0), errors="coerce").fillna(0.0)
np2 = pd.to_numeric(df.get("NetProfit2", 0), errors="coerce").fillna(0.0)

series = pd.DataFrame({
    "Date": df["__dt__"].dt.normalize(),
    "NetProfit1": np1,
    "NetProfit2": np2
})

# sommo per data (se più partite nello stesso giorno), poi cumulati distinti
daily = series.groupby("Date", as_index=False).sum()
daily["Cum_NetProfit1"] = daily["NetProfit1"].cumsum()
daily["Cum_NetProfit2"] = daily["NetProfit2"].cumsum()

st.subheader("💰 NetProfit cumulato (1 e 2)")
plot_df = daily.set_index("Date")[["Cum_NetProfit1","Cum_NetProfit2"]]
st.line_chart(plot_df)
