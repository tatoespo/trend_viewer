import streamlit as st
import pandas as pd
import io
import os

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ---------------------------------
# PAGE SETUP + THEME (font stile diretta/sofascore)
# ---------------------------------
st.set_page_config(page_title="Trend Deep-Dive", layout="wide")
st.markdown("""
<style>
:root {
  --base-font: Inter, Roboto, "Segoe UI", system-ui, -apple-system, sans-serif;
}
html, body, [class*="css"]  { font-family: var(--base-font); }
h1, h2, h3, h4 { font-weight: 800; letter-spacing: -0.2px; }
thead tr th { font-weight: 700; }
</style>
""", unsafe_allow_html=True)

st.title("Trend Deep-Dive")

# ---------------------------------
# CONFIG: split_date
# - Se esiste app/config.yaml legge da lì (chiave: split_date)
# - Altrimenti default = 2022-08-01
# Accetta formati "2022-08-01" o "1/08/2022"
# ---------------------------------
def load_split_date():
    default_str = "2022-08-01"
    cfg_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    split_str = default_str
    try:
        import yaml  # richiede pyyaml
        if os.path.exists(cfg_path):
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
                split_str = cfg.get("split_date", default_str)
    except Exception:
        # se pyyaml non è installato, usa il default
        split_str = default_str

    # normalizza in oggetto date
    split_dt = pd.to_datetime(str(split_str), dayfirst=True, errors="coerce")
    if pd.isna(split_dt):
        split_dt = pd.to_datetime(default_str)
    return split_dt.normalize()

SPLIT_DATE = load_split_date()

# ---------------------------------
# CREDENZIALI: dai Secrets di Streamlit Cloud
# ---------------------------------
creds_dict = st.secrets["google_service_account"]
creds = service_account.Credentials.from_service_account_info(
    creds_dict,
    scopes=["https://www.googleapis.com/auth/drive.readonly"]
)
drive_service = build("drive", "v3", credentials=creds)

# ---------------------------------
# Parametro trend nell'URL
# ---------------------------------
trend = st.query_params.get("trend", None)
if not trend:
    st.warning("⚠️ Nessun trend passato nell’URL.")
    st.stop()

base_trend = trend[:-1]
st.caption(f"Trend selezionato: {trend}  •  Split date: {SPLIT_DATE.date().isoformat()}")

# ---------------------------------
# Recupera parquet da Google Drive
# ---------------------------------
query = f"name='{base_trend}.parquet'"
results = drive_service.files().list(q=query, fields="files(id, name)").execute()
files = results.get("files", [])
if not files:
    st.error(f"Nessun file trovato per {base_trend}.parquet")
    st.stop()

file_id = files[0]["id"]
request = drive_service.files().get_media(fileId=file_id)
fh = io.BytesIO()
downloader = MediaIoBaseDownload(fh, request)
done = False
while not done:
    status, done = downloader.next_chunk()
fh.seek(0)

df = pd.read_parquet(fh)

# ---------------------------------
# Prepara DateTime e filtra dopo split_date
# ---------------------------------
# Gestisce formati tipo 15/08/2025 20:30
date_str = df.get("Date", pd.Series("", index=df.index)).astype(str).fillna("")
time_str = df.get("Time", pd.Series("", index=df.index)).astype(str).fillna("")
dt = pd.to_datetime((date_str + " " + time_str).str.strip(),
                    dayfirst=True, errors="coerce")
df["__dt__"] = dt
df = df[df["__dt__"] >= SPLIT_DATE].sort_values("__dt__").reset_index(drop=True)

# Se dopo il filtro non resta nulla
if df.empty:
    st.info("Nessun evento dopo la split_date.")
    st.stop()

# ---------------------------------
# Tabella in stile 'diretta' (blocco unico, no frame scrollabile)
# ---------------------------------
wanted_cols = [
    "Date", "Time", "HomeTeam", "AwayTeam", "FAV_odds", "P>2.5",
    "FAV_goal", "SFAV_goal", "FAV_goal_1T", "SFAV_goal_1T",
    "p_t", "mu_t", "Bet1", "Odds1", "NetProfit1", "Bet2", "Odds2", "NetProfit2"
]
present_cols = [c for c in wanted_cols if c in df.columns]
table = df[present_cols].copy()

# formattazioni leggere
for c in ["FAV_odds", "Odds1", "Odds2"]:
    if c in table.columns:
        table[c] = pd.to_numeric(table[c], errors="coerce").round(2)
for c in ["p_t", "mu_t", "P>2.5"]:
    if c in table.columns:
        # mostra come percentuale se già in 0-1, altrimenti lascia
        vals = pd.to_numeric(table[c], errors="coerce")
        if vals.between(0, 1).all(skipna=True):
            table[c] = (vals * 100).round(2).astype(str) + "%"

st.subheader("📋 Partite (dopo split_date)")
# st.table rende il blocco pieno senza scrollbar interna
st.table(table)

# ---------------------------------
# Grafico: NetProfit cumulato per data
# - usa NetProfit1 e NetProfit2 (somma)
# ---------------------------------
np1 = pd.to_numeric(df.get("NetProfit1", 0), errors="coerce").fillna(0.0)
np2 = pd.to_numeric(df.get("NetProfit2", 0), errors="coerce").fillna(0.0)
df["NetProfit_total"] = (np1 + np2)

cum = df[["__dt__", "NetProfit_total"]].copy()
cum = cum.groupby("__dt__", as_index=False)["NetProfit_total"].sum()
cum["CumProfit"] = cum["NetProfit_total"].cumsum()

st.subheader("💰 NetProfit cumulato")
cum_disp = cum.rename(columns={"__dt__": "Date"})
cum_disp = cum_disp[["Date", "CumProfit"]]
# line_chart usa automaticamente l'asse X come datetime
st.line_chart(cum_disp.set_index("Date"))
