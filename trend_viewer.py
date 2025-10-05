import os
import io
import re
import pandas as pd
import streamlit as st
import altair as alt

# Google Drive API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ============== CONFIG GENERALE ==============
st.set_page_config(page_title="Trend Deep-Dive", layout="wide")

# --- Colori e stile richiesti ---
PAGE_BG   = "#EFE9E6"     # sfondo pagina e grafici
TEXT_COL  = "#1E1E1E"
LINE_COLS = ["#287271", "#495371"]  # serie 1 e 2

# ---------- CSS: sfondo, allineamento ai margini, tabella leggibile ----------
st.markdown(
    f"""
    <style>
    html, body, [class*="stApp"] {{
        background-color: {PAGE_BG};
        color: {TEXT_COL};
    }}

    .block-container {{
        max-width: 1600px;
        padding-top: 0.6rem;
        padding-left: 1.0rem;   /* margine sinistro omogeneo */
        padding-right: 1.0rem;
    }}

    h1, h2, h3 {{
        margin-top: 0.2rem;
        margin-bottom: 0.5rem;
    }}

    /* Tabella nativa: font più grande e righe più alte */
    div[data-testid="stDataFrame"] table {{
        font-size: 16px !important;        /* testi più grandi */
    }}
    div[data-testid="stDataFrame"] tbody tr {{
        height: 36px !important;           /* righe più alte */
    }}

    /* Assicura che gli elementi grafici siano allineati a sinistra */
    div[data-testid="stVegaLiteChart"] > div, 
    div[data-testid="stDataFrame"] {{
        margin-left: 0 !important;
    }}
    </style>
    """,
    unsafe_allow_html=True
)

st.title("Trend Deep-Dive")

# ============== SPLIT DATE ==============
def _parse_split(val):
    if val is None:
        return None
    s = str(val).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):           # ISO: niente warning
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

# formattazioni percentuali/decimali
for c in ["p_t","mu_t","P>2.5"]:
    if c in tbl.columns:
        v = pd.to_numeric(tbl[c], errors="coerce")
        if v.dropna().between(0,1).all():
            tbl[c] = (v*100).round(1).astype(str) + "%"

for c in ["FAV_odds","Odds1","Odds2","NetProfit1","NetProfit2"]:
    if c in tbl.columns:
        tbl[c] = pd.to_numeric(tbl[c], errors="coerce").round(2)

tbl = tbl.fillna("")

# ============== TABELLA NATIVA STREAMLIT ==============
st.subheader("Partite (dopo split_date)")
st.dataframe(
    tbl,
    use_container_width=True,
    hide_index=True
)

# ============== NETPROFIT CUMULATO (ALTAIR, STESSO SFONDO) ==============
np1 = pd.to_numeric(df.get("NetProfit1", 0), errors="coerce").fillna(0.0)
np2 = pd.to_numeric(df.get("NetProfit2", 0), errors="coerce").fillna(0.0)

by_day = (
    pd.DataFrame({"Date": df["__date__"], "NetProfit1": np1, "NetProfit2": np2})
      .groupby("Date", as_index=False).sum()
      .sort_values("Date")
)
by_day["Cum_NetProfit1"] = by_day["NetProfit1"].cumsum()
by_day["Cum_NetProfit2"] = by_day["NetProfit2"].cumsum()

# dati in formato "long" per Altair
chart_df = by_day.melt(
    id_vars="Date",
    value_vars=["Cum_NetProfit1", "Cum_NetProfit2"],
    var_name="Serie",
    value_name="Valore"
)

color_scale = alt.Scale(
    domain=["Cum_NetProfit1", "Cum_NetProfit2"],
    range=LINE_COLS
)

base = alt.Chart(chart_df).encode(
    x=alt.X("Date:T", axis=alt.Axis(title=None, labelColor=TEXT_COL, tickColor=TEXT_COL, domainColor=TEXT_COL)),
    y=alt.Y("Valore:Q", axis=alt.Axis(title=None, labelColor=TEXT_COL, tickColor=TEXT_COL, domainColor=TEXT_COL)),
    color=alt.Color("Serie:N", scale=color_scale, legend=alt.Legend(title=None, labelColor=TEXT_COL))
)

line = base.mark_line().properties(
    width="container",
    height=360,
    background=PAGE_BG
).configure_view(
    strokeWidth=0,   # senza bordo esterno
).configure_axis(
    grid=True,
    gridColor="#d7d2cd",
    gridOpacity=0.7
)

st.subheader("NetProfit cumulato")
st.altair_chart(line, use_container_width=True)
