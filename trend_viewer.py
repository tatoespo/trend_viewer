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

# ===================== CONFIG GENERALE =====================
st.set_page_config(page_title="Trend Deep-Dive", layout="wide")

# Colori / stile pagina
PAGE_BG   = "#EFE9E6"
TEXT_COL  = "#1E1E1E"
GRID_COL  = "#d7d2cd"
LINE_COLS = ["#287271", "#495371"]  # serie grafico

# ---------- CSS pagina, tabella e grafico ----------
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
        padding-left: 1.0rem;
        padding-right: 1.0rem;
    }}
    h1, h2, h3 {{
        margin-top: 0.2rem;
        margin-bottom: 0.6rem;
    }}

    /* DataFrame: font più grande e righe più alte + stesso sfondo */
    div[data-testid="stDataFrame"] table {{
        font-size: 16px !important;
        background-color: {PAGE_BG} !important;
        border-collapse: separate !important;
        border-spacing: 0 !important;
    }}
    div[data-testid="stDataFrame"] tbody tr {{
        height: 36px !important;
    }}

    /* solo bordi orizzontali */
    div[data-testid="stDataFrame"] thead th {{
        background-color: {PAGE_BG} !important;
        color: {TEXT_COL} !important;
        border-top: 2px solid {TEXT_COL} !important;
        border-bottom: 2px solid {TEXT_COL} !important;
        border-left: 0 !important;
        border-right: 0 !important;
        font-weight: 700 !important;
    }}
    div[data-testid="stDataFrame"] tbody td {{
        background-color: {PAGE_BG} !important;
        border-top: 1px solid {GRID_COL} !important;
        border-bottom: 1px solid {GRID_COL} !important;
        border-left: 0 !important;
        border-right: 0 !important;
    }}

    /* Allineamento elementi ai margini */
    div[data-testid="stDataFrame"],
    div[data-testid="stVegaLiteChart"] > div {{
        margin-left: 0 !important;
    }}
    </style>
    """,
    unsafe_allow_html=True
)

st.title("Trend Deep-Dive")

# ===================== SPLIT DATE =====================
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

# ===================== PARAMETRI URL =====================
trend = st.query_params.get("trend")
if not trend:
    st.warning("⚠️ Nessun trend passato nell’URL. Usa ?trend=CODICE_TREND.")
    st.stop()

base_trend = trend[:-1]
st.caption(f"Trend selezionato: **{trend}** • Split date: **{SPLIT_DATE.date()}**")

# ===================== GOOGLE DRIVE =====================
try:
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["google_service_account"],
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
except KeyError:
    st.error("Manca la sezione `google_service_account` nei secrets Streamlit.")
    st.stop()

drive = build("drive", "v3", credentials=creds)

# ===================== DOWNLOAD PARQUET =====================
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

# ===================== PREPARA DATI =====================
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

# Date
tbl["Date"] = df["__date__"].dt.strftime("%d/%m/%Y")

# Time: +1h e senza secondi
_time_parsed = pd.to_datetime(tbl["Time"], errors="coerce")
_time_shifted = _time_parsed + pd.to_timedelta(1, unit="h")
tbl["Time"] = _time_shifted.dt.strftime("%H:%M").fillna(tbl["Time"])

# FT e PT
if {"FAV_goal","SFAV_goal"}.issubset(tbl.columns):
    tbl["FT"] = tbl["FAV_goal"].astype("Int64").astype(str) + "-" + tbl["SFAV_goal"].astype("Int64").astype(str)
    tbl.drop(columns=["FAV_goal","SFAV_goal"], inplace=True, errors="ignore")
if {"FAV_goal_1T","SFAV_goal_1T"}.issubset(tbl.columns):
    tbl["PT"] = tbl["FAV_goal_1T"].astype("Int64").astype(str) + "-" + tbl["SFAV_goal_1T"].astype("Int64").astype(str)
    tbl.drop(columns=["FAV_goal_1T","SFAV_goal_1T"], inplace=True, errors="ignore")

# Percentuali/decimali (manteniamo NetProfit numerici!)
for c in ["p_t","mu_t","P>2.5"]:
    if c in tbl.columns:
        v = pd.to_numeric(tbl[c], errors="coerce")
        if v.dropna().between(0,1).all():
            tbl[c] = (v*100).round(1).astype(str) + "%"

for c in ["FAV_odds","Odds1","Odds2"]:
    if c in tbl.columns:
        tbl[c] = pd.to_numeric(tbl[c], errors="coerce").round(2)

# NetProfit rimangono numerici
for c in ["NetProfit1","NetProfit2"]:
    if c in tbl.columns:
        tbl[c] = pd.to_numeric(tbl[c], errors="coerce")

# Bet1/Bet2 → icone
def bet_icon(v):
    try:
        v = int(v)
    except Exception:
        return "—"
    if v == 1:   return "▲"
    if v == -1:  return "▼"
    return "—"

if "Bet1" in tbl.columns:
    tbl["Bet1"] = tbl["Bet1"].apply(bet_icon)
if "Bet2" in tbl.columns:
    tbl["Bet2"] = tbl["Bet2"].apply(bet_icon)

# Ordine colonne
order = [c for c in ["Date","Time","HomeTeam","AwayTeam","FT","PT","FAV_odds","P>2.5","p_t","mu_t",
                     "Bet1","Odds1","NetProfit1","Bet2","Odds2","NetProfit2"] if c in tbl.columns]
tbl = tbl[order]

# ===================== STYLER: SOLO BORDI ORIZZONTALI + BARRE =====================
numeric_right = [c for c in ["FAV_odds","Odds1","Odds2"] if c in tbl.columns]

def style_bet(s):
    styles = []
    for v in s:
        if v == "▲":
            styles.append("color:#2e7d32;font-weight:700")
        elif v == "▼":
            styles.append("color:#2e7d32;font-weight:700")
        else:
            styles.append("color:#b68b00;font-weight:700")
    return styles

base_styles = [
    {"selector":"table", "props":[("background-color", PAGE_BG), ("color", TEXT_COL), ("border-collapse","separate"), ("border-spacing","0")]},
    {"selector":"thead th", "props":[("background-color", PAGE_BG), ("color", TEXT_COL),
                                     ("border-top", f"2px solid {TEXT_COL}"),
                                     ("border-bottom", f"2px solid {TEXT_COL}"),
                                     ("border-left","0"), ("border-right","0"),
                                     ("font-weight","700")]},
    {"selector":"tbody td", "props":[("border-top", f"1px solid {GRID_COL}"),
                                     ("border-bottom", f"1px solid {GRID_COL}"),
                                     ("border-left","0"), ("border-right","0"),
                                     ("background-color", PAGE_BG)]},
]

styler = tbl.style.set_table_styles(base_styles).set_properties(**{"text-align":"left"})
if numeric_right:
    styler = styler.set_properties(subset=numeric_right, **{"text-align":"right"})

for bet_col in ["Bet1","Bet2"]:
    if bet_col in tbl.columns:
        styler = styler.apply(style_bet, subset=[bet_col])

# Barrette per NetProfit (verde>0, rosso<0) – colonne rimangono numeriche
for np_col in ["NetProfit1","NetProfit2"]:
    if np_col in tbl.columns:
        styler = styler.bar(
            subset=[np_col],
            align="mid",
            color=["#c0392b", "#2e7d32"],  # rosso, verde
            vmin=tbl[np_col].min(skipna=True),
            vmax=tbl[np_col].max(skipna=True),
        ).format({np_col: "{:.2f}"})

# ===================== RENDER TABELLA =====================
st.subheader("Partite (dopo split_date)")
st.dataframe(styler, use_container_width=True, hide_index=True)

# ===================== NETPROFIT CUMULATO (ALTAIR) =====================
np1 = pd.to_numeric(df.get("NetProfit1", 0), errors="coerce").fillna(0.0)
np2 = pd.to_numeric(df.get("NetProfit2", 0), errors="coerce").fillna(0.0)

by_day = (
    pd.DataFrame({"Date": df["__date__"], "NetProfit1": np1, "NetProfit2": np2})
      .groupby("Date", as_index=False).sum()
      .sort_values("Date")
)
by_day["Cum_NetProfit1"] = by_day["NetProfit1"].cumsum()
by_day["Cum_NetProfit2"] = by_day["NetProfit2"].cumsum()

chart_df = by_day.melt(
    id_vars="Date",
    value_vars=["Cum_NetProfit1","Cum_NetProfit2"],
    var_name="Serie",
    value_name="Valore"
)

color_scale = alt.Scale(
    domain=["Cum_NetProfit1","Cum_NetProfit2"],
    range=LINE_COLS
)

chart = (
    alt.Chart(chart_df, background=PAGE_BG)
      .mark_line()
      .encode(
          x=alt.X("Date:T", axis=alt.Axis(title=None, labelColor=TEXT_COL, tickColor=TEXT_COL, domainColor=TEXT_COL)),
          y=alt.Y("Valore:Q", axis=alt.Axis(title=None, labelColor=TEXT_COL, tickColor=TEXT_COL, domainColor=TEXT_COL)),
          color=alt.Color("Serie:N", scale=color_scale, legend=alt.Legend(title=None, labelColor=TEXT_COL))
      )
      .properties(height=380, width="container")
      .configure_view(strokeWidth=0)
      .configure_axis(grid=True, gridColor=GRID_COL, gridOpacity=0.7)
)

st.subheader("NetProfit cumulato")
st.altair_chart(chart, use_container_width=True)
