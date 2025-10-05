# trend_viewer.py — DEBUG BUILD MINIMALE
import os, io, traceback, re
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Trend Deep-Dive (debug)", layout="wide")
st.title("Trend Deep-Dive — modalità debug")

def section(title):
    st.markdown(f"### {title}")

try:
    section("1) Lettura query params")
    qp = st.query_params
    st.write("Query params:", dict(qp))

    trend = qp.get("trend")
    if not trend:
        st.warning("Manca ?trend=... nell’URL. Esempio: ?trend=110FTH1201362")
        st.stop()
    base_trend = trend[:-1]
    st.write("trend:", trend, " → base_trend:", base_trend)

    # ---- Split date
    def _parse_split(val):
        if val is None: return None
        s = str(val).strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return pd.to_datetime(s).normalize()
        return pd.to_datetime(s, dayfirst=True, errors="coerce").normalize()

    split = _parse_split(qp.get("split")) or pd.Timestamp(2022,8,1)
    st.write("split_date:", split.date())

    # ---- Secrets
    section("2) Verifica secrets")
    if "google_service_account" not in st.secrets:
        st.error("Nei secrets manca la chiave [google_service_account].")
        st.stop()
    svc = st.secrets["google_service_account"]
    st.write("Service account presente. client_email:", svc.get("client_email","<n/d>"))

    # ---- Google Drive client
    section("3) Inizializzo Google Drive API")
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload

    creds = service_account.Credentials.from_service_account_info(
        svc, scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    drive = build("drive", "v3", credentials=creds)
    st.success("Drive API ok")

    # ---- Cerco il parquet
    section("4) Cerco il file parquet su Drive")
    q = f"name='{base_trend}.parquet'"
    st.code(f"Drive query: {q}")
    resp = drive.files().list(q=q, fields="files(id,name)").execute()
    files = resp.get("files", [])
    st.write("Risultati:", files)

    if not files:
        st.error(f"Nessun file {base_trend}.parquet trovato su Drive.")
        st.stop()

    # ---- Scarico e leggo il parquet
    section("5) Download & lettura parquet")
    buf = io.BytesIO()
    req = drive.files().get_media(fileId=files[0]["id"])
    down = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = down.next_chunk()
    buf.seek(0)

    df = pd.read_parquet(buf)
    st.success(f"Parquet letto: {len(df):,} righe, {len(df.columns)} colonne")
    st.dataframe(df.head(10), use_container_width=True)

    # ---- Parsing date/time + filtro split
    section("6) Parsing date/time e filtro split_date")
    date_str = df.get("Date", pd.Series("", index=df.index)).astype(str).fillna("")
    time_str = df.get("Time", pd.Series("", index=df.index)).astype(str).fillna("")
    dt = pd.to_datetime((date_str + " " + time_str).str.strip(), dayfirst=True, errors="coerce")
    df["__dt__"] = dt
    df["__date__"] = df["__dt__"].dt.normalize()
    st.write("Null nelle datetime:", int(df["__dt__"].isna().sum()))

    df2 = df[df["__date__"] >= split].sort_values("__dt__").reset_index(drop=True)
    st.success(f"Dopo filtro split: {len(df2):,} righe")
    st.dataframe(df2.head(20), use_container_width=True)

    section("✅ Fine debug")
    st.info("Se sei arrivato fin qui, l’app base funziona. Il problema era nello styling/extra.")
except Exception as e:
    st.error("❌ Eccezione catturata. Stacktrace completo qui sotto:")
    st.exception(e)
    st.code(traceback.format_exc())
