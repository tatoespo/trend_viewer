# ===================== DIAGNOSTIC BUILD =====================
# Mostra a schermo TUTTE le eccezioni con traceback e stampa
# lo stato di ogni step (segreti, Drive, parquet, ecc.)
# Sostituisci temporaneamente il tuo trend_viewer.py con questo.

import os, io, sys, traceback
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Trend Deep-Dive • DIAGNOSTIC", layout="wide")
st.title("🔎 Trend Deep-Dive — DIAGNOSTIC")

def ok(msg):    st.success(msg)
def info(msg):  st.info(msg)
def warn(msg):  st.warning(msg)
def err(msg):   st.error(msg)

# Per mostrare sempre i dettagli errori (lato client)
st.write("Booting…")

def show_versions():
    import platform
    st.subheader("Environment")
    st.write({
        "python": sys.version,
        "platform": platform.platform(),
        "streamlit": st.__version__,
        "pandas": pd.__version__,
    })

def show_exception(e: Exception, where: str):
    err(f"❌ Crash in: **{where}**")
    st.exception(e)
    st.code("".join(traceback.format_exception(type(e), e, e.__traceback__)), language="text")

try:
    show_versions()

    # =============== 1) Query params ===============
    st.subheader("Step 1 — Query params")
    qp = st.query_params
    st.write("Query params:", dict(qp))
    trend = qp.get("trend")
    if not trend:
        warn("Parametro `trend` assente. Prova ad aprire l’app con `?trend=CODICE_TREND`.")
        st.stop()
    base_trend = trend[:-1]  # come nel tuo codice originale
    ok(f"trend: **{trend}**  / base_trend: **{base_trend}**")

    # =============== 2) Split date ===============
    st.subheader("Step 2 — Split date")
    import re
    def _parse_split(val):
        if val is None: return None
        s = str(val).strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return pd.to_datetime(s).normalize()
        return pd.to_datetime(s, dayfirst=True, errors="coerce").normalize()

    split = _parse_split(qp.get("split")) or pd.Timestamp(2022,8,1)
    ok(f"split_date: **{split.date()}**")

    # =============== 3) Segreti ===============
    st.subheader("Step 3 — Streamlit secrets")
    try:
        svc = st.secrets["google_service_account"]
        # Non stampiamo la chiave; controlliamo i campi
        required = ["type","client_email","private_key","token_uri"]
        missing = [k for k in required if not svc.get(k)]
        if missing:
            raise RuntimeError(f"Nei secrets mancano i campi: {missing}")
        st.write({
            "type": svc.get("type"),
            "client_email": svc.get("client_email"),
            "private_key_len": len(svc.get("private_key","")),
        })
        ok("Secrets: OK")
    except Exception as e:
        show_exception(e, "Lettura secrets")
        st.stop()

    # =============== 4) Google Drive build() ===============
    st.subheader("Step 4 — Google Drive client")
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseDownload

        creds = service_account.Credentials.from_service_account_info(
            svc, scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        drive = build("drive", "v3", credentials=creds)
        ok("Client Google Drive: OK")
    except Exception as e:
        show_exception(e, "Build Google Drive client")
        st.stop()

    # =============== 5) Ricerca file su Drive ===============
    st.subheader("Step 5 — Ricerca file su Drive")
    try:
        resp = drive.files().list(
            q=f"name='{base_trend}.parquet'",
            fields="files(id,name,size,modifiedTime)",
            pageSize=10
        ).execute()
        files = resp.get("files", [])
        st.write("Files trovati:", files)
        if not files:
            raise FileNotFoundError(f"Nessun file {base_trend}.parquet")
        file_id = files[0]["id"]
        ok(f"File trovato: **{files[0]['name']}** (id: {file_id})")
    except Exception as e:
        show_exception(e, "Ricerca file Drive")
        st.stop()

    # =============== 6) Download e lettura parquet ===============
    st.subheader("Step 6 — Download & read_parquet")
    try:
        buf = io.BytesIO()
        req = drive.files().get_media(fileId=file_id)
        down = MediaIoBaseDownload(buf, req)
        done = False
        while not done:
            _, done = down.next_chunk()
        buf.seek(0)

        df = pd.read_parquet(buf)  # usa pyarrow
        ok(f"Parquet letto: **{len(df)}** righe, **{len(df.columns)}** colonne")
        st.write("Preview:", df.head(5))
    except Exception as e:
        show_exception(e, "Lettura parquet")
        st.stop()

    # =============== 7) Parse Date/Time + filtro ===============
    st.subheader("Step 7 — Parse Date/Time + filtro")
    try:
        date_str = df.get("Date", pd.Series("", index=df.index)).astype(str).fillna("")
        time_str = df.get("Time", pd.Series("", index=df.index)).astype(str).fillna("")
        dt = pd.to_datetime((date_str + " " + time_str).str.strip(), dayfirst=True, errors="coerce")
        df["__dt__"] = dt
        df["__date__"] = df["__dt__"].dt.normalize()
        df = df[df["__date__"] >= split].sort_values("__dt__").reset_index(drop=True)
        ok(f"Dopo filtro split: **{len(df)}** righe")
        st.write(df.head(10))
        if df.empty:
            warn("Nessun evento dopo la split_date. Fine diagnostica.")
            st.stop()
    except Exception as e:
        show_exception(e, "Parsing date/time & filtro")
        st.stop()

    # =============== 8) Mini output finale ===============
    st.subheader("Step 8 — Fine diagnostica")
    ok("La pipeline è passata. A questo punto il problema NON è nei segreti, Drive o parquet.")
    st.write("Ora possiamo rimettere la tabella Matplotlib e i grafici con calma.")
except Exception as e:
    show_exception(e, "MAIN (errore non catturato)")
