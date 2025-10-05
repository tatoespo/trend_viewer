# trend_viewer.py — modalità DIAGNOSTICA (minima possibile)

import sys, platform, os
import streamlit as st

st.set_page_config(page_title="Trend Deep-Dive (diagnosi)", layout="wide")
st.title("Trend Deep-Dive — diagnostica minimale")

# Mostra info ambiente per capire se il processo arriva fin qui
st.write({
    "python": sys.version,
    "platform": platform.platform(),
    "streamlit": st.__version__,
    "cwd": os.getcwd(),
})

# Query params (API nuove; se non esistessero, fallback)
try:
    qp = st.query_params
except Exception:
    qp = {}  # fallback se l'API non esiste
st.write("Query params:", dict(qp))

# Piccolo dataframe in memoria, niente pandas necessario
data = [
    {"Date": "01/08/2022", "HomeTeam": "Napoli", "AwayTeam": "Monza", "NetProfit1": 1.0},
    {"Date": "02/08/2022", "HomeTeam": "Roma",   "AwayTeam": "Cremonese", "NetProfit1": -1.0},
]
st.write("Tabella demo (inline):", data)

st.success("Se vedi questa pagina, l'app è viva. Il crash era altrove (import/dep/codice).")

