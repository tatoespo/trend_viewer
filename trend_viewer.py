# trend_viewer.py — versione stabile (NO font custom)

import os, io, re
import pandas as pd
import streamlit as st

# Matplotlib solo per la tabella stile sonofacorner
import matplotlib
import matplotlib.pyplot as plt

# ============== CONFIG BASICA ==============
st.set_page_config(page_title="Trend Deep-Dive", layout="wide")
PAGE_BG   = "#F7F5F2"
HEADER_BG = "#EFECE6"
ROW_EVEN  = "#FBFAF7"
GRID_COL  = "#C6C6C6"
TEXT_COL  = "#1E1E1E"

# CSS semplice, niente font custom
st.markdown(
    f"""
    <style>
      html, body, [class*="stApp"] {{
        background-color:{PAGE_BG};
        color:{TEXT_COL};
        font-family: system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial, sans-serif;
      }}
      .block-container {{ max-width: 1600px; padding-top: 1rem; }}
    </style>
    """,
    unsafe_allow_html=True
)

matplotlib.rcParams.update({"figure.facecolor": PAGE_BG, "axes.facecolor": PAGE_BG})

# ============== UTILS ==============
def _parse_split(val):
    if val is None:
        return None
    s = str(val).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return pd.to_datetime(s).normalize()
    return pd.to_datetime(s, dayfirst=True, errors="coerce").normalize()

def draw_mpl_table(dataframe: pd.DataFrame, max_rows: int = 150):
    df = dataframe.head(max_rows)
    ncol, nrow = df.shape[1], df.shape[0]

    fig_w = min(24, 6 + 0.9 * ncol)
    base_row_h = 0.34
    header_h   = base_row_h * 1.1
    fig_h = min(30, 1.0 + header_h + base_row_h * nrow)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=150)
    fig.patch.set_facecolor(PAGE_BG)
    ax.set_facecolor(PAGE_BG)
    ax.axis("off")

    table = ax.table(
        cellText=df.astype(str).values.tolist(),
        colLabels=list(df.columns),
        cellLoc='center', colLoc='center',
        loc='upper left', colColours=[HEADER_BG]*ncol
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    for i in range(ncol):
        table.auto_set_column_width(i)

    y_under_header = None
    body_y = []
    numeric_cols = {
        "FAV_odds","P>2.5","FAV_goal","SFAV_goal","FAV_goal_1T","SFAV_goal_1T",
        "p_t","mu_t","Bet1","Odds1","NetProfit1","Bet2","Odds2","NetProfit2"
    }

    for (row, col), cell in table.get_celld().items():
        cell.set_linewidth(0.0)  # niente verticali
        if row == 0:
            cell.set_height(header_h)
            cell.get_text().set_color(TEXT_COL)
            cell.get_text().set_fontweight('bold')
            y_under_header = cell.xy[1]
        else:
            cell.set_height(base_row_h)
            if row % 2 == 0: cell.set_facecolor(ROW_EVEN)
            if col == 0: body_y.append(cell.xy[1])
        if list(df.columns)[col] in numeric_cols:
            cell._text.set_ha('right')

    if y_under_header is not None:
        ax.hlines(y_under_header, 0, 1, colors=GRID_COL, linewidth=1.0, linestyles='solid')
    for y in sorted(set(body_y), reverse=True):
        ax.hlines(y, 0, 1, colors=GRID_COL, linewidth=0.8, linestyles=(0,(3,3)))

    plt.tight_layout()
    return fig

# ============== APP MAIN (con errori mostrati a schermo) ==============
try:
    st.title("Trend Deep-Dive")

    # Query params
    trend = st.query_params.get("trend")
    SPLIT_DATE = _parse_split(st.query_params.get("split")) or pd.Timestamp(2022, 8, 1)

    # Se manca trend, uso un dataset demo così la pagina comunque parte
    if not trend:
        st.info("Nessun trend nell’URL (?trend=CODICE). Carico un dataset demo per verificare l'app…")
        demo = pd.DataFrame({
            "Date":["01/08/2022","02/08/2022","03/08/2022","04/08/2022"],
            "Time":["17:30:00","19:45:00","14:00:00","17:00:00"],
            "HomeTeam":["Napoli","Roma","Inter","Juventus"],
            "AwayTeam":["Monza","Cremonese","Monza","Spezia"],
            "FAV_odds":[1.35,1.24,1.31,1.29],
            "P>2.5":[0.53,0.64,0.53,0.52],
            "FAV_goal":[4,1,3,2],
            "SFAV_goal":[0,0,0,0],
            "FAV_goal_1T":[2,0,2,1],
            "SFAV_goal_1T":[0,0,0,0],
            "p_t":[0.65,0.65,0.65,0.65],
            "mu_t":[0.70,0.63,0.70,0.62],
            "Bet1":[0,0,0,0],
            "Odds1":[2.59,1.54,2.64,1.59],
            "NetProfit1":[1.0,-1.0,1.0,0.59],
            "Bet2":[0,0,0,0],
            "Odds2":[1.51,2.63,1.50,2.51],
            "NetProfit2":[0.51,0.53,0.5,0.0],
        })
        df = demo.copy()
    else:
        # === Caricamento reale da Google Drive ===
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseDownload

        base_trend = trend[:-1]  # parquet senza ultima cifra
        st.caption(f"Trend selezionato: **{trend}** • Split date: **{SPLIT_DATE.date()}**")

        creds = service_account.Credentials.from_service_account_info(
            st.secrets["google_service_account"],
            scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        drive = build("drive", "v3", credentials=creds)
        resp = drive.files().list(q=f"name='{base_trend}.parquet'", fields="files(id,name)").execute()
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

    # === Prep date/time + filtro ===
    date_str = df.get("Date", pd.Series("", index=df.index)).astype(str).fillna("")
    time_str = df.get("Time", pd.Series("", index=df.index)).astype(str).fillna("")
    dt = pd.to_datetime((date_str + " " + time_str).str.strip(), dayfirst=True, errors="coerce")
    df["__dt__"] = dt
    df["__date__"] = df["__dt__"].dt.normalize()

    df = df[df["__date__"] >= SPLIT_DATE].sort_values("__dt__").reset_index(drop=True)
    if df.empty:
        st.info("Nessun evento dopo la split_date."); st.stop()

    # === Tabella “sonofacorner-like” ===
    wanted = [
        "Date","Time","HomeTeam","AwayTeam","FAV_odds","P>2.5",
        "FAV_goal","SFAV_goal","FAV_goal_1T","SFAV_goal_1T",
        "p_t","mu_t","Bet1","Odds1","NetProfit1","Bet2","Odds2","NetProfit2"
    ]
    cols = [c for c in wanted if c in df.columns]
    tbl = df[cols].copy()
    tbl["Date"] = df["__date__"].dt.strftime("%d/%m/%Y")

    for c in ["p_t","mu_t","P>2.5"]:
        if c in tbl.columns:
            v = pd.to_numeric(tbl[c], errors="coerce")
            if v.dropna().between(0,1).all():
                tbl[c] = (v*100).round(1).astype(str) + "%"

    for c in ["FAV_odds","Odds1","Odds2","NetProfit1","NetProfit2"]:
        if c in tbl.columns:
            tbl[c] = pd.to_numeric(tbl[c], errors="coerce").round(2)

    tbl = tbl.fillna("")
    st.subheader("Partite (dopo split_date)")
    fig_table = draw_mpl_table(tbl)
    st.pyplot(fig_table, use_container_width=True)

    # === Grafico cumulato interattivo ===
    np1 = pd.to_numeric(df.get("NetProfit1", 0), errors="coerce").fillna(0.0)
    np2 = pd.to_numeric(df.get("NetProfit2", 0), errors="coerce").fillna(0.0)
    by_day = (
        pd.DataFrame({"Date": df["__date__"], "NetProfit1": np1, "NetProfit2": np2})
        .groupby("Date", as_index=False).sum().sort_values("Date")
    )
    by_day["Cum_NetProfit1"] = by_day["NetProfit1"].cumsum()
    by_day["Cum_NetProfit2"] = by_day["NetProfit2"].cumsum()

    st.subheader("NetProfit cumulato")
    st.line_chart(by_day.set_index("Date")[["Cum_NetProfit1","Cum_NetProfit2"]],
                  use_container_width=True)

except Exception as e:
    st.error("⚠️ Errore non gestito nell’app. Lo mostro qui sotto:")
    st.exception(e)
