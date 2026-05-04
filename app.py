"""GOPV — login, registro y navbar."""

import io
import os
import subprocess
import tempfile
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components
from fpdf import FPDF
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

import database

st.set_page_config(
    page_title="GOPV",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

MES_NOMBRES = (
    "Enero",
    "Febrero",
    "Marzo",
    "Abril",
    "Mayo",
    "Junio",
    "Julio",
    "Agosto",
    "Septiembre",
    "Octubre",
    "Noviembre",
    "Diciembre",
)

COLS_VENTAS_EDIT = [
    "fact_rep_mostrador",
    "fact_rep_taller",
    "desc_mostrador",
    "desc_taller",
    "util_pct_mostrador",
    "util_pct_taller",
    "fact_servicios",
    "fact_maquinarias",
    "fact_alquileres",
    "gastos_fijos_usd",
    "gastos_var_otros_usd",
]

# Formato tipo printf (sprintf.js) para NumberColumn — 2 decimales
_FMT_ARS = "$ %,.2f"
_FMT_USD = "US$ %,.2f"
_FMT_PCT = "%.2f %%"

# Paleta UI (compatible claro/oscuro): gris, amarillo, negro + naranja opaco.
_COL_GRAY = "#7A7A7A"
_COL_YELLOW = "#E0B100"
_COL_BLACK = "#1A1A1A"
_COL_ORANGE_MUTED = "#B56A3B"

_PREVIEW_MONEY_COLS = (
    "fact_rep_mostrador",
    "fact_rep_taller",
    "desc_mostrador",
    "desc_taller",
    "fact_servicios",
    "fact_maquinarias",
    "fact_alquileres",
    "total_repuestos",
    "total_bruto",
    "gastos_variables_tot",
    "gastos_total",
)
_PREVIEW_FRAC_PCT_COLS = (
    "util_pct_mostrador",
    "util_pct_taller",
    "util_prom_pct",
)

# En vista previa: montos ya en USD (no convertir con TC).
_PREVIEW_USD_COLS = (
    "gastos_fijos_usd",
    "gastos_var_otros_usd",
)
# En grilla y en filas de preview: guardados en USD (no aplicar TC al mostrar).
_PREVIEW_STORED_USD_COLS = (
    "fact_maquinarias",
    "fact_alquileres",
)

# Vista previa por sucursal: sin margen/resultado/factor (no se discriminan por sucursal).
_PREVIEW_DROP_COLS = (
    "gastos_variables_tot",
    "gastos_total",
    "margen_contrib",
    "margen_contrib_pct",
    "resultado",
    "factor_absorcion",
    "util_pct_servicios",
)

_PREVIEW_LABELS: dict[str, str] = {
    "fact_rep_mostrador": "Fact. rep. mostrador",
    "fact_rep_taller": "Fact. rep. taller",
    "desc_mostrador": "Desc. mostrador",
    "desc_taller": "Desc. taller",
    "fact_servicios": "Fact. servicios",
    "fact_maquinarias": "Ventas maquinarias",
    "fact_alquileres": "Ventas alquileres",
    "gastos_fijos_usd": "Gastos fijos",
    "gastos_var_otros_usd": "Otros gastos var.",
    "total_repuestos": "Total repuestos",
    "total_bruto": "Facturación total",
    "gastos_variables_tot": "Gastos variables",
    "gastos_total": "Gastos total (línea)",
}

_PREVIEW_FRAC_PCT_LABELS: dict[str, str] = {
    "util_pct_mostrador": "Util. % rep. mostrador",
    "util_pct_taller": "Util. % rep. taller",
    "util_prom_pct": "Util. % rep. promedio",
}


def _column_config_cierre_editor() -> dict:
    return {
        "sucursal": st.column_config.TextColumn("Sucursal", disabled=True, width="small"),
        "fact_rep_mostrador": st.column_config.NumberColumn(
            "Fact. rep. Mostrador (ARS)", format=_FMT_ARS, min_value=0.0, step=0.01
        ),
        "fact_rep_taller": st.column_config.NumberColumn(
            "Fact. rep. Taller (ARS)", format=_FMT_ARS, min_value=0.0, step=0.01
        ),
        "desc_mostrador": st.column_config.NumberColumn(
            "Desc. Mostrador (ARS)", format=_FMT_ARS, min_value=0.0, step=0.01
        ),
        "desc_taller": st.column_config.NumberColumn(
            "Desc. Taller (ARS)", format=_FMT_ARS, min_value=0.0, step=0.01
        ),
        "util_pct_mostrador": st.column_config.NumberColumn(
            "Util. venta % rep. Mostrador",
            format=_FMT_PCT,
            min_value=0.0,
            max_value=100.0,
            step=0.01,
        ),
        "util_pct_taller": st.column_config.NumberColumn(
            "Util. venta % rep. Taller",
            format=_FMT_PCT,
            min_value=0.0,
            max_value=100.0,
            step=0.01,
        ),
        "fact_servicios": st.column_config.NumberColumn(
            "Fact. servicios (ARS)", format=_FMT_ARS, min_value=0.0, step=0.01
        ),
        "fact_maquinarias": st.column_config.NumberColumn(
            "Ventas maquinarias (USD)", format=_FMT_USD, min_value=0.0, step=0.01
        ),
        "fact_alquileres": st.column_config.NumberColumn(
            "Ventas alquileres (USD)", format=_FMT_USD, min_value=0.0, step=0.01
        ),
        "gastos_fijos_usd": st.column_config.NumberColumn(
            "Gastos fijos (USD)", format=_FMT_USD, min_value=0.0, step=0.01
        ),
        "gastos_var_otros_usd": st.column_config.NumberColumn(
            "Otros gastos variables (USD)", format=_FMT_USD, min_value=0.0, step=0.01
        ),
    }


def _prepare_preview_display(show: pd.DataFrame, *, ver_usd: bool, tc: float) -> tuple[pd.DataFrame, dict]:
    """Escala utilidades a 0–100 para el %; opcionalmente convierte montos a USD; arma column_config."""
    disp = show.copy()
    tc_ok = float(tc) > 0
    use_usd_display = bool(ver_usd and tc_ok)
    money_fmt = _FMT_USD if use_usd_display else _FMT_ARS

    for c in _PREVIEW_MONEY_COLS:
        if c not in disp.columns:
            continue
        disp[c] = pd.to_numeric(disp[c], errors="coerce")
        if c in _PREVIEW_STORED_USD_COLS:
            continue
        if use_usd_display:
            disp[c] = disp[c] / float(tc)

    for c in _PREVIEW_USD_COLS:
        if c not in disp.columns:
            continue
        disp[c] = pd.to_numeric(disp[c], errors="coerce")

    for c in _PREVIEW_FRAC_PCT_COLS:
        if c not in disp.columns:
            continue
        disp[c] = pd.to_numeric(disp[c], errors="coerce") * 100.0

    cfg: dict = {}
    for c in disp.columns:
        if c == "sucursal":
            cfg[c] = st.column_config.TextColumn("Sucursal", width="small")
        elif c in _PREVIEW_STORED_USD_COLS:
            lab = _PREVIEW_LABELS.get(c, c)
            cfg[c] = st.column_config.NumberColumn(f"{lab} (USD)", format=_FMT_USD, step=0.01)
        elif c in _PREVIEW_MONEY_COLS:
            unit = "USD" if use_usd_display else "ARS"
            lab = _PREVIEW_LABELS.get(c, c)
            cfg[c] = st.column_config.NumberColumn(f"{lab} ({unit})", format=money_fmt, step=0.01)
        elif c in _PREVIEW_USD_COLS:
            lab = _PREVIEW_LABELS.get(c, c)
            cfg[c] = st.column_config.NumberColumn(f"{lab} (USD)", format=_FMT_USD, step=0.01)
        elif c in _PREVIEW_FRAC_PCT_COLS:
            lab = _PREVIEW_FRAC_PCT_LABELS.get(c, c)
            cfg[c] = st.column_config.NumberColumn(lab, format=_FMT_PCT, step=0.01)
        elif pd.api.types.is_numeric_dtype(disp[c]):
            cfg[c] = st.column_config.NumberColumn(c, format="%.2f", step=0.01)

    for c in disp.columns:
        if c != "sucursal" and pd.api.types.is_numeric_dtype(disp[c]):
            disp[c] = disp[c].round(2)

    return disp, cfg


def _column_config_hist_cierres(df: pd.DataFrame) -> dict:
    cfg: dict = {}
    if "id" in df.columns:
        cfg["id"] = st.column_config.NumberColumn("Id", format="%d", step=1)
    if "anio" in df.columns:
        cfg["anio"] = st.column_config.NumberColumn("Año", format="%d", step=1)
    if "mes" in df.columns:
        cfg["mes"] = st.column_config.NumberColumn("Mes", format="%d", step=1)
    if "tipo_cambio_ars_usd" in df.columns:
        cfg["tipo_cambio_ars_usd"] = st.column_config.NumberColumn(
            "TC (ARS por USD)", format="%.2f", step=0.01
        )
    return cfg


def _app_password() -> str:
    env = os.environ.get("APP_PASSWORD")
    if env:
        return env
    try:
        sec = st.secrets.get("APP_PASSWORD")
        if sec:
            return str(sec)
    except Exception:
        pass
    return "Patagonia2815$"


def _layout_css() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] { display: none !important; }
        [data-testid="collapsedControl"] { display: none !important; }
        div[data-testid="stDecoration"] { display: none !important; }
        [data-testid="stMainBlockContainer"] {
            padding-top: 4rem !important;
            max-width: 100% !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _chrome_spacer() -> None:
    components.html(
        "<style>html,body{margin:0;background:transparent}</style>",
        height=104,
        scrolling=False,
    )


def _bootstrap_database() -> None:
    """Crea tablas y migraciones ligeras (sin cache: debe correr en cada arranque de sesión)."""
    database.init_database()


def _df_editor_cierre_ventas(anio: int, mes: int) -> pd.DataFrame:
    c = database.get_cierre_ventas_mes(anio, mes)
    rows = []
    if c is None:
        for suc in database.SUCURSALES_VENTAS_REAL:
            rows.append({"sucursal": suc, **{k: 0.0 for k in COLS_VENTAS_EDIT}})
        return pd.DataFrame(rows)
    df = database.get_lineas_cierre_ventas(int(c["id"]))
    by_suc = {str(r["sucursal"]).strip(): r for _, r in df.iterrows()} if len(df) else {}
    for suc in database.SUCURSALES_VENTAS_REAL:
        if suc in by_suc:
            r = by_suc[suc]
            rows.append(
                {
                    "sucursal": suc,
                    "fact_rep_mostrador": float(r.get("fact_rep_mostrador") or 0),
                    "fact_rep_taller": float(r.get("fact_rep_taller") or 0),
                    "desc_mostrador": float(r.get("desc_mostrador") or 0),
                    "desc_taller": float(r.get("desc_taller") or 0),
                    "util_pct_mostrador": float(r.get("util_pct_mostrador") or 0) * 100.0,
                    "util_pct_taller": float(r.get("util_pct_taller") or 0) * 100.0,
                    "fact_servicios": float(r.get("fact_servicios") or 0),
                    "fact_maquinarias": float(r.get("fact_maquinarias") or 0),
                    "fact_alquileres": float(r.get("fact_alquileres") or 0),
                    "gastos_fijos_usd": float(r.get("gastos_fijos") or 0),
                    "gastos_var_otros_usd": float(r.get("gastos_var_otros_usd") or 0),
                }
            )
        else:
            rows.append({"sucursal": suc, **{k: 0.0 for k in COLS_VENTAS_EDIT}})
    out = pd.DataFrame(rows)
    n = len(database.SUCURSALES_VENTAS_REAL)
    sum_gf = float(pd.to_numeric(out["gastos_fijos_usd"], errors="coerce").fillna(0).sum())
    sum_go = float(pd.to_numeric(out["gastos_var_otros_usd"], errors="coerce").fillna(0).sum())
    hdr_gf = float(c.get("gastos_fijos_global") or 0)
    hdr_go = float(c.get("gastos_var_otros") or 0)
    if sum_gf < 1e-9 and hdr_gf > 0 and n > 0:
        out["gastos_fijos_usd"] = hdr_gf / float(n)
    if sum_go < 1e-9 and hdr_go > 0 and n > 0:
        out["gastos_var_otros_usd"] = hdr_go / float(n)
    return out


def _row_to_db_dict(s: pd.Series) -> dict:
    gf_u = float(s.get("gastos_fijos_usd") or 0)
    go_u = float(s.get("gastos_var_otros_usd") or 0)
    return {
        "sucursal": str(s["sucursal"]).strip(),
        "fact_rep_mostrador": float(s["fact_rep_mostrador"] or 0),
        "fact_rep_taller": float(s["fact_rep_taller"] or 0),
        "desc_mostrador": float(s["desc_mostrador"] or 0),
        "desc_taller": float(s["desc_taller"] or 0),
        "util_pct_mostrador": float(s["util_pct_mostrador"] or 0) / 100.0,
        "util_pct_taller": float(s["util_pct_taller"] or 0) / 100.0,
        "util_pct_servicios": 0.0,
        "fact_servicios": float(s["fact_servicios"] or 0),
        "fact_maquinarias": float(s.get("fact_maquinarias") or 0),
        "fact_alquileres": float(s.get("fact_alquileres") or 0),
        "gastos_fijos": gf_u,
        "gastos_fijos_usd": gf_u,
        "gastos_var_otros_usd": go_u,
    }


def _cierre_hdr_float(c: dict | None, key: str, default: float | None = None) -> float | None:
    if not c:
        return default
    v = c.get(key)
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return default
    return float(v)


def _registro_financiero_totales(
    edited: pd.DataFrame,
    tc_val: float,
    rubro_db: str | None,
    gastos_var_maquinarias_usd: float = 0.0,
    gastos_fijos_concesionario_usd: float = 0.0,
) -> dict:
    tc_val = max(float(tc_val), 1e-9)
    gf_suc = float(pd.to_numeric(edited.get("gastos_fijos_usd"), errors="coerce").fillna(0).sum())
    gfc = max(float(gastos_fijos_concesionario_usd), 0.0)
    gastos_fijos_usd = gf_suc + gfc
    gastos_otros_usd = float(pd.to_numeric(edited.get("gastos_var_otros_usd"), errors="coerce").fillna(0).sum())
    um_c = _util_promedio_simple(edited, "util_pct_mostrador")
    ut_c = _util_promedio_simple(edited, "util_pct_taller")
    otros_ars = float(gastos_otros_usd) * tc_val
    # Maq./alq. en USD en grilla; para mezclar con servicios en ARS en el modelo global:
    s_maq_usd = float(pd.to_numeric(edited.get("fact_maquinarias"), errors="coerce").fillna(0).sum())
    s_alq_usd = float(pd.to_numeric(edited.get("fact_alquileres"), errors="coerce").fillna(0).sum())
    s_maq = s_maq_usd * tc_val
    s_alq = s_alq_usd * tc_val
    gv = database.compute_gastos_variables_globales(
        fact_rep_mos_conc=float(edited["fact_rep_mostrador"].sum()),
        desc_rep_mos_conc=float(edited["desc_mostrador"].sum()),
        fact_rep_tal_conc=float(edited["fact_rep_taller"].sum()),
        desc_rep_tal_conc=float(edited["desc_taller"].sum()),
        fact_serv_conc=float(edited["fact_servicios"].sum()),
        util_mos_conc=um_c,
        util_tal_conc=ut_c,
        util_serv_conc=1.0,
        gastos_fijos_global=0.0,
        gastos_var_otros=otros_ars,
        gastos_var_otros_rubro=rubro_db,
        fact_maquinarias_conc=s_maq,
        fact_alquileres_conc=s_alq,
    )
    gv_serv_usd = float(gv["gv_servicios_ajustado"]) / tc_val
    gv_rep_usd = float(gv["gv_repuestos_ajustado"]) / tc_val
    gv_mos_usd = float(gv["gv_rep_mostrador"]) / tc_val
    gv_tal_usd = float(gv["gv_rep_taller"]) / tc_val
    gvm = max(float(gastos_var_maquinarias_usd), 0.0)
    gastos_var_total_usd = gv_serv_usd + gv_rep_usd + gvm
    gastos_total_usd = float(gastos_fijos_usd) + gastos_var_total_usd
    fact_total_ars = (
        float(edited["fact_rep_mostrador"].sum())
        + float(edited["fact_rep_taller"].sum())
        - float(edited["desc_mostrador"].sum())
        - float(edited["desc_taller"].sum())
        + float(edited["fact_servicios"].sum())
        + float(s_maq_usd) * tc_val
        + float(s_alq_usd) * tc_val
    )
    fact_total_usd = fact_total_ars / tc_val
    margen_global_usd = fact_total_usd - gastos_var_total_usd
    margen_global_ratio = _safe_ratio(margen_global_usd, fact_total_usd)
    resultado_global_usd = margen_global_usd - float(gastos_fijos_usd)
    factor_abs_global_pct = (
        (margen_global_usd / float(gastos_fijos_usd)) * 100.0 if float(gastos_fijos_usd) > 0 else None
    )
    punto_equilibrio_usd = (
        float(gastos_fijos_usd) / float(margen_global_ratio)
        if margen_global_ratio is not None and margen_global_ratio > 0
        else None
    )
    return {
        "tc_val": tc_val,
        "gv_mos_usd": gv_mos_usd,
        "gv_tal_usd": gv_tal_usd,
        "gv_serv_usd": gv_serv_usd,
        "gv_rep_usd": gv_rep_usd,
        "gastos_var_maq_usd": gvm,
        "gastos_var_total_usd": gastos_var_total_usd,
        "gastos_total_usd": gastos_total_usd,
        "fact_total_ars": fact_total_ars,
        "fact_total_usd": fact_total_usd,
        "margen_global_usd": margen_global_usd,
        "margen_global_ratio": margen_global_ratio,
        "resultado_global_usd": resultado_global_usd,
        "factor_abs_global_pct": factor_abs_global_pct,
        "punto_equilibrio_usd": punto_equilibrio_usd,
        "gastos_fijos_usd": gastos_fijos_usd,
        "gastos_fijos_sucursales_usd": gf_suc,
        "gastos_fijos_concesionario_usd": gfc,
        "gastos_otros_usd": gastos_otros_usd,
    }


def _df_lineas_cierre_usd(edited: pd.DataFrame, tc_val: float) -> pd.DataFrame:
    tc_val = max(float(tc_val), 1e-9)
    out = edited.copy()
    money_ars = (
        "fact_rep_mostrador",
        "fact_rep_taller",
        "desc_mostrador",
        "desc_taller",
        "fact_servicios",
    )
    money_usd_editor = ("fact_maquinarias", "fact_alquileres")
    for col in money_ars:
        if col not in out.columns:
            out[col] = 0.0
        out[f"{col}_usd"] = pd.to_numeric(out[col], errors="coerce").fillna(0.0) / tc_val
    for col in money_usd_editor:
        if col not in out.columns:
            out[col] = 0.0
        out[f"{col}_usd"] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    for col in ("gastos_fijos_usd", "gastos_var_otros_usd"):
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    keep = (
        ["sucursal"]
        + [f"{c}_usd" for c in money_ars]
        + [f"{c}_usd" for c in money_usd_editor]
        + ["gastos_fijos_usd", "gastos_var_otros_usd", "util_pct_mostrador", "util_pct_taller"]
    )
    return out[keep].round(4)


def _excel_registro_bytes(
    *,
    anio: int,
    mes: int,
    edited: pd.DataFrame,
    tot: dict,
    inventario_usd: float,
    resultado_cero_ventas_pct: float,
    fill_rate_pct: float | None,
    rotacion_inventario: float | None,
) -> bytes:
    periodo = f"{int(anio)}-{int(mes):02d}"
    resumen = pd.DataFrame(
        [
            {
                "periodo": periodo,
                "anio": int(anio),
                "mes": int(mes),
                "tipo_cambio_ars_usd": tot["tc_val"],
                "facturacion_total_usd": round(tot["fact_total_usd"], 4),
                "margen_contribucion_usd": round(tot["margen_global_usd"], 4),
                "resultado_usd": round(tot["resultado_global_usd"], 4),
                "gastos_fijos_usd": round(float(tot.get("gastos_fijos_usd") or 0), 4),
                "gastos_variables_total_usd": round(tot["gastos_var_total_usd"], 4),
                "gastos_total_usd": round(tot["gastos_total_usd"], 4),
                "inventario_usd": round(float(inventario_usd), 4),
                "resultado_cero_ventas_pct": round(float(resultado_cero_ventas_pct), 4),
                "fill_rate_pct": fill_rate_pct,
                "rotacion_inventario": rotacion_inventario,
            }
        ]
    )
    df_gastos = pd.DataFrame(
        {
            "Concepto": [
                "Gastos variables repuestos mostrador",
                "Gastos variables repuestos taller",
                "Otros gastos variables (USD cargados)",
                "Gastos variables maquinaria (USD cargados)",
                "Total gastos variables",
                "Gastos fijos sucursales",
                "Gastos fijos concesionario",
                "Total gastos fijos",
                "Total gastos",
            ],
            "Importe_USD": [
                round(tot["gv_mos_usd"], 4),
                round(tot["gv_tal_usd"], 4),
                round(float(tot.get("gastos_otros_usd") or 0), 4),
                round(float(tot.get("gastos_var_maq_usd") or 0), 4),
                round(tot["gastos_var_total_usd"], 4),
                round(float(tot.get("gastos_fijos_sucursales_usd") or 0), 4),
                round(float(tot.get("gastos_fijos_concesionario_usd") or 0), 4),
                round(float(tot.get("gastos_fijos_usd") or 0), 4),
                round(tot["gastos_total_usd"], 4),
            ],
        }
    )
    lineas = _df_lineas_cierre_usd(edited, tot["tc_val"])
    lineas.insert(0, "mes", int(mes))
    lineas.insert(0, "anio", int(anio))
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        resumen.to_excel(writer, sheet_name="Resumen", index=False)
        lineas.to_excel(writer, sheet_name="Lineas_USD", index=False)
        df_gastos.to_excel(writer, sheet_name="Gastos_variables", index=False)
    buf.seek(0)
    return buf.getvalue()


def _df_cierres_ventas_en_rango(anio_d: int, mes_d: int, anio_h: int, mes_h: int) -> pd.DataFrame:
    """Cabeceras de cierre en un rango (misma consulta que database.list_cierres_ventas_en_rango)."""
    conn = database.get_connection()
    try:
        df = database._read_sql(
            """
            SELECT *
            FROM cierre_ventas_mes
            WHERE (anio > ? OR (anio = ? AND mes >= ?))
              AND (anio < ? OR (anio = ? AND mes <= ?))
            ORDER BY anio ASC, mes ASC
            """,
            conn,
            (anio_d, anio_d, mes_d, anio_h, anio_h, mes_h),
        )
        return df if df is not None else pd.DataFrame()
    finally:
        conn.close()


def _excel_registro_rango_bytes(anio_d: int, mes_d: int, anio_h: int, mes_h: int) -> bytes | None:
    hdrs = _df_cierres_ventas_en_rango(anio_d, mes_d, anio_h, mes_h)
    if hdrs is None or len(hdrs) == 0:
        return None
    resumen_rows = []
    lineas_parts = []
    gastos_parts = []
    for _, c in hdrs.iterrows():
        a, m = int(c["anio"]), int(c["mes"])
        edited = _df_editor_cierre_ventas(a, m)
        tc = float(c["tipo_cambio_ars_usd"])
        rub = c.get("gastos_var_otros_rubro")
        if rub is not None and isinstance(rub, float) and pd.isna(rub):
            rub = None
        rub = str(rub).strip() if rub else None
        if rub is not None and (rub == "" or rub.lower() == "nan"):
            rub = None
        gvm = float(c.get("gastos_var_maquinarias_usd") or 0)
        gfc = float(c.get("gastos_fijos_concesionario_usd") or 0)
        tot = _registro_financiero_totales(edited, tc, rub, gvm, gfc)
        inv = float(c.get("inventario_usd") or 0)
        cv = float(c.get("resultado_cero_ventas_pct") or 0)
        fr = c.get("fill_rate_pct")
        if fr is not None and not (isinstance(fr, float) and pd.isna(fr)):
            fr = float(fr)
        else:
            fr = None
        rot = c.get("rotacion_inventario")
        if rot is not None and not (isinstance(rot, float) and pd.isna(rot)):
            rot = float(rot)
        else:
            rot = None
        resumen_rows.append(
            {
                "periodo": f"{a}-{m:02d}",
                "anio": a,
                "mes": m,
                "tipo_cambio_ars_usd": tot["tc_val"],
                "facturacion_total_usd": round(tot["fact_total_usd"], 4),
                "margen_contribucion_usd": round(tot["margen_global_usd"], 4),
                "resultado_usd": round(tot["resultado_global_usd"], 4),
                "gastos_fijos_usd": round(float(tot.get("gastos_fijos_usd") or 0), 4),
                "gastos_variables_total_usd": round(tot["gastos_var_total_usd"], 4),
                "gastos_total_usd": round(tot["gastos_total_usd"], 4),
                "inventario_usd": round(inv, 4),
                "resultado_cero_ventas_pct": round(cv, 4),
                "fill_rate_pct": fr,
                "rotacion_inventario": rot,
            }
        )
        ln = _df_lineas_cierre_usd(edited, tot["tc_val"])
        ln.insert(0, "mes", m)
        ln.insert(0, "anio", a)
        lineas_parts.append(ln)
        df_g = pd.DataFrame(
            {
                "anio": [a] * 9,
                "mes": [m] * 9,
                "Concepto": [
                    "Gastos variables repuestos mostrador",
                    "Gastos variables repuestos taller",
                    "Otros gastos variables (USD cargados)",
                    "Gastos variables maquinaria (USD cargados)",
                    "Total gastos variables",
                    "Gastos fijos sucursales",
                    "Gastos fijos concesionario",
                    "Total gastos fijos",
                    "Total gastos",
                ],
                "Importe_USD": [
                    round(tot["gv_mos_usd"], 4),
                    round(tot["gv_tal_usd"], 4),
                    round(float(tot.get("gastos_otros_usd") or 0), 4),
                    round(gvm, 4),
                    round(tot["gastos_var_total_usd"], 4),
                    round(float(tot.get("gastos_fijos_sucursales_usd") or 0), 4),
                    round(float(tot.get("gastos_fijos_concesionario_usd") or 0), 4),
                    round(float(tot.get("gastos_fijos_usd") or 0), 4),
                    round(tot["gastos_total_usd"], 4),
                ],
            }
        )
        gastos_parts.append(df_g)
    resumen_df = pd.DataFrame(resumen_rows)
    lineas_df = pd.concat(lineas_parts, ignore_index=True)
    gastos_df = pd.concat(gastos_parts, ignore_index=True)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        resumen_df.to_excel(writer, sheet_name="Resumen", index=False)
        lineas_df.to_excel(writer, sheet_name="Lineas_USD", index=False)
        gastos_df.to_excel(writer, sheet_name="Gastos_variables", index=False)
    buf.seek(0)
    return buf.getvalue()


def _util_ponderado(df: pd.DataFrame, canal: str) -> float | None:
    """Utilidad % repuestos en fracción, ponderada por (fact − desc) del canal."""
    num = 0.0
    den = 0.0
    if canal == "mos":
        fk, dk, uk = "fact_rep_mostrador", "desc_mostrador", "util_pct_mostrador"
    else:
        fk, dk, uk = "fact_rep_taller", "desc_taller", "util_pct_taller"
    for _, r in df.iterrows():
        w = max(float(r[fk] or 0) - float(r[dk] or 0), 0.0)
        u = (float(r[uk] or 0) / 100.0) if pd.notna(r[uk]) else 0.0
        den += w
        num += u * w
    return (num / den) if den > 0 else None


def _util_promedio_simple(df: pd.DataFrame, col: str) -> float | None:
    vals = pd.to_numeric(df.get(col), errors="coerce").dropna()
    vals = vals[vals > 0]
    if len(vals) == 0:
        return None
    # En editor vienen 0-100; en BD de cierres vienen fracción 0-1.
    if float(vals.max()) > 1.0:
        vals = vals / 100.0
    vals = vals.clip(lower=0.0, upper=1.0)
    return float(vals.mean())


def _avg_non_zero_pair(a: float | None, b: float | None) -> float:
    vals = [float(v) for v in (a, b) if v is not None and float(v) > 0]
    if not vals:
        return 0.0
    return float(sum(vals) / len(vals))


def _fila_concesionario(df_edit: pd.DataFrame, gastos_fijos_concesionario_usd: float = 0.0, tc: float = 1200.0) -> dict:
    tc = max(float(tc), 1e-9)
    sfm = float(df_edit["fact_rep_mostrador"].sum())
    sft = float(df_edit["fact_rep_taller"].sum())
    sdm = float(df_edit["desc_mostrador"].sum())
    sdt = float(df_edit["desc_taller"].sum())
    sfs = float(df_edit["fact_servicios"].sum())
    smaq_usd = float(pd.to_numeric(df_edit.get("fact_maquinarias"), errors="coerce").fillna(0).sum())
    salq_usd = float(pd.to_numeric(df_edit.get("fact_alquileres"), errors="coerce").fillna(0).sum())
    um = _util_promedio_simple(df_edit, "util_pct_mostrador")
    ut = _util_promedio_simple(df_edit, "util_pct_taller")
    if um is None:
        um = 0.0
    if ut is None:
        ut = 0.0
    calc = database.compute_cierre_venta_linea(
        sfm,
        sft,
        sdm,
        sdt,
        um,
        ut,
        sfs,
        fact_maquinarias=smaq_usd,
        fact_alquileres=salq_usd,
        tipo_cambio_ars_usd=tc,
    )
    sgf_suc = float(pd.to_numeric(df_edit.get("gastos_fijos_usd"), errors="coerce").fillna(0).sum())
    sgo = float(pd.to_numeric(df_edit.get("gastos_var_otros_usd"), errors="coerce").fillna(0).sum())
    gfc = max(float(gastos_fijos_concesionario_usd), 0.0)
    sgf_tot = sgf_suc + gfc
    return {
        "sucursal": "CONCESIONARIO",
        "fact_rep_mostrador": sfm,
        "fact_rep_taller": sft,
        "desc_mostrador": sdm,
        "desc_taller": sdt,
        "util_pct_mostrador": um,
        "util_pct_taller": ut,
        "fact_servicios": sfs,
        "fact_maquinarias": smaq_usd,
        "fact_alquileres": salq_usd,
        "gastos_fijos": sgf_tot,
        "gastos_fijos_usd": sgf_tot,
        "gastos_var_otros_usd": sgo,
        **calc,
    }


def _preview_tabla(df_edit: pd.DataFrame, gastos_fijos_concesionario_usd: float = 0.0, tc: float = 1200.0) -> pd.DataFrame:
    filas = []
    for _, s in df_edit.iterrows():
        d = _row_to_db_dict(s)
        calc = database.compute_cierre_venta_linea(
            d["fact_rep_mostrador"],
            d["fact_rep_taller"],
            d["desc_mostrador"],
            d["desc_taller"],
            d["util_pct_mostrador"],
            d["util_pct_taller"],
            d["fact_servicios"],
            fact_maquinarias=d.get("fact_maquinarias") or 0,
            fact_alquileres=d.get("fact_alquileres") or 0,
            tipo_cambio_ars_usd=max(float(tc), 1e-9),
        )
        filas.append({**d, **calc})
    base = pd.DataFrame(filas)
    base = pd.concat(
        [base, pd.DataFrame([_fila_concesionario(df_edit, gastos_fijos_concesionario_usd, tc)])],
        ignore_index=True,
    )
    return base


def _rubro_otros_a_db(etiqueta: str) -> str | None:
    e = (etiqueta or "").strip().lower()
    if e in ("servicios", "servicio"):
        return "servicios"
    if e in ("repuestos", "repuesto"):
        return "repuestos"
    return None


def _rubro_db_a_select(rubro: str | None) -> str:
    r = (rubro or "").strip().lower()
    if r in ("servicios", "servicio"):
        return "Servicios"
    if r in ("repuestos", "repuesto"):
        return "Repuestos"
    return "— Ninguno —"


def _mes_corto(mes: int) -> str:
    return MES_NOMBRES[int(mes) - 1][:3]


def _safe_ratio(num: float, den: float) -> float | None:
    if den <= 0:
        return None
    return float(num) / float(den)


def _fmt_usd(v: float | None) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"US$ {float(v):,.2f}"


def _fmt_pct(v: float | None) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):,.2f}%"


def _get_gemini_api_key() -> str | None:
    env = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if env:
        return env
    try:
        # Formato plano: GEMINI_API_KEY=...
        sec = st.secrets.get("GEMINI_API_KEY") or st.secrets.get("GOOGLE_API_KEY")
        if sec:
            return str(sec)
        # Formato seccionado: [Gemini] GEMINI_API_KEY=...
        for section_key in ("Gemini", "gemini", "GOOGLE", "google"):
            section = st.secrets.get(section_key)
            if section:
                sec2 = section.get("GEMINI_API_KEY") or section.get("GOOGLE_API_KEY")
                if sec2:
                    return str(sec2)
    except Exception:
        pass
    return None


def _build_gemini_report_analysis(sel: dict, compare_df: pd.DataFrame) -> tuple[str | None, str]:
    api_key = _get_gemini_api_key()
    if not api_key:
        return None, "No hay API key de Gemini configurada (GEMINI_API_KEY / GOOGLE_API_KEY)."
    try:
        import google.generativeai as genai

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-2.0-flash")
        branches = sel["branches"].copy()
        branch_lines = []
        for _, b in branches.iterrows():
            branch_lines.append(
                f"- {b['sucursal']}: fact={float(b['fact_total_usd']):.2f}, "
                f"mostrador={float(b['fact_mostrador_usd']):.2f}, "
                f"taller={float(b['fact_taller_usd']):.2f}, servicios={float(b['fact_servicios_usd']):.2f}, "
                f"maq={float(b['fact_maquinarias_usd']):.2f}, alq={float(b['fact_alquileres_usd']):.2f}, "
                f"gf={float(b['gastos_fijos_usd']):.2f}, otros_var={float(b['gastos_var_otros_usd']):.2f}"
            )
        comp_lines = []
        if compare_df is not None and len(compare_df):
            for _, r in compare_df.iterrows():
                comp_lines.append(
                    f"- {r['Métrica']}: mes={float(r['Mes seleccionado']):.2f}, "
                    f"prom3m={float(r['Promedio 3 meses previos']):.2f}, dif_pct={float(r['Dif. %']):.2f}"
                )
        prompt = (
            "Actuá como consultor financiero de postventa.\n"
            "Devolvé SOLO 6 bullets en español, breves y accionables:\n"
            "1) resumen ejecutivo del mes (orden: postventa, maquinarias, alquileres, global),\n"
            "2) comparación vs últimos 3 meses,\n"
            "3) sucursales que crecen/caen,\n"
            "4) mix por línea de negocio (postventa: mostrador/taller/servicios; maq.; alq.),\n"
            "5) riesgos (costos fijos por sucursal vs central, variables),\n"
            "6) 2 acciones concretas.\n\n"
            f"Mes: {MES_NOMBRES[int(sel['mes'])-1]} {int(sel['anio'])}\n"
            "Postventa (USD):\n"
            f"  - Repuestos neto: {float(sel.get('ventas_repuestos_neto_usd') or 0):.2f} | Servicios: {float(sel.get('ventas_servicios_usd') or 0):.2f} | "
            f"Subtotal postventa: {float(sel.get('ventas_postventa_usd') or 0):.2f}\n"
            f"  - CMV modelo (mos/tal/serv.): {float(sel.get('cmv_postventa_modelo_usd') or 0):.2f}\n"
            "Venta maquinarias (USD):\n"
            f"  - Ventas: {float(sel.get('ventas_maquinarias_usd') or 0):.2f} | Gasto variable maq.: {float(sel.get('gastos_var_maquinarias_usd') or 0):.2f}\n"
            "Alquileres (USD):\n"
            f"  - Ventas: {float(sel.get('ventas_alquileres_usd') or 0):.2f}\n"
            "Global (USD):\n"
            f"  - Facturación total: {float(sel['fact_total_usd']):.2f}\n"
            f"  - Gastos variables total: {float(sel['gastos_variables_usd']):.2f} "
            f"(otros cargados: {float(sel.get('gastos_var_otros_usd') or 0):.2f})\n"
            f"  - Gastos fijos total: {float(sel['gastos_fijos_usd']):.2f} "
            f"(sucursales: {float(sel.get('gastos_fijos_sucursales_usd') or 0):.2f}, concesionario: {float(sel.get('gastos_fijos_concesionario_usd') or 0):.2f})\n"
            f"  - Margen: {float(sel['margen_usd']):.2f} ({float(sel['margen_pct'] or 0):.2f}%) | Resultado: {float(sel['resultado_usd']):.2f}\n"
            f"  - Factor absorción: {float(sel['factor_abs_pct'] or 0):.2f}% | Punto equilibrio: {float(sel['punto_equilibrio_usd'] or 0):.2f}\n"
            "Detalle sucursales:\n" + "\n".join(branch_lines) + "\n"
            "Comparación 3 meses previos:\n" + ("\n".join(comp_lines) if comp_lines else "Sin datos suficientes")
        )
        resp = model.generate_content(prompt)
        text = (resp.text or "").strip()
        if text:
            return text, "OK"
        return None, "Gemini respondió vacío."
    except Exception as exc:
        return None, f"Error Gemini: {exc}"


def _build_comparison_last3(data: list[dict], sel: dict) -> pd.DataFrame:
    ordered = sorted(data, key=lambda x: (x["anio"], x["mes"]))
    idx = next(
        (i for i, d in enumerate(ordered) if int(d["anio"]) == int(sel["anio"]) and int(d["mes"]) == int(sel["mes"])),
        None,
    )
    if idx is None:
        return pd.DataFrame()
    prev = ordered[max(0, idx - 3) : idx]
    if not prev:
        return pd.DataFrame()
    prev_df = pd.DataFrame(prev)
    base_fact = float(prev_df["fact_total_usd"].mean())
    base_margen = float(prev_df["margen_usd"].mean())
    base_res = float(prev_df["resultado_usd"].mean())
    base_gvar = float(prev_df["gastos_variables_usd"].mean()) if "gastos_variables_usd" in prev_df.columns else 0.0
    base_gtot = float(prev_df["total_gastos_usd"].mean()) if "total_gastos_usd" in prev_df.columns else 0.0
    if "ventas_postventa_usd" in prev_df.columns:
        base_post = float(prev_df["ventas_postventa_usd"].mean())
    else:
        n = len(prev_df)
        ix = prev_df.index
        srs_m = (
            prev_df["ventas_repuestos_neto_usd"]
            if "ventas_repuestos_neto_usd" in prev_df.columns
            else pd.Series([0.0] * n, index=ix)
        )
        srs_s = (
            prev_df["ventas_servicios_usd"]
            if "ventas_servicios_usd" in prev_df.columns
            else pd.Series([0.0] * n, index=ix)
        )
        base_post = float((pd.to_numeric(srs_m, errors="coerce").fillna(0) + pd.to_numeric(srs_s, errors="coerce").fillna(0)).mean())
    base_maq = (
        float(prev_df["ventas_maquinarias_usd"].mean()) if "ventas_maquinarias_usd" in prev_df.columns else 0.0
    )
    base_alq = (
        float(prev_df["ventas_alquileres_usd"].mean()) if "ventas_alquileres_usd" in prev_df.columns else 0.0
    )
    sel_post = float(sel.get("ventas_postventa_usd") or 0) or (
        float(sel.get("ventas_repuestos_neto_usd") or 0) + float(sel.get("ventas_servicios_usd") or 0)
    )

    rows = [
        {
            "Métrica": "Facturación total",
            "Mes seleccionado": float(sel["fact_total_usd"]),
            "Promedio 3 meses previos": base_fact,
            "Dif. %": (_safe_ratio(float(sel["fact_total_usd"]) - base_fact, base_fact) or 0.0) * 100.0,
        },
        {
            "Métrica": "Postventa (rep. neto + servicios)",
            "Mes seleccionado": sel_post,
            "Promedio 3 meses previos": base_post,
            "Dif. %": ((_safe_ratio(sel_post - base_post, base_post) or 0.0) * 100.0),
        },
        {
            "Métrica": "Venta maquinarias",
            "Mes seleccionado": float(sel.get("ventas_maquinarias_usd") or 0),
            "Promedio 3 meses previos": base_maq,
            "Dif. %": (
                (_safe_ratio(float(sel.get("ventas_maquinarias_usd") or 0) - base_maq, base_maq) or 0.0) * 100.0
            ),
        },
        {
            "Métrica": "Alquileres",
            "Mes seleccionado": float(sel.get("ventas_alquileres_usd") or 0),
            "Promedio 3 meses previos": base_alq,
            "Dif. %": (
                (_safe_ratio(float(sel.get("ventas_alquileres_usd") or 0) - base_alq, base_alq) or 0.0) * 100.0
            ),
        },
        {
            "Métrica": "Margen contribución",
            "Mes seleccionado": float(sel["margen_usd"]),
            "Promedio 3 meses previos": base_margen,
            "Dif. %": (_safe_ratio(float(sel["margen_usd"]) - base_margen, base_margen) or 0.0) * 100.0,
        },
        {
            "Métrica": "Gastos variables",
            "Mes seleccionado": float(sel["gastos_variables_usd"]),
            "Promedio 3 meses previos": base_gvar,
            "Dif. %": (_safe_ratio(float(sel["gastos_variables_usd"]) - base_gvar, base_gvar) or 0.0) * 100.0,
        },
        {
            "Métrica": "Total gastos",
            "Mes seleccionado": float(sel["total_gastos_usd"]),
            "Promedio 3 meses previos": base_gtot,
            "Dif. %": (_safe_ratio(float(sel["total_gastos_usd"]) - base_gtot, base_gtot) or 0.0) * 100.0,
        },
        {
            "Métrica": "Resultado",
            "Mes seleccionado": float(sel["resultado_usd"]),
            "Promedio 3 meses previos": base_res,
            "Dif. %": (_safe_ratio(float(sel["resultado_usd"]) - base_res, base_res) or 0.0) * 100.0,
        },
    ]
    return pd.DataFrame(rows)


def _build_inicio_report_pdf_bytes(
    trend_fact_fig,
    detail_pie_fig,
    detail_bar_fig,
    detail_stack_fig,
    *,
    trend_df: pd.DataFrame,
    sel: dict,
    compare_df: pd.DataFrame,
    ai_analysis: str | None = None,
) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        p_hist = os.path.join(td, "hist.png")
        p_pie = os.path.join(td, "pie.png")
        p_bar = os.path.join(td, "bar.png")
        p_stack = os.path.join(td, "stack.png")
        images_ok = True
        image_engine = "plotly"
        try:
            trend_fact_fig.write_image(p_hist, width=1400, height=700, scale=2)
            detail_pie_fig.write_image(p_pie, width=1200, height=700, scale=2)
            detail_bar_fig.write_image(p_bar, width=1200, height=700, scale=2)
            detail_stack_fig.write_image(p_stack, width=1400, height=700, scale=2)
        except Exception:
            # Intento de autocorrección para entornos sin Chrome instalado.
            try:
                subprocess.run(["plotly_get_chrome", "-y"], check=True, capture_output=True, text=True)
                trend_fact_fig.write_image(p_hist, width=1400, height=700, scale=2)
                detail_pie_fig.write_image(p_pie, width=1200, height=700, scale=2)
                detail_bar_fig.write_image(p_bar, width=1200, height=700, scale=2)
                detail_stack_fig.write_image(p_stack, width=1400, height=700, scale=2)
            except Exception:
                # Fallback 2: generar gráficos estáticos con Matplotlib.
                try:
                    image_engine = "matplotlib"
                    # Histórico facturación
                    hist_df = trend_df.copy().reset_index(drop=True)
                    plt.figure(figsize=(11, 4))
                    bars = plt.bar(hist_df["periodo"], hist_df["fact_total_usd"], color=_COL_YELLOW)
                    plt.xticks(rotation=45, ha="right")
                    plt.title("Facturación total - todos los meses")
                    for b, v in zip(bars, hist_df["fact_total_usd"]):
                        plt.text(
                            b.get_x() + b.get_width() / 2.0,
                            b.get_height(),
                            f"US$ {float(v):,.0f}",
                            ha="center",
                            va="bottom",
                            fontsize=8,
                        )
                    plt.tight_layout()
                    plt.savefig(p_hist, dpi=180)
                    plt.close()

                    branches = sel["branches"].copy().reset_index(drop=True)
                    # Pie por sucursal
                    plt.figure(figsize=(7, 5))
                    pie_colors = [_COL_GRAY, _COL_YELLOW, _COL_BLACK][: len(branches)]
                    plt.pie(
                        branches["fact_total_usd"],
                        labels=branches["sucursal"],
                        autopct="%1.1f%%",
                        colors=pie_colors,
                    )
                    plt.title("Participación de facturación por sucursal")
                    plt.tight_layout()
                    plt.savefig(p_pie, dpi=180)
                    plt.close()

                    # Barras por sucursal
                    plt.figure(figsize=(8, 5))
                    plt.bar(branches["sucursal"], branches["fact_total_usd"], color=pie_colors)
                    plt.title("Comparación de facturación por sucursal")
                    plt.tight_layout()
                    plt.savefig(p_bar, dpi=180)
                    plt.close()

                    # Apilado canal
                    x = range(len(branches))
                    plt.figure(figsize=(10, 5))
                    mos = branches["fact_mostrador_usd"]
                    tal = branches["fact_taller_usd"]
                    ser = branches["fact_servicios_usd"]
                    maq = branches["fact_maquinarias_usd"]
                    alq = branches["fact_alquileres_usd"]
                    b1 = mos
                    b2 = b1 + tal
                    b3 = b2 + ser
                    b4 = b3 + maq
                    plt.bar(x, mos, color=_COL_GRAY, label="Mostrador")
                    plt.bar(x, tal, bottom=b1, color=_COL_ORANGE_MUTED, label="Taller")
                    plt.bar(x, ser, bottom=b2, color=_COL_YELLOW, label="Servicios")
                    plt.bar(x, maq, bottom=b3, color="#8B7355", label="Maquinarias")
                    plt.bar(x, alq, bottom=b4, color="#5C7A99", label="Alquileres")
                    plt.xticks(list(x), branches["sucursal"])
                    plt.title("Facturación desagregada por canal")
                    plt.legend()
                    plt.tight_layout()
                    plt.savefig(p_stack, dpi=180)
                    plt.close()
                except Exception:
                    images_ok = False

        pdf = FPDF(orientation="P", unit="mm", format="A4")
        pdf.set_auto_page_break(auto=True, margin=12)
        pdf.add_page()
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 8, "GOPV - Informe mensual", ln=True)
        pdf.set_font("Arial", "", 10)
        pdf.cell(0, 6, f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M')}", ln=True)
        pdf.cell(0, 6, f"Mes analizado: {MES_NOMBRES[int(sel['mes'])-1]} {int(sel['anio'])}", ln=True)
        pdf.ln(2)
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 7, "Resumen agregado (todos los meses de la serie)", ln=True)
        pdf.set_font("Arial", "", 10)
        v_post = trend_df["ventas_postventa_usd"] if "ventas_postventa_usd" in trend_df.columns else None
        if v_post is None and "ventas_repuestos_neto_usd" in trend_df.columns:
            v_post = trend_df["ventas_repuestos_neto_usd"].fillna(0) + trend_df["ventas_servicios_usd"].fillna(0)
        v_maq = trend_df["ventas_maquinarias_usd"] if "ventas_maquinarias_usd" in trend_df.columns else None
        v_alq = trend_df["ventas_alquileres_usd"] if "ventas_alquileres_usd" in trend_df.columns else None
        pdf.set_font("Arial", "B", 11)
        pdf.cell(0, 6, "Postventa", ln=True)
        pdf.set_font("Arial", "", 10)
        if v_post is not None:
            pdf.cell(0, 6, f"  Acumulado (rep. neto + servicios): {_fmt_usd(float(v_post.sum()))}", ln=True)
            pdf.cell(0, 6, f"  Promedio mensual: {_fmt_usd(float(v_post.mean()))}", ln=True)
        pdf.ln(1)
        pdf.set_font("Arial", "B", 11)
        pdf.cell(0, 6, "Venta maquinarias", ln=True)
        pdf.set_font("Arial", "", 10)
        if v_maq is not None:
            pdf.cell(0, 6, f"  Acumulado: {_fmt_usd(float(v_maq.sum()))} | Promedio mensual: {_fmt_usd(float(v_maq.mean()))}", ln=True)
        pdf.ln(1)
        pdf.set_font("Arial", "B", 11)
        pdf.cell(0, 6, "Alquileres", ln=True)
        pdf.set_font("Arial", "", 10)
        if v_alq is not None:
            pdf.cell(0, 6, f"  Acumulado: {_fmt_usd(float(v_alq.sum()))} | Promedio mensual: {_fmt_usd(float(v_alq.mean()))}", ln=True)
        pdf.ln(1)
        pdf.set_font("Arial", "B", 11)
        pdf.cell(0, 6, "Global", ln=True)
        pdf.set_font("Arial", "", 10)
        pdf.cell(0, 6, f"  Facturacion total acumulada: {_fmt_usd(float(trend_df['fact_total_usd'].sum()))}", ln=True)
        pdf.cell(0, 6, f"  Promedio mensual facturacion total: {_fmt_usd(float(trend_df['fact_total_usd'].mean()))}", ln=True)
        pdf.ln(2)
        if images_ok:
            pdf.image(p_hist, x=10, w=190)
        else:
            pdf.multi_cell(
                0,
                6,
                "Nota: no se pudieron exportar los graficos en este entorno (dependencias de Chrome/Kaleido). "
                "El informe se genera en modo texto con todos los KPIs y comparativos.",
            )

        pdf.add_page()
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 7, "Detalle del mes seleccionado", ln=True)
        pdf.set_font("Arial", "", 10)
        lines = [
            "Postventa",
            f"  Repuestos (neto): {_fmt_usd(sel.get('ventas_repuestos_neto_usd'))} | Servicios: {_fmt_usd(sel.get('ventas_servicios_usd'))}",
            f"  Facturacion postventa: {_fmt_usd(sel.get('ventas_postventa_usd'))}",
            f"  CMV modelo: mostrador {_fmt_usd(sel.get('cmv_rep_mostrador_usd'))} | taller {_fmt_usd(sel.get('cmv_rep_taller_usd'))} | serv./afines {_fmt_usd(sel.get('cmv_servicios_afines_usd'))}",
            f"  CMV postventa (suma modelo): {_fmt_usd(sel.get('cmv_postventa_modelo_usd'))}",
            "",
            "Venta maquinarias",
            f"  Ventas: {_fmt_usd(sel.get('ventas_maquinarias_usd'))}",
            f"  Gasto variable maquinaria: {_fmt_usd(sel.get('gastos_var_maquinarias_usd'))}",
            "",
            "Alquileres",
            f"  Ventas: {_fmt_usd(sel.get('ventas_alquileres_usd'))}",
            "",
            "Global",
            f"  Facturacion total: {_fmt_usd(sel['fact_total_usd'])}",
            f"  Gastos variables (total): {_fmt_usd(sel['gastos_variables_usd'])}",
            f"    Otros variables (cargados): {_fmt_usd(sel.get('gastos_var_otros_usd'))}",
            f"  Gastos fijos (total): {_fmt_usd(sel['gastos_fijos_usd'])}",
            f"    Sucursales: {_fmt_usd(sel.get('gastos_fijos_sucursales_usd'))} | Concesionario: {_fmt_usd(sel.get('gastos_fijos_concesionario_usd'))}",
            f"  Margen contribucion: {_fmt_usd(sel['margen_usd'])} ({_fmt_pct(sel['margen_pct'])})",
            f"  Resultado: {_fmt_usd(sel['resultado_usd'])}",
            f"  Factor absorcion: {_fmt_pct(sel['factor_abs_pct'])}",
            f"  Punto de equilibrio: {_fmt_usd(sel['punto_equilibrio_usd'])}",
        ]
        for line in lines:
            if line == "":
                pdf.ln(2)
            else:
                pdf.cell(0, 6, line, ln=True)
        pdf.ln(2)
        if images_ok:
            pdf.set_font("Arial", "B", 11)
            pdf.cell(0, 6, "Graficos - global (participacion y mix por sucursal)", ln=True)
            pdf.set_font("Arial", "", 9)
            pdf.cell(0, 5, "Postventa, maquinarias y alquileres se ven apilados por canal en el grafico inferior.", ln=True)
            pdf.ln(1)
            pdf.image(p_pie, x=10, w=190)
            pdf.ln(2)
            pdf.image(p_bar, x=10, w=190)
            pdf.ln(2)
            pdf.image(p_stack, x=10, w=190)
            if image_engine == "matplotlib":
                pdf.ln(2)
                pdf.set_font("Arial", "I", 9)
                pdf.cell(0, 5, "Nota: gráficos renderizados con fallback Matplotlib.", ln=True)
        else:
            branches = sel["branches"].copy()
            pdf.cell(0, 6, "Detalle por sucursal:", ln=True)
            for _, b in branches.iterrows():
                pdf.cell(
                    0,
                    6,
                    f"{b['sucursal']}: Fact {_fmt_usd(b['fact_total_usd'])} | Mostrador {_fmt_usd(b['fact_mostrador_usd'])} | "
                    f"Taller {_fmt_usd(b['fact_taller_usd'])} | Servicios {_fmt_usd(b['fact_servicios_usd'])} | "
                    f"Maq. {_fmt_usd(b['fact_maquinarias_usd'])} | Alq. {_fmt_usd(b['fact_alquileres_usd'])} | "
                    f"Fijos {_fmt_usd(b['gastos_fijos_usd'])} | Otros var. {_fmt_usd(b['gastos_var_otros_usd'])}",
                    ln=True,
                )

        pdf.add_page()
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 7, "Comparacion contra ultimos 3 meses", ln=True)
        pdf.set_font("Arial", "", 10)
        if compare_df is None or len(compare_df) == 0:
            pdf.cell(0, 6, "No hay suficientes meses previos para comparar.", ln=True)
        else:
            for _, r in compare_df.iterrows():
                pdf.cell(
                    0,
                    6,
                    f"{r['Métrica']}: mes {_fmt_usd(r['Mes seleccionado'])} vs prom 3m {_fmt_usd(r['Promedio 3 meses previos'])} (dif {_fmt_pct(r['Dif. %'])})",
                    ln=True,
                )

            # Insight simple de mix por sucursal/canal
            branches = sel["branches"].copy()
            top = branches.sort_values("fact_total_usd", ascending=False).iloc[0]
            most_mos = branches.sort_values("fact_mostrador_usd", ascending=False).iloc[0]
            most_ts = branches.assign(
                ts=branches["fact_taller_usd"]
                + branches["fact_servicios_usd"]
                + branches["fact_maquinarias_usd"]
                + branches["fact_alquileres_usd"]
            ).sort_values("ts", ascending=False).iloc[0]
            pdf.ln(3)
            pdf.cell(0, 6, f"Sucursal con mayor facturacion: {top['sucursal']} ({_fmt_usd(top['fact_total_usd'])})", ln=True)
            pdf.cell(0, 6, f"Mayor mostrador: {most_mos['sucursal']} ({_fmt_usd(most_mos['fact_mostrador_usd'])})", ln=True)
            pdf.cell(
                0,
                6,
                f"Mayor fuera de mostrador (taller, servicios, maq., alq.): {most_ts['sucursal']} ({_fmt_usd(float(most_ts['ts']))})",
                ln=True,
            )
        if ai_analysis:
            pdf.ln(3)
            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 7, "Analisis IA (Gemini)", ln=True)
            pdf.set_font("Arial", "", 10)
            for line in [ln.strip() for ln in ai_analysis.splitlines() if ln.strip()]:
                clean = line.encode("latin-1", errors="replace").decode("latin-1")
                pdf.multi_cell(0, 6, clean)

        out = pdf.output(dest="S")
        if isinstance(out, str):
            return out.encode("latin-1", errors="replace")
        return bytes(out)


def _build_cierre_dashboard_metrics(cierre: dict, lineas: pd.DataFrame) -> dict | None:
    if cierre is None or lineas is None or len(lineas) == 0:
        return None

    df = lineas.copy()
    money_cols = [
        "fact_rep_mostrador",
        "fact_rep_taller",
        "desc_mostrador",
        "desc_taller",
        "fact_servicios",
        "fact_maquinarias",
        "fact_alquileres",
    ]
    for c in money_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        else:
            df[c] = 0.0

    tc = max(float(cierre.get("tipo_cambio_ars_usd") or 1.0), 1e-9)
    gf_line = float(pd.to_numeric(df.get("gastos_fijos"), errors="coerce").fillna(0).sum())
    go_line = float(pd.to_numeric(df.get("gastos_var_otros_usd"), errors="coerce").fillna(0).sum())
    hdr_gf = float(cierre.get("gastos_fijos_global") or 0.0)
    hdr_go = float(cierre.get("gastos_var_otros") or 0.0)
    gfc = max(float(cierre.get("gastos_fijos_concesionario_usd") or 0.0), 0.0)
    if gf_line < 1e-9 and hdr_gf > 0 and gfc < 1e-9:
        gf_line = hdr_gf
    if go_line < 1e-9 and hdr_go > 0:
        go_line = hdr_go
    gastos_fijos_usd = gf_line + gfc
    gastos_otros_usd = go_line
    gastos_var_maq_usd = float(cierre.get("gastos_var_maquinarias_usd") or 0.0)
    rubro_otros = cierre.get("gastos_var_otros_rubro")

    fm = float(df["fact_rep_mostrador"].sum())
    ft = float(df["fact_rep_taller"].sum())
    dm = float(df["desc_mostrador"].sum())
    dt = float(df["desc_taller"].sum())
    fs = float(df["fact_servicios"].sum())
    fmaq_usd = float(df["fact_maquinarias"].sum())
    falq_usd = float(df["fact_alquileres"].sum())

    um = _util_promedio_simple(df, "util_pct_mostrador") or 0.0
    ut = _util_promedio_simple(df, "util_pct_taller") or 0.0
    util_prom_total = _avg_non_zero_pair(um, ut)

    gv = database.compute_gastos_variables_globales(
        fact_rep_mos_conc=fm,
        desc_rep_mos_conc=dm,
        fact_rep_tal_conc=ft,
        desc_rep_tal_conc=dt,
        fact_serv_conc=fs,
        util_mos_conc=um,
        util_tal_conc=ut,
        util_serv_conc=1.0,
        gastos_fijos_global=0.0,
        gastos_var_otros=gastos_otros_usd * tc,
        gastos_var_otros_rubro=rubro_otros,
        fact_maquinarias_conc=fmaq_usd * tc,
        fact_alquileres_conc=falq_usd * tc,
    )

    net_rep_r = (
        (df["fact_rep_mostrador"] - df["desc_mostrador"]).clip(lower=0.0)
        + (df["fact_rep_taller"] - df["desc_taller"]).clip(lower=0.0)
        + df["fact_servicios"]
    )
    fact_total_usd_line = net_rep_r / tc + df["fact_maquinarias"] + df["fact_alquileres"]
    fact_total_usd = float(fact_total_usd_line.sum())
    denom_part = fact_total_usd
    df["participacion_facturacion"] = fact_total_usd_line / denom_part if denom_part > 0 else 0.0

    neto_rep_ars = max(fm + ft - dm - dt, 0.0)
    ventas_repuestos_neto_usd = neto_rep_ars / tc
    ventas_servicios_usd = fs / tc
    ventas_maquinarias_usd = fmaq_usd
    ventas_alquileres_usd = falq_usd
    ventas_maq_alq_usd = ventas_maquinarias_usd + ventas_alquileres_usd
    ventas_postventa_usd = ventas_repuestos_neto_usd + ventas_servicios_usd
    cmv_rep_mostrador_usd = float(gv["gv_rep_mostrador"]) / tc
    cmv_rep_taller_usd = float(gv["gv_rep_taller"]) / tc
    cmv_servicios_afines_usd = float(gv["gv_servicios"]) / tc
    cmv_postventa_modelo_usd = cmv_rep_mostrador_usd + cmv_rep_taller_usd + cmv_servicios_afines_usd
    gastos_var_usd = (float(gv["gv_servicios_ajustado"]) + float(gv["gv_repuestos_ajustado"])) / tc
    gastos_var_usd += max(gastos_var_maq_usd, 0.0)
    total_gastos_usd = gastos_fijos_usd + gastos_var_usd
    margen_usd = fact_total_usd - gastos_var_usd
    margen_ratio = _safe_ratio(margen_usd, fact_total_usd)
    resultado_usd = margen_usd - gastos_fijos_usd
    factor_abs_ratio = _safe_ratio(margen_usd, gastos_fijos_usd)
    punto_equilibrio_usd = (gastos_fijos_usd / margen_ratio) if margen_ratio is not None and margen_ratio > 0 else None

    margen_postventa_usd = ventas_postventa_usd - cmv_postventa_modelo_usd
    margen_postventa_pct = (
        (margen_postventa_usd / ventas_postventa_usd * 100.0) if ventas_postventa_usd > 1e-9 else None
    )
    share_postventa_fact = (ventas_postventa_usd / fact_total_usd) if fact_total_usd > 1e-9 else 0.0
    fijos_atrib_postventa_usd = gastos_fijos_usd * share_postventa_fact
    factor_abs_postventa_pct = (
        (margen_postventa_usd / fijos_atrib_postventa_usd * 100.0) if fijos_atrib_postventa_usd > 1e-9 else None
    )

    cmv_rep_sucursal_usd: list[float] = []
    for _, r in df.iterrows():
        calc_ln = database.compute_cierre_venta_linea(
            float(r.get("fact_rep_mostrador") or 0),
            float(r.get("fact_rep_taller") or 0),
            float(r.get("desc_mostrador") or 0),
            float(r.get("desc_taller") or 0),
            float(r.get("util_pct_mostrador") or 0),
            float(r.get("util_pct_taller") or 0),
            float(r.get("fact_servicios") or 0),
            fact_maquinarias=float(r.get("fact_maquinarias") or 0),
            fact_alquileres=float(r.get("fact_alquileres") or 0),
            tipo_cambio_ars_usd=tc,
        )
        cmv_rep_sucursal_usd.append(float(calc_ln["cmv_repuestos_ars"]) / tc)

    gf_col = pd.to_numeric(df.get("gastos_fijos"), errors="coerce").fillna(0.0)
    go_col = pd.to_numeric(df.get("gastos_var_otros_usd"), errors="coerce").fillna(0.0)
    branches = pd.DataFrame(
        {
            "sucursal": df["sucursal"],
            "fact_total_usd": fact_total_usd_line,
            "fact_mostrador_usd": (df["fact_rep_mostrador"] - df["desc_mostrador"]).clip(lower=0.0) / tc,
            "fact_taller_usd": (df["fact_rep_taller"] - df["desc_taller"]).clip(lower=0.0) / tc,
            "fact_servicios_usd": df["fact_servicios"] / tc,
            "fact_maquinarias_usd": df["fact_maquinarias"],
            "fact_alquileres_usd": df["fact_alquileres"],
            "cmv_repuestos_usd": cmv_rep_sucursal_usd,
            "gastos_fijos_usd": gf_col,
            "gastos_var_otros_usd": go_col,
            "participacion_pct": df["participacion_facturacion"] * 100.0,
        }
    ).sort_values("fact_total_usd", ascending=False)

    return {
        "cierre_id": int(cierre["id"]),
        "anio": int(cierre["anio"]),
        "mes": int(cierre["mes"]),
        "periodo": f"{_mes_corto(int(cierre['mes']))} {int(cierre['anio'])}",
        "fact_total_usd": fact_total_usd,
        "tipo_cambio_ars_usd": tc,
        "gastos_fijos_usd": gastos_fijos_usd,
        "gastos_fijos_sucursales_usd": float(gf_line),
        "gastos_fijos_concesionario_usd": gfc,
        "gastos_variables_usd": gastos_var_usd,
        "gastos_var_otros_usd": gastos_otros_usd,
        "gastos_var_maquinarias_usd": gastos_var_maq_usd,
        "cmv_rep_mostrador_usd": cmv_rep_mostrador_usd,
        "cmv_rep_taller_usd": cmv_rep_taller_usd,
        "cmv_servicios_afines_usd": cmv_servicios_afines_usd,
        "ventas_repuestos_neto_usd": ventas_repuestos_neto_usd,
        "ventas_servicios_usd": ventas_servicios_usd,
        "ventas_maquinarias_usd": ventas_maquinarias_usd,
        "ventas_alquileres_usd": ventas_alquileres_usd,
        "ventas_postventa_usd": ventas_postventa_usd,
        "cmv_postventa_modelo_usd": cmv_postventa_modelo_usd,
        "margen_postventa_usd": margen_postventa_usd,
        "margen_postventa_pct": margen_postventa_pct,
        "factor_abs_postventa_pct": factor_abs_postventa_pct,
        "fijos_atrib_postventa_usd": fijos_atrib_postventa_usd,
        "ventas_maq_alq_usd": ventas_maq_alq_usd,
        "total_gastos_usd": total_gastos_usd,
        "margen_usd": margen_usd,
        "margen_pct": (margen_ratio * 100.0) if margen_ratio is not None else None,
        "resultado_usd": resultado_usd,
        "factor_abs_pct": (factor_abs_ratio * 100.0) if factor_abs_ratio is not None else None,
        "punto_equilibrio_usd": punto_equilibrio_usd,
        "util_prom_total_pct": util_prom_total * 100.0,
        "branches": branches,
    }


def _load_inicio_dashboard_data(limit: int = 36) -> list[dict]:
    cierres = database.list_cierres_ventas_dashboard(limit)
    if cierres is None or len(cierres) == 0:
        return []

    items: list[dict] = []
    for _, r in cierres.iterrows():
        cierre = r.to_dict()
        lineas = database.get_lineas_cierre_ventas(int(cierre["id"]))
        item = _build_cierre_dashboard_metrics(cierre, lineas)
        if item is not None:
            items.append(item)

    items.sort(key=lambda x: (x["anio"], x["mes"]), reverse=True)
    return items


def _render_inicio_dashboard() -> None:
    st.subheader("Inicio")
    st.markdown("### Dashboard mensual")

    data = _load_inicio_dashboard_data(limit=60)
    if not data:
        st.info("Todavía no hay cierres guardados. Cargá y guardá meses en Registro para ver el dashboard.")
        return

    trend_df = pd.DataFrame(
        [
            {
                "periodo": d["periodo"],
                "anio": d["anio"],
                "mes": d["mes"],
                "fact_total_usd": d["fact_total_usd"],
                "ventas_postventa_usd": (
                    float(d["ventas_postventa_usd"])
                    if "ventas_postventa_usd" in d
                    else float(d.get("ventas_repuestos_neto_usd") or 0) + float(d.get("ventas_servicios_usd") or 0)
                ),
                "ventas_maquinarias_usd": float(d.get("ventas_maquinarias_usd") or 0),
                "ventas_alquileres_usd": float(d.get("ventas_alquileres_usd") or 0),
                "ventas_repuestos_neto_usd": float(d.get("ventas_repuestos_neto_usd") or 0),
                "ventas_servicios_usd": float(d.get("ventas_servicios_usd") or 0),
                "ventas_maq_alq_usd": float(d.get("ventas_maq_alq_usd") or 0),
                "cmv_postventa_modelo_usd": (
                    float(d["cmv_postventa_modelo_usd"])
                    if "cmv_postventa_modelo_usd" in d
                    else float(d.get("cmv_rep_mostrador_usd") or 0)
                    + float(d.get("cmv_rep_taller_usd") or 0)
                    + float(d.get("cmv_servicios_afines_usd") or 0)
                ),
                "gastos_fijos_usd": d["gastos_fijos_usd"],
                "gastos_fijos_sucursales_usd": float(d.get("gastos_fijos_sucursales_usd") or 0),
                "gastos_fijos_concesionario_usd": float(d.get("gastos_fijos_concesionario_usd") or 0),
                "gastos_variables_usd": d["gastos_variables_usd"],
                "gastos_var_maquinarias_usd": float(d.get("gastos_var_maquinarias_usd") or 0),
                "total_gastos_usd": d["total_gastos_usd"],
                "margen_usd": d["margen_usd"],
                "margen_pct": d["margen_pct"],
                "resultado_usd": d["resultado_usd"],
                "factor_abs_pct": d["factor_abs_pct"],
                "punto_equilibrio_usd": d["punto_equilibrio_usd"],
                "margen_postventa_usd": float(d.get("margen_postventa_usd") or 0),
                "margen_postventa_pct": float(d["margen_postventa_pct"])
                if d.get("margen_postventa_pct") is not None
                else float("nan"),
                "factor_abs_postventa_pct": float(d["factor_abs_postventa_pct"])
                if d.get("factor_abs_postventa_pct") is not None
                else float("nan"),
            }
            for d in data
        ]
    )
    trend_df = trend_df.sort_values(["anio", "mes"], ascending=[False, False])
    trend_12 = trend_df.head(12).sort_values(["anio", "mes"], ascending=[True, True]).reset_index(drop=True)
    trend_all = trend_df.sort_values(["anio", "mes"], ascending=[True, True]).reset_index(drop=True)

    st.markdown("#### Selección de período")
    years = sorted({int(d["anio"]) for d in data}, reverse=True)
    c1, c2 = st.columns([1, 1])
    with c1:
        anio_sel = st.selectbox("Año", options=years, key="inicio_anio")
    meses_disponibles = sorted({int(d["mes"]) for d in data if int(d["anio"]) == int(anio_sel)}, reverse=True)
    with c2:
        mes_sel = st.selectbox(
            "Mes",
            options=meses_disponibles,
            format_func=lambda m: MES_NOMBRES[m - 1],
            key="inicio_mes",
        )

    sel = next((d for d in data if int(d["anio"]) == int(anio_sel) and int(d["mes"]) == int(mes_sel)), None)
    if sel is None:
        st.warning("No hay datos para el período seleccionado.")
        return

    df_suc = sel["branches"].copy()

    st.markdown("### Postventa")
    st.caption(
        "Facturación: repuestos neto + servicios. CMV: modelo global repartido en mostrador, taller y servicios/afines."
    )
    pv1, pv2, pv3, pv4 = st.columns(4)
    pv1.metric("Venta repuestos (neto)", f"US$ {float(sel.get('ventas_repuestos_neto_usd') or 0):,.2f}")
    pv2.metric("Venta servicios", f"US$ {float(sel.get('ventas_servicios_usd') or 0):,.2f}")
    pv3.metric("Facturación total postventa", f"US$ {float(sel.get('ventas_postventa_usd') or 0):,.2f}")
    pv4.metric("CMV (modelo)", f"US$ {float(sel.get('cmv_postventa_modelo_usd') or 0):,.2f}")
    pv5, pv6, pv7, pv8 = st.columns(4)
    pv5.metric(
        "Margen contribución postventa",
        f"US$ {float(sel.get('margen_postventa_usd') or 0):,.2f}",
    )
    pv6.metric(
        "Margen % postventa",
        (
            f"{float(sel['margen_postventa_pct']):,.2f} %"
            if sel.get("margen_postventa_pct") is not None
            else "—"
        ),
    )
    pv7.metric(
        "Utilidad prom. repuestos (mostr.+taller)",
        f"{float(sel.get('util_prom_total_pct') or 0.0):,.2f} %",
        help="Promedio simple de % utilidad cargados en Registro (canales con dato > 0).",
    )
    pv8.metric(
        "Factor absorción postventa",
        f"{float(sel.get('factor_abs_postventa_pct') or 0.0):,.2f} %"
        if sel.get("factor_abs_postventa_pct") is not None
        else "—",
        help="Margen postventa ÷ fijos prorrateados (fact. postventa / fact. total × fijos totales).",
    )
    with st.expander("Qué necesitás para un resultado serio de postventa"):
        st.markdown(
            """
            - **Registro fiel:** ventas y descuentos en ARS por sucursal, **% utilidad** de mostrador y taller realistas (definen el CMV de repuestos).
            - **Tipo de cambio** del mes acorde al cierre (afecta el paso a USD de todo lo que viene en pesos).
            - **«Otros» variables** y su **rubro** en cabecera: pueden imputarse al CMV repuestos o servicios en el modelo global.
            - **Maquinarias / alquileres** en USD entran en el CMV de servicios/afines a nivel concesionario (no en el desglose CMV mostr.+taller arriba).
            - **Factor de absorción postventa** usa **fijos prorrateados** por participación de la facturación postventa en el total; es una aproximación si los fijos no son causales al canal.
            """
        )
    pvc1, pvc2 = st.columns(2)
    with pvc1:
        fig_pv_ventas = px.bar(
            trend_all[["periodo", "ventas_postventa_usd"]].copy(),
            x="periodo",
            y="ventas_postventa_usd",
            text_auto=".2f",
            title="Facturación postventa (rep. neto + servicios) — todos los meses",
        )
        fig_pv_ventas.update_traces(marker_color=_COL_YELLOW)
        fig_pv_ventas.update_layout(xaxis_title="", yaxis_title="")
        st.plotly_chart(fig_pv_ventas, use_container_width=True)
    with pvc2:
        fig_pv_cmv = px.bar(
            trend_all[["periodo", "cmv_postventa_modelo_usd"]].copy(),
            x="periodo",
            y="cmv_postventa_modelo_usd",
            text_auto=".2f",
            title="CMV modelo postventa — todos los meses",
        )
        fig_pv_cmv.update_traces(marker_color=_COL_ORANGE_MUTED)
        fig_pv_cmv.update_layout(xaxis_title="", yaxis_title="")
        st.plotly_chart(fig_pv_cmv, use_container_width=True)
    stack_pv = df_suc.melt(
        id_vars=["sucursal"],
        value_vars=["fact_mostrador_usd", "fact_taller_usd", "fact_servicios_usd"],
        var_name="canal",
        value_name="importe_usd",
    )
    stack_pv["canal"] = stack_pv["canal"].map(
        {
            "fact_mostrador_usd": "Mostrador",
            "fact_taller_usd": "Taller",
            "fact_servicios_usd": "Servicios",
        }
    )
    fig_stack_pv = px.bar(
        stack_pv,
        x="sucursal",
        y="importe_usd",
        color="canal",
        barmode="stack",
        title="Postventa por sucursal (mostrador, taller, servicios) — mes en foco",
        color_discrete_map={
            "Mostrador": _COL_GRAY,
            "Taller": _COL_ORANGE_MUTED,
            "Servicios": _COL_YELLOW,
        },
    )
    fig_stack_pv.update_layout(xaxis_title="", yaxis_title="")
    st.plotly_chart(fig_stack_pv, use_container_width=True)

    st.markdown("### Venta de maquinarias")
    st.caption("Ventas en USD (concesionario) y gasto variable de maquinaria del mes.")
    res_maq = float(sel.get("ventas_maquinarias_usd") or 0) - float(sel.get("gastos_var_maquinarias_usd") or 0)
    mq1, mq2, mq3 = st.columns(3)
    mq1.metric("Venta maquinarias", f"US$ {float(sel.get('ventas_maquinarias_usd') or 0):,.2f}")
    mq2.metric("Gastos maquinaria", f"US$ {float(sel.get('gastos_var_maquinarias_usd') or 0):,.2f}")
    mq3.metric("Resultado", f"US$ {res_maq:,.2f}")
    fig_maq_trend = px.bar(
        trend_all[["periodo", "ventas_maquinarias_usd"]].copy(),
        x="periodo",
        y="ventas_maquinarias_usd",
        text_auto=".2f",
        title="Ventas maquinarias — todos los meses",
    )
    fig_maq_trend.update_traces(marker_color="#8B7355")
    fig_maq_trend.update_layout(xaxis_title="", yaxis_title="")
    st.plotly_chart(fig_maq_trend, use_container_width=True)

    st.markdown("### Alquileres")
    st.caption("Ventas de alquileres en USD (concesionario).")
    st.metric("Venta alquileres", f"US$ {float(sel.get('ventas_alquileres_usd') or 0):,.2f}")
    fig_alq_trend = px.bar(
        trend_all[["periodo", "ventas_alquileres_usd"]].copy(),
        x="periodo",
        y="ventas_alquileres_usd",
        text_auto=".2f",
        title="Ventas alquileres — todos los meses",
    )
    fig_alq_trend.update_traces(marker_color="#5C7A99")
    fig_alq_trend.update_layout(xaxis_title="", yaxis_title="")
    st.plotly_chart(fig_alq_trend, use_container_width=True)

    st.markdown("### Otras tendencias (global, todos los meses)")
    st.caption("Evolución de facturación, gastos y resultado del concesionario.")
    fig_trend_fact = px.bar(
        trend_all[["periodo", "fact_total_usd"]],
        x="periodo",
        y="fact_total_usd",
        text_auto=".2f",
        title="Facturación total",
    )
    fig_trend_fact.update_traces(marker_color=_COL_YELLOW)
    fig_trend_fact.update_layout(xaxis_title="", yaxis_title="")
    ot1, ot2, ot3 = st.columns(3)
    with ot1:
        st.plotly_chart(fig_trend_fact, use_container_width=True)
    with ot2:
        fig_ot_fij = px.bar(
            trend_all[["periodo", "gastos_fijos_usd"]].copy(),
            x="periodo",
            y="gastos_fijos_usd",
            text_auto=".2f",
            title="Total gastos fijos",
        )
        fig_ot_fij.update_traces(marker_color=_COL_GRAY)
        fig_ot_fij.update_layout(xaxis_title="", yaxis_title="")
        st.plotly_chart(fig_ot_fij, use_container_width=True)
    with ot3:
        fig_ot_var = px.bar(
            trend_all[["periodo", "gastos_variables_usd"]].copy(),
            x="periodo",
            y="gastos_variables_usd",
            text_auto=".2f",
            title="Total gastos variables",
        )
        fig_ot_var.update_traces(marker_color=_COL_ORANGE_MUTED)
        fig_ot_var.update_layout(xaxis_title="", yaxis_title="")
        st.plotly_chart(fig_ot_var, use_container_width=True)
    ot4, ot5 = st.columns(2)
    with ot4:
        fig_ot_gt = px.bar(
            trend_all[["periodo", "total_gastos_usd"]].copy(),
            x="periodo",
            y="total_gastos_usd",
            text_auto=".2f",
            title="Total gastos",
        )
        fig_ot_gt.update_traces(marker_color="#6B6B6B")
        fig_ot_gt.update_layout(xaxis_title="", yaxis_title="")
        st.plotly_chart(fig_ot_gt, use_container_width=True)
    with ot5:
        fig_ot_res = px.bar(
            trend_all[["periodo", "resultado_usd"]].copy(),
            x="periodo",
            y="resultado_usd",
            text_auto=".2f",
            title="Resultado",
        )
        fig_ot_res.update_traces(marker_color=_COL_BLACK)
        fig_ot_res.update_layout(xaxis_title="", yaxis_title="")
        st.plotly_chart(fig_ot_res, use_container_width=True)

    st.markdown("### Global (concesionario)")
    st.caption("Tendencia 12 meses por métrica, mix por sucursal y tablas del mes.")

    metric_map = {
        "Postventa — Facturación (rep. neto + servicios)": ("ventas_postventa_usd", _FMT_USD),
        "Postventa — CMV modelo (mostr.+taller+serv.)": ("cmv_postventa_modelo_usd", _FMT_USD),
        "Postventa — Margen contribución ($)": ("margen_postventa_usd", _FMT_USD),
        "Postventa — Margen contribución (%)": ("margen_postventa_pct", _FMT_PCT),
        "Postventa — Factor absorción (fijos prorr.)": ("factor_abs_postventa_pct", _FMT_PCT),
        "Maquinarias — Ventas": ("ventas_maquinarias_usd", _FMT_USD),
        "Maquinarias — Gasto variable": ("gastos_var_maquinarias_usd", _FMT_USD),
        "Alquileres — Ventas": ("ventas_alquileres_usd", _FMT_USD),
        "Global — Facturación total": ("fact_total_usd", _FMT_USD),
        "Global — Gastos fijos (total)": ("gastos_fijos_usd", _FMT_USD),
        "Global — Fijos sucursales": ("gastos_fijos_sucursales_usd", _FMT_USD),
        "Global — Fijos concesionario": ("gastos_fijos_concesionario_usd", _FMT_USD),
        "Global — Gastos variables": ("gastos_variables_usd", _FMT_USD),
        "Global — Total gastos": ("total_gastos_usd", _FMT_USD),
        "Global — Margen de contribución ($)": ("margen_usd", _FMT_USD),
        "Global — Margen de contribución (%)": ("margen_pct", _FMT_PCT),
        "Global — Resultado": ("resultado_usd", _FMT_USD),
        "Global — Factor de absorción": ("factor_abs_pct", _FMT_PCT),
        "Global — Punto de equilibrio": ("punto_equilibrio_usd", _FMT_USD),
    }

    _metric_opts = list(metric_map.keys())
    _def_idx = (
        _metric_opts.index("Global — Facturación total")
        if "Global — Facturación total" in _metric_opts
        else 0
    )
    sel_metric = st.selectbox("Métrica (últimos 12 meses)", options=_metric_opts, index=_def_idx)
    metric_col, _ = metric_map[sel_metric]
    if sel_metric == "Global — Punto de equilibrio":
        chart_df = trend_12[["periodo", "fact_total_usd", "punto_equilibrio_usd"]].copy()
        chart_df["fact_total_usd"] = pd.to_numeric(chart_df["fact_total_usd"], errors="coerce")
        chart_df["punto_equilibrio_usd"] = pd.to_numeric(chart_df["punto_equilibrio_usd"], errors="coerce")
        chart_df["dif_pct"] = (
            (chart_df["fact_total_usd"] - chart_df["punto_equilibrio_usd"]) / chart_df["punto_equilibrio_usd"] * 100.0
        )
        long_df = chart_df.melt(
            id_vars=["periodo", "dif_pct"],
            value_vars=["fact_total_usd", "punto_equilibrio_usd"],
            var_name="serie",
            value_name="valor",
        )
        labels = {"fact_total_usd": "Facturación total", "punto_equilibrio_usd": "Punto de equilibrio"}
        long_df["serie"] = long_df["serie"].map(labels)
        fig_trend = px.bar(
            long_df,
            x="periodo",
            y="valor",
            color="serie",
            barmode="group",
            text_auto=".2f",
            title="Tendencia 12 meses — Punto de equilibrio vs Facturación",
            color_discrete_map={
                "Facturación total": _COL_YELLOW,
                "Punto de equilibrio": _COL_GRAY,
            },
        )
        fig_trend.update_layout(xaxis_title="", yaxis_title="")
        st.plotly_chart(fig_trend, use_container_width=True)

        dif_df = chart_df[["periodo", "dif_pct"]].copy()
        dif_df["Dif. % (Facturación vs P.E.)"] = dif_df["dif_pct"].map(
            lambda v: "—" if pd.isna(v) else f"{v:,.2f} %"
        )
        st.dataframe(
            dif_df.drop(columns=["dif_pct"]),
            hide_index=True,
            use_container_width=True,
            column_config={
                "periodo": st.column_config.TextColumn("Período", width="small"),
                "Dif. % (Facturación vs P.E.)": st.column_config.TextColumn("Dif. % (Facturación vs P.E.)", width="medium"),
            },
        )
    else:
        chart_df = trend_12[["periodo", metric_col]].copy()
        chart_df[metric_col] = pd.to_numeric(chart_df[metric_col], errors="coerce")

        fig_trend = px.bar(
            chart_df,
            x="periodo",
            y=metric_col,
            text_auto=".2f",
            title=f"Tendencia 12 meses — {sel_metric}",
        )
        fig_trend.update_traces(marker_color=_COL_YELLOW)
        fig_trend.update_layout(xaxis_title="", yaxis_title="")
        st.plotly_chart(fig_trend, use_container_width=True)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Facturación total", f"US$ {float(sel['fact_total_usd']):,.2f}")
    k2.metric("Utilidad promedio total", f"{float(sel['util_prom_total_pct'] or 0.0):,.2f} %")
    k3.metric("Total gastos fijos", f"US$ {float(sel['gastos_fijos_usd']):,.2f}")
    k4.metric("Total gastos variables", f"US$ {float(sel['gastos_variables_usd']):,.2f}")
    k5, k6, k7, k8 = st.columns(4)
    k5.metric("Total gastos", f"US$ {float(sel['total_gastos_usd']):,.2f}")
    k6.metric("Margen de contribución", f"US$ {float(sel['margen_usd']):,.2f}")
    k7.metric("Margen de contribución %", f"{float(sel['margen_pct'] or 0.0):,.2f} %")
    k8.metric("Resultado", f"US$ {float(sel['resultado_usd']):,.2f}")
    k9, k10, k11, k12 = st.columns(4)
    k9.metric("Factor de absorción", f"{float(sel['factor_abs_pct'] or 0.0):,.2f} %")
    k10.metric(
        "Punto de equilibrio",
        f"US$ {float(sel['punto_equilibrio_usd']):,.2f}" if sel["punto_equilibrio_usd"] is not None else "—",
    )
    k11.metric("Postventa (rep.+serv.)", f"US$ {float(sel.get('ventas_postventa_usd') or 0):,.2f}")
    k12.metric("Otros variables (cargados)", f"US$ {float(sel.get('gastos_var_otros_usd') or 0):,.2f}")
    k13, k14, _, _ = st.columns(4)
    k13.metric("Fijos sucursales", f"US$ {float(sel.get('gastos_fijos_sucursales_usd') or 0):,.2f}")
    k14.metric("Fijos concesionario", f"US$ {float(sel.get('gastos_fijos_concesionario_usd') or 0):,.2f}")

    st.markdown("#### Mix y participación por sucursal (global)")
    color_map = {
        "RIO GRANDE": _COL_GRAY,
        "RIO GALLEGOS": _COL_YELLOW,
        "COMODORO": _COL_BLACK,
    }

    c3, c4 = st.columns([1, 1])
    with c3:
        fig_pie = px.pie(
            df_suc,
            names="sucursal",
            values="fact_total_usd",
            title="Participación de facturación por sucursal",
            color="sucursal",
            color_discrete_map=color_map,
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    with c4:
        fig_bar = px.bar(
            df_suc,
            x="sucursal",
            y="fact_total_usd",
            color="sucursal",
            text=df_suc["participacion_pct"].map(lambda v: f"{v:,.2f}%"),
            title="Comparación de facturación por sucursal",
            color_discrete_map=color_map,
        )
        fig_bar.update_layout(showlegend=False, xaxis_title="", yaxis_title="")
        st.plotly_chart(fig_bar, use_container_width=True)

    stack_df = df_suc.melt(
        id_vars=["sucursal"],
        value_vars=[
            "fact_mostrador_usd",
            "fact_taller_usd",
            "fact_servicios_usd",
            "fact_maquinarias_usd",
            "fact_alquileres_usd",
        ],
        var_name="canal",
        value_name="importe_usd",
    )
    stack_labels = {
        "fact_mostrador_usd": "Mostrador",
        "fact_taller_usd": "Taller",
        "fact_servicios_usd": "Servicios",
        "fact_maquinarias_usd": "Maquinarias",
        "fact_alquileres_usd": "Alquileres",
    }
    stack_df["canal"] = stack_df["canal"].map(stack_labels)
    fig_stack = px.bar(
        stack_df,
        x="sucursal",
        y="importe_usd",
        color="canal",
        barmode="stack",
        title="Facturación por sucursal (mostrador, taller, servicios, maquinarias, alquileres)",
        color_discrete_map={
            "Mostrador": _COL_GRAY,
            "Taller": _COL_ORANGE_MUTED,
            "Servicios": _COL_YELLOW,
            "Maquinarias": "#8B7355",
            "Alquileres": "#5C7A99",
        },
    )
    fig_stack.update_layout(xaxis_title="", yaxis_title="")
    st.plotly_chart(fig_stack, use_container_width=True)

    st.markdown("#### Gastos por sucursal (global, USD)")
    gst = df_suc.melt(
        id_vars=["sucursal"],
        value_vars=["cmv_repuestos_usd", "gastos_fijos_usd", "gastos_var_otros_usd"],
        var_name="concepto",
        value_name="importe_usd",
    )
    gst["concepto"] = gst["concepto"].map(
        {
            "cmv_repuestos_usd": "CMV repuestos (modelo línea)",
            "gastos_fijos_usd": "Gastos fijos",
            "gastos_var_otros_usd": "Otros variables",
        }
    )
    fig_gastos_suc = px.bar(
        gst,
        x="sucursal",
        y="importe_usd",
        color="concepto",
        barmode="group",
        title="CMV repuestos, fijos y otros variables por sucursal",
        color_discrete_map={
            "CMV repuestos (modelo línea)": _COL_YELLOW,
            "Gastos fijos": _COL_GRAY,
            "Otros variables": _COL_ORANGE_MUTED,
        },
    )
    fig_gastos_suc.update_layout(xaxis_title="", yaxis_title="")
    st.plotly_chart(fig_gastos_suc, use_container_width=True)

    st.markdown("### Tablas — mes en foco (USD)")
    tc_sel = float(sel.get("tipo_cambio_ars_usd") or 1.0)
    st.caption(f"TC del cierre: {tc_sel:,.4f} ARS/USD")
    _col_usd = {
        "Concepto": st.column_config.TextColumn("Concepto", width="medium"),
        "Importe (USD)": st.column_config.NumberColumn("Importe (USD)", format=_FMT_USD, step=0.01),
    }
    df_pv_ventas = pd.DataFrame(
        {
            "Concepto": [
                "Repuestos (neto fact.)",
                "Servicios",
                "Subtotal postventa",
            ],
            "Importe (USD)": [
                float(sel.get("ventas_repuestos_neto_usd") or 0),
                float(sel.get("ventas_servicios_usd") or 0),
                float(sel.get("ventas_postventa_usd") or 0),
            ],
        }
    )
    df_pv_cmv = pd.DataFrame(
        {
            "Concepto": [
                "CMV rep. mostrador",
                "CMV rep. taller",
                "CMV servicios / afines (modelo)",
                "Subtotal CMV postventa",
            ],
            "Importe (USD)": [
                float(sel.get("cmv_rep_mostrador_usd") or 0),
                float(sel.get("cmv_rep_taller_usd") or 0),
                float(sel.get("cmv_servicios_afines_usd") or 0),
                float(sel.get("cmv_postventa_modelo_usd") or 0),
            ],
        }
    )
    df_maq_v = pd.DataFrame(
        {"Concepto": ["Ventas maquinarias"], "Importe (USD)": [float(sel.get("ventas_maquinarias_usd") or 0)]}
    )
    df_alq_v = pd.DataFrame(
        {"Concepto": ["Ventas alquileres"], "Importe (USD)": [float(sel.get("ventas_alquileres_usd") or 0)]}
    )
    df_global = pd.DataFrame(
        {
            "Concepto": [
                "Facturación total",
                "Otros variables (cargados)",
                "Variables maquinaria (gasto)",
                "Total gastos variables",
                "Total gastos fijos",
                "Margen de contribución",
                "Resultado",
            ],
            "Importe (USD)": [
                float(sel["fact_total_usd"]),
                float(sel.get("gastos_var_otros_usd") or 0),
                float(sel.get("gastos_var_maquinarias_usd") or 0),
                float(sel["gastos_variables_usd"]),
                float(sel["gastos_fijos_usd"]),
                float(sel["margen_usd"]),
                float(sel["resultado_usd"]),
            ],
        }
    )
    tp1, tp2 = st.columns(2)
    with tp1:
        st.markdown("##### Postventa — ventas")
        st.dataframe(df_pv_ventas, hide_index=True, use_container_width=True, column_config=_col_usd)
    with tp2:
        st.markdown("##### Postventa — CMV (modelo)")
        st.caption("«Otros» y rubro pueden imputarse al CMV rep. o servicios en el modelo.")
        st.dataframe(df_pv_cmv, hide_index=True, use_container_width=True, column_config=_col_usd)
    tm1, tm2 = st.columns(2)
    with tm1:
        st.markdown("##### Venta maquinarias")
        st.dataframe(df_maq_v, hide_index=True, use_container_width=True, column_config=_col_usd)
    with tm2:
        st.markdown("##### Alquileres")
        st.dataframe(df_alq_v, hide_index=True, use_container_width=True, column_config=_col_usd)
    st.markdown("##### Global — totales del mes")
    st.dataframe(df_global, hide_index=True, use_container_width=True, column_config=_col_usd)

    st.markdown("### Exportar informe")
    compare_df = _build_comparison_last3(data, sel)
    cexp0, cexp1, cexp2 = st.columns([1, 1, 2])
    with cexp0:
        include_ai = st.checkbox("Incluir análisis IA", value=True)
    with cexp1:
        if st.button("Generar informe PDF", use_container_width=True):
            try:
                ai_analysis = None
                ai_status = "No solicitado."
                if include_ai:
                    ai_analysis, ai_status = _build_gemini_report_analysis(sel, compare_df)
                pdf_bytes = _build_inicio_report_pdf_bytes(
                    fig_trend_fact,
                    fig_pie,
                    fig_bar,
                    fig_stack,
                    trend_df=trend_all,
                    sel=sel,
                    compare_df=compare_df,
                    ai_analysis=ai_analysis,
                )
                st.session_state["inicio_pdf_bytes"] = pdf_bytes
                if include_ai and ai_analysis:
                    st.success("Informe generado con análisis IA.")
                elif include_ai:
                    st.warning(f"Informe generado sin análisis IA. Motivo: {ai_status}")
                else:
                    st.success("Informe generado.")
            except Exception as exc:
                st.error(
                    f"No se pudo generar el PDF: {exc}. Verificá tener instalada la dependencia kaleido."
                )
    with cexp2:
        pdf_bytes = st.session_state.get("inicio_pdf_bytes")
        st.download_button(
            "Descargar informe PDF",
            data=pdf_bytes if pdf_bytes else b"",
            file_name=f"informe_gopv_{int(sel['anio'])}_{int(sel['mes']):02d}.pdf",
            mime="application/pdf",
            disabled=not bool(pdf_bytes),
            use_container_width=True,
        )


def _render_registro_ventas() -> None:
    st.subheader("Registro")
    st.caption(
        "Cargá el mes y el tipo de cambio. En la grilla: repuestos, descuentos y servicios en **ARS**; "
        "**maquinarias y alquileres en USD**; gastos fijos y otros variables por sucursal en **USD**. "
        "Los **fijos del concesionario** (central) van aparte; el gasto variable de maquinaria del mes también es global."
    )

    st.markdown("##### 1. Período y tipo de cambio")
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        mes = st.selectbox(
            "Mes",
            options=list(range(1, 13)),
            format_func=lambda m: MES_NOMBRES[m - 1],
            key="cv_mes",
        )
    with c2:
        anio = st.number_input("Año", min_value=2020, max_value=2035, value=2026, key="cv_anio")
    cierre = database.get_cierre_ventas_mes(int(anio), int(mes))
    default_tc = float(cierre["tipo_cambio_ars_usd"]) if cierre else 1200.0
    with c3:
        tc = st.number_input(
            "Tipo de cambio (ARS por 1 USD)",
            min_value=0.0001,
            value=default_tc,
            format="%.4f",
            help="Repuestos y servicios de la grilla están en ARS; maquinarias y alquileres en USD. "
            "Este TC (ARS por USD) convierte maq./alq. a pesos al guardar y alimenta los cálculos en USD.",
            key=f"cv_tc_{int(anio)}_{int(mes)}",
        )

    st.markdown("##### 2. Ventas y gastos por sucursal")
    st.caption(
        "**ARS:** repuestos (mostrador/taller), descuentos y servicios. "
        "**USD:** maquinarias, alquileres, gastos fijos y otros gastos variables por sucursal "
        "(el TC de arriba convierte maq./alq. a pesos al guardar y al calcular totales). "
        "El rubro de «otros» es único para el mes (abajo). La fila Concesionario en la vista previa suma totales."
    )
    df_base = _df_editor_cierre_ventas(int(anio), int(mes))
    edited = st.data_editor(
        df_base,
        key=f"cv_editor_{anio}_{mes}",
        hide_index=True,
        column_config=_column_config_cierre_editor(),
    )

    gvm_def = float(cierre.get("gastos_var_maquinarias_usd") or 0) if cierre else 0.0
    gfc_def = float(cierre.get("gastos_fijos_concesionario_usd") or 0) if cierre else 0.0
    rub_def = _rubro_db_a_select(cierre.get("gastos_var_otros_rubro") if cierre else None)

    inv_def = float(cierre.get("inventario_usd") or 0) if cierre else 0.0
    cv_def = float(cierre.get("resultado_cero_ventas_pct") or 0) if cierre else 0.0
    fr_v = _cierre_hdr_float(cierre, "fill_rate_pct")
    fr_def = float(fr_v) if fr_v is not None else 0.0
    rot_v = _cierre_hdr_float(cierre, "rotacion_inventario")
    rot_def = float(rot_v) if rot_v is not None else 0.0

    st.markdown("##### 3. Gastos globales del mes (USD)")
    st.caption(
        "**Maquinaria:** variable del mes (no por sucursal). **Fijos concesionario:** costos fijos centrales que no asignás a una sucursal. "
        "**Rubro de «otros»:** aplica al total de «otros variables» de la grilla."
    )
    g1, g2, g3 = st.columns(3)
    with g1:
        gastos_var_maquinarias_usd = st.number_input(
            "Gastos variables — maquinaria (mes)",
            min_value=0.0,
            value=gvm_def,
            format="%.2f",
            help="Costo variable de maquinaria del mes (USD). Se suma al total de gastos variables.",
            key=f"cv_gvm_{int(anio)}_{int(mes)}",
        )
    with g2:
        gastos_fijos_concesionario_usd = st.number_input(
            "Gastos fijos — concesionario",
            min_value=0.0,
            value=float(gfc_def),
            format="%.2f",
            help="Fijos del concesionario (central), en USD. Se suman a los fijos cargados por sucursal en la grilla.",
            key=f"cv_gfc_{int(anio)}_{int(mes)}",
        )
    with g3:
        opts = ["— Ninguno —", "Servicios", "Repuestos"]
        idx = opts.index(rub_def) if rub_def in opts else 0
        rubro_sel = st.selectbox(
            "Rubro de «otros» variables (suma sucursales)",
            options=opts,
            index=idx,
            key=f"cv_gr_{int(anio)}_{int(mes)}",
        )
    rubro_db = _rubro_otros_a_db(rubro_sel) if rubro_sel != "— Ninguno —" else None

    with st.expander("Inventario y abastecimiento (opcional)", expanded=False):
        st.caption("Valores de referencia del mes; se guardan con el cierre.")
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            val_inventario_usd = st.number_input(
                "Inventario (valor USD)",
                min_value=0.0,
                value=float(inv_def),
                format="%.2f",
                key=f"cv_inv_{int(anio)}_{int(mes)}",
            )
        with k2:
            val_cero_ventas_pct = st.number_input(
                "Resultado Cero Ventas (%)",
                min_value=-999.99,
                max_value=999.99,
                value=float(cv_def),
                format="%.2f",
                help="Indicador en porcentaje (no en moneda).",
                key=f"cv_cv0_{int(anio)}_{int(mes)}",
            )
        with k3:
            val_fill_rate = st.number_input(
                "Fill rate (%)",
                min_value=0.0,
                max_value=100.0,
                value=float(fr_def),
                format="%.2f",
                key=f"cv_fr_{int(anio)}_{int(mes)}",
            )
        with k4:
            val_rotacion = st.number_input(
                "Rotación inventario",
                min_value=0.0,
                value=float(rot_def),
                format="%.4f",
                key=f"cv_rot_{int(anio)}_{int(mes)}",
            )

    tot = _registro_financiero_totales(
        edited,
        float(tc),
        rubro_db,
        gastos_var_maquinarias_usd=float(gastos_var_maquinarias_usd),
        gastos_fijos_concesionario_usd=float(gastos_fijos_concesionario_usd),
    )
    fact_total_usd = tot["fact_total_usd"]
    margen_global_usd = tot["margen_global_usd"]
    resultado_global_usd = tot["resultado_global_usd"]
    factor_abs_global_pct = tot["factor_abs_global_pct"]
    punto_equilibrio_usd = tot["punto_equilibrio_usd"]
    gastos_var_total_usd = tot["gastos_var_total_usd"]
    gastos_total_usd = tot["gastos_total_usd"]
    gv_mos_usd = tot["gv_mos_usd"]
    gv_tal_usd = tot["gv_tal_usd"]

    st.markdown("##### Resumen de gastos (USD, con el TC del período)")
    df_gastos_calc = pd.DataFrame(
        {
            "Concepto": [
                "Gastos variables repuestos mostrador",
                "Gastos variables repuestos taller",
                "Otros gastos variables (cargados)",
                "Gastos variables maquinaria (cargados)",
                "Total gastos variables",
                "Gastos fijos sucursales",
                "Gastos fijos concesionario",
                "Total gastos fijos",
                "Total gastos",
            ],
            "Importe (USD)": [
                round(gv_mos_usd, 2),
                round(gv_tal_usd, 2),
                round(float(tot.get("gastos_otros_usd") or 0), 2),
                round(float(tot.get("gastos_var_maq_usd") or 0), 2),
                round(gastos_var_total_usd, 2),
                round(float(tot.get("gastos_fijos_sucursales_usd") or 0), 2),
                round(float(tot.get("gastos_fijos_concesionario_usd") or 0), 2),
                round(float(tot.get("gastos_fijos_usd") or 0), 2),
                round(gastos_total_usd, 2),
            ],
        }
    )
    st.dataframe(
        df_gastos_calc,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Concepto": st.column_config.TextColumn("Concepto", width="large"),
            "Importe (USD)": st.column_config.NumberColumn("Importe (USD)", format=_FMT_USD, step=0.01),
        },
    )
    st.caption(
        "Mostrador y taller: CMV desde facturación neta de repuestos y % utilidad de la grilla. "
        "Fijos sucursales y «otros» variables vienen de la grilla; **fijos concesionario** se suman aparte. "
        "Total gastos = fijos (sucursales + concesionario) + variables."
    )
    st.markdown("**Indicadores globales (Concesionario)**")
    df_global = pd.DataFrame(
        {
            "Indicador": [
                "Facturación total",
                "Margen de contribución",
                "Factor de absorción",
                "Punto de equilibrio",
                "Resultado",
            ],
            "Valor": [
                f"US$ {fact_total_usd:,.2f}",
                f"US$ {margen_global_usd:,.2f}",
                (f"{factor_abs_global_pct:,.2f} %" if factor_abs_global_pct is not None else "—"),
                (f"US$ {punto_equilibrio_usd:,.2f}" if punto_equilibrio_usd is not None else "—"),
                f"US$ {resultado_global_usd:,.2f}",
            ],
        }
    )
    st.dataframe(
        df_global,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Indicador": st.column_config.TextColumn("Indicador", width="large"),
            "Valor": st.column_config.TextColumn("Valor", width="medium"),
        },
    )
    st.caption(
        "Margen = facturación total − gastos variables. "
        "Factor de absorción y punto de equilibrio usan el **total de gastos fijos** (sucursales + concesionario). "
        "Resultado = margen − total fijos."
    )

    st.markdown("**Inventario y logística (referencia del mes)**")
    df_inv = pd.DataFrame(
        {
            "Indicador": [
                "Inventario (USD)",
                "Resultado Cero Ventas (%)",
                "Fill rate",
                "Rotación inventario",
            ],
            "Valor": [
                _fmt_usd(val_inventario_usd),
                f"{val_cero_ventas_pct:,.2f} %",
                f"{val_fill_rate:,.2f} %",
                f"{val_rotacion:,.4f}",
            ],
        }
    )
    st.dataframe(
        df_inv,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Indicador": st.column_config.TextColumn("Indicador", width="large"),
            "Valor": st.column_config.TextColumn("Valor", width="medium"),
        },
    )

    st.markdown("### Exportar datos en USD (Excel)")
    ex_a, ex_b = st.columns(2)
    with ex_a:
        xls_mes = _excel_registro_bytes(
            anio=int(anio),
            mes=int(mes),
            edited=edited,
            tot=tot,
            inventario_usd=float(val_inventario_usd),
            resultado_cero_ventas_pct=float(val_cero_ventas_pct),
            fill_rate_pct=float(val_fill_rate),
            rotacion_inventario=float(val_rotacion),
        )
        st.download_button(
            "Descargar Excel — mes en pantalla",
            data=xls_mes,
            file_name=f"gopv_registro_{int(anio)}_{int(mes):02d}_usd.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key=f"dl_xls_mes_{anio}_{mes}",
        )
        st.caption("Incluye resumen, líneas por sucursal en USD y desglose de gastos variables.")
    with ex_b:
        st.markdown("**Varios meses** (solo cierres ya guardados)")
        r1, r2, r3, r4 = st.columns(4)
        with r1:
            mes_d = st.selectbox(
                "Mes desde",
                options=list(range(1, 13)),
                format_func=lambda m: MES_NOMBRES[m - 1],
                key="cv_xls_md",
            )
        with r2:
            anio_d = st.number_input("Año desde", min_value=2020, max_value=2035, value=int(anio), key="cv_xls_ad")
        with r3:
            mes_h = st.selectbox(
                "Mes hasta",
                options=list(range(1, 13)),
                index=int(mes) - 1,
                format_func=lambda m: MES_NOMBRES[m - 1],
                key="cv_xls_mh",
            )
        with r4:
            anio_h = st.number_input("Año hasta", min_value=2020, max_value=2035, value=int(anio), key="cv_xls_ah")
        p_desde = int(anio_d) * 12 + int(mes_d)
        p_hasta = int(anio_h) * 12 + int(mes_h)
        xls_rango = None
        if p_desde <= p_hasta:
            xls_rango = _excel_registro_rango_bytes(int(anio_d), int(mes_d), int(anio_h), int(mes_h))
        st.download_button(
            "Descargar Excel — rango",
            data=xls_rango if xls_rango else b"",
            file_name=f"gopv_registro_{int(anio_d)}_{int(mes_d):02d}_a_{int(anio_h)}_{int(mes_h):02d}_usd.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            disabled=xls_rango is None,
            key=f"dl_xls_rg_{anio_d}_{mes_d}_{anio_h}_{mes_h}",
        )
        if p_desde > p_hasta:
            st.warning("El período «desde» debe ser anterior o igual al «hasta».")
        elif xls_rango is None:
            st.caption("No hay cierres guardados en ese rango.")

    ver_usd = st.checkbox("Vista previa en USD (usa el TC de arriba)", value=False)
    prev = _preview_tabla(edited, float(gastos_fijos_concesionario_usd), float(tc))
    drop_show = [c for c in prev.columns if c in _PREVIEW_DROP_COLS or c == "gastos_fijos"]
    prev_show = prev.drop(columns=drop_show, errors="ignore")
    st.markdown("**Vista previa — ventas por sucursal y Concesionario**")
    show_disp, prev_cfg = _prepare_preview_display(prev_show, ver_usd=ver_usd, tc=float(tc))
    st.dataframe(
        show_disp,
        use_container_width=True,
        hide_index=True,
        column_config=prev_cfg,
    )

    b1, b2 = st.columns(2)
    with b1:
        if st.button("Guardar mes", type="primary", use_container_width=True):
            try:
                gf_sum = float(pd.to_numeric(edited["gastos_fijos_usd"], errors="coerce").fillna(0).sum())
                go_sum = float(pd.to_numeric(edited["gastos_var_otros_usd"], errors="coerce").fillna(0).sum())
                cid = database.upsert_cierre_ventas_mes_header(
                    int(anio),
                    int(mes),
                    float(tc),
                    notas=None,
                    gastos_fijos_global=gf_sum,
                    gastos_fijos_concesionario_usd=float(gastos_fijos_concesionario_usd),
                    gastos_var_otros=go_sum,
                    gastos_var_otros_rubro=rubro_db,
                    gastos_var_maquinarias_usd=float(gastos_var_maquinarias_usd),
                    inventario_usd=float(val_inventario_usd),
                    resultado_cero_ventas_pct=float(val_cero_ventas_pct),
                    fill_rate_pct=float(val_fill_rate),
                    rotacion_inventario=float(val_rotacion),
                )
                filas = [_row_to_db_dict(edited.loc[i]) for i in range(len(edited))]
                database.replace_lineas_cierre_ventas(cid, filas)
                st.success("Guardado.")
                st.rerun()
            except Exception as exc:
                st.error(f"No se pudo guardar: {exc}")
    with b2:
        if st.button("Recalcular meses guardados", use_container_width=True):
            try:
                n = database.recalculate_cierres_ventas_derivados()
                st.success(f"Recalculado OK. Filas actualizadas: {n}")
            except Exception as exc:
                st.error(f"No se pudo recalcular: {exc}")

        hist = database.list_cierres_ventas_mes(12)
        if len(hist):
            st.caption("Últimos cierres guardados")
            st.dataframe(
                hist,
                hide_index=True,
                use_container_width=True,
                column_config=_column_config_hist_cierres(hist),
            )


def _render_registro() -> None:
    _render_registro_ventas()


if "is_authed" not in st.session_state:
    st.session_state.is_authed = False
if "ui_pantalla" not in st.session_state:
    st.session_state.ui_pantalla = "inicio"

_bootstrap_database()
_layout_css()
_chrome_spacer()

brand_col, actions_col = st.columns([2, 3])
with brand_col:
    st.markdown("### GOPV")
with actions_col:
    if st.session_state.is_authed:
        a1, a2, a3 = st.columns(3)
        with a1:
            if st.button("Inicio", use_container_width=True, key="nav_inicio"):
                st.session_state.ui_pantalla = "inicio"
                st.rerun()
        with a2:
            if st.button("Registro", use_container_width=True, key="nav_registro"):
                st.session_state.ui_pantalla = "registro"
                st.rerun()
        with a3:
            if st.button("Cerrar sesión", use_container_width=True, key="nav_logout"):
                st.session_state.is_authed = False
                st.session_state.ui_pantalla = "inicio"
                st.rerun()

st.divider()

if not st.session_state.is_authed:
    with st.form("auth_form", clear_on_submit=True):
        pwd = st.text_input("Clave de acceso", type="password")
        ok = st.form_submit_button("Ingresar")
    if ok:
        if pwd == _app_password():
            st.session_state.is_authed = True
            st.success("Acceso concedido.")
            st.rerun()
        else:
            st.error("Clave incorrecta.")
            st.stop()
    else:
        st.info("Ingresa la clave para continuar.")
        st.stop()
elif st.session_state.ui_pantalla == "registro":
    _render_registro()
else:
    _render_inicio_dashboard()
