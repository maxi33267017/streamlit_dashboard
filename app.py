"""GOPV — login, registro y navbar."""

import os
import subprocess
import tempfile
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components
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
        if use_usd_display:
            disp[c] = disp[c] / float(tc)

    for c in _PREVIEW_FRAC_PCT_COLS:
        if c not in disp.columns:
            continue
        disp[c] = pd.to_numeric(disp[c], errors="coerce") * 100.0

    cfg: dict = {}
    for c in disp.columns:
        if c == "sucursal":
            cfg[c] = st.column_config.TextColumn("Sucursal", width="small")
        elif c in _PREVIEW_MONEY_COLS:
            unit = "USD" if use_usd_display else "ARS"
            lab = _PREVIEW_LABELS.get(c, c)
            cfg[c] = st.column_config.NumberColumn(f"{lab} ({unit})", format=money_fmt, step=0.01)
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
                }
            )
        else:
            rows.append({"sucursal": suc, **{k: 0.0 for k in COLS_VENTAS_EDIT}})
    return pd.DataFrame(rows)


def _row_to_db_dict(s: pd.Series) -> dict:
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
    }


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


def _fila_concesionario(df_edit: pd.DataFrame) -> dict:
    sfm = float(df_edit["fact_rep_mostrador"].sum())
    sft = float(df_edit["fact_rep_taller"].sum())
    sdm = float(df_edit["desc_mostrador"].sum())
    sdt = float(df_edit["desc_taller"].sum())
    sfs = float(df_edit["fact_servicios"].sum())
    um = _util_promedio_simple(df_edit, "util_pct_mostrador")
    ut = _util_promedio_simple(df_edit, "util_pct_taller")
    if um is None:
        um = 0.0
    if ut is None:
        ut = 0.0
    calc = database.compute_cierre_venta_linea(sfm, sft, sdm, sdt, um, ut, sfs)
    return {
        "sucursal": "CONCESIONARIO",
        "fact_rep_mostrador": sfm,
        "fact_rep_taller": sft,
        "desc_mostrador": sdm,
        "desc_taller": sdt,
        "util_pct_mostrador": um,
        "util_pct_taller": ut,
        "fact_servicios": sfs,
        **calc,
    }


def _preview_tabla(df_edit: pd.DataFrame) -> pd.DataFrame:
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
        )
        filas.append({**d, **calc})
    base = pd.DataFrame(filas)
    base = pd.concat([base, pd.DataFrame([_fila_concesionario(df_edit)])], ignore_index=True)
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
                f"taller={float(b['fact_taller_usd']):.2f}, servicios={float(b['fact_servicios_usd']):.2f}"
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
            "1) resumen ejecutivo del mes,\n"
            "2) comparación vs últimos 3 meses,\n"
            "3) sucursales que crecen/caen,\n"
            "4) mix mostrador vs taller+servicios,\n"
            "5) riesgos,\n"
            "6) 2 acciones concretas.\n\n"
            f"Mes: {MES_NOMBRES[int(sel['mes'])-1]} {int(sel['anio'])}\n"
            f"Facturación total: {float(sel['fact_total_usd']):.2f}\n"
            f"Gastos variables: {float(sel['gastos_variables_usd']):.2f}\n"
            f"Gastos fijos: {float(sel['gastos_fijos_usd']):.2f}\n"
            f"Margen: {float(sel['margen_usd']):.2f} ({float(sel['margen_pct'] or 0):.2f}%)\n"
            f"Resultado: {float(sel['resultado_usd']):.2f}\n"
            f"Factor absorción: {float(sel['factor_abs_pct'] or 0):.2f}%\n"
            f"Punto equilibrio: {float(sel['punto_equilibrio_usd'] or 0):.2f}\n"
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

    rows = [
        {
            "Métrica": "Facturación total",
            "Mes seleccionado": float(sel["fact_total_usd"]),
            "Promedio 3 meses previos": base_fact,
            "Dif. %": (_safe_ratio(float(sel["fact_total_usd"]) - base_fact, base_fact) or 0.0) * 100.0,
        },
        {
            "Métrica": "Margen contribución",
            "Mes seleccionado": float(sel["margen_usd"]),
            "Promedio 3 meses previos": base_margen,
            "Dif. %": (_safe_ratio(float(sel["margen_usd"]) - base_margen, base_margen) or 0.0) * 100.0,
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
                    plt.bar(x, mos, color=_COL_GRAY, label="Mostrador")
                    plt.bar(x, tal, bottom=mos, color=_COL_ORANGE_MUTED, label="Taller")
                    plt.bar(x, ser, bottom=mos + tal, color=_COL_YELLOW, label="Servicios")
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
        pdf.cell(0, 7, "Resumen agregado de todos los meses", ln=True)
        pdf.set_font("Arial", "", 10)
        pdf.cell(0, 6, f"Facturacion acumulada: {_fmt_usd(float(trend_df['fact_total_usd'].sum()))}", ln=True)
        pdf.cell(0, 6, f"Promedio mensual facturacion: {_fmt_usd(float(trend_df['fact_total_usd'].mean()))}", ln=True)
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
            f"Facturacion total: {_fmt_usd(sel['fact_total_usd'])}",
            f"Gastos variables: {_fmt_usd(sel['gastos_variables_usd'])}",
            f"Gastos fijos: {_fmt_usd(sel['gastos_fijos_usd'])}",
            f"Margen contribucion: {_fmt_usd(sel['margen_usd'])} ({_fmt_pct(sel['margen_pct'])})",
            f"Resultado: {_fmt_usd(sel['resultado_usd'])}",
            f"Factor absorcion: {_fmt_pct(sel['factor_abs_pct'])}",
            f"Punto de equilibrio: {_fmt_usd(sel['punto_equilibrio_usd'])}",
        ]
        for line in lines:
            pdf.cell(0, 6, line, ln=True)
        pdf.ln(2)
        if images_ok:
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
                    f"Taller {_fmt_usd(b['fact_taller_usd'])} | Servicios {_fmt_usd(b['fact_servicios_usd'])}",
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
            most_ts = branches.assign(ts=branches["fact_taller_usd"] + branches["fact_servicios_usd"]).sort_values(
                "ts", ascending=False
            ).iloc[0]
            pdf.ln(3)
            pdf.cell(0, 6, f"Sucursal con mayor facturacion: {top['sucursal']} ({_fmt_usd(top['fact_total_usd'])})", ln=True)
            pdf.cell(0, 6, f"Mayor mostrador: {most_mos['sucursal']} ({_fmt_usd(most_mos['fact_mostrador_usd'])})", ln=True)
            pdf.cell(
                0,
                6,
                f"Mayor taller+servicios: {most_ts['sucursal']} ({_fmt_usd(float(most_ts['ts']))})",
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
    ]
    for c in money_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        else:
            df[c] = 0.0

    df["fact_total_ars"] = (
        df["fact_rep_mostrador"] + df["fact_rep_taller"] - df["desc_mostrador"] - df["desc_taller"] + df["fact_servicios"]
    )

    tc = max(float(cierre.get("tipo_cambio_ars_usd") or 1.0), 1e-9)
    gastos_fijos_usd = float(cierre.get("gastos_fijos_global") or 0.0)
    gastos_otros_usd = float(cierre.get("gastos_var_otros") or 0.0)
    rubro_otros = cierre.get("gastos_var_otros_rubro")

    fm = float(df["fact_rep_mostrador"].sum())
    ft = float(df["fact_rep_taller"].sum())
    dm = float(df["desc_mostrador"].sum())
    dt = float(df["desc_taller"].sum())
    fs = float(df["fact_servicios"].sum())

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
    )

    fact_total_ars = float(df["fact_total_ars"].sum())
    fact_total_usd = fact_total_ars / tc
    gastos_var_usd = (float(gv["gv_servicios_ajustado"]) + float(gv["gv_repuestos_ajustado"])) / tc
    total_gastos_usd = gastos_fijos_usd + gastos_var_usd
    margen_usd = fact_total_usd - gastos_var_usd
    margen_ratio = _safe_ratio(margen_usd, fact_total_usd)
    resultado_usd = margen_usd - gastos_fijos_usd
    factor_abs_ratio = _safe_ratio(margen_usd, gastos_fijos_usd)
    punto_equilibrio_usd = (gastos_fijos_usd / margen_ratio) if margen_ratio is not None and margen_ratio > 0 else None

    df["participacion_facturacion"] = df["fact_total_ars"] / fact_total_ars if fact_total_ars > 0 else 0.0
    branches = pd.DataFrame(
        {
            "sucursal": df["sucursal"],
            "fact_total_usd": df["fact_total_ars"] / tc,
            "fact_mostrador_usd": (df["fact_rep_mostrador"] - df["desc_mostrador"]).clip(lower=0.0) / tc,
            "fact_taller_usd": (df["fact_rep_taller"] - df["desc_taller"]).clip(lower=0.0) / tc,
            "fact_servicios_usd": df["fact_servicios"] / tc,
            "participacion_pct": df["participacion_facturacion"] * 100.0,
        }
    ).sort_values("fact_total_usd", ascending=False)

    return {
        "cierre_id": int(cierre["id"]),
        "anio": int(cierre["anio"]),
        "mes": int(cierre["mes"]),
        "periodo": f"{_mes_corto(int(cierre['mes']))} {int(cierre['anio'])}",
        "fact_total_usd": fact_total_usd,
        "gastos_fijos_usd": gastos_fijos_usd,
        "gastos_variables_usd": gastos_var_usd,
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
                "gastos_fijos_usd": d["gastos_fijos_usd"],
                "gastos_variables_usd": d["gastos_variables_usd"],
                "total_gastos_usd": d["total_gastos_usd"],
                "margen_usd": d["margen_usd"],
                "margen_pct": d["margen_pct"],
                "resultado_usd": d["resultado_usd"],
                "factor_abs_pct": d["factor_abs_pct"],
                "punto_equilibrio_usd": d["punto_equilibrio_usd"],
            }
            for d in data
        ]
    )
    trend_df = trend_df.sort_values(["anio", "mes"], ascending=[False, False])
    trend_12 = trend_df.head(12).sort_values(["anio", "mes"], ascending=[True, True]).reset_index(drop=True)
    trend_all = trend_df.sort_values(["anio", "mes"], ascending=[True, True]).reset_index(drop=True)

    fig_trend_fact = px.bar(
        trend_all[["periodo", "fact_total_usd"]],
        x="periodo",
        y="fact_total_usd",
        text_auto=".2f",
        title="Facturación total - todos los meses",
    )
    fig_trend_fact.update_traces(marker_color=_COL_YELLOW)
    fig_trend_fact.update_layout(xaxis_title="", yaxis_title="")

    metric_map = {
        "Facturación total": ("fact_total_usd", _FMT_USD),
        "Gastos fijos": ("gastos_fijos_usd", _FMT_USD),
        "Gastos variables": ("gastos_variables_usd", _FMT_USD),
        "Total gastos": ("total_gastos_usd", _FMT_USD),
        "Margen de contribución ($)": ("margen_usd", _FMT_USD),
        "Margen de contribución (%)": ("margen_pct", _FMT_PCT),
        "Resultado": ("resultado_usd", _FMT_USD),
        "Factor de absorción": ("factor_abs_pct", _FMT_PCT),
        "Punto de equilibrio": ("punto_equilibrio_usd", _FMT_USD),
    }

    sel_metric = st.selectbox("Métrica (últimos 12 meses)", options=list(metric_map.keys()), index=0)
    metric_col, _ = metric_map[sel_metric]
    if sel_metric == "Punto de equilibrio":
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

    st.markdown("### Detalle por mes")
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
    k9, k10, _, _ = st.columns(4)
    k9.metric("Factor de absorción", f"{float(sel['factor_abs_pct'] or 0.0):,.2f} %")
    k10.metric(
        "Punto de equilibrio",
        f"US$ {float(sel['punto_equilibrio_usd']):,.2f}" if sel["punto_equilibrio_usd"] is not None else "—",
    )

    df_suc = sel["branches"].copy()
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
        value_vars=["fact_mostrador_usd", "fact_taller_usd", "fact_servicios_usd"],
        var_name="canal",
        value_name="importe_usd",
    )
    stack_labels = {
        "fact_mostrador_usd": "Mostrador",
        "fact_taller_usd": "Taller",
        "fact_servicios_usd": "Servicios",
    }
    stack_df["canal"] = stack_df["canal"].map(stack_labels)
    fig_stack = px.bar(
        stack_df,
        x="sucursal",
        y="importe_usd",
        color="canal",
        barmode="stack",
        title="Facturación por sucursal desagregada (Mostrador + Taller + Servicios)",
        color_discrete_map={
            "Mostrador": _COL_GRAY,
            "Taller": _COL_ORANGE_MUTED,
            "Servicios": _COL_YELLOW,
        },
    )
    fig_stack.update_layout(xaxis_title="", yaxis_title="")
    st.plotly_chart(fig_stack, use_container_width=True)

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
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        mes = st.selectbox(
            "Mes",
            options=list(range(1, 13)),
            format_func=lambda m: MES_NOMBRES[m - 1],
            key="cv_mes",
        )
    with c2:
        anio = st.number_input("Año", min_value=2020, max_value=2035, value=2025, key="cv_anio")
    cierre = database.get_cierre_ventas_mes(int(anio), int(mes))
    default_tc = float(cierre["tipo_cambio_ars_usd"]) if cierre else 1200.0
    with c3:
        tc = st.number_input(
            "Tipo de cambio (ARS por 1 USD)",
            min_value=0.0001,
            value=default_tc,
            format="%.4f",
            help="La grilla de ventas está en ARS. Este TC convierte a USD los gastos variables calculados a partir de esas ventas.",
            key=f"cv_tc_{int(anio)}_{int(mes)}",
        )

    df_base = _df_editor_cierre_ventas(int(anio), int(mes))
    st.caption(
        "Grilla en **ARS**. **Concesionario** suma facturación y pondera utilidades. "
        "Los gastos globales del mes (abajo) se cargan y muestran en **USD**; los calculados pasan de ARS a USD con el TC."
    )
    edited = st.data_editor(
        df_base,
        key=f"cv_editor_{anio}_{mes}",
        hide_index=True,
        column_config=_column_config_cierre_editor(),
    )

    st.markdown("### Gastos del mes (globales, USD)")
    gf_def = float(cierre.get("gastos_fijos_global") or 0) if cierre else 0.0
    go_def = float(cierre.get("gastos_var_otros") or 0) if cierre else 0.0
    rub_def = _rubro_db_a_select(cierre.get("gastos_var_otros_rubro") if cierre else None)

    tc_val = max(float(tc), 1e-9)

    g1, g2, g3 = st.columns(3)
    with g1:
        gastos_fijos_usd = st.number_input(
            "1. Gastos fijos (USD)",
            min_value=0.0,
            value=gf_def,
            format="%.2f",
            key=f"cv_gf_{int(anio)}_{int(mes)}",
        )
    with g2:
        gastos_otros_usd = st.number_input(
            "4. Otros gastos variables (USD)",
            min_value=0.0,
            value=go_def,
            format="%.2f",
            key=f"cv_go_{int(anio)}_{int(mes)}",
        )
    with g3:
        opts = ["— Ninguno —", "Servicios", "Repuestos"]
        idx = opts.index(rub_def) if rub_def in opts else 0
        rubro_sel = st.selectbox(
            "Rubro de otros (suma a variables servicios o repuestos)",
            options=opts,
            index=idx,
            key=f"cv_gr_{int(anio)}_{int(mes)}",
        )
    rubro_db = _rubro_otros_a_db(rubro_sel) if rubro_sel != "— Ninguno —" else None

    um_c = _util_promedio_simple(edited, "util_pct_mostrador")
    ut_c = _util_promedio_simple(edited, "util_pct_taller")
    otros_ars = float(gastos_otros_usd) * tc_val
    # Sin % utilidad de servicios en la grilla: CMV de servicios = 0 salvo «otros» con rubro Servicios.
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
    )

    gv_serv_usd = float(gv["gv_servicios_ajustado"]) / tc_val
    gv_rep_usd = float(gv["gv_repuestos_ajustado"]) / tc_val
    gv_mos_usd = float(gv["gv_rep_mostrador"]) / tc_val
    gv_tal_usd = float(gv["gv_rep_taller"]) / tc_val
    gastos_var_total_usd = gv_serv_usd + gv_rep_usd
    gastos_total_usd = float(gastos_fijos_usd) + gastos_var_total_usd
    fact_total_ars = (
        float(edited["fact_rep_mostrador"].sum())
        + float(edited["fact_rep_taller"].sum())
        - float(edited["desc_mostrador"].sum())
        - float(edited["desc_taller"].sum())
        + float(edited["fact_servicios"].sum())
    )
    fact_total_usd = fact_total_ars / tc_val
    margen_global_usd = fact_total_usd - gastos_var_total_usd
    margen_global_ratio = _safe_ratio(margen_global_usd, fact_total_usd)
    resultado_global_usd = margen_global_usd - float(gastos_fijos_usd)
    factor_abs_global_pct = (
        (margen_global_usd / float(gastos_fijos_usd)) * 100.0 if float(gastos_fijos_usd) > 0 else None
    )
    punto_equilibrio_usd = (
        float(gastos_fijos_usd) / float(margen_global_ratio) if margen_global_ratio is not None and margen_global_ratio > 0 else None
    )

    st.markdown("**Gastos calculados (Concesionario → USD, con el TC de arriba)**")
    df_gastos_calc = pd.DataFrame(
        {
            "Concepto": [
                "Gastos variables repuestos mostrador",
                "Gastos variables repuestos taller",
                "Otros gastos variables",
                "Total gastos variables",
                "Gastos fijos",
                "Total gastos",
            ],
            "Importe (USD)": [
                round(gv_mos_usd, 2),
                round(gv_tal_usd, 2),
                round(float(gastos_otros_usd), 2),
                round(gastos_var_total_usd, 2),
                round(float(gastos_fijos_usd), 2),
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
        "«Otros» es el importe en USD que cargás; el rubro indica si entra en variables repuestos o en variables servicios al total. "
        "Si hay «otros» y el rubro es «Ninguno», ese monto no entra al total de variables hasta que elijas rubro. "
        "Total gastos = fijos + total variables."
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
        "Factor de absorción = margen / gastos fijos. "
        "Punto de equilibrio = gastos fijos / % de margen (en fracción). "
        "Resultado = margen − gastos fijos."
    )

    ver_usd = st.checkbox("Vista previa en USD (usa el TC de arriba)", value=False)
    prev = _preview_tabla(edited)
    drop_show = [c for c in prev.columns if c in _PREVIEW_DROP_COLS]
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
                cid = database.upsert_cierre_ventas_mes_header(
                    int(anio),
                    int(mes),
                    float(tc),
                    notas=None,
                    gastos_fijos_global=float(gastos_fijos_usd),
                    gastos_var_otros=float(gastos_otros_usd),
                    gastos_var_otros_rubro=rubro_db,
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
