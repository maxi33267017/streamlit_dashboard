"""GOPV — login, registro y navbar."""

import os

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components

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
    return float(vals.mean()) / 100.0


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

    wm = (df["fact_rep_mostrador"] - df["desc_mostrador"]).clip(lower=0.0)
    wt = (df["fact_rep_taller"] - df["desc_taller"]).clip(lower=0.0)
    um_num = 0.0
    ut_num = 0.0
    if "util_pct_mostrador" in df.columns:
        um_num = float((pd.to_numeric(df["util_pct_mostrador"], errors="coerce").fillna(0.0) * wm).sum())
    if "util_pct_taller" in df.columns:
        ut_num = float((pd.to_numeric(df["util_pct_taller"], errors="coerce").fillna(0.0) * wt).sum())
    um_den = float(wm.sum())
    ut_den = float(wt.sum())
    um = (um_num / um_den) if um_den > 0 else 0.0
    ut = (ut_num / ut_den) if ut_den > 0 else 0.0

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
    util_prom_total = _safe_ratio(um_num + ut_num, um_den + ut_den)

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
        "util_prom_total_pct": (util_prom_total * 100.0) if util_prom_total is not None else None,
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
            }
            for d in data
        ]
    )
    trend_df = trend_df.sort_values(["anio", "mes"], ascending=[False, False])
    trend_12 = trend_df.head(12).sort_values(["anio", "mes"], ascending=[True, True]).reset_index(drop=True)

    metric_map = {
        "Facturación total": ("fact_total_usd", _FMT_USD),
        "Gastos fijos": ("gastos_fijos_usd", _FMT_USD),
        "Gastos variables": ("gastos_variables_usd", _FMT_USD),
        "Total gastos": ("total_gastos_usd", _FMT_USD),
        "Margen de contribución ($)": ("margen_usd", _FMT_USD),
        "Margen de contribución (%)": ("margen_pct", _FMT_PCT),
        "Resultado": ("resultado_usd", _FMT_USD),
        "Factor de absorción": ("factor_abs_pct", _FMT_PCT),
    }

    sel_metric = st.selectbox("Métrica (últimos 12 meses)", options=list(metric_map.keys()), index=0)
    metric_col, _ = metric_map[sel_metric]
    chart_df = trend_12[["periodo", metric_col]].copy()
    chart_df[metric_col] = pd.to_numeric(chart_df[metric_col], errors="coerce")

    fig_trend = px.bar(
        chart_df,
        x="periodo",
        y=metric_col,
        text_auto=".2f",
        title=f"Tendencia 12 meses — {sel_metric}",
    )
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
    k1.metric("Utilidad promedio total", f"{float(sel['util_prom_total_pct'] or 0.0):,.2f} %")
    k2.metric("Total gastos fijos", f"US$ {float(sel['gastos_fijos_usd']):,.2f}")
    k3.metric("Total gastos variables", f"US$ {float(sel['gastos_variables_usd']):,.2f}")
    k4.metric("Total gastos", f"US$ {float(sel['total_gastos_usd']):,.2f}")
    k5, k6, k7, k8 = st.columns(4)
    k5.metric("Margen de contribución", f"US$ {float(sel['margen_usd']):,.2f}")
    k6.metric("Margen de contribución %", f"{float(sel['margen_pct'] or 0.0):,.2f} %")
    k7.metric("Resultado", f"US$ {float(sel['resultado_usd']):,.2f}")
    k8.metric("Factor de absorción", f"{float(sel['factor_abs_pct'] or 0.0):,.2f} %")

    df_suc = sel["branches"].copy()
    color_map = {
        "RIO GRANDE": "#1f77b4",
        "RIO GALLEGOS": "#ff7f0e",
        "COMODORO": "#2ca02c",
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
    )
    fig_stack.update_layout(xaxis_title="", yaxis_title="")
    st.plotly_chart(fig_stack, use_container_width=True)


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

    um_c = _util_ponderado(edited, "mos")
    ut_c = _util_ponderado(edited, "tal")
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
    resultado_global_usd = margen_global_usd - float(gastos_fijos_usd)
    factor_abs_global_pct = (
        (margen_global_usd / float(gastos_fijos_usd)) * 100.0 if float(gastos_fijos_usd) > 0 else None
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
                "Resultado",
            ],
            "Valor": [
                f"US$ {fact_total_usd:,.2f}",
                f"US$ {margen_global_usd:,.2f}",
                (f"{factor_abs_global_pct:,.2f} %" if factor_abs_global_pct is not None else "—"),
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
