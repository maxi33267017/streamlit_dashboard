"""Streamlit Postventa - rewrite branch skeleton"""
import os
import tempfile

import pandas as pd
import plotly.express as px
import streamlit as st
import numpy as np
import matplotlib.pyplot as plt
plt.switch_backend("Agg")

JD_BRAND_COLORS = {
    "negro": "#212121",
    "amarillo": "#ffcd00",
    "gris": "#7a7a7a",
}

from datetime import date, datetime
from fpdf import FPDF
from gastos_automaticos import obtener_gastos_totales_con_automaticos
from ai_analysis import get_ai_summary
import database

from database import (
    delete_gasto,
    delete_venta,
    get_gasto_by_id,
    get_gastos,
    get_venta_by_id,
    get_ventas,
    init_database,
    insert_gasto,
    insert_venta,
    update_gasto,
    update_venta,
)

st.set_page_config(
    page_title="Postventa FY - Rewrite",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

@st.cache_resource(show_spinner=False)
def bootstrap_database():
    """Ensure the SQLite database exists before the UI renders."""
    init_database()
    return True

bootstrap_database()

NAVIGATION = {
    "📊 Dashboard": "overview",
    "💰 Ventas": "sales",
    "💸 Gastos": "expenses",
    "📈 Reportes": "reports",
    "⚙️ Configuración": "settings",
}

st.sidebar.title("Menú principal")
current_page = st.sidebar.radio("Navegación", list(NAVIGATION.keys()))
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") or st.secrets.get("GEMINI_API_KEY")
# Indicador explícito de qué base de datos está activa
st.sidebar.info("DB: Postgres" if database.USE_POSTGRES else "DB: SQLite")

def get_summary(period_label: str = "Período completo") -> dict:
    df_ventas = get_ventas()
    df_gastos = get_gastos()

    total_ingresos = df_ventas["total"].sum() if len(df_ventas) else 0.0
    total_gastos = (df_gastos["total_pct_se"].fillna(0) + df_gastos["total_pct_re"].fillna(0)).sum() if len(df_gastos) else 0.0
    resultado = total_ingresos - total_gastos

    return {
        "label": period_label,
        "ingresos": total_ingresos,
        "gastos": total_gastos,
        "resultado": resultado,
        "ventas_count": len(df_ventas),
        "gastos_count": len(df_gastos),
    }

def format_currency(value: float) -> str:
    return f"${value:,.2f}"


def format_percentage(value: float) -> str:
    return f"{value:.1f}%"


def sanitize_latin1(text: str | None) -> str:
    """Remueve caracteres fuera de latin-1 para evitar errores en FPDF."""
    if text is None:
        return ""
    if not isinstance(text, str):
        text = str(text)
    return text.encode("latin-1", "ignore").decode("latin-1")


def sanitize_list_latin1(items: list[str] | None, limit: int | None = None) -> list[str]:
    if not items:
        return []
    cleaned = [sanitize_latin1(item) for item in items if item]
    return cleaned[:limit] if limit else cleaned


def sanitize_top_clients(records: list[dict] | None, limit: int = 10) -> list[dict]:
    sanitized = []
    for row in records or []:
        sanitized.append(
            {
                "cliente": sanitize_latin1(row.get("cliente")),
                "sucursal": sanitize_latin1(row.get("sucursal")),
                "total": float(row.get("total", 0.0) or 0.0),
            }
        )
        if len(sanitized) >= limit:
            break
    return sanitized


def create_stacked_chart_image(labels: list[str], series: list[dict], ylabel: str, figsize=(7.2, 2.9)) -> str | None:
    if not labels or not series:
        return None
    fig, ax = plt.subplots(figsize=figsize)
    x = np.arange(len(labels))
    bottom = np.zeros(len(labels))
    totales = np.zeros(len(labels))
    handled = False
    color_cycle = [
        JD_BRAND_COLORS["negro"],
        JD_BRAND_COLORS["amarillo"],
        JD_BRAND_COLORS["gris"],
    ]
    for idx, serie in enumerate(series):
        values = np.array(serie.get("values", []), dtype=float)
        if len(values) == 0:
            continue
        color = color_cycle[idx % len(color_cycle)]
        ax.bar(
            x,
            values,
            bottom=bottom,
            label=serie.get("label", "Serie"),
            color=color,
        )
        bottom += values
        totales += values
        handled = True
    if not handled:
        plt.close(fig)
        return None
    ax.set_ylabel(ylabel)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1), fontsize=8)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    max_total = totales.max() if len(totales) else 0
    for idx, total in enumerate(totales):
        ax.text(
            x[idx],
            total + (max_total * 0.01 if max_total else 1000),
            f"{total:,.0f}",
            ha="center",
            va="bottom",
            fontsize=7,
        )
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
    fig.tight_layout()
    fig.savefig(tmp_file.name, dpi=140)
    plt.close(fig)
    return tmp_file.name


def create_line_chart_image(labels: list[str], series: list[dict], ylabel: str, figsize=(7.2, 2.9)) -> str | None:
    if not labels or not series:
        return None
    fig, ax = plt.subplots(figsize=figsize)
    x = np.arange(len(labels))
    handled = False
    color_cycle = [
        JD_BRAND_COLORS["negro"],
        JD_BRAND_COLORS["amarillo"],
        JD_BRAND_COLORS["gris"],
    ]
    for idx, serie in enumerate(series):
        values = np.array(serie.get("values", []), dtype=float)
        if len(values) == 0:
            continue
        color = color_cycle[idx % len(color_cycle)]
        ax.plot(
            x,
            values,
            marker='o',
            label=serie.get("label", "Serie"),
            color=color,
        )
        handled = True
    if not handled:
        plt.close(fig)
        return None
    ax.set_ylabel(ylabel)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1), fontsize=8)
    ax.grid(True, linestyle="--", alpha=0.3)
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
    fig.tight_layout()
    fig.savefig(tmp_file.name, dpi=140)
    plt.close(fig)
    return tmp_file.name


def build_historic_distributions(hist_start: date, hist_end: date) -> dict | None:
    df_hist_ventas = get_ventas(str(hist_start), str(hist_end))
    gastos_hist = obtener_gastos_totales_con_automaticos(str(hist_start), str(hist_end))
    df_hist_gastos = gastos_hist["gastos_todos"]

    if len(df_hist_ventas) == 0 and len(df_hist_gastos) == 0:
        return None

    months = pd.period_range(hist_start, hist_end, freq="M")
    month_labels = [m.strftime("%b %y") for m in months]

    ventas_hist = df_hist_ventas.copy()
    if len(ventas_hist):
        ventas_hist["fecha"] = pd.to_datetime(ventas_hist["fecha"])
        ventas_hist["month"] = ventas_hist["fecha"].dt.to_period("M")
        ventas_hist["sucursal"] = ventas_hist["sucursal"].fillna("SIN SUC")
        ventas_hist = ventas_hist[~ventas_hist["sucursal"].str.upper().eq("COMPARTIDOS")]
    branches = ["COMODORO", "RIO GRANDE", "RIO GALLEGOS"]
    other_branches = [
        suc
        for suc in ventas_hist["sucursal"].unique()
        if suc not in branches
    ] if len(ventas_hist) else []
    branch_order = [s for s in branches if len(ventas_hist) and s in ventas_hist["sucursal"].unique()] + other_branches[:3]
    ventas_series = []
    for branch in branch_order:
        values = []
        branch_mask = (
            ventas_hist["sucursal"].str.upper().eq(branch.upper()) if len(ventas_hist) else None
        )
        for month in months:
            if len(ventas_hist):
                month_mask = ventas_hist["month"] == month
                val = ventas_hist.loc[month_mask & branch_mask, "total"].sum()
            else:
                val = 0.0
            values.append(float(val))
        ventas_series.append({"label": branch.title(), "values": values})

    gastos_fixed = []
    gastos_variable = []
    resultados_series_map = {branch: [] for branch in branch_order}

    for month in months:
        month_start = month.to_timestamp(how="start").date()
        month_end = month.to_timestamp(how="end").date()
        gastos_mes = obtener_gastos_totales_con_automaticos(str(month_start), str(month_end))
        df_gastos_mes = gastos_mes["gastos_todos"].copy()
        df_gastos_mes["monto"] = (
            df_gastos_mes["total_pct_se"].fillna(0) + df_gastos_mes["total_pct_re"].fillna(0)
        )
        total_mes = df_gastos_mes["monto"].sum()
        df_reg_mes = gastos_mes["gastos_registrados"].copy()
        df_reg_mes["monto"] = (
            df_reg_mes["total_pct_se"].fillna(0) + df_reg_mes["total_pct_re"].fillna(0)
        )
        fijo_mes = df_reg_mes.loc[df_reg_mes["tipo"].fillna("").str.upper().eq("FIJO"), "monto"].sum()
        gastos_fixed.append(float(fijo_mes))
        gastos_variable.append(float(max(total_mes - fijo_mes, 0.0)))

        for branch in branch_order:
            ingresos_branch = 0.0
            if len(ventas_hist):
                ingresos_branch = ventas_hist.loc[
                    (ventas_hist["month"] == month)
                    & ventas_hist["sucursal"].str.upper().eq(branch.upper()),
                    "total",
                ].sum()
            gastos_branch = df_gastos_mes.loc[
                df_gastos_mes["sucursal"].fillna("").str.upper().eq(branch.upper()),
                "monto",
            ].sum()
            resultados_series_map[branch].append(float(ingresos_branch - gastos_branch))

    resultados_series = [{"label": branch.title(), "values": resultados_series_map[branch]} for branch in branch_order]

    return {
        "labels": month_labels,
        "ventas": {"series": ventas_series},
        "gastos": {"fixed": gastos_fixed, "variable": gastos_variable},
        "resultados": {"series": resultados_series},
    }


def compute_working_hours(start_date: date, end_date: date) -> tuple[float, int]:
    """
    Calcula las horas hábiles totales (considerando 9 h de lunes a viernes y 4 h los sábados)
    y la cantidad de días trabajados dentro del período seleccionado.
    """
    if start_date > end_date:
        return 0.0, 0

    day_range = pd.date_range(start_date, end_date, freq="D")
    total_hours = 0.0
    working_days = 0

    for day in day_range:
        weekday = day.weekday()
        if weekday < 5:  # Lunes a viernes
            total_hours += 9
            working_days += 1
        elif weekday == 5:  # Sábados
            total_hours += 4
            working_days += 1
        # Domingos no suman

    return total_hours, working_days


def render_reports_gastos():
    st.caption("Explora gastos fijos y variables registrados y calculados.")

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        fecha_inicio = st.date_input(
            "Fecha inicio",
            value=date(date.today().year, date.today().month, 1),
            key="gastos_fecha_inicio",
        )
    with col_f2:
        fecha_fin = st.date_input(
            "Fecha fin",
            value=date.today(),
            key="gastos_fecha_fin",
        )

    horas_disponibles_total = 0.0
    horas_habiles_periodo = 0.0
    dias_habiles_periodo = 0

    if fecha_inicio > fecha_fin:
        st.error("La fecha de inicio no puede ser mayor a la fecha fin.")
        return

    df_gastos = get_gastos(str(fecha_inicio), str(fecha_fin))
    gastos_totales = obtener_gastos_totales_con_automaticos(str(fecha_inicio), str(fecha_fin))
    df_gastos_calc = gastos_totales["gastos_automaticos"]

    def _coerce_numeric(df, cols):
        for c in cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df

    num_cols = ["total_pct", "total_pct_se", "total_pct_re", "total_usd", "total_pesos"]
    df_gastos = _coerce_numeric(df_gastos, num_cols)
    df_gastos_calc = _coerce_numeric(df_gastos_calc, num_cols)

    if len(df_gastos) == 0 and len(df_gastos_calc) == 0:
        st.info("No hay gastos para el período seleccionado.")
        return

    df_gastos = df_gastos.copy()
    if len(df_gastos) > 0 and "total_pct" not in df_gastos.columns:
        df_gastos["total_pct"] = df_gastos["total_pct_se"].fillna(0) + df_gastos["total_pct_re"].fillna(0)

    if len(df_gastos_calc) > 0:
        df_todos = pd.concat([df_gastos, df_gastos_calc], ignore_index=True)
    else:
        df_todos = df_gastos.copy()

    total_registrado = (
        df_gastos["total_pct_se"].fillna(0) + df_gastos["total_pct_re"].fillna(0)
    ).sum() if len(df_gastos) else 0.0
    total_calculado = (
        df_gastos_calc["total_pct_se"].fillna(0) + df_gastos_calc["total_pct_re"].fillna(0)
    ).sum() if len(df_gastos_calc) else 0.0

    df_reg_fijo = df_gastos[df_gastos["tipo"] == "FIJO"] if "tipo" in df_gastos.columns else pd.DataFrame()
    df_reg_variable = df_gastos[df_gastos["tipo"] == "VARIABLE"] if "tipo" in df_gastos.columns else pd.DataFrame()

    gasto_fijo_total = (
        df_reg_fijo["total_pct_se"].fillna(0).sum() + df_reg_fijo["total_pct_re"].fillna(0).sum()
    ) if len(df_reg_fijo) else 0.0
    gasto_variable_total = (
        df_reg_variable["total_pct_se"].fillna(0).sum() + df_reg_variable["total_pct_re"].fillna(0).sum()
    ) if len(df_reg_variable) else 0.0
    gasto_variable_total += total_calculado

    total_general = gasto_fijo_total + gasto_variable_total

    col_top1, col_top2, col_top3 = st.columns(3)
    col_top1.metric("Gasto total", format_currency(total_general))
    col_top2.metric("Gasto fijo total", format_currency(gasto_fijo_total))
    col_top3.metric("Gasto variable total", format_currency(gasto_variable_total))

    st.divider()
    st.subheader("Impacto real Servicios vs Repuestos (según % asignado)")
    impacto_servicios = df_todos["total_pct_se"].fillna(0).sum()
    impacto_repuestos = df_todos["total_pct_re"].fillna(0).sum()

    servicios_fijo = df_reg_fijo["total_pct_se"].fillna(0).sum()
    servicios_variable_reg = df_reg_variable["total_pct_se"].fillna(0).sum()
    servicios_variable_calc = df_gastos_calc["total_pct_se"].fillna(0).sum() if len(df_gastos_calc) else 0.0
    servicios_variable = servicios_variable_reg + servicios_variable_calc

    repuestos_fijo = df_reg_fijo["total_pct_re"].fillna(0).sum()
    repuestos_variable_reg = df_reg_variable["total_pct_re"].fillna(0).sum()
    repuestos_variable_calc = df_gastos_calc["total_pct_re"].fillna(0).sum() if len(df_gastos_calc) else 0.0
    repuestos_variable = repuestos_variable_reg + repuestos_variable_calc

    total_subareas = impacto_servicios + impacto_repuestos
    col_sub1, col_sub2 = st.columns(2)
    col_sub1.metric(
        "Impacto Servicios",
        format_currency(impacto_servicios),
        delta=f"{(impacto_servicios / total_subareas * 100):.1f}%" if total_subareas else None,
    )
    col_sub2.metric(
        "Impacto Repuestos",
        format_currency(impacto_repuestos),
        delta=f"{(impacto_repuestos / total_subareas * 100):.1f}%" if total_subareas else None,
    )

    col_det1, col_det2 = st.columns(2)
    col_det1.metric(
        "Servicios Fijo",
        format_currency(servicios_fijo),
        delta=f"{(servicios_fijo / impacto_servicios * 100):.1f}% del impacto SE" if impacto_servicios else None,
    )
    col_det1.metric(
        "Servicios Variable",
        format_currency(servicios_variable),
        delta=f"{(servicios_variable / impacto_servicios * 100):.1f}% del impacto SE"
        if impacto_servicios
        else None,
    )
    col_det2.metric(
        "Repuestos Fijo",
        format_currency(repuestos_fijo),
        delta=f"{(repuestos_fijo / impacto_repuestos * 100):.1f}% del impacto RE" if impacto_repuestos else None,
    )
    col_det2.metric(
        "Repuestos Variable",
        format_currency(repuestos_variable),
        delta=f"{(repuestos_variable / impacto_repuestos * 100):.1f}% del impacto RE"
        if impacto_repuestos
        else None,
    )

    fig_sub = px.pie(
        pd.DataFrame(
            {"Subárea": ["Servicios", "Repuestos"], "Monto USD": [impacto_servicios, impacto_repuestos]}
        ),
        values="Monto USD",
        names="Subárea",
        title="Distribución final por subárea",
        hole=0.45,
    )
    st.plotly_chart(fig_sub, use_container_width=True, key="gastos_subareas_pie")
    st.caption("Incluye tanto gastos registrados como los calculados automáticamente.")

    st.subheader("Gasto fijo por sucursal")

    if len(df_reg_fijo) > 0:
        gasto_fijo_sucursal = (
            df_reg_fijo.groupby("sucursal")["total_pct_se"].sum()
            + df_reg_fijo.groupby("sucursal")["total_pct_re"].sum()
        )
        gasto_fijo_sucursal = gasto_fijo_sucursal.reset_index(name="Monto USD")
        fig_fijo = px.bar(
            gasto_fijo_sucursal,
            x="sucursal",
            y="Monto USD",
            labels={"sucursal": "Sucursal", "Monto USD": "USD"},
            title="Gasto fijo por sucursal",
            color="Monto USD",
            color_continuous_scale="Blues",
        )
        st.plotly_chart(fig_fijo, use_container_width=True, key="gastos_fijos_sucursal")
    else:
        st.caption("No se registran gastos fijos en el período.")

    st.subheader("Gasto variable por sucursal (registrado + calculado)")
    df_variable_all = pd.concat(
        [
            df_reg_variable,
            df_gastos_calc.assign(tipo="VARIABLE(CALC)"),
        ],
        ignore_index=True,
    ) if len(df_reg_variable) or len(df_gastos_calc) else pd.DataFrame()

    if len(df_variable_all):
        gasto_variable_sucursal = (
            df_variable_all.groupby("sucursal")["total_pct_se"].sum()
            + df_variable_all.groupby("sucursal")["total_pct_re"].sum()
        )
        gasto_variable_sucursal = gasto_variable_sucursal.reset_index(name="Monto USD")
        fig_var = px.bar(
            gasto_variable_sucursal,
            x="sucursal",
            y="Monto USD",
            labels={"sucursal": "Sucursal", "Monto USD": "USD"},
            title="Gasto variable por sucursal",
            color="Monto USD",
            color_continuous_scale="Greens",
        )
        st.plotly_chart(fig_var, use_container_width=True, key="gastos_variable_sucursal")
    else:
        st.caption("No se registran gastos variables en el período.")

    st.divider()
    st.subheader("Composición de gastos - Fijo vs Variable")

    composicion_tipo = pd.DataFrame(
        [
            {"Tipo": "Fijo", "Monto USD": gasto_fijo_total},
            {"Tipo": "Variable", "Monto USD": gasto_variable_total},
        ]
    )
    composicion_tipo["Porcentaje"] = (
        composicion_tipo["Monto USD"] / composicion_tipo["Monto USD"].sum() * 100
    ).round(1)

    fig_pie_tipo = px.pie(
        composicion_tipo,
        values="Monto USD",
        names="Tipo",
        title="Composición Fijo vs Variable",
        hole=0.45,
    )
    st.plotly_chart(fig_pie_tipo, use_container_width=True)
    st.divider()
    st.subheader("Composición por clasificación")

    if len(df_todos) > 0:
        df_clasificacion = df_todos.copy()
        df_clasificacion["clasificacion"] = df_clasificacion["clasificacion"].fillna("Sin clasificación")
        df_clasificacion["Monto USD"] = (
            df_clasificacion["total_pct_se"].fillna(0) + df_clasificacion["total_pct_re"].fillna(0)
        )
        resumen_clasificacion = (
            df_clasificacion.groupby("clasificacion")["Monto USD"].sum().reset_index()
        )
        resumen_clasificacion = resumen_clasificacion.sort_values("Monto USD", ascending=False)

        fig_clasif = px.bar(
            resumen_clasificacion.head(10),
            x="clasificacion",
            y="Monto USD",
            title="Top 10 clasificaciones por gasto",
            labels={"clasificacion": "Clasificación", "Monto USD": "USD"},
        )
        st.plotly_chart(fig_clasif, use_container_width=True, key="gastos_top_clasificacion")

        st.caption("Participación por clasificación")
        resumen_clasificacion["Porcentaje"] = (
            resumen_clasificacion["Monto USD"] / resumen_clasificacion["Monto USD"].sum() * 100
        ).round(1)
        st.dataframe(
            resumen_clasificacion.assign(
                **{
                    "Monto USD": resumen_clasificacion["Monto USD"].apply(format_currency),
                    "Porcentaje": resumen_clasificacion["Porcentaje"].apply(lambda x: f"{x}%"),
                }
            ),
            use_container_width=True,
        )
    else:
        st.caption("No hay clasificaciones para mostrar.")


def render_reports_operativo():
    st.caption("Compara ingresos, gastos y punto de equilibrio.")

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        fecha_inicio = st.date_input(
            "Fecha inicio",
            value=date(date.today().year, date.today().month, 1),
            key="operativo_fecha_inicio",
        )
    with col_f2:
        fecha_fin = st.date_input(
            "Fecha fin",
            value=date.today(),
            key="operativo_fecha_fin",
        )

    col_cfg1, col_cfg2 = st.columns(2)
    with col_cfg1:
        tecnicos_activos = st.number_input(
            "Técnicos disponibles",
            min_value=1,
            max_value=50,
            value=7,
            step=1,
            key="operativo_tecnicos_count",
            help="Cantidad de técnicos operativos en el período.",
        )
    with col_cfg2:
        tarifa_hora_tecnico = st.number_input(
            "Tarifa promedio mano de obra (USD/h)",
            min_value=1.0,
            max_value=500.0,
            value=60.0,
            step=1.0,
            key="operativo_tarifa_hora",
            help="Tarifa promedio facturada por hora técnica, usada para estimar horas vendidas.",
        )

    if fecha_inicio > fecha_fin:
        st.error("La fecha de inicio no puede ser mayor a la fecha fin.")
        return

    ingresos_mo_y_asistencia = 0.0
    horas_vendidas_estimadas = 0.0

    df_ventas = get_ventas(str(fecha_inicio), str(fecha_fin))
    gastos_totales = obtener_gastos_totales_con_automaticos(str(fecha_inicio), str(fecha_fin))
    df_gastos_reg = gastos_totales["gastos_registrados"]
    df_gastos_calc = gastos_totales["gastos_automaticos"]
    df_gastos_todos = gastos_totales["gastos_todos"]

    for df_target in [df_gastos_reg, df_gastos_todos]:
        if len(df_target) and "total_pct" not in df_target.columns:
            df_target["total_pct"] = df_target["total_pct_se"].fillna(0) + df_target["total_pct_re"].fillna(0)

    if len(df_ventas) == 0 and len(df_gastos_todos) == 0:
        st.info("No hay datos suficientes para este período.")
        return

    total_ingresos = df_ventas["total"].sum() if len(df_ventas) else 0.0
    ventas_se = df_ventas[df_ventas["tipo_re_se"] == "SE"] if len(df_ventas) else pd.DataFrame()
    ventas_re = df_ventas[df_ventas["tipo_re_se"] == "RE"] if len(df_ventas) else pd.DataFrame()
    ingresos_servicios = ventas_se["total"].sum() if len(ventas_se) else 0.0
    ingresos_repuestos = ventas_re["total"].sum() if len(ventas_re) else 0.0
    porcentaje_iibb = 0.045
    descuento_iibb = total_ingresos * porcentaje_iibb
    ingresos_netos = total_ingresos - descuento_iibb

    df_gastos_todos = df_gastos_todos.copy()
    df_gastos_todos["monto"] = df_gastos_todos["total_pct_se"].fillna(0) + df_gastos_todos["total_pct_re"].fillna(0)
    total_gastos = df_gastos_todos["monto"].sum()
    resultado = ingresos_netos - total_gastos

    col_sum1, col_sum2, col_sum3 = st.columns(3)
    col_sum1.metric(
        "Ingresos netos (post IIBB)",
        format_currency(ingresos_netos),
        delta=f"-{format_currency(descuento_iibb)} IIBB",
    )
    col_sum2.metric("Gastos totales", format_currency(total_gastos))
    col_sum3.metric(
        "Resultado operativo",
        format_currency(resultado),
        delta="Positivo" if resultado >= 0 else "Negativo",
    )

    st.divider()
    st.subheader("Ingresos vs Gastos por sucursal")
    comparacion = pd.DataFrame()
    comparacion_pdf_records: list[dict] = []
    if len(df_ventas) > 0 or len(df_gastos_todos) > 0:
        ingresos_sucursal = (
            df_ventas.groupby("sucursal")["total"].sum().reset_index(name="ingresos")
            if len(df_ventas)
            else pd.DataFrame(columns=["sucursal", "ingresos"])
        )
        gastos_sucursal = df_gastos_todos.groupby("sucursal")["monto"].sum().reset_index(name="gastos_totales")
        comparacion = ingresos_sucursal.merge(gastos_sucursal, on="sucursal", how="outer").fillna(0)
        comparacion = comparacion[~comparacion["sucursal"].fillna("").str.upper().eq("COMPARTIDOS")]
        comparacion["resultado"] = comparacion["ingresos"] - comparacion["gastos_totales"]

        if len(comparacion):
            comparacion_pdf_records = comparacion.rename(
                columns={
                    "sucursal": "Sucursal",
                    "ingresos": "Ingresos",
                    "gastos_totales": "Gastos",
                    "resultado": "Resultado",
                }
            ).to_dict("records")

            comparacion_display = pd.DataFrame(comparacion_pdf_records)
            comparacion_display["Ingresos"] = comparacion_display["Ingresos"].apply(format_currency)
            comparacion_display["Gastos"] = comparacion_display["Gastos"].apply(format_currency)
            comparacion_display["Resultado"] = comparacion_display["Resultado"].apply(format_currency)
            st.dataframe(comparacion_display, use_container_width=True)

            fig_comp = px.bar(
                comparacion.melt(id_vars="sucursal", value_vars=["ingresos", "gastos_totales"]),
                x="sucursal",
                y="value",
                color="variable",
                barmode="group",
                title="Ingresos vs Gastos por sucursal",
                labels={"sucursal": "Sucursal", "value": "USD", "variable": ""},
            )
            st.plotly_chart(fig_comp, use_container_width=True, key="operativo_ingresos_gastos")
        else:
            st.caption("No hay datos de sucursales para mostrar.")
    else:
        st.caption("No hay datos de sucursales para mostrar.")

    st.divider()
    st.subheader("Punto de equilibrio")

    df_fijos_periodo = (
        df_gastos_reg[df_gastos_reg["tipo"] == "FIJO"].copy()
        if len(df_gastos_reg) and "tipo" in df_gastos_reg.columns
        else pd.DataFrame()
    )
    if len(df_fijos_periodo):
        df_fijos_periodo["monto"] = df_fijos_periodo["total_pct_se"].fillna(0) + df_fijos_periodo["total_pct_re"].fillna(0)
        gastos_fijos_bruto = df_fijos_periodo["monto"].sum()
    else:
        gastos_fijos_bruto = 0.0

    direct_classifications = {"SUELDO", "CARGAS SOCIALES", "OBRA SOCIAL"}
    direct_costos_tecnicos = 0.0
    if len(df_fijos_periodo):
        clasif_series = df_fijos_periodo.get("clasificacion", pd.Series(dtype=str)).fillna("").str.upper()
        proveedor_series = df_fijos_periodo.get("proveedor", pd.Series(dtype=str)).fillna("").str.upper()
        mask_direct = clasif_series.isin(direct_classifications) & proveedor_series.eq("TECNICOS")
        if mask_direct.any():
            direct_costos_tecnicos = df_fijos_periodo.loc[mask_direct, "monto"].fillna(0).sum()

    gastos_fijos_periodo = max(gastos_fijos_bruto - direct_costos_tecnicos, 0.0)

    gastos_fijos_servicios = df_fijos_periodo["total_pct_se"].fillna(0).sum() if len(df_fijos_periodo) else 0.0
    gastos_fijos_repuestos = df_fijos_periodo["total_pct_re"].fillna(0).sum() if len(df_fijos_periodo) else 0.0

    factor_abs_total = (total_ingresos / gastos_fijos_periodo * 100) if gastos_fijos_periodo else 0.0
    factor_abs_servicios = (ingresos_servicios / gastos_fijos_servicios * 100) if gastos_fijos_servicios else 0.0
    factor_abs_repuestos = (ingresos_repuestos / gastos_fijos_repuestos * 100) if gastos_fijos_repuestos else 0.0

    variable_costos_periodo = max(total_gastos - gastos_fijos_periodo, 0.0)
    contrib_total = ingresos_netos - variable_costos_periodo
    margen_promedio = (contrib_total / total_ingresos) if total_ingresos else 0
    ventas_equilibrio = gastos_fijos_periodo / margen_promedio if margen_promedio > 0 else 0

    col_pe1, col_pe2, col_pe3 = st.columns(3)
    col_pe1.metric("Gastos fijos del período", format_currency(gastos_fijos_periodo))
    col_pe2.metric("Ventas necesarias (equilibrio)", format_currency(ventas_equilibrio))
    brecha_vs_ventas = ingresos_netos - ventas_equilibrio
    col_pe3.metric(
        "Brecha vs ventas netas",
        format_currency(brecha_vs_ventas),
        delta="Superávit" if brecha_vs_ventas >= 0 else "Déficit",
    )

    st.caption(
        "El punto de equilibrio usa los gastos fijos registrados y el margen promedio del período. "
        "Considera RE + SE; puedes estrechar el período arriba."
    )

    st.subheader("EBIT y factores de absorción")
    absorcion_rows = [
        {"Indicador": "EBIT", "Valor": format_currency(resultado)},
        {"Indicador": "Factor absorción postventa", "Valor": format_percentage(factor_abs_total)},
        {"Indicador": "Factor absorción repuestos", "Valor": format_percentage(factor_abs_repuestos)},
        {"Indicador": "Factor absorción servicios", "Valor": format_percentage(factor_abs_servicios)},
    ]
    st.table(pd.DataFrame(absorcion_rows))

    st.subheader("Punto de equilibrio por sucursal")
    pe_pdf_records: list[dict] = []
    if len(comparacion):
        if len(df_fijos_periodo):
            gastos_fijos_suc = (
                df_fijos_periodo.groupby("sucursal")["monto"].sum().reset_index(name="gastos_fijos")
            )
        else:
            gastos_fijos_suc = pd.DataFrame(columns=["sucursal", "gastos_fijos"])

        comparacion_pe = comparacion.merge(gastos_fijos_suc, on="sucursal", how="left").fillna(0)
        comparacion_pe["gastos_variables"] = (comparacion_pe["gastos_totales"] - comparacion_pe["gastos_fijos"]).clip(lower=0)
        comparacion_pe["contribucion"] = comparacion_pe["ingresos"] - comparacion_pe["gastos_variables"]
        comparacion_pe["margen"] = comparacion_pe.apply(
            lambda row: (row["contribucion"] / row["ingresos"]) if row["ingresos"] else 0, axis=1
        )
        comparacion_pe["ventas_necesarias"] = comparacion_pe.apply(
            lambda row: row["gastos_fijos"] / row["margen"] if row["margen"] > 0 else 0,
            axis=1,
        )
        comparacion_pe["brecha"] = comparacion_pe["ingresos"] - comparacion_pe["ventas_necesarias"]

        pe_pdf_records = comparacion_pe.rename(
            columns={
                "sucursal": "Sucursal",
                "ingresos": "Ventas actuales",
                "gastos_fijos": "Gastos fijos",
                "ventas_necesarias": "Ventas necesarias",
                "brecha": "Brecha",
            }
        ).to_dict("records")

        df_pe_display = pd.DataFrame(pe_pdf_records)
        df_pe_display["Ventas actuales"] = comparacion_pe["ingresos"].apply(format_currency)
        df_pe_display["Gastos fijos"] = comparacion_pe["gastos_fijos"].apply(format_currency)
        df_pe_display["Ventas necesarias"] = comparacion_pe["ventas_necesarias"].apply(format_currency)
        df_pe_display["Brecha"] = comparacion_pe["brecha"].apply(format_currency)

        st.dataframe(df_pe_display, use_container_width=True)
    else:
        st.caption("No hay información por sucursal disponible.")

    periodo_label = f"{fecha_inicio.strftime('%d/%m/%Y')} - {fecha_fin.strftime('%d/%m/%Y')}"
    ai_state_key = f"ia_operativo_{fecha_inicio.isoformat()}_{fecha_fin.isoformat()}"
    ai_result = st.session_state.get(ai_state_key)

    should_autorun_ai = (
        GEMINI_API_KEY
        and ai_state_key not in st.session_state
        and (len(df_ventas) or len(df_gastos_todos))
    )
    def pct_str(value: float, base: float) -> str:
        if base <= 0:
            return "N/A"
        return f"{value / base:.1%}"

    repuestos_mostrador = 0.0
    if len(ventas_re):
        if "repuestos" in ventas_re.columns and ventas_re["repuestos"].notna().any():
            repuestos_mostrador = ventas_re["repuestos"].fillna(ventas_re["total"]).sum()
        else:
            repuestos_mostrador = ventas_re["total"].sum()
    repuestos_servicios = ventas_se["repuestos"].fillna(0).sum() if "repuestos" in ventas_se.columns else 0.0
    mano_obra_total = ventas_se["mano_obra"].fillna(0).sum() if "mano_obra" in ventas_se.columns else 0.0
    asistencia_total = ventas_se["asistencia"].fillna(0).sum() if "asistencia" in ventas_se.columns else 0.0
    ingresos_servicios_totales = mano_obra_total + asistencia_total
    terceros_total = ventas_se["terceros"].fillna(0).sum() if "terceros" in ventas_se.columns else 0.0
    ventas_repuestos_total = repuestos_mostrador + repuestos_servicios

    costo_repuestos_auto = 0.0
    if len(df_gastos_calc) and "clasificacion" in df_gastos_calc.columns:
        mask_costo = df_gastos_calc["clasificacion"].str.contains("COSTO DE REPUESTOS", case=False, na=False)
        costo_repuestos_auto = df_gastos_calc.loc[mask_costo, "total_usd"].fillna(0).sum()
    if costo_repuestos_auto == 0 and ventas_repuestos_total > 0:
        costo_repuestos_auto = ventas_repuestos_total * 0.65
    margen_repuestos_val = ventas_repuestos_total - costo_repuestos_auto

    if len(df_gastos_reg) == 0:
        df_gastos_reg = pd.DataFrame(columns=df_gastos_todos.columns)
    costos_tecnicos = 0.0
    if len(df_gastos_reg):
        mask_tecnicos = df_gastos_reg.get("area", pd.Series(dtype=str)).str.upper().eq("SERVICIO")
        costos_tecnicos = (
            df_gastos_reg.loc[mask_tecnicos, "total_pct"].fillna(0).sum()
            if mask_tecnicos.any()
            else 0.0
        )
    if costos_tecnicos == 0.0 and len(df_gastos_todos):
        costos_tecnicos = df_gastos_todos["total_pct_se"].fillna(0).sum()
    margen_mano_obra = ingresos_servicios_totales - costos_tecnicos

    ticket_se_total = (ventas_se["total"].sum() / len(ventas_se)) if len(ventas_se) else 0.0
    ticket_re_total = (ventas_re["total"].sum() / len(ventas_re)) if len(ventas_re) else 0.0

    horas_col = next((col for col in ["horas", "hs", "horas_trabajadas"] if col in df_ventas.columns), None)
    horas_totales = df_ventas[horas_col].fillna(0).sum() if horas_col else None
    if horas_totales and horas_totales < 0:
        horas_totales = None
    horas_promedio = (horas_totales / len(ventas_se)) if horas_totales and len(ventas_se) else None
    ingreso_total_por_hora = None
    repuestos_por_hora = None
    if horas_totales and horas_totales > 0:
        ingreso_total_por_hora = (mano_obra_total + repuestos_servicios) / horas_totales
        repuestos_por_hora = repuestos_servicios / horas_totales

    horas_habiles_periodo, dias_habiles_periodo = compute_working_hours(fecha_inicio, fecha_fin)
    horas_disponibles_total = (
        horas_habiles_periodo * tecnicos_activos if horas_habiles_periodo and tecnicos_activos else 0.0
    )
    ingresos_mo_y_asistencia = mano_obra_total + asistencia_total
    horas_vendidas_estimadas = ingresos_mo_y_asistencia / tarifa_hora_tecnico if tarifa_hora_tecnico > 0 else 0.0
    productividad_taller = (
        (horas_vendidas_estimadas / horas_disponibles_total) if horas_disponibles_total > 0 else 0.0
    )

    st.subheader("Productividad del taller")
    col_prod1, col_prod2, col_prod3 = st.columns(3)
    col_prod1.metric(
        "Horas vendidas (estimadas)",
        f"{horas_vendidas_estimadas:,.1f} h" if horas_vendidas_estimadas else "0 h",
        delta=f"Ingresos MO + Asistencia: {format_currency(ingresos_mo_y_asistencia)}",
    )
    horas_disp_label = (
        f"{horas_disponibles_total:,.1f} h" if horas_disponibles_total else "0 h"
    )
    horas_disp_delta = (
        f"{tecnicos_activos} técnicos x {horas_habiles_periodo:,.1f} h"
        if horas_habiles_periodo and tecnicos_activos
        else None
    )
    col_prod2.metric("Horas hábiles disponibles", horas_disp_label, delta=horas_disp_delta)
    col_prod3.metric(
        "Productividad",
        format_percentage(productividad_taller * 100),
        delta=f"{dias_habiles_periodo} días trabajados" if dias_habiles_periodo else None,
    )
    st.caption(
        "Se estiman las horas vendidas dividiendo los ingresos de mano de obra + asistencia por la tarifa promedio."
    )

    productividad_context = {
        "ingresos_mo_asistencia": ingresos_mo_y_asistencia,
        "horas_vendidas": horas_vendidas_estimadas,
        "horas_disponibles": horas_disponibles_total,
        "dias_habiles": dias_habiles_periodo,
        "tecnicos": tecnicos_activos,
        "tarifa": tarifa_hora_tecnico,
        "productividad_pct": productividad_taller,
    }

    if should_autorun_ai:
        with st.spinner("Calculando insights IA del período..."):
            auto_ai_payload = get_ai_summary(
                df_ventas=df_ventas.copy(),
                df_gastos=df_gastos_todos.copy(),
                gemini_api_key=GEMINI_API_KEY,
                fecha_inicio=str(fecha_inicio),
                fecha_fin=str(fecha_fin),
                gastos_context=gastos_totales,
                productividad_context=productividad_context,
            )
        st.session_state[ai_state_key] = auto_ai_payload
        ai_result = auto_ai_payload

    estado_resultados_rows = []
    if total_ingresos > 0:
        utilidad_bruta = total_ingresos - variable_costos_periodo
        resultado_estado = utilidad_bruta - gastos_fijos_periodo
        estado_resultados_rows = [
            {
                "concepto": "(+) Ingresos brutos totales",
                "monto": total_ingresos,
                "porcentaje": "100.0%",
            },
            {
                "concepto": "(-) Costos directos de venta (Rep + Tec + Ter)",
                "monto": variable_costos_periodo,
                "porcentaje": pct_str(variable_costos_periodo, total_ingresos),
            },
            {
                "concepto": "(=) Utilidad bruta (Margen contribución)",
                "monto": utilidad_bruta,
                "porcentaje": pct_str(utilidad_bruta, total_ingresos),
            },
            {
                "concepto": "(-) Gastos de estructura (Adm + Sist)",
                "monto": gastos_fijos_periodo,
                "porcentaje": pct_str(gastos_fijos_periodo, total_ingresos),
            },
            {
                "concepto": "(=) Resultado operativo bruto",
                "monto": resultado_estado,
                "porcentaje": pct_str(resultado_estado, total_ingresos),
            },
        ]

    ventas_suc_resumen = []
    if len(comparacion_pdf_records) and total_ingresos:
        ventas_suc_resumen = [
            {
                "Sucursal": row["Sucursal"],
                "Venta": row["Ingresos"],
                "Porcentaje": pct_str(row["Ingresos"], total_ingresos),
            }
            for row in comparacion_pdf_records
        ]

    resumen_cards = [
        ("Ingresos netos", format_currency(ingresos_netos)),
        ("Gastos totales", format_currency(total_gastos)),
        ("Resultado operativo", format_currency(resultado)),
        ("Gastos fijos", format_currency(gastos_fijos_periodo)),
    ]
    if ventas_repuestos_total > 0:
        resumen_cards.append(
            (
                "Margen repuestos",
                f"{format_currency(margen_repuestos_val)} ({pct_str(margen_repuestos_val, ventas_repuestos_total)})",
            )
        )
    if ingresos_servicios_totales > 0:
        resumen_cards.append(
            (
                "Margen servicios (MO + Asist)",
                f"{format_currency(margen_mano_obra)} ({pct_str(margen_mano_obra, ingresos_servicios_totales)})",
            )
        )
    if comparacion_pdf_records:
        mejor = max(comparacion_pdf_records, key=lambda row: row["Resultado"])
        peor = min(comparacion_pdf_records, key=lambda row: row["Resultado"])
        resumen_cards.append(("Mejor sucursal", f"{mejor['Sucursal']} ({format_currency(mejor['Resultado'])})"))
        if peor["Sucursal"] != mejor["Sucursal"]:
            resumen_cards.append(("Mayor presión", f"{peor['Sucursal']} ({format_currency(peor['Resultado'])})"))

    HIST_START = date(2025, 11, 1)
    HIST_END = date(2026, 10, 31)
    historicos_pdf = build_historic_distributions(HIST_START, HIST_END)

    detalles_pdf = {
        "empresa": "Patagonia Maquinarias",
        "moneda": "USD",
        "estado_resultados": estado_resultados_rows,
        "ingresos_detalle": {
            "repuestos_mostrador": repuestos_mostrador,
            "repuestos_servicios": repuestos_servicios,
            "mano_obra": mano_obra_total,
            "asistencia": asistencia_total,
            "terceros": terceros_total,
        },
        "ventas_sucursales": ventas_suc_resumen,
        "tickets": {
            "se_total": ticket_se_total,
            "re_total": ticket_re_total,
        },
        "eficiencia": {
            "tecnicos_activos": None,
            "ordenes_servicio": len(ventas_se),
            "ordenes_repuestos": len(ventas_re),
            "horas_totales": horas_totales,
            "horas_promedio": horas_promedio,
            "ingreso_por_hora": ingreso_total_por_hora,
            "repuestos_por_hora": repuestos_por_hora,
            "horas_disponibles": horas_disponibles_total,
            "horas_vendidas_estimadas": horas_vendidas_estimadas,
            "productividad": productividad_taller,
            "tarifa_hora": tarifa_hora_tecnico,
            "tecnicos_config": tecnicos_activos,
            "dias_habiles": dias_habiles_periodo,
        },
        "margenes_negocio": {
            "repuestos": {
                "ventas": ventas_repuestos_total,
                "costo": costo_repuestos_auto,
                "margen": margen_repuestos_val,
                "margen_pct": pct_str(margen_repuestos_val, ventas_repuestos_total)
                if ventas_repuestos_total
                else "N/A",
                "comentario": "El mix taller/mostrador permite atar repuestos a mano de obra.",
            },
            "servicios": {
                "ventas": ingresos_servicios_totales,
                "costo": costos_tecnicos,
                "margen": margen_mano_obra,
                "margen_pct": pct_str(margen_mano_obra, ingresos_servicios_totales) if ingresos_servicios_totales else "N/A",
                "ingresos_por_hora": ingreso_total_por_hora,
            },
        },
        "productividad": {
            "ingresos_mo_asistencia": ingresos_mo_y_asistencia,
            "horas_vendidas": horas_vendidas_estimadas,
            "horas_disponibles": horas_disponibles_total,
            "dias_habiles": dias_habiles_periodo,
            "tecnicos": tecnicos_activos,
            "tarifa": tarifa_hora_tecnico,
            "ocupacion_pct": productividad_taller,
        },
        "resumen_cards": resumen_cards,
        "historicos": historicos_pdf,
    }

    ai_section_for_pdf = None
    if ai_result:
        insights_for_pdf = ai_result.get("insights", {})
        ai_section_for_pdf = {
            "timestamp": sanitize_latin1(ai_result.get("timestamp_analisis")),
            "tendencias": sanitize_list_latin1(insights_for_pdf.get("tendencias"), limit=4),
            "alertas": sanitize_list_latin1(insights_for_pdf.get("alertas"), limit=4),
            "recomendaciones": sanitize_list_latin1(insights_for_pdf.get("recomendaciones"), limit=4),
            "recomendaciones_extra": sanitize_list_latin1(ai_result.get("recomendaciones"), limit=3),
            "recomendaciones_sucursales": sanitize_list_latin1(
                ai_result.get("recomendaciones_sucursales"), limit=4
            ),
            "recomendaciones_mix": sanitize_list_latin1(ai_result.get("recomendaciones_mix"), limit=4),
            "oportunidades": sanitize_list_latin1(ai_result.get("oportunidades"), limit=4),
            "riesgos": sanitize_list_latin1(ai_result.get("riesgos"), limit=4),
            "top_clientes": sanitize_top_clients(ai_result.get("top_clientes_detalle")),
            "prediccion": ai_result.get("prediccion") or {},
            "alertas_criticas": [
                {
                    "titulo": sanitize_latin1(alerta.get("titulo")),
                    "descripcion": sanitize_latin1(alerta.get("descripcion")),
                }
                for alerta in (ai_result.get("alertas_criticas") or [])
            ],
            "anomalias": [
                {
                    "tipo": sanitize_latin1(anomalia.get("tipo")),
                    "descripcion": sanitize_latin1(anomalia.get("descripcion")),
                }
                for anomalia in (ai_result.get("anomalias") or [])
            ],
            "productividad": ai_result.get("productividad"),
        }
    detalles_pdf["ai_insights"] = ai_section_for_pdf

    resumen_pdf = {
        "ingresos_netos": ingresos_netos,
        "total_ingresos_brutos": total_ingresos,
        "gastos_totales": total_gastos,
        "resultado": resultado,
        "gastos_fijos": gastos_fijos_periodo,
        "ventas_equilibrio": ventas_equilibrio,
        "brecha_ventas": brecha_vs_ventas,
        "margen_promedio": margen_promedio,
        "variable_costos": variable_costos_periodo,
        "contribucion_total": contrib_total,
        "ebit": resultado,
        "factor_abs_total": factor_abs_total,
        "factor_abs_repuestos": factor_abs_repuestos,
        "factor_abs_servicios": factor_abs_servicios,
        "horas_habiles": horas_habiles_periodo,
        "horas_disponibles": horas_disponibles_total,
        "horas_vendidas_estimadas": horas_vendidas_estimadas,
    }
    pdf_bytes = build_operativo_pdf(
        periodo_label,
        resumen_pdf,
        comparacion_pdf_records,
        pe_pdf_records,
        detalles_pdf,
    )
    st.download_button(
        "📄 Exportar informe (PDF)",
        data=pdf_bytes,
        file_name=f"informe_operativo_{fecha_inicio.strftime('%Y%m%d')}_{fecha_fin.strftime('%Y%m%d')}.pdf",
        mime="application/pdf",
    )

    st.subheader("📊 Análisis operativo integral")
    if not GEMINI_API_KEY:
        st.info("Configura la variable de entorno GEMINI_API_KEY para habilitar el análisis automático.")
    else:
        col_ai_btn, col_ai_help = st.columns([1, 3])
        with col_ai_btn:
            trigger_ai = st.button(
                "Actualizar análisis IA",
                key=f"btn_ai_{ai_state_key}",
                help="Genera recomendaciones inteligentes para el período mostrado.",
            )
        with col_ai_help:
            st.caption("Gemini analiza ventas, gastos y KPIs para sugerir acciones concretas.")

        if trigger_ai and (len(df_ventas) or len(df_gastos_todos)):
            with st.spinner("Generando análisis inteligente con Gemini..."):
                ai_payload = get_ai_summary(
                    df_ventas=df_ventas.copy(),
                    df_gastos=df_gastos_todos.copy(),
                    gemini_api_key=GEMINI_API_KEY,
                    fecha_inicio=str(fecha_inicio),
                    fecha_fin=str(fecha_fin),
                    gastos_context=gastos_totales,
                    productividad_context=productividad_context,
                )
            st.session_state[ai_state_key] = ai_payload

        ai_result = st.session_state.get(ai_state_key)
        if ai_result:
            gemini_status = ai_result.get("gemini_status", {})
            if gemini_status.get("error"):
                st.warning(
                    f"No se pudo usar Gemini: {gemini_status['error']}. "
                    "Se muestran los hallazgos estadísticos locales."
                )
            elif gemini_status.get("activo"):
                st.success("Gemini aportó insights específicos para este período.")

            insights = ai_result.get("insights", {})

            def _render_insight_list(container, title, items):
                container.markdown(f"**{title}**")
                if items:
                    for item in items:
                        container.write(f"- {item}")
                else:
                    container.caption("Sin datos en esta categoría.")

            col_tend, col_alert, col_rec = st.columns(3)
            _render_insight_list(col_tend, "Tendencias", insights.get("tendencias"))
            _render_insight_list(col_alert, "Alertas", insights.get("alertas"))
            _render_insight_list(col_rec, "Recomendaciones", insights.get("recomendaciones"))

            recomendaciones_extra = ai_result.get("recomendaciones", [])
            if recomendaciones_extra:
                st.markdown("**Recomendaciones adicionales**")
                for rec in recomendaciones_extra:
                    st.write(f"- {rec}")

            extra_sections = [
                ("Recomendaciones por sucursal", ai_result.get("recomendaciones_sucursales")),
                ("Recomendaciones mix RE vs SE", ai_result.get("recomendaciones_mix")),
                ("Oportunidades detectadas", ai_result.get("oportunidades")),
                ("Riesgos detectados", ai_result.get("riesgos")),
            ]
            for title, items in extra_sections:
                if not items:
                    continue
                with st.expander(title, expanded=False):
                    for item in items:
                        st.write(f"- {item}")

            prod_ai = ai_result.get("productividad")
            if prod_ai:
                with st.expander("Productividad (estimación base)", expanded=False):
                    st.write(
                        f"Ingresos MO + asistencia: {format_currency(prod_ai.get('ingresos_mo_asistencia', 0))}."
                    )
                    st.write(
                        f"Horas vendidas: {prod_ai.get('horas_vendidas', 0):,.1f} h | "
                        f"Horas disponibles: {prod_ai.get('horas_disponibles', 0):,.1f} h "
                        f"({prod_ai.get('dias_habiles', 0)} días hábiles, {prod_ai.get('tecnicos', 0)} técnicos)."
                    )
                    st.write(
                        f"Ocupación estimada: {format_percentage(prod_ai.get('productividad_pct', 0) * 100)} "
                        f"con tarifa promedio {format_currency(prod_ai.get('tarifa', 0))}/h."
                    )

            top_clientes = ai_result.get("top_clientes_detalle") or []
            if top_clientes:
                with st.expander("Top 10 clientes del período", expanded=False):
                    df_top = pd.DataFrame(top_clientes)
                    if not df_top.empty:
                        df_top = df_top.rename(columns={"cliente": "Cliente", "sucursal": "Sucursal", "total": "Ventas"})
                        df_top["Ventas"] = df_top["Ventas"].apply(format_currency)
                        st.dataframe(df_top, use_container_width=True)
                    else:
                        st.caption("Sin datos de clientes para este período.")

            prediccion = ai_result.get("prediccion") or {}
            if prediccion.get("prediccion"):
                dias_habiles = prediccion.get("dias_habiles")
                horizonte = prediccion.get("horizonte_dias", 30)
                label_pronostico = (
                    f"Pronóstico {dias_habiles} días hábiles"
                    if dias_habiles
                    else "Pronóstico 30 días"
                )
                delta_msg = prediccion.get("mensaje", "")
                if dias_habiles:
                    delta_msg = f"{delta_msg} | {dias_habiles} días hábiles" if delta_msg else f"{dias_habiles} días hábiles"
                col_pred, col_conf = st.columns(2)
                col_pred.metric(
                    label_pronostico,
                    format_currency(prediccion["prediccion"]),
                    delta_msg,
                )
                promedio_habil = prediccion.get("promedio_diario_habil")
                promedio_calendar = prediccion.get("promedio_diario")
                conf_delta = []
                if promedio_habil:
                    conf_delta.append(f"Prom/día hábil: {format_currency(promedio_habil)}")
                if promedio_calendar and horizonte:
                    conf_delta.append(f"Prom/calendario: {format_currency(promedio_calendar)}")
                col_conf.metric(
                    "Confianza del modelo",
                    prediccion.get("confianza", "N/A"),
                    " | ".join(conf_delta) if conf_delta else None,
                )

            if ai_result.get("alertas_criticas"):
                with st.expander("Alertas críticas detectadas", expanded=True):
                    for alerta in ai_result["alertas_criticas"]:
                        titulo = alerta.get("titulo", "Alerta")
                        descripcion = alerta.get("descripcion", "")
                        st.write(f"**{titulo}** — {descripcion}")

            if ai_result.get("anomalias"):
                with st.expander("Anomalías detectadas"):
                    for anomalia in ai_result["anomalias"]:
                        st.write(
                            f"- {anomalia.get('tipo', 'Anomalía')}: {anomalia.get('descripcion', '')}"
                        )


def render_reports_ventas():
    st.caption("Analiza las ventas por segmento y sucursal.")

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        fecha_inicio = st.date_input(
            "Fecha inicio",
            value=date(date.today().year, date.today().month, 1),
            key="ventas_fecha_inicio",
        )
    with col_f2:
        fecha_fin = st.date_input(
            "Fecha fin",
            value=date.today(),
            key="ventas_fecha_fin",
        )

    if fecha_inicio > fecha_fin:
        st.error("La fecha de inicio no puede ser mayor a la fecha fin.")
        return
    
    df_ventas = get_ventas(str(fecha_inicio), str(fecha_fin))
    if len(df_ventas) == 0:
        st.info("No hay ventas para el período seleccionado.")
        return

    total_ventas = df_ventas["total"].sum()
    ventas_re = df_ventas[df_ventas["tipo_re_se"] == "RE"]
    ventas_se = df_ventas[df_ventas["tipo_re_se"] == "SE"]

    total_re = ventas_re["total"].sum() if len(ventas_re) else 0.0
    total_se = ventas_se["total"].sum() if len(ventas_se) else 0.0

    col_top1, col_top2, col_top3 = st.columns(3)
    col_top1.metric("Ventas totales", format_currency(total_ventas))
    col_top2.metric("Ventas repuestos (RE)", format_currency(total_re))
    col_top3.metric("Ventas servicios (SE)", format_currency(total_se))

    st.divider()
    st.subheader("Ventas totales por sucursal")
    ventas_sucursal = df_ventas.groupby("sucursal")["total"].sum().reset_index(name="Monto USD")
    fig_total_suc = px.bar(
        ventas_sucursal,
        x="sucursal",
        y="Monto USD",
        title="Ventas totales por sucursal",
        labels={"sucursal": "Sucursal", "Monto USD": "USD"},
        color="Monto USD",
        color_continuous_scale="Purples",
    )
    st.plotly_chart(fig_total_suc, use_container_width=True)

    st.subheader("Ventas RE vs SE por sucursal")
    ventas_sucursal_tipo = df_ventas.groupby(["sucursal", "tipo_re_se"])["total"].sum().reset_index()
    fig_re_se = px.bar(
        ventas_sucursal_tipo,
        x="sucursal",
        y="total",
        color="tipo_re_se",
        barmode="group",
        labels={"sucursal": "Sucursal", "total": "USD", "tipo_re_se": "Tipo"},
        title="Ventas RE y SE por sucursal",
    )
    st.plotly_chart(fig_re_se, use_container_width=True)

    st.divider()
    st.subheader("Repuestos por sucursal (Mostrador vs Servicios)")

    mostrador = ventas_re.copy()
    if "repuestos" in mostrador.columns and mostrador["repuestos"].notna().any():
        mostrador["repuestos_mostrador"] = mostrador["repuestos"].fillna(mostrador["total"])
    else:
        mostrador["repuestos_mostrador"] = mostrador["total"]

    repuestos_mostrador = mostrador.groupby("sucursal")["repuestos_mostrador"].sum().reset_index(name="Mostrador")
    repuestos_servicio = (
        ventas_se.groupby("sucursal")["repuestos"].sum().reset_index(name="Servicios")
        if "repuestos" in ventas_se.columns
        else pd.DataFrame({"sucursal": [], "Servicios": []})
    )
    repuestos_servicio["Servicios"] = repuestos_servicio["Servicios"].fillna(0)

    df_repuestos = pd.merge(repuestos_mostrador, repuestos_servicio, on="sucursal", how="outer").fillna(0)
    df_repuestos["Total"] = df_repuestos["Mostrador"] + df_repuestos["Servicios"]

    fig_repuestos = px.bar(
        df_repuestos.melt(id_vars="sucursal", value_vars=["Mostrador", "Servicios"]),
        x="sucursal",
        y="value",
        color="variable",
        barmode="group",
        title="Repuestos por sucursal (Mostrador vs Servicios)",
        labels={"sucursal": "Sucursal", "value": "USD", "variable": "Origen"},
    )
    st.plotly_chart(fig_repuestos, use_container_width=True)

    st.divider()
    st.subheader("Tickets promedio por sucursal")

    def ticket_promedio(dataframe: pd.DataFrame) -> pd.DataFrame:
        if len(dataframe) == 0:
            return pd.DataFrame(columns=["sucursal", "Ticket"])
        df = dataframe.groupby("sucursal").agg({"total": ["sum", "count"]})
        df.columns = ["total_sum", "total_count"]
        df.reset_index(inplace=True)
        df["Ticket"] = df["total_sum"] / df["total_count"]
        return df[["sucursal", "Ticket"]]

    tickets_se = ticket_promedio(ventas_se)
    tickets_re = ticket_promedio(ventas_re)

    if len(tickets_se) or len(tickets_re):
        col_ticket1, col_ticket2 = st.columns(2)
        with col_ticket1:
            st.write("**Ticket promedio SE**")
            if len(ventas_se):
                total_se_tickets = (ventas_se["total"].sum() / len(ventas_se)) if len(ventas_se) else 0
                st.metric("Ticket promedio SE (total)", format_currency(total_se_tickets))
            if len(tickets_se):
                st.dataframe(
                    tickets_se.assign(Ticket=tickets_se["Ticket"].apply(format_currency)),
                    use_container_width=True,
                )
            else:
                st.caption("Sin ventas SE en el período.")

        with col_ticket2:
            st.write("**Ticket promedio RE**")
            if len(ventas_re):
                total_re_tickets = (ventas_re["total"].sum() / len(ventas_re)) if len(ventas_re) else 0
                st.metric("Ticket promedio RE (total)", format_currency(total_re_tickets))
            if len(tickets_re):
                st.dataframe(
                    tickets_re.assign(Ticket=tickets_re["Ticket"].apply(format_currency)),
                    use_container_width=True,
                )
            else:
                st.caption("Sin ventas RE en el período.")
    else:
        st.caption("No hay datos suficientes para calcular tickets promedio.")

def build_operativo_pdf(
    periodo: str,
    resumen: dict,
    comparacion: list[dict],
    pe_table: list[dict],
    detalles: dict | None = None,
) -> bytes:
    detalles = detalles or {}
    empresa = detalles.get("empresa", "Patagonia Maquinarias")
    moneda = detalles.get("moneda", "USD")

    JD_COLORS = {
        "negro": (33, 33, 33),
        "gris": (120, 120, 120),
        "amarillo": (255, 205, 0),
        "blanco": (255, 255, 255),
    }

    def ensure_space(pdf_obj, needed=40):
        if pdf_obj.get_y() + needed > 270:
            pdf_obj.add_page()

    def draw_costs_bar(pdf_obj, datos_resumen):
        ensure_space(pdf_obj, 35)
        costos = max(datos_resumen.get("variable_costos", 0.0), 0.0)
        estructura = max(datos_resumen.get("gastos_fijos", 0.0), 0.0)
        resultado = max(datos_resumen.get("resultado", 0.0), 0.0)
        total = costos + estructura + resultado
        if total <= 0:
            return
        bar_width = 180
        bar_height = 6
        start_x = 15
        y = pdf_obj.get_y()
        pdf_obj.ln(2)
        pdf_obj.set_font("Arial", "B", 11)
        pdf_obj.cell(0, 6, "Distribución sobre ventas netas", ln=1)
        pdf_obj.set_y(pdf_obj.get_y())
        pdf_obj.set_x(start_x)
        segments = [
            ("Costos directos", costos, JD_COLORS["negro"]),
            ("Gastos estructura", estructura, JD_COLORS["gris"]),
            ("Resultado", resultado, JD_COLORS["amarillo"]),
        ]
        curr_x = start_x
        pdf_obj.set_y(pdf_obj.get_y())
        pdf_obj.set_x(start_x)
        for _, value, color in segments:
            if value <= 0:
                continue
            width = bar_width * (value / total)
            pdf_obj.set_fill_color(*color)
            pdf_obj.rect(curr_x, pdf_obj.get_y(), width, bar_height, "F")
            curr_x += width
        pdf_obj.ln(bar_height + 2)
        pdf_obj.set_font("Arial", "", 10)
        for label, value, color in segments:
            if value <= 0:
                continue
            pdf_obj.set_fill_color(*color)
            pdf_obj.rect(start_x, pdf_obj.get_y(), 5, 5, "F")
            pdf_obj.set_x(start_x + 7)
            pdf_obj.cell(
                0,
                5,
                f"{label}: {format_currency(value)} ({value / total:.1%})",
                ln=1,
            )
        pdf_obj.ln(2)

    def draw_branch_result_table(pdf_obj, data):
        if not data:
            return
        ensure_space(pdf_obj, 40)
        pdf_obj.set_font("Arial", "B", 13)
        pdf_obj.cell(0, 8, "Resultado operativo por sucursal", ln=1)
        pdf_obj.set_font("Arial", "B", 11)
        pdf_obj.set_fill_color(*JD_COLORS["gris"])
        pdf_obj.set_text_color(*JD_COLORS["blanco"])
        pdf_obj.cell(50, 7, "Sucursal", border=1, align="L", fill=True)
        pdf_obj.cell(45, 7, "Ingresos", border=1, align="R", fill=True)
        pdf_obj.cell(45, 7, "Gastos", border=1, align="R", fill=True)
        pdf_obj.cell(0, 7, "Resultado", border=1, align="R", fill=True)
        pdf_obj.ln()
        pdf_obj.set_font("Arial", "", 10)
        pdf_obj.set_text_color(0, 0, 0)
        for row in data:
            pdf_obj.cell(50, 6, row["Sucursal"], border=1)
            pdf_obj.cell(45, 6, format_currency(row["Ingresos"]), border=1, align="R")
            pdf_obj.cell(45, 6, format_currency(row["Gastos"]), border=1, align="R")
            pdf_obj.cell(0, 6, format_currency(row["Resultado"]), border=1, align="R", ln=1)
        pdf_obj.ln(2)

    def draw_summary_grid(pdf_obj, items, cols=2):
        if not items:
            return
        ensure_space(pdf_obj, 40)
        pdf_obj.set_font("Arial", "B", 13)
        pdf_obj.cell(0, 8, "5. Resumen ejecutivo", ln=1)
        pdf_obj.set_font("Arial", "", 11)
        col_width = 190 / cols
        for idx, (title, value) in enumerate(items):
            if idx % cols == 0:
                pdf_obj.ln(2)
            x = pdf_obj.get_x()
            y = pdf_obj.get_y()
            pdf_obj.set_fill_color(240, 240, 240)
            pdf_obj.set_font("Arial", "B", 10)
            pdf_obj.cell(col_width - 5, 6, title, border=1, ln=2, fill=True)
            pdf_obj.set_font("Arial", "", 10)
            pdf_obj.cell(col_width - 5, 6, value, border=1, ln=0)
            if idx % cols == cols - 1:
                pdf_obj.ln(6)
            else:
                pdf_obj.set_xy(x + col_width, y)

    def draw_chart_image(pdf_obj, title, image_path):
        if not image_path:
            return
        ensure_space(pdf_obj, 85)
        pdf_obj.set_font("Arial", "B", 13)
        pdf_obj.cell(0, 8, title, ln=1)
        pdf_obj.image(image_path, x=11, w=192, h=58)
        pdf_obj.ln(5)

    def draw_ai_section(pdf_obj, ai_data):
        if not ai_data:
            return
        ensure_space(pdf_obj, 65)
        pdf_obj.set_font("Arial", "B", 13)
        pdf_obj.cell(0, 8, "6. Análisis de resultados", ln=1)
        pdf_obj.set_font("Arial", "", 11)

        pred = ai_data.get("prediccion") or {}
        if pred.get("prediccion"):
            pdf_obj.set_fill_color(*JD_COLORS["gris"])
            pdf_obj.set_text_color(*JD_COLORS["blanco"])
            label = "Pronóstico"
            dias_habiles = pred.get("dias_habiles")
            horizonte = pred.get("horizonte_dias")
            if dias_habiles:
                label = f"Pronóstico {dias_habiles} días hábiles"
            pdf_obj.cell(0, 8, f"{label}: {format_currency(pred['prediccion'])}", ln=1, fill=True)
            pdf_obj.set_text_color(0, 0, 0)
            pdf_obj.set_font("Arial", "", 10)
            mensaje_pred = sanitize_latin1(pred.get("mensaje"))
            confianza = sanitize_latin1(pred.get("confianza"))
            promedio_diario = pred.get("promedio_diario")
            promedio_habil = pred.get("promedio_diario_habil")
            if confianza or mensaje_pred:
                detalle_pred = " | ".join(
                    [part for part in [f"Confianza: {confianza}" if confianza else None, mensaje_pred] if part]
                )
                pdf_obj.multi_cell(0, 5, detalle_pred)
            if horizonte and dias_habiles:
                pdf_obj.multi_cell(
                    0,
                    5,
                    f"Horizonte considerado: {dias_habiles} días hábiles ({horizonte} días calendario).",
                )
            if promedio_diario:
                pdf_obj.multi_cell(
                    0,
                    5,
                    f"Promedio diario proyectado: {format_currency(promedio_diario)}",
                )
            if promedio_habil:
                pdf_obj.multi_cell(
                    0,
                    5,
                    f"Promedio por día hábil: {format_currency(promedio_habil)}",
                )
            pdf_obj.ln(2)

        def bullet_block(title, items, max_items=4):
            pdf_obj.set_font("Arial", "B", 11)
            pdf_obj.cell(0, 6, sanitize_latin1(title), ln=1)
            pdf_obj.set_font("Arial", "", 10)
            if items:
                for item in items[:max_items]:
                    pdf_obj.multi_cell(0, 5, f"· {sanitize_latin1(item)}")
            else:
                pdf_obj.multi_cell(0, 5, "· Sin datos destacados.")
            pdf_obj.ln(1)

        bullet_block("Tendencias detectadas", ai_data.get("tendencias"))
        bullet_block("Alertas", ai_data.get("alertas"))
        bullet_block("Recomendaciones clave", ai_data.get("recomendaciones"))

        extra_recs = ai_data.get("recomendaciones_extra")
        if extra_recs:
            bullet_block("Recomendaciones adicionales", extra_recs)

        bullet_block("Recomendaciones por sucursal", ai_data.get("recomendaciones_sucursales"))
        bullet_block("Recomendaciones mix RE vs SE", ai_data.get("recomendaciones_mix"))
        bullet_block("Oportunidades destacadas", ai_data.get("oportunidades"))
        bullet_block("Riesgos identificados", ai_data.get("riesgos"))

        top_clients = ai_data.get("top_clientes") or []
        if top_clients:
            pdf_obj.set_font("Arial", "B", 11)
            pdf_obj.cell(0, 6, "Top 10 clientes del período", ln=1)
            pdf_obj.set_font("Arial", "", 10)
            for row in top_clients:
                pdf_obj.multi_cell(
                    0,
                    5,
                    f"· {row.get('cliente', '-')}"
                    f" ({row.get('sucursal', '-')})"
                    f": {format_currency(row.get('total', 0.0))}",
                )
            pdf_obj.ln(1)

        alertas_criticas = [
            f"{sanitize_latin1(alerta.get('titulo', 'Alerta'))}: {sanitize_latin1(alerta.get('descripcion', ''))}"
            for alerta in (ai_data.get("alertas_criticas") or [])
        ]
        if alertas_criticas:
            bullet_block("Alertas críticas", alertas_criticas, max_items=3)

        anomalias = [
            f"{sanitize_latin1(anomalia.get('tipo', 'Anomalía'))}: {sanitize_latin1(anomalia.get('descripcion', ''))}"
            for anomalia in (ai_data.get("anomalias") or [])
        ]
        bullet_block("Anomalías relevantes", anomalias, max_items=3)

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 16)
    pdf.cell(0, 10, f"Informe de Gestión Postventa - {empresa}", ln=1)

    pdf.set_font("Arial", "", 12)
    pdf.cell(0, 8, f"Período: {periodo}", ln=1)
    pdf.cell(0, 8, f"Moneda: {moneda}", ln=1)
    pdf.ln(4)

    chart_files = []
    historicos = detalles.get("historicos")
    if historicos:
        labels_hist = historicos.get("labels", [])
        ventas_hist = historicos.get("ventas", {})
        if ventas_hist and ventas_hist.get("series"):
            img = create_stacked_chart_image(labels_hist, ventas_hist.get("series", []), "USD", figsize=(6, 2))
            if img:
                chart_files.append(img)
                draw_chart_image(pdf, "Ventas históricas por sucursal (Nov-25 a Oct-26)", img)
        gastos_hist = historicos.get("gastos", {})
        if gastos_hist:
            series = [
                {"label": "Fijos", "values": gastos_hist.get("fixed", [])},
                {"label": "Variables", "values": gastos_hist.get("variable", [])},
            ]
            img = create_stacked_chart_image(labels_hist, series, "USD", figsize=(6, 2))
            if img:
                chart_files.append(img)
                draw_chart_image(pdf, "Gastos históricos (fijo vs variable)", img)
        resultados_hist = historicos.get("resultados", {})
        if resultados_hist and resultados_hist.get("series"):
            img = create_line_chart_image(labels_hist, resultados_hist.get("series", []), "USD", figsize=(6, 2))
            if img:
                chart_files.append(img)
                draw_chart_image(pdf, "Resultado mensual por sucursal", img)

    estado_resultados = detalles.get("estado_resultados", [])
    if estado_resultados:
        ensure_space(pdf, 50)
        pdf.set_font("Arial", "B", 13)
        pdf.cell(0, 8, "1. Estado de Resultados Operativo", ln=1)
        pdf.set_font("Arial", "B", 11)
        pdf.set_fill_color(*JD_COLORS["gris"])
        pdf.set_text_color(*JD_COLORS["blanco"])
        pdf.cell(100, 7, "Concepto", border=1, align="L", fill=True)
        pdf.cell(40, 7, "Monto", border=1, align="R", fill=True)
        pdf.cell(0, 7, "% s/Ventas", border=1, align="R", fill=True)
        pdf.ln()
        pdf.set_font("Arial", "", 11)
        pdf.set_text_color(0, 0, 0)
        for row in estado_resultados:
            pdf.cell(100, 6, row["concepto"], border="L")
            pdf.cell(40, 6, format_currency(row["monto"]), align="R", border=0)
            pdf.cell(0, 6, row["porcentaje"], align="R", border="R", ln=1)
        pdf.ln(2)
        ingresos_detalle = detalles.get("ingresos_detalle", {})
        if ingresos_detalle:
            pdf.set_font("Arial", "BU", 11)
            pdf.cell(0, 6, "Desglose", ln=1)
            pdf.set_font("Arial", "", 10)
            desglose_items = [
                ("Repuestos por mostrador", ingresos_detalle.get("repuestos_mostrador", 0.0)),
                ("Repuestos por servicios", ingresos_detalle.get("repuestos_servicios", 0.0)),
                ("Mano de obra servicios", ingresos_detalle.get("mano_obra", 0.0)),
                ("Gastos asistencia", ingresos_detalle.get("asistencia", 0.0)),
                ("Trabajos terceros", ingresos_detalle.get("terceros", 0.0)),
            ]
            for label, value in desglose_items:
                pdf.set_font("Arial", "B", 10)
                pdf.cell(95, 6, f"- {label}:", ln=0)
                pdf.set_font("Arial", "", 10)
                pdf.cell(0, 6, format_currency(value), ln=1)
            pdf.ln(2)
        draw_costs_bar(pdf, resumen)

    if comparacion:
        ensure_space(pdf, 60)
        pdf.set_font("Arial", "B", 13)
        pdf.cell(0, 8, "Ingresos vs Gastos por sucursal", ln=1)
        pdf.set_font("Arial", "", 11)
        max_ing = max(row["Ingresos"] for row in comparacion)
        if max_ing > 0:
            bar_width = 110
            bar_height = 6
            start_x = 20
            spacing = 6
            for row in comparacion:
                pdf.set_font("Arial", "B", 11)
                pdf.cell(0, 6, row["Sucursal"], ln=1)
                ingreso_width = bar_width * (row["Ingresos"] / max_ing)
                gasto_width = bar_width * (row["Gastos"] / max_ing)
                y_start = pdf.get_y()
                pdf.set_fill_color(*JD_COLORS["negro"])
                pdf.rect(start_x, y_start, ingreso_width, bar_height, "F")
                pdf.set_fill_color(*JD_COLORS["gris"])
                pdf.rect(start_x, y_start + bar_height + 2, gasto_width, bar_height, "F")
                pdf.set_font("Arial", "", 9)
                pdf.set_text_color(*JD_COLORS["negro"])
                pdf.set_xy(start_x + ingreso_width + 3, y_start)
                pdf.cell(
                    0,
                    bar_height,
                    f"{format_currency(row['Ingresos'])}",
                    ln=0,
                )
                pdf.set_xy(start_x + gasto_width + 3, y_start + bar_height + 2)
                pdf.cell(
                    0,
                    bar_height,
                    f"{format_currency(row['Gastos'])}",
                    ln=0,
                )
                pdf.set_y(y_start + bar_height * 2 + spacing)
            pdf.set_text_color(0, 0, 0)

    margenes = detalles.get("margenes_negocio", {})
    if margenes:
        ensure_space(pdf, 45)
        pdf.set_font("Arial", "B", 13)
        pdf.cell(0, 8, "2. Análisis de Márgenes por Unidad de Negocio", ln=1)
        pdf.set_font("Arial", "", 11)
        rep = margenes.get("repuestos")
        if rep:
            pdf.multi_cell(
                0,
                6,
                f"A. Repuestos (Mostrador + Taller): Venta {format_currency(rep['ventas'])} | "
                f"CMV {format_currency(rep['costo'])} | Margen {format_currency(rep['margen'])} "
                f"({rep['margen_pct']})",
            )
            pdf.multi_cell(
                0,
                6,
                rep.get(
                    "comentario",
                    "El mix de repuestos en servicios mejora la recurrencia y fideliza a los clientes.",
                ),
            )
        serv = margenes.get("servicios")
        if serv:
            pdf.multi_cell(
                0,
                6,
                f"B. Servicios: Mano de obra + asistencia {format_currency(serv['ventas'])} | "
                f"Costo técnicos {format_currency(serv['costo'])} | Margen {format_currency(serv['margen'])} "
                f"({serv['margen_pct']})",
            )
            if serv.get("ingresos_por_hora"):
                pdf.multi_cell(
                    0,
                    6,
                    f"Cada hora vendida aporta {format_currency(serv['ingresos_por_hora'])} entre mano de obra y repuestos.",
                )
        pdf.ln(2)

    eficiencia = detalles.get("eficiencia", {})
    if eficiencia:
        ensure_space(pdf, 50)
        pdf.set_font("Arial", "B", 13)
        pdf.cell(0, 8, "3. Eficiencia Operativa", ln=1)
        pdf.set_font("Arial", "", 11)
        tecnicos = eficiencia.get("tecnicos_activos")
        if tecnicos:
            pdf.multi_cell(0, 6, f"Técnicos activos: {tecnicos}")
        if eficiencia.get("horas_totales") is not None:
            pdf.multi_cell(
                0,
                6,
                f"Horas facturadas: {eficiencia['horas_totales']:.1f} h "
                f"({eficiencia['horas_promedio']:.1f} h/promedio por servicio)",
            )
        if eficiencia.get("horas_totales") is None:
            pdf.multi_cell(0, 6, "Horas facturadas: Dato no disponible en los registros.")
        pdf.multi_cell(
            0,
            6,
            f"Órdenes de servicio: {eficiencia.get('ordenes_servicio', 0)} | "
            f"Órdenes de repuestos: {eficiencia.get('ordenes_repuestos', 0)}",
        )
        tickets = detalles.get("tickets", {})
        if tickets:
            if tickets.get("se_total"):
                pdf.multi_cell(
                    0,
                    6,
                    f"Ticket promedio servicios: {format_currency(tickets['se_total'])}",
                )
            if tickets.get("re_total"):
                pdf.multi_cell(
                    0,
                    6,
                    f"Ticket promedio repuestos mostrador: {format_currency(tickets['re_total'])}",
                )
        if eficiencia.get("ingreso_por_hora"):
            pdf.multi_cell(
                0,
                6,
                f"Ingreso total asociado por hora: {format_currency(eficiencia['ingreso_por_hora'])}/h "
                f"(Repuestos {format_currency(eficiencia.get('repuestos_por_hora', 0))}/h).",
            )
        if eficiencia.get("horas_disponibles"):
            pdf.multi_cell(
                0,
                6,
                f"Horas disponibles: {eficiencia['horas_disponibles']:,.1f} h | "
                f"Técnicos configurados: {eficiencia.get('tecnicos_config', 'N/D')} | "
                f"Días hábiles: {eficiencia.get('dias_habiles', 'N/D')}",
            )
        if eficiencia.get("horas_vendidas_estimadas"):
            pdf.multi_cell(
                0,
                6,
                f"Horas vendidas estimadas: {eficiencia['horas_vendidas_estimadas']:,.1f} h "
                f"(Tarifa: {format_currency(eficiencia.get('tarifa_hora', 0))}/h).",
            )
        if eficiencia.get("productividad") is not None:
            pdf.multi_cell(
                0,
                6,
                f"Productividad del taller: {format_percentage(eficiencia['productividad'] * 100)}.",
            )
        pdf.ln(2)

    productividad_pdf = detalles.get("productividad")
    if productividad_pdf:
        ensure_space(pdf, 35)
        pdf.set_font("Arial", "B", 13)
        pdf.cell(0, 8, "3.b Productividad del taller", ln=1)
        pdf.set_font("Arial", "", 11)
        pdf.multi_cell(
            0,
            6,
            f"Ingresos MO + asistencia: {format_currency(productividad_pdf.get('ingresos_mo_asistencia', 0))}.",
        )
        pdf.multi_cell(
            0,
            6,
            f"Horas vendidas estimadas: {productividad_pdf.get('horas_vendidas', 0):,.1f} h "
            f"| Horas hábiles disponibles: {productividad_pdf.get('horas_disponibles', 0):,.1f} h "
            f"({productividad_pdf.get('dias_habiles', 0)} días, {productividad_pdf.get('tecnicos', 0)} técnicos).",
        )
        pdf.multi_cell(
            0,
            6,
            f"Tarifa promedio: {format_currency(productividad_pdf.get('tarifa', 0))}/h "
            f"| Ocupación: {format_percentage(productividad_pdf.get('ocupacion_pct', 0) * 100)}.",
        )
        pdf.ln(2)

    ventas_suc = detalles.get("ventas_sucursales", [])
    if ventas_suc:
        ensure_space(pdf, 45)
        pdf.set_font("Arial", "B", 13)
        pdf.cell(0, 8, "4. Ventas por sucursal", ln=1)
        pdf.set_font("Arial", "", 11)
        for suc in ventas_suc:
            pdf.multi_cell(
                0,
                6,
                f"· {suc['Sucursal']}: {format_currency(suc['Venta'])} ({suc['Porcentaje']})",
            )
        pdf.ln(2)
        if comparacion:
            draw_branch_result_table(pdf, comparacion)
            pdf.ln(2)

    draw_summary_grid(pdf, detalles.get("resumen_cards", []))

    draw_ai_section(pdf, detalles.get("ai_insights"))

    pdf_bytes = pdf.output(dest="S").encode("latin1")
    for path in chart_files:
        try:
            os.remove(path)
        except OSError:
            pass
    return pdf_bytes


def get_month_to_date_overview(reference_date: date | None = None) -> dict:
    today = reference_date or date.today()
    start_of_month = date(today.year, today.month, 1)
    start_str = start_of_month.isoformat()
    end_str = today.isoformat()

    df_ventas = get_ventas(start_str, end_str)
    total_bruto = 0.0
    if len(df_ventas):
        total_bruto = pd.to_numeric(df_ventas.get("total"), errors="coerce").fillna(0).sum()
    iibb = float(total_bruto) * 0.045
    total_neto = float(total_bruto) - iibb

    gastos_totales = obtener_gastos_totales_con_automaticos(start_str, end_str)
    gastos_val = gastos_totales.get("gastos_postventa_total", 0.0)
    # Manejar scalars, Series o listas con coerción robusta
    try:
        if np.isscalar(gastos_val):
            gastos_postventa = float(pd.to_numeric(gastos_val, errors="coerce") or 0.0)
        else:
            gastos_postventa = float(pd.to_numeric(gastos_val, errors="coerce").fillna(0).sum())
    except Exception:
        try:
            gastos_postventa = float(pd.to_numeric(gastos_val, errors="coerce").fillna(0).sum())
        except Exception:
            gastos_postventa = 0.0

    resultado = float(total_neto) - float(gastos_postventa)

    return {
        "bruto": total_bruto,
        "iibb": iibb,
        "neto": total_neto,
        "gastos": gastos_postventa,
        "resultado": resultado,
        "fecha_inicio": start_of_month,
        "fecha_fin": today,
    }


def render_dashboard():
    st.title("📊 Dashboard")
    month_summary = get_month_to_date_overview()

    st.caption(
        f"Período en curso: {month_summary['fecha_inicio'].strftime('%d/%m/%Y')} - "
        f"{month_summary['fecha_fin'].strftime('%d/%m/%Y')}"
    )

    kpi_cols = st.columns(3)
    kpi_cols[0].metric(
        "Ventas netas (después 4.5% IIBB)",
        format_currency(month_summary["neto"]),
        delta=f"Descuento IIBB: -{format_currency(month_summary['iibb'])}",
    )
    kpi_cols[1].metric(
        "Gastos totales",
        format_currency(month_summary["gastos"]),
    )
    kpi_cols[2].metric(
        "Resultado del mes",
        format_currency(month_summary["resultado"]),
        delta="Positivo" if month_summary["resultado"] >= 0 else "Negativo",
    )
    st.divider()
    st.subheader("🎯 Objetivo de Ventas de Repuestos - FY26")

    OBJETIVO_REPUESTOS_FY26 = 1_300_000.0
    fecha_inicio_fy26 = date(2025, 11, 1)
    fecha_fin_actual = date.today()

    df_ventas_fy26 = get_ventas(str(fecha_inicio_fy26), str(fecha_fin_actual))

    if len(df_ventas_fy26) > 0:
        ventas_re = df_ventas_fy26[df_ventas_fy26["tipo_re_se"] == "RE"]
        ventas_se = df_ventas_fy26[df_ventas_fy26["tipo_re_se"] == "SE"]

        total_repuestos = 0.0
        if len(ventas_re):
            if "repuestos" in ventas_re.columns and ventas_re["repuestos"].notna().any():
                total_repuestos += ventas_re["repuestos"].fillna(ventas_re["total"]).sum()
            else:
                total_repuestos += ventas_re["total"].sum()
        if len(ventas_se) and "repuestos" in ventas_se.columns:
            total_repuestos += ventas_se["repuestos"].fillna(0).sum()

        porcentaje = (total_repuestos / OBJETIVO_REPUESTOS_FY26 * 100) if OBJETIVO_REPUESTOS_FY26 else 0
        restante = OBJETIVO_REPUESTOS_FY26 - total_repuestos

        col_obj1, col_obj2, col_obj3 = st.columns(3)
        col_obj1.metric("💰 Vendido hasta ahora", format_currency(total_repuestos))
        col_obj2.metric(
            "🎯 Objetivo FY26",
            format_currency(OBJETIVO_REPUESTOS_FY26),
            delta=f"{porcentaje:.1f}% completado",
        )
        col_obj3.metric(
            "📊 Restante",
            format_currency(abs(restante)),
            delta="Falta" if restante > 0 else "Superado",
        )

        st.progress(min(max(porcentaje / 100, 0), 1))

        col_info1, col_info2 = st.columns(2)
        col_info1.caption(
            f"📅 Período: {fecha_inicio_fy26.strftime('%d/%m/%Y')} - {fecha_fin_actual.strftime('%d/%m/%Y')}"
        )
        col_info2.caption(f"⏱️ Tiempo transcurrido: {((fecha_fin_actual - fecha_inicio_fy26).days / 365) * 100:.1f}% del año fiscal")
    else:
        st.info("📊 Todavía no hay ventas registradas para el objetivo FY26.")

    st.divider()
    st.subheader("🛠️ Objetivo de Servicios - FY26")

    OBJETIVO_SERVICIOS_FY26 = 660_000.0
    df_servicios_fy26 = df_ventas_fy26[df_ventas_fy26["tipo_re_se"] == "SE"] if len(df_ventas_fy26) else pd.DataFrame()

    if len(df_servicios_fy26) > 0 and OBJETIVO_SERVICIOS_FY26 > 0:
        total_mano_obra = df_servicios_fy26["mano_obra"].fillna(0).sum() if "mano_obra" in df_servicios_fy26.columns else 0
        total_asistencia = df_servicios_fy26["asistencia"].fillna(0).sum() if "asistencia" in df_servicios_fy26.columns else 0
        total_terceros = df_servicios_fy26["terceros"].fillna(0).sum() if "terceros" in df_servicios_fy26.columns else 0
        total_servicios = total_mano_obra + total_asistencia + total_terceros

        porcentaje_servicios = (total_servicios / OBJETIVO_SERVICIOS_FY26) * 100
        restante_servicios = OBJETIVO_SERVICIOS_FY26 - total_servicios

        col_se1, col_se2, col_se3 = st.columns(3)
        col_se1.metric("🛠️ Ingresos Servicios (MO+Asist+Terc)", format_currency(total_servicios))
        col_se2.metric("🎯 Objetivo Servicios FY26", format_currency(OBJETIVO_SERVICIOS_FY26),
                       delta=f"{porcentaje_servicios:.1f}% completado")
        col_se3.metric("📊 Restante Servicios", format_currency(abs(restante_servicios)),
                       delta="Falta" if restante_servicios > 0 else "Superado")

        st.progress(min(max(porcentaje_servicios / 100, 0), 1))

        with st.expander("Detalle por componente", expanded=False):
            st.write(f"🔧 Mano de Obra: {format_currency(total_mano_obra)}")
            st.write(f"🧰 Asistencia: {format_currency(total_asistencia)}")
            st.write(f"🤝 Terceros: {format_currency(total_terceros)}")

        col_se_info1, col_se_info2 = st.columns(2)
        col_se_info1.caption(
            f"📅 Período: {fecha_inicio_fy26.strftime('%d/%m/%Y')} - {fecha_fin_actual.strftime('%d/%m/%Y')}"
        )
        col_se_info2.caption(
            f"⏱️ Tiempo transcurrido: {((fecha_fin_actual - fecha_inicio_fy26).days / 365) * 100:.1f}% del año fiscal"
        )
    else:
        st.info("📊 Aún no hay ingresos de servicios cargados para evaluar este objetivo.")
    st.info(
        "Esta es una versión base. Usa esta sección para diseñar los KPIs que realmente necesites."
    )

def render_sales_page():
    st.title("💰 Ventas")
    st.subheader("Registrar nueva venta")

    sucursales_default = ["COMODORO", "RIO GRANDE", "RIO GALLEGOS", "COMPARTIDOS"]
    tipos_trabajo = ["EXTERNO", "INTERNO"]

    with st.form("form_crear_venta"):
        col_a, col_b = st.columns(2)
        with col_a:
            fecha = st.date_input("Fecha", value=date.today())
            sucursal = st.selectbox("Sucursal", sucursales_default)
            cliente = st.text_input("Cliente / Cuenta")
            tipo_comprobante = st.selectbox(
                "Tipo Comprobante",
                ["FACTURA VENTA", "NOTA CREDITO", "NOTA DE CREDITO JD", "OTRO"],
            )
            trabajo = st.selectbox("Trabajo", tipos_trabajo)
        with col_b:
            pin = st.text_input("PIN / Identificador", value="")
            n_comprobante = st.text_input("N° de comprobante")
            tipo_re_se = st.selectbox("Tipo (RE o SE)", ["RE", "SE"])
            detalles = st.text_area("Detalles", height=90)

        st.markdown("**Componentes económicos (USD)**")
        col_m1, col_m2, col_m3 = st.columns(3)
        with col_m1:
            mano_obra = st.number_input("Mano de obra", min_value=0.0, value=0.0, step=0.01)
            asistencia = st.number_input("Asistencia", min_value=0.0, value=0.0, step=0.01)
        with col_m2:
            repuestos = st.number_input("Repuestos", min_value=0.0, value=0.0, step=0.01)
            terceros = st.number_input("Terceros", min_value=0.0, value=0.0, step=0.01)
        with col_m3:
            descuento = st.number_input("Descuento", min_value=0.0, value=0.0, step=0.01)

        total_calculado = mano_obra + asistencia + repuestos + terceros - descuento
        st.metric(
            "💰 Total calculado",
            format_currency(total_calculado),
            delta=(
                f"MO {format_currency(mano_obra)} + Asist {format_currency(asistencia)} + "
                f"Rep {format_currency(repuestos)} + Terc {format_currency(terceros)} - Desc {format_currency(descuento)}"
            ),
        )
        st.caption("El total se calcula automáticamente y se vuelve negativo si la nota de crédito lo requiere.")

        submit = st.form_submit_button("💾 Guardar venta")
        if submit:
            total = total_calculado
            es_nota_credito = (
                tipo_comprobante
                and "NOTA" in tipo_comprobante.upper()
                and "CREDITO" in tipo_comprobante.upper()
                and "JD" not in tipo_comprobante.upper()
            )
            if es_nota_credito and total > 0:
                total = -total

            if total == 0:
                st.error("El total calculado no puede ser 0.")
            else:
                venta_data = {
                    "mes": fecha.strftime("%B"),
                    "fecha": fecha,
                    "sucursal": sucursal,
                    "cliente": cliente or None,
                    "pin": pin or None,
                    "comprobante": tipo_comprobante,
                    "tipo_comprobante": tipo_comprobante,
                    "trabajo": trabajo,
                    "n_comprobante": n_comprobante or None,
                    "tipo_re_se": tipo_re_se,
                    "mano_obra": mano_obra,
                    "asistencia": asistencia,
                    "repuestos": repuestos,
                    "terceros": terceros,
                    "descuento": descuento,
                    "total": total,
                    "detalles": detalles or None,
                    "archivo_comprobante": None,
                }
                try:
                    insert_venta(venta_data)
                    st.success("✅ Venta registrada correctamente.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"❌ Error al guardar: {exc}")

    st.divider()
    st.subheader("Ventas registradas")
    df_ventas = get_ventas()
    if len(df_ventas) == 0:
        st.info("Aún no hay ventas cargadas.")
        return

    st.dataframe(
        df_ventas.sort_values("fecha", ascending=False).head(50),
        use_container_width=True,
    )

    st.subheader("Editar o eliminar")
    opciones = ["(ninguna)"] + [str(v) for v in df_ventas.sort_values("fecha", ascending=False)["id"].tolist()]
    selected = st.selectbox("Selecciona una venta", opciones)
    if selected == "(ninguna)":
        return

    registro = get_venta_by_id(int(selected))
    if not registro:
        st.warning("No se encontró el registro.")
        return

    fecha_reg = datetime.strptime(str(registro["fecha"]), "%Y-%m-%d").date()

    with st.form("form_editar_venta"):
        col_a, col_b = st.columns(2)
        with col_a:
            fecha_edit = st.date_input("Fecha", value=fecha_reg, key="venta_fecha_edit")
            sucursal_edit = st.selectbox(
                "Sucursal",
                sucursales_default,
                index=sucursales_default.index(registro["sucursal"]) if registro["sucursal"] in sucursales_default else 0,
                key="venta_sucursal_edit",
            )
            cliente_edit = st.text_input("Cliente", value=registro.get("cliente") or "", key="venta_cliente_edit")
            tipo_comprobante_edit = st.selectbox(
                "Tipo Comprobante",
                ["FACTURA VENTA", "NOTA CREDITO", "OTRO"],
                index=["FACTURA VENTA", "NOTA CREDITO", "OTRO"].index(registro["tipo_comprobante"])
                if registro["tipo_comprobante"] in ["FACTURA VENTA", "NOTA CREDITO", "OTRO"]
                else 0,
                key="venta_tipo_comp_edit",
            )
            trabajo_edit = st.selectbox(
                "Trabajo",
                tipos_trabajo,
                index=tipos_trabajo.index(registro.get("trabajo", "EXTERNO")) if registro.get("trabajo", "EXTERNO") in tipos_trabajo else 0,
                key="venta_trabajo_edit",
            )
        with col_b:
            pin_edit = st.text_input("PIN / Identificador", value=registro.get("pin") or "", key="venta_pin_edit")
            n_comp_edit = st.text_input("N° de comprobante", value=registro.get("n_comprobante") or "", key="venta_ncomp_edit")
            tipo_re_se_edit = st.selectbox(
                "Tipo (RE o SE)", ["RE", "SE"],
                index=0 if registro["tipo_re_se"] == "RE" else 1,
                key="venta_tipo_edit",
            )
            detalles_edit = st.text_area("Detalles", value=registro.get("detalles") or "", key="venta_detalles_edit")

        col_m1, col_m2, col_m3 = st.columns(3)
        with col_m1:
            mano_obra_edit = st.number_input(
                "Mano de obra",
                min_value=0.0,
                value=float(registro.get("mano_obra") or 0),
                step=0.01,
                key="venta_mano_edit",
            )
            asistencia_edit = st.number_input(
                "Asistencia",
                min_value=0.0,
                value=float(registro.get("asistencia") or 0),
                step=0.01,
                key="venta_asistencia_edit",
            )
        with col_m2:
            repuestos_edit = st.number_input(
                "Repuestos",
                min_value=0.0,
                value=float(registro.get("repuestos") or 0),
                step=0.01,
                key="venta_repuestos_edit",
            )
            terceros_edit = st.number_input(
                "Terceros",
                min_value=0.0,
                value=float(registro.get("terceros") or 0),
                step=0.01,
                key="venta_terceros_edit",
            )
        with col_m3:
            descuento_edit = st.number_input(
                "Descuento",
                min_value=0.0,
                value=float(registro.get("descuento") or 0),
                step=0.01,
                key="venta_descuento_edit",
            )
            total_edit = st.number_input(
                "Total facturado",
                min_value=0.0,
                value=float(registro.get("total") or 0),
                step=0.01,
                key="venta_total_edit",
            )

        col_btn1, col_btn2 = st.columns([3, 1])
        with col_btn1:
            actualizar = st.form_submit_button("💾 Actualizar venta")
        with col_btn2:
            eliminar = st.form_submit_button("🗑️ Eliminar", help="Eliminar definitivamente la venta seleccionada")

    if actualizar:
        venta_actualizada = {
            "mes": fecha_edit.strftime("%B"),
            "fecha": fecha_edit,
            "sucursal": sucursal_edit,
            "cliente": cliente_edit or None,
            "pin": pin_edit or None,
            "comprobante": tipo_comprobante_edit,
            "tipo_comprobante": tipo_comprobante_edit,
            "trabajo": trabajo_edit,
            "n_comprobante": n_comp_edit or None,
            "tipo_re_se": tipo_re_se_edit,
            "mano_obra": mano_obra_edit,
            "asistencia": asistencia_edit,
            "repuestos": repuestos_edit,
            "terceros": terceros_edit,
            "descuento": descuento_edit,
            "total": total_edit,
            "detalles": detalles_edit or None,
            "archivo_comprobante": registro.get("archivo_comprobante"),
        }
        try:
            update_venta(int(selected), venta_actualizada)
            st.success("✅ Venta actualizada.")
            st.rerun()
        except Exception as exc:
            st.error(f"❌ Error al actualizar: {exc}")

    if eliminar:
        try:
            delete_venta(int(selected))
            st.warning("Venta eliminada.")
            st.rerun()
        except Exception as exc:
            st.error(f"❌ Error al eliminar: {exc}")

def render_expenses_page():
    st.title("💸 Gastos")
    st.subheader("Registrar gasto")

    sucursales_default = ["COMODORO", "RIO GRANDE", "RIO GALLEGOS", "COMPARTIDOS"]
    areas_default = ["POSTVENTA", "SERVICIO", "REPUESTOS"]

    with st.form("form_crear_gasto"):
        col_a, col_b = st.columns(2)
        with col_a:
            fecha = st.date_input("Fecha", value=date.today(), key="gasto_fecha")
            sucursal = st.selectbox("Sucursal", sucursales_default, key="gasto_sucursal")
            area = st.selectbox("Área", areas_default, key="gasto_area")
            tipo = st.selectbox("Tipo", ["FIJO", "VARIABLE"], key="gasto_tipo")
            clasificacion = st.text_input("Clasificación", key="gasto_clasificacion")
        with col_b:
            proveedor = st.text_input("Proveedor (opcional)", key="gasto_proveedor")
            pct_postventa = st.slider("% Postventa", 0.0, 1.0, 1.0, 0.05, key="gasto_pct_postventa")
            pct_servicios = st.slider("% Servicios", 0.0, 1.0, 1.0, 0.05, key="gasto_pct_servicios")
            pct_repuestos = st.slider("% Repuestos", 0.0, 1.0, 0.0, 0.05, key="gasto_pct_repuestos")

        total_usd = st.number_input(
            "Total USD", min_value=-1_000_000.0, value=0.0, step=0.01, key="gasto_total_usd"
        )
        detalles = st.text_area("Detalles", key="gasto_detalles")

        submit = st.form_submit_button("💾 Guardar gasto")
        if submit:
            if not clasificacion:
                st.error("Completa la clasificación.")
            elif total_usd == 0:
                st.error("El total no puede ser 0.")
            else:
                total_pct = total_usd * pct_postventa
                gasto_data = {
                    "mes": fecha.strftime("%B"),
                    "fecha": fecha,
                    "sucursal": sucursal,
                    "area": area,
                    "pct_postventa": pct_postventa,
                    "pct_servicios": pct_servicios,
                    "pct_repuestos": pct_repuestos,
                    "tipo": tipo,
                    "clasificacion": clasificacion,
                    "proveedor": proveedor or None,
                    "total_pesos": None,
                    "total_usd": total_usd,
                    "total_pct": total_pct,
                    "total_pct_se": total_pct * pct_servicios,
                    "total_pct_re": total_pct * pct_repuestos,
                    "detalles": detalles or None,
                }
                try:
                    insert_gasto(gasto_data)
                    st.success("✅ Gasto registrado.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"❌ Error al guardar: {exc}")
            
            st.divider()
    st.subheader("Gastos registrados")
    df_gastos = get_gastos()
    if len(df_gastos) == 0:
        st.info("Aún no hay gastos cargados.")
        return

    st.dataframe(
        df_gastos.sort_values("fecha", ascending=False).head(50),
        use_container_width=True,
    )

    st.subheader("Editar o eliminar")
    opciones = ["(ninguno)"] + [str(g) for g in df_gastos.sort_values("fecha", ascending=False)["id"].tolist()]
    selected = st.selectbox("Selecciona un gasto", opciones)
    if selected == "(ninguno)":
        return

    registro = get_gasto_by_id(int(selected))
    if not registro:
        st.warning("No se encontró el gasto.")
        return

    fecha_reg = datetime.strptime(str(registro["fecha"]), "%Y-%m-%d").date()

    with st.form("form_editar_gasto"):
        col_a, col_b = st.columns(2)
        with col_a:
            fecha_edit = st.date_input("Fecha", value=fecha_reg, key="gasto_edit_fecha")
            sucursal_edit = st.selectbox(
                "Sucursal",
                sucursales_default,
                index=sucursales_default.index(registro["sucursal"])
                if registro["sucursal"] in sucursales_default
                else 0,
                key="gasto_edit_sucursal",
            )
            area_edit = st.selectbox(
                "Área",
                areas_default,
                index=areas_default.index(registro["area"])
                if registro["area"] in areas_default
                else 0,
                key="gasto_edit_area",
            )
            tipo_edit = st.selectbox(
                "Tipo",
                ["FIJO", "VARIABLE"],
                index=0 if registro.get("tipo") == "FIJO" else 1,
                key="gasto_edit_tipo",
            )
            clasificacion_edit = st.text_input(
                "Clasificación", value=registro.get("clasificacion") or "", key="gasto_edit_clasificacion"
            )
        with col_b:
            proveedor_edit = st.text_input(
                "Proveedor (opcional)", value=registro.get("proveedor") or "", key="gasto_edit_proveedor"
            )
            pct_postventa_edit = st.slider(
                "% Postventa",
                0.0,
                1.0,
                float(registro.get("pct_postventa") or 0),
                0.05,
                key="gasto_edit_pct_postventa",
            )
            pct_servicios_edit = st.slider(
                "% Servicios",
                0.0,
                1.0,
                float(registro.get("pct_servicios") or 0),
                0.05,
                key="gasto_edit_pct_servicios",
            )
            pct_repuestos_edit = st.slider(
                "% Repuestos",
                0.0,
                1.0,
                float(registro.get("pct_repuestos") or 0),
                0.05,
                key="gasto_edit_pct_repuestos",
            )

        total_usd_edit = st.number_input(
            "Total USD",
            value=float(registro.get("total_usd") or 0),
            step=0.01,
            key="gasto_edit_total_usd",
        )
        detalles_edit = st.text_area("Detalles", value=registro.get("detalles") or "", key="gasto_edit_detalles")

        col_btn1, col_btn2 = st.columns([3, 1])
        with col_btn1:
            actualizar = st.form_submit_button("💾 Actualizar gasto")
        with col_btn2:
            eliminar = st.form_submit_button("🗑️ Eliminar", help="Eliminar gasto")

    if actualizar:
        total_pct = total_usd_edit * pct_postventa_edit
        gasto_actualizado = {
            "mes": fecha_edit.strftime("%B"),
            "fecha": fecha_edit,
            "sucursal": sucursal_edit,
            "area": area_edit,
            "pct_postventa": pct_postventa_edit,
            "pct_servicios": pct_servicios_edit,
            "pct_repuestos": pct_repuestos_edit,
            "tipo": tipo_edit,
            "clasificacion": clasificacion_edit,
            "proveedor": proveedor_edit or None,
            "total_pesos": registro.get("total_pesos"),
            "total_usd": total_usd_edit,
            "total_pct": total_pct,
            "total_pct_se": total_pct * pct_servicios_edit,
            "total_pct_re": total_pct * pct_repuestos_edit,
            "detalles": detalles_edit or None,
        }
        try:
            update_gasto(int(selected), gasto_actualizado)
            st.success("✅ Gasto actualizado.")
            st.rerun()
        except Exception as exc:
            st.error(f"❌ Error al actualizar: {exc}")

    if eliminar:
        try:
            delete_gasto(int(selected))
            st.warning("Gasto eliminado.")
            st.rerun()
        except Exception as exc:
            st.error(f"❌ Error al eliminar: {exc}")

    st.title("📈 Reportes")
    tab_gastos, tab_ventas = st.tabs(["💸 Gastos", "💰 Ventas"])

    with tab_gastos:
        render_reports_gastos()

    with tab_ventas:
        render_reports_ventas()

def render_reports_page():
    st.title("📈 Reportes")
    tab_gastos, tab_ventas, tab_operativo = st.tabs(["💸 Gastos", "💰 Ventas", "⚖️ Análisis Operativo"])

    with tab_gastos:
        render_reports_gastos()

    with tab_ventas:
        render_reports_ventas()

    with tab_operativo:
        render_reports_operativo()


def render_settings_page():
    st.title("⚙️ Configuración")
    st.write("- Acá podés agregar toggles, credenciales o cualquier ajuste global.")
    st.write("- Es buen lugar para exponer backup / restore si lo necesitás en la nueva versión.")

if NAVIGATION[current_page] == "overview":
    render_dashboard()
elif NAVIGATION[current_page] == "sales":
    render_sales_page()
elif NAVIGATION[current_page] == "expenses":
    render_expenses_page()
elif NAVIGATION[current_page] == "reports":
    render_reports_page()
else:
    render_settings_page()


