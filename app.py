"""GOPV — login, registro y navbar."""

import os

import pandas as pd
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
    "util_pct_servicios",
    "fact_servicios",
]


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
                    "util_pct_servicios": float(r.get("util_pct_servicios") or 0) * 100.0,
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
        "util_pct_servicios": float(s["util_pct_servicios"] or 0) / 100.0,
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


def _util_ponderado_servicios(df: pd.DataFrame) -> float | None:
    """Utilidad % servicios en fracción, ponderada por facturación de servicios."""
    num = 0.0
    den = 0.0
    for _, r in df.iterrows():
        fs = max(float(r.get("fact_servicios") or 0), 0.0)
        u = (float(r.get("util_pct_servicios") or 0) / 100.0) if pd.notna(r.get("util_pct_servicios")) else 0.0
        den += fs
        num += u * fs
    return (num / den) if den > 0 else None


def _fila_concesionario(df_edit: pd.DataFrame) -> dict:
    sfm = float(df_edit["fact_rep_mostrador"].sum())
    sft = float(df_edit["fact_rep_taller"].sum())
    sdm = float(df_edit["desc_mostrador"].sum())
    sdt = float(df_edit["desc_taller"].sum())
    sfs = float(df_edit["fact_servicios"].sum())
    um = _util_ponderado(df_edit, "mos")
    ut = _util_ponderado(df_edit, "tal")
    us = _util_ponderado_servicios(df_edit)
    if um is None:
        um = 0.0
    if ut is None:
        ut = 0.0
    if us is None:
        us = 0.0
    calc = database.compute_cierre_venta_linea(sfm, sft, sdm, sdt, um, ut, sfs)
    return {
        "sucursal": "CONCESIONARIO",
        "fact_rep_mostrador": sfm,
        "fact_rep_taller": sft,
        "desc_mostrador": sdm,
        "desc_taller": sdt,
        "util_pct_mostrador": um,
        "util_pct_taller": ut,
        "util_pct_servicios": us,
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
        column_config={
            "sucursal": st.column_config.TextColumn("Sucursal", disabled=True, width="small"),
            "fact_rep_mostrador": st.column_config.NumberColumn("Fact. rep. Mostrador (ARS)", format="%.2f"),
            "fact_rep_taller": st.column_config.NumberColumn("Fact. rep. Taller (ARS)", format="%.2f"),
            "desc_mostrador": st.column_config.NumberColumn("Desc. Mostrador (ARS)", format="%.2f"),
            "desc_taller": st.column_config.NumberColumn("Desc. Taller (ARS)", format="%.2f"),
            "util_pct_mostrador": st.column_config.NumberColumn("Util. venta % rep. Mostrador", format="%.2f", min_value=0.0, max_value=100.0),
            "util_pct_taller": st.column_config.NumberColumn("Util. venta % rep. Taller", format="%.2f", min_value=0.0, max_value=100.0),
            "util_pct_servicios": st.column_config.NumberColumn("Util. venta % servicios", format="%.2f", min_value=0.0, max_value=100.0),
            "fact_servicios": st.column_config.NumberColumn("Fact. servicios (ARS)", format="%.2f"),
        },
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
    us_c = _util_ponderado_servicios(edited)
    otros_ars = float(gastos_otros_usd) * tc_val
    gv = database.compute_gastos_variables_globales(
        fact_rep_mos_conc=float(edited["fact_rep_mostrador"].sum()),
        desc_rep_mos_conc=float(edited["desc_mostrador"].sum()),
        fact_rep_tal_conc=float(edited["fact_rep_taller"].sum()),
        desc_rep_tal_conc=float(edited["desc_taller"].sum()),
        fact_serv_conc=float(edited["fact_servicios"].sum()),
        util_mos_conc=um_c,
        util_tal_conc=ut_c,
        util_serv_conc=us_c,
        gastos_fijos_global=0.0,
        gastos_var_otros=otros_ars,
        gastos_var_otros_rubro=rubro_db,
    )

    gv_serv_usd = float(gv["gv_servicios_ajustado"]) / tc_val
    gv_rep_usd = float(gv["gv_repuestos_ajustado"]) / tc_val
    gv_mos_usd = float(gv["gv_rep_mostrador"]) / tc_val
    gv_tal_usd = float(gv["gv_rep_taller"]) / tc_val
    gastos_total_usd = float(gastos_fijos_usd) + gv_serv_usd + gv_rep_usd

    st.markdown("**Cálculo (Concesionario → USD con el TC)**")
    c2a, c2b, c2c, c2d, c2e = st.columns(5)
    c2a.metric("2. Var. servicios (USD)", f"US$ {gv_serv_usd:,.2f}")
    c2b.metric("3. Var. repuestos (USD)", f"US$ {gv_rep_usd:,.2f}")
    c2c.metric("… rep. mostrador (CMV)", f"US$ {gv_mos_usd:,.2f}")
    c2d.metric("… rep. taller (CMV)", f"US$ {gv_tal_usd:,.2f}")
    c2e.metric("5. Gasto total (USD)", f"US$ {gastos_total_usd:,.2f}")
    st.caption(
        "En ARS: CMV mostrador/taller y variables servicios; luego ÷ TC. "
        "Fijos y «otros» los cargás en USD (los «otros» se convierten a ARS solo para sumar al bucket correcto). "
        "Total USD = fijos + variables servicios + variables repuestos (incluye otros según rubro)."
    )

    ver_usd = st.checkbox("Vista previa en USD (usa el TC de arriba)", value=False)
    prev = _preview_tabla(edited)
    drop_show = [c for c in prev.columns if c in ("gastos_variables_tot", "gastos_total", "factor_absorcion")]
    prev_show = prev.drop(columns=drop_show, errors="ignore")
    st.markdown("**Vista previa — ventas por sucursal y Concesionario**")
    show = prev_show.copy()
    if ver_usd and tc > 0:
        money_cols = [
            "fact_rep_mostrador",
            "fact_rep_taller",
            "desc_mostrador",
            "desc_taller",
            "fact_servicios",
            "total_repuestos",
            "total_bruto",
            "margen_contrib",
            "resultado",
        ]
        for col in money_cols:
            if col in show.columns:
                show[col] = show[col].astype(float) / tc
    st.dataframe(show.round(4), use_container_width=True, hide_index=True)

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
        hist = database.list_cierres_ventas_mes(12)
        if len(hist):
            st.caption("Últimos cierres guardados")
            st.dataframe(hist, hide_index=True, use_container_width=True)


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
