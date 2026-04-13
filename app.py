"""Postventa FY — login, registro (ventas / gastos), navbar."""

import os

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

import database

st.set_page_config(
    page_title="Postventa FY",
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
    "gastos_fijos",
    "gastos_var_s",
    "gastos_var_r",
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


@st.cache_resource(show_spinner=False)
def _bootstrap_database():
    """Crea tablas (históricas + cierres de ventas en registro)."""
    database.init_database()
    return True


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
                    "gastos_fijos": float(r.get("gastos_fijos") or 0),
                    "gastos_var_s": float(r.get("gastos_var_s") or 0),
                    "gastos_var_r": float(r.get("gastos_var_r") or 0),
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
        "fact_servicios": float(s["fact_servicios"] or 0),
        "gastos_fijos": float(s["gastos_fijos"] or 0),
        "gastos_var_s": float(s["gastos_var_s"] or 0),
        "gastos_var_r": float(s["gastos_var_r"] or 0),
    }


def _util_ponderado(df: pd.DataFrame, canal: str) -> float | None:
    """Utilidad % en fracción, ponderada por (fact − desc) del canal."""
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


def _fila_concesionario(df_edit: pd.DataFrame) -> dict:
    sfm = float(df_edit["fact_rep_mostrador"].sum())
    sft = float(df_edit["fact_rep_taller"].sum())
    sdm = float(df_edit["desc_mostrador"].sum())
    sdt = float(df_edit["desc_taller"].sum())
    sfs = float(df_edit["fact_servicios"].sum())
    sgf = float(df_edit["gastos_fijos"].sum())
    sgvs = float(df_edit["gastos_var_s"].sum())
    sgvr = float(df_edit["gastos_var_r"].sum())
    um = _util_ponderado(df_edit, "mos")
    ut = _util_ponderado(df_edit, "tal")
    if um is None:
        um = 0.0
    if ut is None:
        ut = 0.0
    calc = database.compute_cierre_venta_linea(
        sfm, sft, sdm, sdt, um, ut, sfs, sgf, sgvs, sgvr
    )
    return {
        "sucursal": "CONCESIONARIO",
        "fact_rep_mostrador": sfm,
        "fact_rep_taller": sft,
        "desc_mostrador": sdm,
        "desc_taller": sdt,
        "util_pct_mostrador": um,
        "util_pct_taller": ut,
        "fact_servicios": sfs,
        "gastos_fijos": sgf,
        "gastos_var_s": sgvs,
        "gastos_var_r": sgvr,
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
            d["gastos_fijos"],
            d["gastos_var_s"],
            d["gastos_var_r"],
        )
        filas.append({**d, **calc})
    base = pd.DataFrame(filas)
    base = pd.concat([base, pd.DataFrame([_fila_concesionario(df_edit)])], ignore_index=True)
    return base


def _render_registro_ventas() -> None:
    st.subheader("Cierre de ventas (mensual)")
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
            help="Los importes se guardan en pesos; en la vista previa podés verlos en USD dividiendo por este valor.",
            key=f"cv_tc_{int(anio)}_{int(mes)}",
        )

    df_base = _df_editor_cierre_ventas(int(anio), int(mes))
    st.caption(
        "Editá solo sucursales reales. **Concesionario** se calcula abajo sumando insumos y aplicando la misma lógica de márgenes."
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
            "util_pct_mostrador": st.column_config.NumberColumn("Util. / venta % Mostrador", format="%.2f", min_value=0.0, max_value=100.0),
            "util_pct_taller": st.column_config.NumberColumn("Util. / venta % Taller", format="%.2f", min_value=0.0, max_value=100.0),
            "fact_servicios": st.column_config.NumberColumn("Fact. servicios (ARS)", format="%.2f"),
            "gastos_fijos": st.column_config.NumberColumn("Gastos fijos (ARS)", format="%.2f"),
            "gastos_var_s": st.column_config.NumberColumn("Gastos var. S (ARS)", format="%.2f"),
            "gastos_var_r": st.column_config.NumberColumn("Gastos var. R (ARS)", format="%.2f"),
        },
    )

    ver_usd = st.checkbox("Vista previa en USD (usa el TC de arriba)", value=False)
    prev = _preview_tabla(edited)
    st.markdown("**Vista previa — totales y Concesionario**")
    show = prev.copy()
    if ver_usd and tc > 0:
        money_cols = [
            "fact_rep_mostrador",
            "fact_rep_taller",
            "desc_mostrador",
            "desc_taller",
            "fact_servicios",
            "gastos_fijos",
            "gastos_var_s",
            "gastos_var_r",
            "total_repuestos",
            "total_bruto",
            "gastos_variables_tot",
            "gastos_total",
            "margen_contrib",
            "resultado",
        ]
        for col in money_cols:
            if col in show.columns:
                show[col] = show[col].astype(float) / tc
    st.dataframe(
        show.round(4),
        use_container_width=True,
        hide_index=True,
    )

    b1, b2 = st.columns(2)
    with b1:
        if st.button("Guardar mes", type="primary", use_container_width=True):
            try:
                cid = database.upsert_cierre_ventas_mes_header(
                    int(anio), int(mes), float(tc), notas=None
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
    tab_v, tab_g = st.tabs(["Ventas", "Gastos"])
    with tab_g:
        st.info("Registro de gastos: lo vemos en la siguiente etapa.")
    with tab_v:
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
    st.markdown("### Postventa FY")
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
