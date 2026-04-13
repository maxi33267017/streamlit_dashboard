"""Postventa FY — esqueleto: login y navbar superior."""

import os

import streamlit as st

st.set_page_config(
    page_title="Postventa FY",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)


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
    """Oculta sidebar y deja margen bajo la barra fija de Streamlit (local y Cloud)."""
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] { display: none !important; }
        [data-testid="collapsedControl"] { display: none !important; }
        div[data-testid="stDecoration"] { display: none !important; }

        /*
         * En Streamlit 1.5x el scroll real está en stMainBlockContainer (no basta con stMain).
         * stHeader / stAppToolbar quedan fixed encima; reservamos alto similar a la toolbar.
         */
        [data-testid="stMainBlockContainer"] {
            padding-top: 7.5rem !important;
            max-width: 100% !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


if "is_authed" not in st.session_state:
    st.session_state.is_authed = False
if "vista" not in st.session_state:
    st.session_state.vista = "inicio"

_layout_css()

# Navbar: marca a la izquierda, acciones a la derecha.
brand_col, actions_col = st.columns([2, 3])
with brand_col:
    st.markdown("### Postventa FY")
with actions_col:
    if st.session_state.is_authed:
        a1, a2, a3 = st.columns(3)
        with a1:
            if st.button("Inicio", use_container_width=True, key="nav_inicio"):
                st.session_state.vista = "inicio"
                st.rerun()
        with a2:
            if st.button("Registro", use_container_width=True, key="nav_registro"):
                st.session_state.vista = "registro"
                st.rerun()
        with a3:
            if st.button("Cerrar sesión", use_container_width=True, key="nav_logout"):
                st.session_state.is_authed = False
                st.session_state.vista = "inicio"
                st.rerun()

st.divider()

if not st.session_state.is_authed:
    with st.form("auth_form", clear_on_submit=True):
        pwd = st.text_input("Clave de acceso", type="password")
        ok = st.form_submit_button("Ingresar")
    if ok:
        if pwd == _app_password():
            st.session_state.is_authed = True
            st.session_state.vista = "inicio"
            st.success("Acceso concedido.")
            st.rerun()
        else:
            st.error("Clave incorrecta.")
            st.stop()
    else:
        st.info("Ingresa la clave para continuar.")
        st.stop()
else:
    if st.session_state.vista == "registro":
        st.subheader("Registro")
    # Vista inicio: área principal en blanco.
