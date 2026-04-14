"""
Módulo de gestión de base de datos con soporte para SQLite y PostgreSQL.
Soporta backup automático y bases de datos persistentes para Streamlit Cloud.
"""
import os
import json
import re
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

try:
    import streamlit as st  # Disponible cuando la app corre en Streamlit
    from streamlit.errors import StreamlitSecretNotFoundError
except ModuleNotFoundError:
    st = None
    StreamlitSecretNotFoundError = None


def _load_postgres_url():
    """
    Construye la URL de conexión a PostgreSQL leyendo primero variables de entorno
    y luego st.secrets['postgres'] (si Streamlit está disponible).
    """
    url = os.environ.get("POSTGRES_URL") or os.environ.get("DATABASE_URL")
    if url:
        return url

    secrets_cfg = None
    if st is not None:
        try:
            if "postgres" in st.secrets:
                secrets_cfg = st.secrets["postgres"]
        except StreamlitSecretNotFoundError:
            # Sin archivo de secrets disponible: usar SQLite
            return None
        except Exception:
            secrets_cfg = None

    if secrets_cfg:
        if isinstance(secrets_cfg, str):
            return secrets_cfg
        if "url" in secrets_cfg:
            return secrets_cfg["url"]
        host = secrets_cfg.get("host")
        database = secrets_cfg.get("database")
        user = secrets_cfg.get("user")
        password = secrets_cfg.get("password")
        port = secrets_cfg.get("port", 5432)
        sslmode = secrets_cfg.get("sslmode", "require")
        if host and database and user and password:
            return f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode={sslmode}"
    return None


POSTGRES_URL = _load_postgres_url()
USE_POSTGRES = bool(POSTGRES_URL)

if USE_POSTGRES:
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError as exc:
        raise RuntimeError(
            "psycopg2-binary es requerido para conectarse a PostgreSQL. "
            "Agrega la dependencia e instala los requirements."
        ) from exc


# Detectar si estamos en Streamlit Cloud
IS_STREAMLIT_CLOUD = os.environ.get("STREAMLIT_SERVER_ENVIRONMENT") == "cloud"

# Configurar ruta de base de datos SQLite (fallback local)
DB_PATH = Path("postventa.db")
BACKUP_DIR = Path("backups")
BACKUP_DIR.mkdir(exist_ok=True)


VENTAS_TABLE_SQLITE = """
    CREATE TABLE IF NOT EXISTS ventas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mes TEXT,
        fecha DATE NOT NULL,
        sucursal TEXT,
        cliente TEXT,
        pin TEXT,
        comprobante TEXT,
        tipo_comprobante TEXT,
        trabajo TEXT,
        n_comprobante TEXT,
        tipo_re_se TEXT,
        mano_obra REAL DEFAULT 0,
        asistencia REAL DEFAULT 0,
        repuestos REAL DEFAULT 0,
        terceros REAL DEFAULT 0,
        servicio REAL DEFAULT 0,
        descuento REAL DEFAULT 0,
        total REAL NOT NULL,
        detalles TEXT,
        archivo_comprobante TEXT,
        campo_taller TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""

VENTAS_TABLE_PG = """
    CREATE TABLE IF NOT EXISTS ventas (
        id SERIAL PRIMARY KEY,
        mes TEXT,
        fecha DATE NOT NULL,
        sucursal TEXT,
        cliente TEXT,
        pin TEXT,
        comprobante TEXT,
        tipo_comprobante TEXT,
        trabajo TEXT,
        n_comprobante TEXT,
        tipo_re_se TEXT,
        mano_obra DOUBLE PRECISION DEFAULT 0,
        asistencia DOUBLE PRECISION DEFAULT 0,
        repuestos DOUBLE PRECISION DEFAULT 0,
        terceros DOUBLE PRECISION DEFAULT 0,
        servicio DOUBLE PRECISION DEFAULT 0,
        descuento DOUBLE PRECISION DEFAULT 0,
        total DOUBLE PRECISION NOT NULL,
        detalles TEXT,
        archivo_comprobante TEXT,
        campo_taller TEXT,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    )
"""

GASTOS_TABLE_SQLITE = """
    CREATE TABLE IF NOT EXISTS gastos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mes TEXT,
        fecha DATE NOT NULL,
        sucursal TEXT,
        area TEXT,
        pct_postventa REAL DEFAULT 0,
        pct_servicios REAL DEFAULT 0,
        pct_repuestos REAL DEFAULT 0,
        tipo TEXT,
        clasificacion TEXT,
        proveedor TEXT,
        total_pesos REAL,
        total_usd REAL NOT NULL,
        total_pct REAL,
        total_pct_se REAL,
        total_pct_re REAL,
        detalles TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""

GASTOS_TABLE_PG = """
    CREATE TABLE IF NOT EXISTS gastos (
        id SERIAL PRIMARY KEY,
        mes TEXT,
        fecha DATE NOT NULL,
        sucursal TEXT,
        area TEXT,
        pct_postventa DOUBLE PRECISION DEFAULT 0,
        pct_servicios DOUBLE PRECISION DEFAULT 0,
        pct_repuestos DOUBLE PRECISION DEFAULT 0,
        tipo TEXT,
        clasificacion TEXT,
        proveedor TEXT,
        total_pesos DOUBLE PRECISION,
        total_usd DOUBLE PRECISION NOT NULL,
        total_pct DOUBLE PRECISION,
        total_pct_se DOUBLE PRECISION,
        total_pct_re DOUBLE PRECISION,
        detalles TEXT,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    )
"""

PLANTILLAS_TABLE_SQLITE = """
    CREATE TABLE IF NOT EXISTS plantillas_gastos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL UNIQUE,
        descripcion TEXT,
        sucursal TEXT,
        area TEXT,
        pct_postventa REAL DEFAULT 0,
        pct_servicios REAL DEFAULT 0,
        pct_repuestos REAL DEFAULT 0,
        tipo TEXT,
        clasificacion TEXT,
        proveedor TEXT,
        detalles TEXT,
        activa INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""

PLANTILLAS_TABLE_PG = """
    CREATE TABLE IF NOT EXISTS plantillas_gastos (
        id SERIAL PRIMARY KEY,
        nombre TEXT NOT NULL UNIQUE,
        descripcion TEXT,
        sucursal TEXT,
        area TEXT,
        pct_postventa DOUBLE PRECISION DEFAULT 0,
        pct_servicios DOUBLE PRECISION DEFAULT 0,
        pct_repuestos DOUBLE PRECISION DEFAULT 0,
        tipo TEXT,
        clasificacion TEXT,
        proveedor TEXT,
        detalles TEXT,
        activa BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    )
"""

HISTORIAL_TABLE_SQLITE = """
    CREATE TABLE IF NOT EXISTS historial_analisis_ia (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        tipo_analisis TEXT NOT NULL,
        fuente TEXT NOT NULL,
        contenido TEXT NOT NULL,
        metadata TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""

HISTORIAL_TABLE_PG = """
    CREATE TABLE IF NOT EXISTS historial_analisis_ia (
        id SERIAL PRIMARY KEY,
        fecha_hora TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        tipo_analisis TEXT NOT NULL,
        fuente TEXT NOT NULL,
        contenido TEXT NOT NULL,
        metadata TEXT,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    )
"""

# Tablas nuevas para el esqueleto actual (Registro / futuras vistas); no alteran ventas ni gastos.
APP_REGISTROS_TABLE_SQLITE = """
    CREATE TABLE IF NOT EXISTS app_registros (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        titulo TEXT,
        detalle TEXT,
        extra_json TEXT
    )
"""

APP_REGISTROS_TABLE_PG = """
    CREATE TABLE IF NOT EXISTS app_registros (
        id SERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        titulo TEXT,
        detalle TEXT,
        extra_json TEXT
    )
"""

# Cierres mensuales de ventas (Registro). Concesionario en UI. gastos_*_global / otros en USD.
CIERRE_VENTAS_MES_SQLITE = """
    CREATE TABLE IF NOT EXISTS cierre_ventas_mes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        anio INTEGER NOT NULL,
        mes INTEGER NOT NULL,
        tipo_cambio_ars_usd REAL NOT NULL DEFAULT 1.0,
        notas TEXT,
        gastos_fijos_global REAL NOT NULL DEFAULT 0,
        gastos_var_otros REAL NOT NULL DEFAULT 0,
        gastos_var_otros_rubro TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (anio, mes)
    )
"""

CIERRE_VENTAS_MES_PG = """
    CREATE TABLE IF NOT EXISTS cierre_ventas_mes (
        id SERIAL PRIMARY KEY,
        anio INTEGER NOT NULL,
        mes INTEGER NOT NULL,
        tipo_cambio_ars_usd DOUBLE PRECISION NOT NULL DEFAULT 1.0,
        notas TEXT,
        gastos_fijos_global DOUBLE PRECISION NOT NULL DEFAULT 0,
        gastos_var_otros DOUBLE PRECISION NOT NULL DEFAULT 0,
        gastos_var_otros_rubro TEXT,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (anio, mes)
    )
"""

CIERRE_VENTAS_LINEA_SQLITE = """
    CREATE TABLE IF NOT EXISTS cierre_ventas_linea (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cierre_id INTEGER NOT NULL,
        sucursal TEXT NOT NULL,
        fact_rep_mostrador REAL NOT NULL DEFAULT 0,
        fact_rep_taller REAL NOT NULL DEFAULT 0,
        desc_mostrador REAL NOT NULL DEFAULT 0,
        desc_taller REAL NOT NULL DEFAULT 0,
        util_pct_mostrador REAL,
        util_pct_taller REAL,
        util_pct_servicios REAL,
        fact_servicios REAL NOT NULL DEFAULT 0,
        gastos_fijos REAL NOT NULL DEFAULT 0,
        gastos_var_s REAL NOT NULL DEFAULT 0,
        gastos_var_r REAL NOT NULL DEFAULT 0,
        total_repuestos REAL,
        util_prom_pct REAL,
        total_bruto REAL,
        gastos_variables_tot REAL,
        gastos_total REAL,
        margen_contrib REAL,
        margen_contrib_pct REAL,
        resultado REAL,
        factor_absorcion REAL,
        FOREIGN KEY (cierre_id) REFERENCES cierre_ventas_mes (id) ON DELETE CASCADE,
        UNIQUE (cierre_id, sucursal)
    )
"""

CIERRE_VENTAS_LINEA_PG = """
    CREATE TABLE IF NOT EXISTS cierre_ventas_linea (
        id SERIAL PRIMARY KEY,
        cierre_id INTEGER NOT NULL REFERENCES cierre_ventas_mes (id) ON DELETE CASCADE,
        sucursal TEXT NOT NULL,
        fact_rep_mostrador DOUBLE PRECISION NOT NULL DEFAULT 0,
        fact_rep_taller DOUBLE PRECISION NOT NULL DEFAULT 0,
        desc_mostrador DOUBLE PRECISION NOT NULL DEFAULT 0,
        desc_taller DOUBLE PRECISION NOT NULL DEFAULT 0,
        util_pct_mostrador DOUBLE PRECISION,
        util_pct_taller DOUBLE PRECISION,
        util_pct_servicios DOUBLE PRECISION,
        fact_servicios DOUBLE PRECISION NOT NULL DEFAULT 0,
        gastos_fijos DOUBLE PRECISION NOT NULL DEFAULT 0,
        gastos_var_s DOUBLE PRECISION NOT NULL DEFAULT 0,
        gastos_var_r DOUBLE PRECISION NOT NULL DEFAULT 0,
        total_repuestos DOUBLE PRECISION,
        util_prom_pct DOUBLE PRECISION,
        total_bruto DOUBLE PRECISION,
        gastos_variables_tot DOUBLE PRECISION,
        gastos_total DOUBLE PRECISION,
        margen_contrib DOUBLE PRECISION,
        margen_contrib_pct DOUBLE PRECISION,
        resultado DOUBLE PRECISION,
        factor_absorcion DOUBLE PRECISION,
        UNIQUE (cierre_id, sucursal)
    )
"""


def _prepare_query(query: str) -> str:
    if USE_POSTGRES:
        return query.replace("?", "%s")
    return query


def _convert_params(params):
    if not USE_POSTGRES or params is None:
        return params
    if isinstance(params, list):
        return tuple(params)
    return params


def _execute(cursor, query: str, params=None):
    query = _prepare_query(query)
    if params is None:
        cursor.execute(query)
    else:
        cursor.execute(query, _convert_params(params))


def _read_sql(query: str, conn, params=None):
    query = _prepare_query(query)
    if USE_POSTGRES:
        # pandas no funciona bien con conexiones que usan RealDictCursor,
        # así que abrimos una conexión simple solo para lecturas con pandas
        with psycopg2.connect(POSTGRES_URL) as tmp_conn:
            return pd.read_sql_query(query, tmp_conn, params=_convert_params(params))
    return pd.read_sql_query(query, conn, params=_convert_params(params))


def _sanitize_dataframe(df: pd.DataFrame, numeric_cols: list[str]) -> pd.DataFrame:
    """
    Limpia un DataFrame convertido desde la base de datos:
    - Convierte columnas numéricas a float manejando strings o valores inválidos
    - Convierte la columna fecha a datetime.date (si existe)
    - Elimina filas donde todas las columnas numéricas quedan como NaN (p. ej. cabeceras mal importadas)
    """
    if df is None or len(df) == 0:
        return df

    df = df.copy()

    # Normalizar fechas para evitar comparaciones lexicográficas inesperadas
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date
        df = df.dropna(subset=["fecha"])

    # Coerción robusta de numéricos
    coerced_cols = []
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            coerced_cols.append(col)

    # Si todas las columnas numéricas de una fila son NaN, descartarla (suele indicar filas con títulos)
    if coerced_cols:
        df = df.dropna(subset=coerced_cols, how="all")

    return df


def _fetch_scalar(cursor, default=0):
    row = cursor.fetchone()
    if row is None:
        return default
    if isinstance(row, dict):
        return next(iter(row.values()))
    return row[0]


def compute_servicio_venta(total, repuestos, tipo_re_se) -> float:
    """Ingreso no-repuestos: total − repuestos (solo SE). En RE queda 0."""
    try:
        t = float(total or 0)
        r = float(repuestos or 0)
    except (TypeError, ValueError):
        return 0.0
    if str(tipo_re_se or "").upper() == "RE":
        return 0.0
    return t - r


def es_nota_credito_no_jd(tipo_comprobante: str | None) -> bool:
    u = (tipo_comprobante or "").upper()
    return bool(u and "CREDITO" in u and "JD" not in u)


def alinear_montos_nota_credito(
    tipo_comprobante: str | None,
    total: float,
    repuestos: float,
    *,
    mano_obra: float = 0.0,
    asistencia: float = 0.0,
    terceros: float = 0.0,
    costo_repuestos: float = 0.0,
) -> tuple[float, float, float, float, float, float]:
    """Para NC (no JD): el total suele volverse negativo; los componentes deben ir en el mismo sentido
    para que total ≈ repuestos + servicio implícito y no se reste dos veces el repuesto.
    ``costo_repuestos`` (FIFO / costo mercadería) sigue el mismo criterio de signo que repuestos."""
    t = float(total or 0)
    r = float(repuestos or 0)
    mo = float(mano_obra or 0)
    a = float(asistencia or 0)
    ter = float(terceros or 0)
    cr = float(costo_repuestos or 0)
    if not es_nota_credito_no_jd(tipo_comprobante):
        return t, r, mo, a, ter, cr
    if t > 0:
        t = -t
    if t < 0:
        if r > 0:
            r = -r
        if mo > 0:
            mo = -mo
        if a > 0:
            a = -a
        if ter > 0:
            ter = -ter
        if cr > 0:
            cr = -cr
    return t, r, mo, a, ter, cr


def _pg_ventas_column_exists(cursor, column_name: str) -> bool:
    _execute(
        cursor,
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'ventas' AND column_name = ?
        """,
        (column_name,),
    )
    return cursor.fetchone() is not None


def _ensure_ventas_optional_columns(cursor, conn) -> None:
    """Añade columnas que el INSERT/UPDATE esperan si la tabla es antigua (p. ej. Postgres en la nube)."""
    if USE_POSTGRES:
        try:
            added_servicio = False
            if not _pg_ventas_column_exists(cursor, "archivo_comprobante"):
                _execute(cursor, "ALTER TABLE ventas ADD COLUMN archivo_comprobante TEXT")
            if not _pg_ventas_column_exists(cursor, "campo_taller"):
                _execute(cursor, "ALTER TABLE ventas ADD COLUMN campo_taller TEXT")
            if not _pg_ventas_column_exists(cursor, "servicio"):
                _execute(
                    cursor,
                    "ALTER TABLE ventas ADD COLUMN servicio DOUBLE PRECISION DEFAULT 0",
                )
                added_servicio = True
            if not _pg_ventas_column_exists(cursor, "costo_repuestos"):
                _execute(
                    cursor,
                    "ALTER TABLE ventas ADD COLUMN costo_repuestos DOUBLE PRECISION",
                )
            if not _pg_ventas_column_exists(cursor, "usuario_autologica"):
                _execute(cursor, "ALTER TABLE ventas ADD COLUMN usuario_autologica TEXT")
            if not _pg_ventas_column_exists(cursor, "utilidad_ventas_pct"):
                _execute(
                    cursor,
                    "ALTER TABLE ventas ADD COLUMN utilidad_ventas_pct DOUBLE PRECISION",
                )
            if not _pg_ventas_column_exists(cursor, "utilidad_ventas_monto"):
                _execute(
                    cursor,
                    "ALTER TABLE ventas ADD COLUMN utilidad_ventas_monto DOUBLE PRECISION",
                )
            if added_servicio:
                _execute(
                    cursor,
                    """
                    UPDATE ventas SET servicio = CASE
                        WHEN UPPER(COALESCE(tipo_re_se, '')) = 'RE' THEN 0
                        ELSE COALESCE(total, 0) - COALESCE(repuestos, 0)
                    END
                    """,
                )
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
    else:
        added_servicio_sqlite = False
        for ddl in (
            "ALTER TABLE ventas ADD COLUMN archivo_comprobante TEXT",
            "ALTER TABLE ventas ADD COLUMN campo_taller TEXT",
        ):
            try:
                _execute(cursor, ddl)
                conn.commit()
            except sqlite3.OperationalError:
                pass
        try:
            _execute(cursor, "ALTER TABLE ventas ADD COLUMN servicio REAL DEFAULT 0")
            conn.commit()
            added_servicio_sqlite = True
        except sqlite3.OperationalError:
            pass
        try:
            _execute(cursor, "ALTER TABLE ventas ADD COLUMN costo_repuestos REAL")
            conn.commit()
        except sqlite3.OperationalError:
            pass
        for ddl in (
            "ALTER TABLE ventas ADD COLUMN usuario_autologica TEXT",
            "ALTER TABLE ventas ADD COLUMN utilidad_ventas_pct REAL",
            "ALTER TABLE ventas ADD COLUMN utilidad_ventas_monto REAL",
        ):
            try:
                _execute(cursor, ddl)
                conn.commit()
            except sqlite3.OperationalError:
                pass
        if added_servicio_sqlite:
            try:
                _execute(
                    cursor,
                    """
                    UPDATE ventas SET servicio = CASE
                        WHEN UPPER(COALESCE(tipo_re_se, '')) = 'RE' THEN 0
                        ELSE COALESCE(total, 0) - COALESCE(repuestos, 0)
                    END
                    """,
                )
                conn.commit()
            except sqlite3.OperationalError:
                pass


def get_connection():
    """Obtiene una conexión a la base de datos"""
    if USE_POSTGRES:
        conn = psycopg2.connect(
            POSTGRES_URL,
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
        return conn
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _pg_cierre_has_column(cursor, table: str, column: str) -> bool:
    _execute(
        cursor,
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = ? AND column_name = ?
        """,
        (table, column),
    )
    return cursor.fetchone() is not None


def _ensure_cierre_ventas_schema_pg(cursor, conn) -> None:
    for table, col, typ in (
        ("cierre_ventas_mes", "gastos_fijos_global", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
        ("cierre_ventas_mes", "gastos_var_otros", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
        ("cierre_ventas_mes", "gastos_var_otros_rubro", "TEXT"),
        ("cierre_ventas_linea", "util_pct_servicios", "DOUBLE PRECISION"),
    ):
        if not _pg_cierre_has_column(cursor, table, col):
            _execute(cursor, f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
    conn.commit()


def _ensure_cierre_ventas_schema_sqlite(cursor, conn) -> None:
    for ddl in (
        "ALTER TABLE cierre_ventas_mes ADD COLUMN gastos_fijos_global REAL NOT NULL DEFAULT 0",
        "ALTER TABLE cierre_ventas_mes ADD COLUMN gastos_var_otros REAL NOT NULL DEFAULT 0",
        "ALTER TABLE cierre_ventas_mes ADD COLUMN gastos_var_otros_rubro TEXT",
        "ALTER TABLE cierre_ventas_linea ADD COLUMN util_pct_servicios REAL",
    ):
        try:
            _execute(cursor, ddl)
            conn.commit()
        except sqlite3.OperationalError:
            pass


def init_database():
    """Inicializa las tablas de la base de datos"""
    conn = get_connection()
    cursor = conn.cursor()

    if USE_POSTGRES:
        _execute(cursor, VENTAS_TABLE_PG)
        try:
            _ensure_ventas_optional_columns(cursor, conn)
        except Exception:
            pass
        _execute(cursor, GASTOS_TABLE_PG)
        _execute(cursor, PLANTILLAS_TABLE_PG)
        _execute(cursor, HISTORIAL_TABLE_PG)
        _execute(cursor, APP_REGISTROS_TABLE_PG)
        _execute(cursor, CIERRE_VENTAS_MES_PG)
        _execute(cursor, CIERRE_VENTAS_LINEA_PG)
        try:
            _ensure_cierre_ventas_schema_pg(cursor, conn)
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
    else:
        _execute(cursor, VENTAS_TABLE_SQLITE)
        try:
            _ensure_ventas_optional_columns(cursor, conn)
        except Exception:
            pass
        _execute(cursor, GASTOS_TABLE_SQLITE)
        _execute(cursor, PLANTILLAS_TABLE_SQLITE)
        _execute(cursor, HISTORIAL_TABLE_SQLITE)
        _execute(cursor, APP_REGISTROS_TABLE_SQLITE)
        _execute(cursor, CIERRE_VENTAS_MES_SQLITE)
        _execute(cursor, CIERRE_VENTAS_LINEA_SQLITE)
        try:
            _ensure_cierre_ventas_schema_sqlite(cursor, conn)
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass

    conn.commit()
    conn.close()


def insert_app_registro(
    titulo: str | None = None,
    detalle: str | None = None,
    extra_json: str | None = None,
) -> int:
    """Inserta una fila en ``app_registros`` (JSON adicional como texto)."""
    conn = get_connection()
    cursor = conn.cursor()
    if USE_POSTGRES:
        _execute(
            cursor,
            """
            INSERT INTO app_registros (titulo, detalle, extra_json)
            VALUES (?, ?, ?)
            RETURNING id
            """,
            (titulo, detalle, extra_json),
        )
        row = cursor.fetchone()
        new_id = row["id"] if isinstance(row, dict) else row[0]
    else:
        _execute(
            cursor,
            "INSERT INTO app_registros (titulo, detalle, extra_json) VALUES (?, ?, ?)",
            (titulo, detalle, extra_json),
        )
        new_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return int(new_id)


def list_app_registros(limit: int = 100) -> pd.DataFrame:
    """Últimas filas de ``app_registros`` (para pantalla Registro o depuración)."""
    conn = get_connection()
    try:
        df = _read_sql(
            "SELECT * FROM app_registros ORDER BY id DESC LIMIT ?",
            conn,
            (limit,),
        )
        return df if df is not None else pd.DataFrame()
    finally:
        conn.close()


SUCURSALES_VENTAS_REAL = ("RIO GRANDE", "RIO GALLEGOS", "COMODORO")


def compute_cierre_venta_linea(
    fact_rep_mostrador: float,
    fact_rep_taller: float,
    desc_mostrador: float,
    desc_taller: float,
    util_pct_mostrador: float | None,
    util_pct_taller: float | None,
    fact_servicios: float,
) -> dict:
    """
    Por sucursal: ventas en ARS y CMV (gastos variables) por línea.
    ``util_pct_*`` en fracción (0.3346 = 33,46 %).

    Facturación total = neto repuestos + fact. servicios.
    Gastos variables = CMV mostrador + CMV taller + CMV servicios (sin % utilidad
    servicios en grilla → utilidad servicios 100 % ⇒ CMV servicios = 0).
    Margen contribución = facturación total − gastos variables.
    Margen % = margen / facturación total (fracción 0–1; None si total ≤ 0).
    """
    fm = float(fact_rep_mostrador or 0)
    ft = float(fact_rep_taller or 0)
    dm = float(desc_mostrador or 0)
    dt = float(desc_taller or 0)
    um = float(util_pct_mostrador) if util_pct_mostrador is not None else 0.0
    ut = float(util_pct_taller) if util_pct_taller is not None else 0.0
    fs = float(fact_servicios or 0)

    um = min(max(um, 0.0), 1.0)
    ut = min(max(ut, 0.0), 1.0)

    neto_rep = fm + ft - dm - dt
    utils_no_cero = [u for u in (um, ut) if u > 0]
    if utils_no_cero:
        util_prom = sum(utils_no_cero) / len(utils_no_cero)
    else:
        util_prom = 0.0

    total_bruto = neto_rep + fs

    net_mos = max(fm - dm, 0.0)
    net_tal = max(ft - dt, 0.0)
    us = 1.0  # alineado con gastos variables globales (sin CMV servicios por %)
    gv_mos = net_mos * (1.0 - um)
    gv_tal = net_tal * (1.0 - ut)
    gv_serv = fs * (1.0 - us)
    gastos_variables_tot = gv_mos + gv_tal + gv_serv
    margen_contrib = total_bruto - gastos_variables_tot

    if total_bruto > 0:
        margen_contrib_pct = margen_contrib / total_bruto
    else:
        margen_contrib_pct = None

    gastos_total = gastos_variables_tot
    resultado = margen_contrib

    return {
        "total_repuestos": neto_rep,
        "util_prom_pct": util_prom,
        "total_bruto": total_bruto,
        "gastos_variables_tot": gastos_variables_tot,
        "gastos_total": gastos_total,
        "margen_contrib": margen_contrib,
        "margen_contrib_pct": margen_contrib_pct,
        "resultado": resultado,
        "factor_absorcion": None,
    }


def compute_gastos_variables_globales(
    *,
    fact_rep_mos_conc: float,
    desc_rep_mos_conc: float,
    fact_rep_tal_conc: float,
    desc_rep_tal_conc: float,
    fact_serv_conc: float,
    util_mos_conc: float | None,
    util_tal_conc: float | None,
    util_serv_conc: float | None,
    gastos_fijos_global: float,
    gastos_var_otros: float,
    gastos_var_otros_rubro: str | None,
) -> dict:
    """
    Gastos variables globales del mes (Concesionario).
    CMV rep. mostrador / taller: (neto fact − desc) × (1 − utilidad canal).
    Servicios: fact. servicios × (1 − utilidad servicios ponderada).
    ``util_*`` en fracción 0–1.
    ``gastos_var_otros`` debe expresarse en **ARS** (p. ej. USD × TC) para sumarse a buckets en ARS.
    ``gastos_fijos_global`` en ARS; la UI puede pasar 0 y sumar fijos en USD aparte.
    """
    um = float(util_mos_conc) if util_mos_conc is not None else 0.0
    ut = float(util_tal_conc) if util_tal_conc is not None else 0.0
    us = float(util_serv_conc) if util_serv_conc is not None else 0.0
    um = min(max(um, 0.0), 1.0)
    ut = min(max(ut, 0.0), 1.0)
    us = min(max(us, 0.0), 1.0)

    net_mos = max(float(fact_rep_mos_conc or 0) - float(desc_rep_mos_conc or 0), 0.0)
    net_tal = max(float(fact_rep_tal_conc or 0) - float(desc_rep_tal_conc or 0), 0.0)
    fs = max(float(fact_serv_conc or 0), 0.0)

    gv_rep_mos = net_mos * (1.0 - um)
    gv_rep_tal = net_tal * (1.0 - ut)
    gv_rep_total = gv_rep_mos + gv_rep_tal
    gv_serv = fs * (1.0 - us)

    otros = max(float(gastos_var_otros or 0), 0.0)
    rubro = (gastos_var_otros_rubro or "").strip().lower()
    gv_serv_adj = gv_serv
    gv_rep_adj = gv_rep_total
    if otros > 0:
        if rubro in ("servicios", "servicio"):
            gv_serv_adj += otros
        elif rubro in ("repuestos", "repuesto"):
            gv_rep_adj += otros

    fijos = max(float(gastos_fijos_global or 0), 0.0)
    gastos_total = fijos + gv_serv_adj + gv_rep_adj

    return {
        "neto_rep_mos_conc": net_mos,
        "neto_rep_tal_conc": net_tal,
        "gv_rep_mostrador": gv_rep_mos,
        "gv_rep_taller": gv_rep_tal,
        "gv_repuestos": gv_rep_total,
        "gv_servicios": gv_serv,
        "gv_servicios_ajustado": gv_serv_adj,
        "gv_repuestos_ajustado": gv_rep_adj,
        "gastos_total": gastos_total,
    }


def get_cierre_ventas_mes(anio: int, mes: int) -> dict | None:
    conn = get_connection()
    try:
        df = _read_sql(
            "SELECT * FROM cierre_ventas_mes WHERE anio = ? AND mes = ?",
            conn,
            (anio, mes),
        )
        if df is None or len(df) == 0:
            return None
        return df.iloc[0].to_dict()
    finally:
        conn.close()


def list_cierres_ventas_mes(limit: int = 36) -> pd.DataFrame:
    conn = get_connection()
    try:
        df = _read_sql(
            """
            SELECT id, anio, mes, tipo_cambio_ars_usd, updated_at
            FROM cierre_ventas_mes
            ORDER BY anio DESC, mes DESC
            LIMIT ?
            """,
            conn,
            (limit,),
        )
        return df if df is not None else pd.DataFrame()
    finally:
        conn.close()


def list_cierres_ventas_dashboard(limit: int = 60) -> pd.DataFrame:
    """Cierres para dashboard de Inicio con campos de cabecera relevantes."""
    conn = get_connection()
    try:
        df = _read_sql(
            """
            SELECT
                id,
                anio,
                mes,
                tipo_cambio_ars_usd,
                gastos_fijos_global,
                gastos_var_otros,
                gastos_var_otros_rubro,
                updated_at
            FROM cierre_ventas_mes
            ORDER BY anio DESC, mes DESC
            LIMIT ?
            """,
            conn,
            (limit,),
        )
        return df if df is not None else pd.DataFrame()
    finally:
        conn.close()


def upsert_cierre_ventas_mes_header(
    anio: int,
    mes: int,
    tipo_cambio_ars_usd: float,
    notas: str | None = None,
    *,
    gastos_fijos_global: float = 0.0,
    gastos_var_otros: float = 0.0,
    gastos_var_otros_rubro: str | None = None,
) -> int:
    if tipo_cambio_ars_usd <= 0:
        raise ValueError("tipo_cambio_ars_usd debe ser > 0 (ARS por 1 USD).")
    exist = get_cierre_ventas_mes(anio, mes)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        if exist is not None:
            cid = int(exist["id"])
            _execute(
                cursor,
                """
                UPDATE cierre_ventas_mes
                SET tipo_cambio_ars_usd = ?, notas = ?,
                    gastos_fijos_global = ?, gastos_var_otros = ?, gastos_var_otros_rubro = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    tipo_cambio_ars_usd,
                    notas,
                    gastos_fijos_global,
                    gastos_var_otros,
                    gastos_var_otros_rubro,
                    cid,
                ),
            )
        else:
            if USE_POSTGRES:
                _execute(
                    cursor,
                    """
                    INSERT INTO cierre_ventas_mes (
                        anio, mes, tipo_cambio_ars_usd, notas,
                        gastos_fijos_global, gastos_var_otros, gastos_var_otros_rubro
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    RETURNING id
                    """,
                    (
                        anio,
                        mes,
                        tipo_cambio_ars_usd,
                        notas,
                        gastos_fijos_global,
                        gastos_var_otros,
                        gastos_var_otros_rubro,
                    ),
                )
                row = cursor.fetchone()
                cid = int(row["id"] if isinstance(row, dict) else row[0])
            else:
                _execute(
                    cursor,
                    """
                    INSERT INTO cierre_ventas_mes (
                        anio, mes, tipo_cambio_ars_usd, notas,
                        gastos_fijos_global, gastos_var_otros, gastos_var_otros_rubro
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        anio,
                        mes,
                        tipo_cambio_ars_usd,
                        notas,
                        gastos_fijos_global,
                        gastos_var_otros,
                        gastos_var_otros_rubro,
                    ),
                )
                cid = int(cursor.lastrowid)
        conn.commit()
        return cid
    finally:
        conn.close()


def get_lineas_cierre_ventas(cierre_id: int) -> pd.DataFrame:
    conn = get_connection()
    try:
        df = _read_sql(
            """
            SELECT * FROM cierre_ventas_linea
            WHERE cierre_id = ?
            ORDER BY sucursal
            """,
            conn,
            (cierre_id,),
        )
        return df if df is not None else pd.DataFrame()
    finally:
        conn.close()


def replace_lineas_cierre_ventas(cierre_id: int, filas: list[dict]) -> None:
    """Reemplaza todas las líneas del cierre por las sucursales dadas (típicamente 3)."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        _execute(cursor, "DELETE FROM cierre_ventas_linea WHERE cierre_id = ?", (cierre_id,))
        for row in filas:
            calc = compute_cierre_venta_linea(
                row["fact_rep_mostrador"],
                row["fact_rep_taller"],
                row["desc_mostrador"],
                row["desc_taller"],
                row.get("util_pct_mostrador"),
                row.get("util_pct_taller"),
                row["fact_servicios"],
            )
            _execute(
                cursor,
                """
                INSERT INTO cierre_ventas_linea (
                    cierre_id, sucursal,
                    fact_rep_mostrador, fact_rep_taller, desc_mostrador, desc_taller,
                    util_pct_mostrador, util_pct_taller, util_pct_servicios, fact_servicios,
                    gastos_fijos, gastos_var_s, gastos_var_r,
                    total_repuestos, util_prom_pct, total_bruto,
                    gastos_variables_tot, gastos_total, margen_contrib, margen_contrib_pct,
                    resultado, factor_absorcion
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cierre_id,
                    row["sucursal"],
                    row["fact_rep_mostrador"],
                    row["fact_rep_taller"],
                    row["desc_mostrador"],
                    row["desc_taller"],
                    row.get("util_pct_mostrador"),
                    row.get("util_pct_taller"),
                    row.get("util_pct_servicios"),
                    row["fact_servicios"],
                    0.0,
                    0.0,
                    0.0,
                    calc["total_repuestos"],
                    calc["util_prom_pct"],
                    calc["total_bruto"],
                    calc["gastos_variables_tot"],
                    calc["gastos_total"],
                    calc["margen_contrib"],
                    calc["margen_contrib_pct"],
                    calc["resultado"],
                    calc["factor_absorcion"],
                ),
            )
        conn.commit()
    finally:
        conn.close()


def get_ventas(fecha_inicio=None, fecha_fin=None):
    """Obtiene todas las ventas, opcionalmente filtradas por fecha"""
    conn = get_connection()
    
    query = "SELECT * FROM ventas WHERE 1=1"
    params = []
    
    if fecha_inicio:
        query += " AND fecha >= ?"
        params.append(fecha_inicio)
    
    if fecha_fin:
        query += " AND fecha <= ?"
        params.append(fecha_fin)
    
    query += " ORDER BY fecha DESC, id DESC"
    
    df = _read_sql(query, conn, params)
    if len(df):
        df = _sanitize_dataframe(
            df,
            [
                "mano_obra",
                "asistencia",
                "repuestos",
                "terceros",
                "servicio",
                "descuento",
                "total",
                "costo_repuestos",
                "utilidad_ventas_pct",
                "utilidad_ventas_monto",
            ],
        )
    conn.close()
    
    return df

def get_venta_by_id(venta_id):
    """Obtiene una venta por su ID"""
    conn = get_connection()
    cursor = conn.cursor()
    _execute(cursor, "SELECT * FROM ventas WHERE id = ?", (venta_id,))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return dict(row)
    return None

def insert_venta(venta_data):
    """Inserta una nueva venta"""
    conn = get_connection()
    cursor = conn.cursor()

    venta_data = dict(venta_data)
    _ensure_ventas_optional_columns(cursor, conn)
    raw_costo = venta_data.get("costo_repuestos")
    costo_in = float(raw_costo) if raw_costo is not None and raw_costo != "" else 0.0
    t, r, mo, a, ter, cr = alinear_montos_nota_credito(
        venta_data.get("tipo_comprobante"),
        float(venta_data.get("total") or 0),
        float(venta_data.get("repuestos") or 0),
        mano_obra=float(venta_data.get("mano_obra") or 0),
        asistencia=float(venta_data.get("asistencia") or 0),
        terceros=float(venta_data.get("terceros") or 0),
        costo_repuestos=costo_in,
    )
    venta_data["total"] = t
    venta_data["repuestos"] = r
    venta_data["mano_obra"] = mo
    venta_data["asistencia"] = a
    venta_data["terceros"] = ter
    costo_para_db = cr if raw_costo is not None and raw_costo != "" else None

    tipo_re = venta_data.get("tipo_re_se")
    servicio_val = compute_servicio_venta(
        venta_data.get("total"),
        venta_data.get("repuestos"),
        tipo_re,
    )

    insert_sql = """
        INSERT INTO ventas (
            mes, fecha, sucursal, cliente, pin, comprobante, tipo_comprobante,
            trabajo, n_comprobante, tipo_re_se, mano_obra, asistencia,
            repuestos, terceros, servicio, descuento, total, detalles, archivo_comprobante, campo_taller,
            costo_repuestos, usuario_autologica, utilidad_ventas_pct, utilidad_ventas_monto
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        venta_data.get('mes'),
        venta_data.get('fecha'),
        venta_data.get('sucursal'),
        venta_data.get('cliente'),
        venta_data.get('pin'),
        venta_data.get('comprobante'),
        venta_data.get('tipo_comprobante'),
        venta_data.get('trabajo'),
        venta_data.get('n_comprobante'),
        venta_data.get('tipo_re_se'),
        venta_data.get('mano_obra', 0),
        venta_data.get('asistencia', 0),
        venta_data.get('repuestos', 0),
        venta_data.get('terceros', 0),
        servicio_val,
        venta_data.get('descuento', 0),
        venta_data.get('total', 0),
        venta_data.get('detalles'),
        venta_data.get('archivo_comprobante'),
        venta_data.get('campo_taller'),
        costo_para_db,
        venta_data.get('usuario_autologica'),
        venta_data.get('utilidad_ventas_pct'),
        venta_data.get('utilidad_ventas_monto'),
    )

    if USE_POSTGRES:
        insert_sql = insert_sql.replace("?)", "?) RETURNING id")
        _execute(cursor, insert_sql, params)
        venta_id = cursor.fetchone()["id"]
    else:
        _execute(cursor, insert_sql, params)
        venta_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    return venta_id

def update_venta(venta_id, venta_data):
    """Actualiza una venta existente"""
    conn = get_connection()
    cursor = conn.cursor()

    venta_data = dict(venta_data)
    _ensure_ventas_optional_columns(cursor, conn)
    raw_costo = venta_data.get("costo_repuestos")
    costo_in = float(raw_costo) if raw_costo is not None and raw_costo != "" else 0.0
    t, r, mo, a, ter, cr = alinear_montos_nota_credito(
        venta_data.get("tipo_comprobante"),
        float(venta_data.get("total") or 0),
        float(venta_data.get("repuestos") or 0),
        mano_obra=float(venta_data.get("mano_obra") or 0),
        asistencia=float(venta_data.get("asistencia") or 0),
        terceros=float(venta_data.get("terceros") or 0),
        costo_repuestos=costo_in,
    )
    venta_data["total"] = t
    venta_data["repuestos"] = r
    venta_data["mano_obra"] = mo
    venta_data["asistencia"] = a
    venta_data["terceros"] = ter
    costo_para_db = cr if raw_costo is not None and raw_costo != "" else None

    tipo_re_u = venta_data.get("tipo_re_se")
    servicio_u = compute_servicio_venta(
        venta_data.get("total"),
        venta_data.get("repuestos"),
        tipo_re_u,
    )

    _execute(cursor, """
        UPDATE ventas SET
            mes = ?, fecha = ?, sucursal = ?, cliente = ?, pin = ?,
            comprobante = ?, tipo_comprobante = ?, trabajo = ?,
            n_comprobante = ?, tipo_re_se = ?, mano_obra = ?,
            asistencia = ?, repuestos = ?, terceros = ?, servicio = ?, descuento = ?,
            total = ?, detalles = ?, archivo_comprobante = ?, campo_taller = ?,
            costo_repuestos = ?, usuario_autologica = ?, utilidad_ventas_pct = ?, utilidad_ventas_monto = ?
        WHERE id = ?
    """, (
        venta_data.get('mes'),
        venta_data.get('fecha'),
        venta_data.get('sucursal'),
        venta_data.get('cliente'),
        venta_data.get('pin'),
        venta_data.get('comprobante'),
        venta_data.get('tipo_comprobante'),
        venta_data.get('trabajo'),
        venta_data.get('n_comprobante'),
        venta_data.get('tipo_re_se'),
        venta_data.get('mano_obra', 0),
        venta_data.get('asistencia', 0),
        venta_data.get('repuestos', 0),
        venta_data.get('terceros', 0),
        servicio_u,
        venta_data.get('descuento', 0),
        venta_data.get('total', 0),
        venta_data.get('detalles'),
        venta_data.get('archivo_comprobante'),
        venta_data.get('campo_taller'),
        costo_para_db,
        venta_data.get('usuario_autologica'),
        venta_data.get('utilidad_ventas_pct'),
        venta_data.get('utilidad_ventas_monto'),
        venta_id
    ))
    
    conn.commit()
    conn.close()

def delete_venta(venta_id):
    """Elimina una venta"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Obtener información de la venta para eliminar archivo adjunto si existe
    _execute(cursor, "SELECT archivo_comprobante FROM ventas WHERE id = ?", (venta_id,))
    row = cursor.fetchone()
    
    archivo_value = None
    if row:
        if isinstance(row, dict):
            archivo_value = row.get("archivo_comprobante")
        else:
            archivo_value = row[0]
    
    if archivo_value:
        archivo_path = Path(archivo_value)
        if archivo_path.exists():
            try:
                archivo_path.unlink()
            except:
                pass
    
    _execute(cursor, "DELETE FROM ventas WHERE id = ?", (venta_id,))
    conn.commit()
    conn.close()


def delete_ventas_entre_fechas(fecha_desde: str, fecha_hasta: str) -> int:
    """Elimina ventas con ``fecha`` entre ``fecha_desde`` y ``fecha_hasta`` (inclusive, formato YYYY-MM-DD).

    Retorna la cantidad de filas eliminadas.
    """
    conn = get_connection()
    cursor = conn.cursor()
    _execute(
        cursor,
        "DELETE FROM ventas WHERE fecha >= ? AND fecha <= ?",
        (fecha_desde, fecha_hasta),
    )
    n = cursor.rowcount if cursor.rowcount is not None else 0
    conn.commit()
    conn.close()
    return int(n)


def inferir_campo_taller_existentes():
    """Infiere campo_taller para registros SE existentes que no lo tengan"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Verificar si la columna existe primero
        if USE_POSTGRES:
            check_query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='ventas' AND column_name='campo_taller'
            """
        else:
            check_query = """
                SELECT name FROM pragma_table_info('ventas') WHERE name='campo_taller'
            """
        
        cursor.execute(check_query)
        col_exists = cursor.fetchone() is not None
        
        if not col_exists:
            # La columna no existe, no hay nada que inferir
            conn.close()
            return 0
        
        # Obtener registros SE sin campo_taller usando _read_sql para compatibilidad
        query = "SELECT id, asistencia FROM ventas WHERE tipo_re_se = 'SE' AND (campo_taller IS NULL OR campo_taller = '')"
        df_registros = _read_sql(query, conn)
        
        actualizados = 0
        for _, row in df_registros.iterrows():
            venta_id = row['id']
            asistencia = row.get('asistencia', 0) or 0
            
            # Si asistencia > 0 es Campo, sino Taller
            campo_taller = "Campo" if asistencia > 0 else "Taller"
            
            update_query = "UPDATE ventas SET campo_taller = ? WHERE id = ?"
            _execute(cursor, update_query, (campo_taller, venta_id))
            actualizados += 1
        
        conn.commit()
        conn.close()
        return actualizados
    except Exception as e:
        # Si hay algún error, cerrar la conexión y retornar 0
        try:
            conn.close()
        except:
            pass
        # No lanzar el error, solo retornar 0 para que la app continúe
        return 0

def get_gastos(fecha_inicio=None, fecha_fin=None):
    """Obtiene todos los gastos, opcionalmente filtrados por fecha"""
    conn = get_connection()
    
    query = "SELECT * FROM gastos WHERE 1=1"
    params = []
    
    if fecha_inicio:
        query += " AND fecha >= ?"
        params.append(fecha_inicio)
    
    if fecha_fin:
        query += " AND fecha <= ?"
        params.append(fecha_fin)
    
    query += " ORDER BY fecha DESC, id DESC"
    
    df = _read_sql(query, conn, params)
    if len(df):
        df = _sanitize_dataframe(
            df,
            [
                "total_pesos",
                "total_usd",
                "total_pct",
                "total_pct_se",
                "total_pct_re",
                "pct_postventa",
                "pct_servicios",
                "pct_repuestos",
            ],
        )
    conn.close()
    
    return df


def delete_gastos_por_clasificacion(clasificaciones):
    """Elimina todos los gastos cuya clasificación coincida con la lista proporcionada."""
    if not clasificaciones:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    placeholders = ",".join("?" for _ in clasificaciones)
    query = f"DELETE FROM gastos WHERE clasificacion IN ({placeholders})"
    _execute(cursor, query, clasificaciones)
    eliminados = cursor.rowcount
    conn.commit()
    conn.close()
    return eliminados

def get_gasto_by_id(gasto_id):
    """Obtiene un gasto por su ID"""
    conn = get_connection()
    cursor = conn.cursor()
    _execute(cursor, "SELECT * FROM gastos WHERE id = ?", (gasto_id,))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return dict(row)
    return None

def insert_gasto(gasto_data):
    """Inserta un nuevo gasto"""
    conn = get_connection()
    cursor = conn.cursor()
    
    insert_sql = """
        INSERT INTO gastos (
            mes, fecha, sucursal, area, pct_postventa, pct_servicios,
            pct_repuestos, tipo, clasificacion, proveedor, total_pesos,
            total_usd, total_pct, total_pct_se, total_pct_re, detalles
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        gasto_data.get('mes'),
        gasto_data.get('fecha'),
        gasto_data.get('sucursal'),
        gasto_data.get('area'),
        gasto_data.get('pct_postventa', 0),
        gasto_data.get('pct_servicios', 0),
        gasto_data.get('pct_repuestos', 0),
        gasto_data.get('tipo'),
        gasto_data.get('clasificacion'),
        gasto_data.get('proveedor'),
        gasto_data.get('total_pesos'),
        gasto_data.get('total_usd', 0),
        gasto_data.get('total_pct', 0),
        gasto_data.get('total_pct_se', 0),
        gasto_data.get('total_pct_re', 0),
        gasto_data.get('detalles')
    )

    if USE_POSTGRES:
        insert_sql = insert_sql.replace("?)", "?) RETURNING id")
        _execute(cursor, insert_sql, params)
        gasto_id = cursor.fetchone()["id"]
    else:
        _execute(cursor, insert_sql, params)
        gasto_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    return gasto_id

def update_gasto(gasto_id, gasto_data):
    """Actualiza un gasto existente"""
    conn = get_connection()
    cursor = conn.cursor()
    
    _execute(cursor, """
        UPDATE gastos SET
            mes = ?, fecha = ?, sucursal = ?, area = ?, pct_postventa = ?,
            pct_servicios = ?, pct_repuestos = ?, tipo = ?, clasificacion = ?,
            proveedor = ?, total_pesos = ?, total_usd = ?, total_pct = ?,
            total_pct_se = ?, total_pct_re = ?, detalles = ?
        WHERE id = ?
    """, (
        gasto_data.get('mes'),
        gasto_data.get('fecha'),
        gasto_data.get('sucursal'),
        gasto_data.get('area'),
        gasto_data.get('pct_postventa', 0),
        gasto_data.get('pct_servicios', 0),
        gasto_data.get('pct_repuestos', 0),
        gasto_data.get('tipo'),
        gasto_data.get('clasificacion'),
        gasto_data.get('proveedor'),
        gasto_data.get('total_pesos'),
        gasto_data.get('total_usd', 0),
        gasto_data.get('total_pct', 0),
        gasto_data.get('total_pct_se', 0),
        gasto_data.get('total_pct_re', 0),
        gasto_data.get('detalles'),
        gasto_id
    ))
    
    conn.commit()
    conn.close()

def delete_gasto(gasto_id):
    """Elimina un gasto"""
    conn = get_connection()
    cursor = conn.cursor()
    _execute(cursor, "DELETE FROM gastos WHERE id = ?", (gasto_id,))
    conn.commit()
    conn.close()

def get_plantillas_gastos(activas_only=False):
    """Obtiene todas las plantillas de gastos.

    Es tolerante a bases donde la tabla aún no existe (p. ej. instalaciones antiguas):
    en ese caso crea la tabla y devuelve un DataFrame vacío.
    """
    conn = get_connection()
    try:
        query = "SELECT * FROM plantillas_gastos"
        if activas_only:
            # En Postgres la columna es BOOLEAN, en SQLite es INTEGER (0/1)
            if USE_POSTGRES:
                query += " WHERE activa = TRUE"
            else:
                query += " WHERE activa = 1"
        query += " ORDER BY nombre"

        try:
            df = _read_sql(query, conn)
        except Exception:
            # Si la tabla no existe o hay un error de esquema, intentamos crearla
            cursor = conn.cursor()
            try:
                if USE_POSTGRES:
                    _execute(cursor, PLANTILLAS_TABLE_PG)
                else:
                    _execute(cursor, PLANTILLAS_TABLE_SQLITE)
                conn.commit()
            except Exception:
                # Si esto también falla, devolvemos DataFrame vacío para no romper la app
                pass
            df = pd.DataFrame()

        return df
    finally:
        try:
            conn.close()
        except Exception:
            pass

def get_plantilla_gasto_by_id(plantilla_id):
    """Obtiene una plantilla de gasto por su ID"""
    conn = get_connection()
    cursor = conn.cursor()
    _execute(cursor, "SELECT * FROM plantillas_gastos WHERE id = ?", (plantilla_id,))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return dict(row)
    return None

def insert_plantilla_gasto(plantilla_data):
    """Inserta una nueva plantilla de gasto"""
    conn = get_connection()
    cursor = conn.cursor()
    
    insert_sql = """
        INSERT INTO plantillas_gastos (
            nombre, descripcion, sucursal, area, pct_postventa, pct_servicios,
            pct_repuestos, tipo, clasificacion, proveedor, detalles, activa
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        plantilla_data.get('nombre'),
        plantilla_data.get('descripcion'),
        plantilla_data.get('sucursal'),
        plantilla_data.get('area'),
        plantilla_data.get('pct_postventa', 0),
        plantilla_data.get('pct_servicios', 0),
        plantilla_data.get('pct_repuestos', 0),
        plantilla_data.get('tipo'),
        plantilla_data.get('clasificacion'),
        plantilla_data.get('proveedor'),
        plantilla_data.get('detalles'),
        plantilla_data.get('activa', 1)
    )
    
    if USE_POSTGRES:
        insert_sql = insert_sql.replace("?)", "?) RETURNING id")
        _execute(cursor, insert_sql, params)
        plantilla_id = cursor.fetchone()["id"]
    else:
        _execute(cursor, insert_sql, params)
        plantilla_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    return plantilla_id

def update_plantilla_gasto(plantilla_id, plantilla_data):
    """Actualiza una plantilla de gasto existente"""
    conn = get_connection()
    cursor = conn.cursor()
    
    _execute(cursor, """
        UPDATE plantillas_gastos SET
            nombre = ?, descripcion = ?, sucursal = ?, area = ?,
            pct_postventa = ?, pct_servicios = ?, pct_repuestos = ?,
            tipo = ?, clasificacion = ?, proveedor = ?, detalles = ?,
            activa = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (
        plantilla_data.get('nombre'),
        plantilla_data.get('descripcion'),
        plantilla_data.get('sucursal'),
        plantilla_data.get('area'),
        plantilla_data.get('pct_postventa', 0),
        plantilla_data.get('pct_servicios', 0),
        plantilla_data.get('pct_repuestos', 0),
        plantilla_data.get('tipo'),
        plantilla_data.get('clasificacion'),
        plantilla_data.get('proveedor'),
        plantilla_data.get('detalles'),
        plantilla_data.get('activa', 1),
        plantilla_id
    ))
    
    conn.commit()
    conn.close()

def delete_plantilla_gasto(plantilla_id):
    """Elimina una plantilla de gasto"""
    conn = get_connection()
    cursor = conn.cursor()
    _execute(cursor, "DELETE FROM plantillas_gastos WHERE id = ?", (plantilla_id,))
    conn.commit()
    conn.close()

def exportar_plantillas_gastos():
    """Exporta todas las plantillas de gastos a un diccionario (para JSON)"""
    df = get_plantillas_gastos()
    
    # Convertir a lista de diccionarios, excluyendo columnas que no son necesarias
    plantillas = []
    for _, row in df.iterrows():
        plantilla = {
            'nombre': row.get('nombre', ''),
            'descripcion': row.get('descripcion', '') if pd.notna(row.get('descripcion')) else '',
            'sucursal': row.get('sucursal', '') if pd.notna(row.get('sucursal')) else None,
            'area': row.get('area', '') if pd.notna(row.get('area')) else None,
            'pct_postventa': float(row.get('pct_postventa', 0)) if pd.notna(row.get('pct_postventa')) else 0.0,
            'pct_servicios': float(row.get('pct_servicios', 0)) if pd.notna(row.get('pct_servicios')) else 0.0,
            'pct_repuestos': float(row.get('pct_repuestos', 0)) if pd.notna(row.get('pct_repuestos')) else 0.0,
            'tipo': row.get('tipo', '') if pd.notna(row.get('tipo')) else None,
            'clasificacion': row.get('clasificacion', '') if pd.notna(row.get('clasificacion')) else None,
            'proveedor': row.get('proveedor', '') if pd.notna(row.get('proveedor')) else None,
            'detalles': row.get('detalles', '') if pd.notna(row.get('detalles')) else None,
            'activa': bool(row.get('activa', True)) if pd.notna(row.get('activa')) else True
        }
        plantillas.append(plantilla)
    
    return plantillas

def importar_plantillas_gastos(plantillas_data, sobrescribir=False):
    """
    Importa plantillas de gastos desde una lista de diccionarios
    
    Args:
        plantillas_data: Lista de diccionarios con los datos de las plantillas
        sobrescribir: Si True, actualiza plantillas existentes con el mismo nombre. Si False, las omite.
    
    Returns:
        dict con 'importadas', 'actualizadas', 'omitidas', 'errores'
    """
    resultado = {
        'importadas': 0,
        'actualizadas': 0,
        'omitidas': 0,
        'errores': []
    }
    
    df_existentes = get_plantillas_gastos()
    nombres_existentes = set(df_existentes['nombre'].str.lower()) if len(df_existentes) > 0 else set()
    
    for idx, plantilla_data in enumerate(plantillas_data):
        try:
            nombre = plantilla_data.get('nombre', '').strip()
            if not nombre:
                resultado['errores'].append(f"Plantilla {idx + 1}: Nombre vacío")
                continue
            
            nombre_lower = nombre.lower()
            
            # Verificar si ya existe
            if nombre_lower in nombres_existentes:
                if sobrescribir:
                    # Buscar la plantilla existente por nombre
                    plantilla_existente = df_existentes[df_existentes['nombre'].str.lower() == nombre_lower]
                    if len(plantilla_existente) > 0:
                        plantilla_id = plantilla_existente.iloc[0]['id']
                        update_plantilla_gasto(plantilla_id, plantilla_data)
                        resultado['actualizadas'] += 1
                    else:
                        # Si no se encuentra, crear nueva
                        insert_plantilla_gasto(plantilla_data)
                        resultado['importadas'] += 1
                        nombres_existentes.add(nombre_lower)
                else:
                    resultado['omitidas'] += 1
            else:
                # Crear nueva plantilla
                insert_plantilla_gasto(plantilla_data)
                resultado['importadas'] += 1
                nombres_existentes.add(nombre_lower)
        except Exception as e:
            resultado['errores'].append(f"Plantilla '{plantilla_data.get('nombre', 'Sin nombre')}': {str(e)}")
    
    return resultado


def inferir_plantillas_gastos_desde_historial(sobrescribir=False):
    """
    Genera o actualiza plantillas de gastos a partir del historial en la tabla 'gastos'.

    Agrupa por (sucursal, area, tipo, clasificacion, proveedor) y toma el registro
    más reciente de cada grupo como configuración estándar de porcentajes.

    Args:
        sobrescribir: si es True, actualiza plantillas existentes con el mismo nombre.

    Returns:
        dict con 'creadas' y 'actualizadas'
    """
    conn = get_connection()
    try:
        df = _read_sql(
            """
            SELECT
                sucursal,
                area,
                tipo,
                clasificacion,
                proveedor,
                pct_postventa,
                pct_servicios,
                pct_repuestos,
                fecha,
                created_at
            FROM gastos
            """,
            conn,
        )
        if df is None or len(df) == 0:
            return {"creadas": 0, "actualizadas": 0}

        df = df.copy()

        # Clave lógica por combinación
        for col in ["sucursal", "area", "tipo", "clasificacion", "proveedor"]:
            if col not in df.columns:
                df[col] = None
        df["clave"] = (
            df["sucursal"].fillna("").astype(str).str.strip().str.upper()
            + "|"
            + df["area"].fillna("").astype(str).str.strip().str.upper()
            + "|"
            + df["tipo"].fillna("").astype(str).str.strip().str.upper()
            + "|"
            + df["clasificacion"].fillna("").astype(str).str.strip().str.upper()
            + "|"
            + df["proveedor"].fillna("").astype(str).str.strip().str.upper()
        )

        # Orden temporal para elegir el registro "más reciente"
        df["fecha_orden"] = pd.to_datetime(df.get("fecha"), errors="coerce")
        if "created_at" in df.columns:
            df["created_at_orden"] = pd.to_datetime(df.get("created_at"), errors="coerce")
        else:
            df["created_at_orden"] = df["fecha_orden"]

        df = df.sort_values(["clave", "fecha_orden", "created_at_orden"])
        df_latest = df.groupby("clave", as_index=False).tail(1)

        # Plantillas existentes para decidir crear/actualizar
        df_existentes = get_plantillas_gastos()
        nombres_existentes = (
            set(df_existentes["nombre"].str.lower()) if len(df_existentes) > 0 else set()
        )

        resultado = {"creadas": 0, "actualizadas": 0, "errores": 0}

        for _, row in df_latest.iterrows():
            suc = row.get("sucursal")
            area = row.get("area")
            tipo = row.get("tipo")
            clas = row.get("clasificacion")
            prov = row.get("proveedor")

            def _norm(value, vacio):
                if pd.isna(value) or value is None:
                    return vacio
                s = str(value).strip()
                return s if s else vacio

            nombre = " | ".join(
                [
                    _norm(prov, "SIN PROVEEDOR"),
                    _norm(clas, "SIN CLASIF"),
                    _norm(suc, "SIN SUC"),
                    _norm(area, "SIN AREA"),
                ]
            )

            plantilla_data = {
                "nombre": nombre,
                "descripcion": "Generada desde historial de gastos",
                "sucursal": suc,
                "area": area,
                "pct_postventa": float(row.get("pct_postventa") or 0.0),
                "pct_servicios": float(row.get("pct_servicios") or 0.0),
                "pct_repuestos": float(row.get("pct_repuestos") or 0.0),
                "tipo": tipo,
                "clasificacion": clas,
                "proveedor": prov,
                "detalles": None,
                # Usar tipo compatible con la base: bool para Postgres, 1/0 para SQLite
                "activa": True if USE_POSTGRES else 1,
            }

            nombre_lower = nombre.lower()
            try:
                if nombre_lower in nombres_existentes:
                    if sobrescribir and len(df_existentes) > 0:
                        fila = df_existentes[
                            df_existentes["nombre"].str.lower() == nombre_lower
                        ]
                        if len(fila) > 0:
                            plantilla_id = int(fila.iloc[0]["id"])
                            update_plantilla_gasto(plantilla_id, plantilla_data)
                            resultado["actualizadas"] += 1
                else:
                    insert_plantilla_gasto(plantilla_data)
                    nombres_existentes.add(nombre_lower)
                    resultado["creadas"] += 1
            except Exception:
                # En caso de error de tipos/esquema en alguna fila, la omitimos
                resultado["errores"] += 1

        return resultado
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _limpiar_valor_monetario(valor):
    """Convierte valores monetarios en texto a float (ej: 'US $556,00' -> 556.0, '-US $700,00' -> -700.0)"""
    if pd.isna(valor):
        return 0.0
    
    # Si ya es numérico, retornarlo
    if isinstance(valor, (int, float)):
        return float(valor)
    
    # Convertir a string y limpiar
    valor_str = str(valor).strip()
    
    # Detectar y preservar signo negativo
    es_negativo = valor_str.startswith('-') or valor_str.startswith('(')
    if es_negativo:
        valor_str = valor_str.lstrip('-(').rstrip(')')
    
    # Remover prefijos comunes (US $, $, etc.)
    valor_str = re.sub(r'^[US\s\$]*', '', valor_str, flags=re.IGNORECASE)
    
    # Reemplazar comas por puntos (formato europeo: 556,00 -> 556.00)
    # Si tiene punto y coma, la coma es decimal
    if ',' in valor_str and '.' in valor_str:
        # Formato: 1.234,56 -> quitar puntos (miles), reemplazar coma por punto
        valor_str = valor_str.replace('.', '').replace(',', '.')
    elif ',' in valor_str:
        # Solo coma: puede ser decimal o miles
        # Si hay más de 3 dígitos antes de la coma, probablemente es separador de miles
        partes = valor_str.split(',')
        if len(partes[0]) > 3:
            valor_str = valor_str.replace(',', '')
        else:
            valor_str = valor_str.replace(',', '.')
    
    # Limpiar espacios y convertir
    valor_str = valor_str.strip()
    try:
        resultado = float(valor_str) if valor_str else 0.0
        # Aplicar signo negativo si estaba presente
        return -resultado if es_negativo else resultado
    except:
        return 0.0

def import_ventas_from_excel(excel_path):
    """Importa ventas desde un archivo Excel"""
    try:
        # Verificar que la hoja existe
        excel_file = pd.ExcelFile(excel_path)
        if "REGISTRO VENTAS" not in excel_file.sheet_names:
            raise ValueError(f"La hoja 'REGISTRO VENTAS' no existe. Hojas disponibles: {excel_file.sheet_names}")
        
        df = pd.read_excel(excel_path, sheet_name="REGISTRO VENTAS")
        
        if len(df) == 0:
            return 0
        
        # Mostrar columnas encontradas para debug
        print(f"Columnas encontradas en REGISTRO VENTAS: {list(df.columns)}")
        
        # Detectar si el Excel tiene formato de exportación (nombres de columnas de BD)
        es_formato_exportacion = 'tipo_re_se' in df.columns or 'total' in df.columns
        
        count = 0
        errores = []
        for idx, row in df.iterrows():
            try:
                # Convertir fecha - buscar columna de fecha (puede tener diferentes nombres)
                fecha_col = None
                for col in df.columns:
                    if 'fecha' in col.lower():
                        fecha_col = col
                        break
                
                if fecha_col is None or pd.isna(row.get(fecha_col)):
                    continue
                
                fecha = pd.to_datetime(row[fecha_col]).date()
                
                # Buscar columnas por nombre flexible (case-insensitive)
                def get_col_value(df, row, posibles_nombres, default=None):
                    # Primero buscar coincidencia exacta
                    for nombre in posibles_nombres:
                        if nombre in df.columns:
                            val = row.get(nombre)
                            if pd.notna(val):
                                return val
                    # Si no encuentra, buscar case-insensitive
                    for nombre in posibles_nombres:
                        for col in df.columns:
                            if col.upper() == nombre.upper():
                                val = row.get(col)
                                if pd.notna(val):
                                    return val
                    return default
                
                # Si es formato de exportación, usar valores directamente
                if es_formato_exportacion:
                    mes_val = row.get('mes', '')
                    if pd.notna(mes_val) and mes_val:
                        mes = str(mes_val)
                    else:
                        mes = fecha.strftime("%B%y")
                    
                    tipo_comprobante = str(row.get('tipo_comprobante', 'FACTURA VENTA')).strip() if pd.notna(row.get('tipo_comprobante')) else 'FACTURA VENTA'
                    total = float(row.get('total', 0)) if pd.notna(row.get('total')) else 0
                    
                    tipo_re_se = str(row.get('tipo_re_se', 'SE')).strip().upper() if pd.notna(row.get('tipo_re_se')) else 'SE'
                    # Validar que sea RE o SE
                    if tipo_re_se not in ['RE', 'SE']:
                        tipo_re_se = 'SE'  # Por defecto SE si no es válido
                    
                    venta_data = {
                        'mes': mes,
                        'fecha': fecha,
                        'sucursal': str(row.get('sucursal', '')).strip() if pd.notna(row.get('sucursal')) else None,
                        'cliente': str(row.get('cliente', '')).strip() if pd.notna(row.get('cliente')) else None,
                        'pin': str(row.get('pin', '')).strip() if pd.notna(row.get('pin')) else None,
                        'comprobante': str(row.get('comprobante', '')).strip() if pd.notna(row.get('comprobante')) else None,
                        'tipo_comprobante': tipo_comprobante,
                        'trabajo': str(row.get('trabajo', 'EXTERNO')).strip() if pd.notna(row.get('trabajo')) else 'EXTERNO',
                        'n_comprobante': str(row.get('n_comprobante', '')).strip() if pd.notna(row.get('n_comprobante')) else None,
                        'tipo_re_se': tipo_re_se,
                        'mano_obra': float(row.get('mano_obra', 0)) if pd.notna(row.get('mano_obra')) else 0,
                        'asistencia': float(row.get('asistencia', 0)) if pd.notna(row.get('asistencia')) else 0,
                        'repuestos': float(row.get('repuestos', 0)) if pd.notna(row.get('repuestos')) else 0,
                        'terceros': float(row.get('terceros', 0)) if pd.notna(row.get('terceros')) else 0,
                        'descuento': float(row.get('descuento', 0)) if pd.notna(row.get('descuento')) else 0,
                        'total': total,
                        'detalles': str(row.get('detalles', '')).strip() if pd.notna(row.get('detalles')) else None,
                    }
                    if "costo_repuestos" in df.columns and pd.notna(row.get("costo_repuestos")):
                        venta_data["costo_repuestos"] = float(row.get("costo_repuestos"))
                else:
                    # Formato original: buscar columnas con nombres descriptivos
                    tipo_comprobante = str(get_col_value(df, row, ['Tipo Comprobante', 'TIPO COMPROBANTE'], 'FACTURA VENTA')).strip() or 'FACTURA VENTA'
                    total = _limpiar_valor_monetario(get_col_value(df, row, ['Total', 'TOTAL'], 0))
                    
                    tipo_re_se_val = get_col_value(df, row, ['Tipo (RE o SE)', 'TIPO (RE o SE)', 'Tipo RE o SE'], 'SE')
                    tipo_re_se = str(tipo_re_se_val).strip().upper() if tipo_re_se_val else 'SE'
                    # Validar que sea RE o SE
                    if tipo_re_se not in ['RE', 'SE']:
                        tipo_re_se = 'SE'  # Por defecto SE si no es válido
                    
                    venta_data = {
                        'mes': fecha.strftime("%B%y"),
                        'fecha': fecha,
                        'sucursal': str(get_col_value(df, row, ['Sucursal', 'SUCURSAL'], '')).strip() or None,
                        'cliente': str(get_col_value(df, row, ['Cliente', 'CLIENTE'], '')).strip() or None,
                        'pin': str(get_col_value(df, row, ['PIN'], '')).strip() or None,
                        'comprobante': str(get_col_value(df, row, ['Comprobante', 'COMPROBANTE'], '')).strip() or None,
                        'tipo_comprobante': tipo_comprobante,
                        'trabajo': str(get_col_value(df, row, ['Trabajo', 'TRABAJO'], 'EXTERNO')).strip() or 'EXTERNO',
                        'n_comprobante': str(get_col_value(df, row, ['N° Comprobante', "N' Comprobante", 'N COMPROBANTE', 'N Comprobante'], '')).strip() or None,
                        'tipo_re_se': tipo_re_se,
                        'mano_obra': _limpiar_valor_monetario(get_col_value(df, row, ['Mano de Obra', 'MANO DE OBRA'], 0)),
                        'asistencia': _limpiar_valor_monetario(get_col_value(df, row, ['Asistencia', 'ASISTENCIA'], 0)),
                        'repuestos': _limpiar_valor_monetario(get_col_value(df, row, ['Repuestos', 'REPUESTOS'], 0)),
                        'terceros': _limpiar_valor_monetario(get_col_value(df, row, ['Terceros', 'TERCEROS'], 0)),
                        'descuento': _limpiar_valor_monetario(get_col_value(df, row, ['Descuento', 'DESCUENTO'], 0)),
                        'total': total,
                        'detalles': str(get_col_value(df, row, ['Detalles', 'DETALLES'], '')).strip() or None
                    }
                
                insert_venta(venta_data)
                count += 1
            except Exception as e:
                errores.append(f"Fila {idx + 2}: {str(e)}")
                continue
        
        if errores:
            print(f"Errores durante la importación: {errores[:5]}")  # Mostrar solo los primeros 5
        
        return count
    except Exception as e:
        raise Exception(f"Error al importar ventas: {str(e)}")


def import_ventas_from_oficio_pdf(pdf_path: Path) -> tuple[int, list[str], dict]:
    """Importa ventas desde el PDF «Ventas detalladas (por comprobante)» de Autologica.

    - Un registro por comprobante (bloques duplicados en el PDF se fusionan).
    - **total** y **repuestos** (línea «Total repuestos» del PDF): neto sin IVA (÷ 1,21) solo si
      aplica a ese comprobante (usuario **ERASJIDO** o sucursal **2** Comodoro); si no, importes
      tal cual. **servicio** (SE) = total − repuestos con esos valores ya alineados.
    - **costo_repuestos** = costo FIFO del PDF (3.er número en «Total repuestos»).
    - **usuario_autologica**, **utilidad_ventas_pct**, **utilidad_ventas_monto** en columnas propias;
      **detalles** solo conserva texto de OR si existe.
    - Notas de crédito se detectan por el texto del comprobante.
    """
    import importlib.util
    import sys

    root = Path(__file__).resolve().parent
    mod_name = "parse_oficio_pdf_to_csv"
    spec = importlib.util.spec_from_file_location(
        mod_name, root / "scripts" / "parse_oficio_pdf_to_csv.py"
    )
    mod = importlib.util.module_from_spec(spec)
    # Registrar en sys.modules antes de exec_module: @dataclass necesita
    # sys.modules[cls.__module__] al definir las clases del archivo.
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)

    df_raw = mod.parse_pdf(Path(pdf_path))
    df = mod.to_app_sales_csv(df_raw, use_total_neto_iva21=True)
    if df.empty:
        return 0, [], {}

    stats = {
        "comprobantes": len(df),
        "segmentos_pdf": df_raw.attrs.get("segmentos_con_comprobante"),
        "fusionados": df_raw.attrs.get("segmentos_fusionados"),
    }

    errores: list[str] = []
    count = 0
    for idx, row in df.iterrows():
        try:
            suc = row.get("sucursal")
            if suc is None or (isinstance(suc, float) and pd.isna(suc)):
                errores.append(f"Fila {idx + 1}: sin sucursal mapeada")
                continue
            if str(suc).strip() == "" or str(suc).lower() == "nan":
                errores.append(f"Fila {idx + 1}: sin sucursal mapeada")
                continue
            fecha = pd.to_datetime(row["fecha"]).date()
            tc = str(row.get("tipo_comprobante") or "FACTURA VENTA")
            cr_pdf = row.get("costo_repuestos")

            def _opt_float_pdf(key: str):
                v = row.get(key)
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                try:
                    return float(v)
                except (TypeError, ValueError):
                    return None

            ua = row.get("usuario_autologica")
            if ua is not None and not (isinstance(ua, float) and pd.isna(ua)):
                ua = str(ua).strip() or None
            else:
                ua = None

            venta_data = {
                "mes": fecha.strftime("%B"),
                "fecha": fecha,
                "sucursal": str(suc).strip(),
                "cliente": None
                if pd.isna(row.get("cliente"))
                else str(row.get("cliente")).strip(),
                "pin": None,
                "comprobante": tc,
                "tipo_comprobante": tc,
                "trabajo": "EXTERNO",
                "n_comprobante": None
                if pd.isna(row.get("n_comprobante"))
                else str(row.get("n_comprobante")).strip(),
                "tipo_re_se": str(row.get("tipo_re_se") or "RE").strip().upper(),
                "mano_obra": 0.0,
                "asistencia": 0.0,
                "repuestos": float(row.get("repuestos") or 0),
                "terceros": 0.0,
                "descuento": 0.0,
                "total": float(row.get("total") or 0),
                "detalles": None
                if pd.isna(row.get("detalles"))
                else str(row.get("detalles"))[:2000],
                "archivo_comprobante": None,
                "campo_taller": None,
                "costo_repuestos": float(cr_pdf)
                if cr_pdf is not None and not (isinstance(cr_pdf, float) and pd.isna(cr_pdf))
                else None,
                "usuario_autologica": ua,
                "utilidad_ventas_pct": _opt_float_pdf("utilidad_ventas_pct"),
                "utilidad_ventas_monto": _opt_float_pdf("utilidad_ventas_monto"),
            }
            insert_venta(venta_data)
            count += 1
        except Exception as e:
            errores.append(f"Fila {idx + 1}: {str(e)}")
            continue

    return count, errores, stats


def import_gastos_from_excel(excel_path):
    """Importa gastos desde un archivo Excel"""
    try:
        # Verificar que la hoja existe
        excel_file = pd.ExcelFile(excel_path)
        if "REGISTRO GASTOS" not in excel_file.sheet_names:
            raise ValueError(f"La hoja 'REGISTRO GASTOS' no existe. Hojas disponibles: {excel_file.sheet_names}")
        
        df = pd.read_excel(excel_path, sheet_name="REGISTRO GASTOS")
        
        if len(df) == 0:
            return 0
        
        # Mostrar columnas encontradas para debug
        print(f"Columnas encontradas en REGISTRO GASTOS: {list(df.columns)}")
        
        # Detectar si el Excel tiene formato de exportación (nombres de columnas de BD)
        es_formato_exportacion = 'total_usd' in df.columns or 'total_pct_se' in df.columns
        
        # Buscar columnas por nombre flexible (case-insensitive)
        def get_col_value(df, row, posibles_nombres, default=None):
            # Primero buscar coincidencia exacta
            for nombre in posibles_nombres:
                if nombre in df.columns:
                    val = row.get(nombre)
                    if pd.notna(val):
                        return val
            # Si no encuentra, buscar case-insensitive
            for nombre in posibles_nombres:
                for col in df.columns:
                    if col.upper() == nombre.upper():
                        val = row.get(col)
                        if pd.notna(val):
                            return val
            return default
        
        count = 0
        errores = []
        for idx, row in df.iterrows():
            try:
                # Convertir fecha - buscar columna de fecha
                fecha_col = None
                for col in df.columns:
                    if 'fecha' in col.lower():
                        fecha_col = col
                        break
                
                if fecha_col is None or pd.isna(row.get(fecha_col)):
                    continue
                
                fecha = pd.to_datetime(row[fecha_col]).date()
                
                # Si es formato de exportación (nombres de columnas de BD), usar directamente
                if es_formato_exportacion:
                    total_usd = float(row.get('total_usd', 0)) if pd.notna(row.get('total_usd')) else 0
                    total_pct_se = float(row.get('total_pct_se', 0)) if pd.notna(row.get('total_pct_se')) else 0
                    total_pct_re = float(row.get('total_pct_re', 0)) if pd.notna(row.get('total_pct_re')) else 0
                    total_pct = float(row.get('total_pct', 0)) if pd.notna(row.get('total_pct')) else (total_pct_se + total_pct_re)
                    pct_postventa = float(row.get('pct_postventa', 0)) if pd.notna(row.get('pct_postventa')) else 0
                    pct_servicios = float(row.get('pct_servicios', 0)) if pd.notna(row.get('pct_servicios')) else 0
                    pct_repuestos = float(row.get('pct_repuestos', 0)) if pd.notna(row.get('pct_repuestos')) else 0
                else:
                    # Formato original: buscar Total USD (puede venir como texto "US $20,87")
                    total_usd_val = get_col_value(df, row, ['Total USD', 'TOTAL USD', 'Total US$'], 0)
                    total_usd = _limpiar_valor_monetario(total_usd_val)
                    
                    # Buscar porcentajes (pueden venir sin espacio: %POSTVENTA)
                    pct_postventa = _limpiar_valor_monetario(get_col_value(df, row, ['% Postventa', '%POSTVENTA', '% POSTVENTA'], 0))
                    pct_servicios = _limpiar_valor_monetario(get_col_value(df, row, ['% Servicios', '%SERVICIOS', '% SERVICIOS'], 0))
                    pct_repuestos = _limpiar_valor_monetario(get_col_value(df, row, ['% Repuestos', '%REPUESTOS', '% REPUESTOS'], 0))
                    
                    total_pct = total_usd * (pct_postventa / 100) if pct_postventa > 0 else 0
                    total_pct_se = total_pct * (pct_servicios / 100) if pct_servicios > 0 else 0
                    total_pct_re = total_pct * (pct_repuestos / 100) if pct_repuestos > 0 else 0
                    
                    # Si hay valores en TOTAL %SE y TOTAL %RE, usarlos directamente
                    total_pct_se_val = get_col_value(df, row, ['TOTAL %SE', 'Total %SE', 'TOTAL % SE'], None)
                    total_pct_re_val = get_col_value(df, row, ['TOTAL %RE', 'Total %RE', 'TOTAL % RE'], None)
                    
                    if total_pct_se_val is not None:
                        total_pct_se = _limpiar_valor_monetario(total_pct_se_val)
                    if total_pct_re_val is not None:
                        total_pct_re = _limpiar_valor_monetario(total_pct_re_val)
                
                if total_usd == 0 and total_pct_se == 0 and total_pct_re == 0:
                    continue  # Saltar si no hay valores
                
                # Obtener otros campos según el formato
                if es_formato_exportacion:
                    mes_val = row.get('mes', '')
                    if pd.notna(mes_val) and mes_val:
                        mes = str(mes_val)
                    else:
                        mes = fecha.strftime("%B%y")
                    
                    gasto_data = {
                        'mes': mes,
                        'fecha': fecha,
                        'sucursal': str(row.get('sucursal', '')).strip() if pd.notna(row.get('sucursal')) else None,
                        'area': str(row.get('area', '')).strip() if pd.notna(row.get('area')) else None,
                        'pct_postventa': pct_postventa,
                        'pct_servicios': pct_servicios,
                        'pct_repuestos': pct_repuestos,
                        'tipo': str(row.get('tipo', '')).strip() if pd.notna(row.get('tipo')) else None,
                        'clasificacion': str(row.get('clasificacion', '')).strip() if pd.notna(row.get('clasificacion')) else None,
                        'proveedor': str(row.get('proveedor', '')).strip() if pd.notna(row.get('proveedor')) else None,
                        'total_pesos': float(row.get('total_pesos', 0)) if pd.notna(row.get('total_pesos')) else None,
                        'total_usd': total_usd,
                        'total_pct': total_pct,
                        'total_pct_se': total_pct_se,
                        'total_pct_re': total_pct_re,
                        'detalles': str(row.get('detalles', '')).strip() if pd.notna(row.get('detalles')) else None
                    }
                else:
                    gasto_data = {
                        'mes': fecha.strftime("%B%y"),
                        'fecha': fecha,
                        'sucursal': str(get_col_value(df, row, ['Sucursal', 'SUCURSAL'], '')).strip() or None,
                        'area': str(get_col_value(df, row, ['Area', 'Área', 'AREA'], '')).strip() or None,
                        'pct_postventa': pct_postventa,
                        'pct_servicios': pct_servicios,
                        'pct_repuestos': pct_repuestos,
                        'tipo': str(get_col_value(df, row, ['Tipo', 'TIPO'], '')).strip() or None,
                        'clasificacion': str(get_col_value(df, row, ['Clasificación', 'Clasificacion', 'CLASIFICACION', 'CLASIFICACIÓN'], '')).strip() or None,
                        'proveedor': str(get_col_value(df, row, ['Proveedor', 'PROVEEDOR'], '')).strip() or None,
                        'total_pesos': _limpiar_valor_monetario(get_col_value(df, row, ['Total Pesos', 'TOTAL PESOS'], 0)) or None,
                        'total_usd': total_usd,
                        'total_pct': total_pct,
                        'total_pct_se': total_pct_se,
                        'total_pct_re': total_pct_re,
                        'detalles': str(get_col_value(df, row, ['Detalles', 'DETALLES'], '')).strip() or None
                    }
                
                insert_gasto(gasto_data)
                count += 1
            except Exception as e:
                errores.append(f"Fila {idx + 2}: {str(e)}")
                continue
        
        if errores:
            print(f"Errores durante la importación: {errores[:5]}")  # Mostrar solo los primeros 5
        
        return count
    except Exception as e:
        raise Exception(f"Error al importar gastos: {str(e)}")

def eliminar_todos_los_registros(eliminar_plantillas=False):
    """
    Elimina todos los registros de ventas y gastos de la base de datos.
    
    Args:
        eliminar_plantillas: Si es True, también elimina las plantillas de gastos.
    
    Returns:
        dict: Diccionario con el conteo de registros eliminados
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Contar registros antes de eliminar
        _execute(cursor, "SELECT COUNT(*) FROM ventas")
        count_ventas = _fetch_scalar(cursor, 0)
        
        _execute(cursor, "SELECT COUNT(*) FROM gastos")
        count_gastos = _fetch_scalar(cursor, 0)
        
        count_plantillas = 0
        if eliminar_plantillas:
            _execute(cursor, "SELECT COUNT(*) FROM plantillas_gastos")
            count_plantillas = _fetch_scalar(cursor, 0)
        
        # Eliminar registros
        _execute(cursor, "DELETE FROM ventas")
        _execute(cursor, "DELETE FROM gastos")
        
        if eliminar_plantillas:
            _execute(cursor, "DELETE FROM plantillas_gastos")
        
        # Resetear los autoincrement IDs
        if USE_POSTGRES:
            tablas = ["ventas", "gastos"]
            if eliminar_plantillas:
                tablas.append("plantillas_gastos")
            for tabla in tablas:
                _execute(
                    cursor,
                    f"SELECT setval(pg_get_serial_sequence('{tabla}', 'id'), COALESCE((SELECT MAX(id) FROM {tabla}), 1), true)"
                )
        else:
            _execute(
                cursor,
                "DELETE FROM sqlite_sequence WHERE name IN ('ventas', 'gastos', 'plantillas_gastos')"
            )
        
        conn.commit()
        
        return {
            'ventas_eliminadas': count_ventas,
            'gastos_eliminados': count_gastos,
            'plantillas_eliminadas': count_plantillas if eliminar_plantillas else 0,
            'exito': True
        }
    except Exception as e:
        conn.rollback()
        return {
            'exito': False,
            'error': str(e)
        }
    finally:
        conn.close()

def guardar_analisis_ia(tipo_analisis: str, fuente: str, contenido: str, metadata: dict = None):
    """
    Guarda un análisis de IA en el historial.
    
    Args:
        tipo_analisis: Tipo de análisis ('tendencia', 'prediccion', 'anomalia', 'recomendacion', 'alerta')
        fuente: Fuente del análisis ('gemini' o 'local')
        contenido: Contenido del análisis (texto)
        metadata: Diccionario con metadatos adicionales (se guarda como JSON)
    
    Returns:
        int: ID del registro guardado
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    metadata_json = json.dumps(metadata) if metadata else None
    
    insert_sql = """
        INSERT INTO historial_analisis_ia (tipo_analisis, fuente, contenido, metadata)
        VALUES (?, ?, ?, ?)
    """
    params = (tipo_analisis, fuente, contenido, metadata_json)

    if USE_POSTGRES:
        insert_sql = insert_sql.replace("?)", "?) RETURNING id")
        _execute(cursor, insert_sql, params)
        registro_id = cursor.fetchone()["id"]
    else:
        _execute(cursor, insert_sql, params)
        registro_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    return registro_id

def get_historial_analisis_ia(limit: int = 50, tipo_analisis: str = None, fuente: str = None):
    """
    Obtiene el historial de análisis de IA.
    
    Args:
        limit: Número máximo de registros a obtener
        tipo_analisis: Filtrar por tipo ('tendencia', 'prediccion', 'anomalia', 'recomendacion', 'alerta')
        fuente: Filtrar por fuente ('gemini' o 'local')
    
    Returns:
        pd.DataFrame: DataFrame con el historial
    """
    conn = get_connection()
    
    query = "SELECT * FROM historial_analisis_ia WHERE 1=1"
    params = []
    
    if tipo_analisis:
        query += " AND tipo_analisis = ?"
        params.append(tipo_analisis)
    
    if fuente:
        query += " AND fuente = ?"
        params.append(fuente)
    
    query += " ORDER BY fecha_hora DESC LIMIT ?"
    params.append(limit)
    
    df = _read_sql(query, conn, params)
    conn.close()
    
    return df

def get_resumen_mensual_analisis_ia(mes: int = None, año: int = None):
    """
    Obtiene un resumen mensual de los análisis de IA, agrupando por tipo y mostrando lo más relevante.
    
    Args:
        mes: Mes a resumir (1-12). Si es None, usa el mes actual.
        año: Año a resumir. Si es None, usa el año actual.
    
    Returns:
        dict: Diccionario con el resumen mensual organizado por tipo
    """
    from datetime import datetime
    
    if mes is None:
        mes = datetime.now().month
    if año is None:
        año = datetime.now().year
    
    conn = get_connection()
    
    # Obtener todos los registros del mes
    if USE_POSTGRES:
        query = """
            SELECT * FROM historial_analisis_ia 
            WHERE EXTRACT(YEAR FROM fecha_hora) = %s 
              AND EXTRACT(MONTH FROM fecha_hora) = %s
            ORDER BY fecha_hora DESC
        """
        params = (año, mes)
    else:
        query = """
            SELECT * FROM historial_analisis_ia 
            WHERE strftime('%Y', fecha_hora) = ? 
              AND strftime('%m', fecha_hora) = ?
            ORDER BY fecha_hora DESC
        """
        params = (str(año), f"{mes:02d}")
    
    df = _read_sql(query, conn, params)
    conn.close()
    
    if len(df) == 0:
        return {
            'mes': mes,
            'año': año,
            'total_registros': 0,
            'resumen': {}
        }
    
    # Convertir fecha_hora a datetime
    df['fecha_hora'] = pd.to_datetime(df['fecha_hora'])
    
    resumen = {
        'mes': mes,
        'año': año,
        'total_registros': len(df),
        'resumen': {}
    }
    
    # Agrupar por tipo de análisis
    for tipo in ['tendencia', 'prediccion', 'anomalia', 'recomendacion', 'alerta']:
        df_tipo = df[df['tipo_analisis'] == tipo]
        
        if len(df_tipo) == 0:
            continue
        
        # Para recomendaciones y alertas, agrupar por contenido similar (usar los más frecuentes)
        if tipo in ['recomendacion', 'alerta']:
            # Contar frecuencia de cada contenido
            conteo = df_tipo['contenido'].value_counts()
            
            # Obtener las top 5 más frecuentes
            top_contenidos = conteo.head(5).to_dict()
            
            resumen['resumen'][tipo] = {
                'total': len(df_tipo),
                'top_items': [
                    {
                        'contenido': contenido,
                        'frecuencia': freq,
                        'fuentes': df_tipo[df_tipo['contenido'] == contenido]['fuente'].unique().tolist(),
                        'ultima_aparicion': df_tipo[df_tipo['contenido'] == contenido]['fecha_hora'].max().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    for contenido, freq in top_contenidos.items()
                ]
            }
        else:
            # Para otros tipos, mostrar todos pero agrupar por fuente
            resumen['resumen'][tipo] = {
                'total': len(df_tipo),
                'por_fuente': {
                    'gemini': len(df_tipo[df_tipo['fuente'] == 'gemini']),
                    'local': len(df_tipo[df_tipo['fuente'] == 'local'])
                },
                'items': df_tipo[['contenido', 'fuente', 'fecha_hora']].to_dict('records')
            }
    
    return resumen

def crear_backup_db():
    """
    Crea un backup de la base de datos.
    
    Returns:
        str: Ruta del archivo de backup creado, o None si falla
    """
    if USE_POSTGRES:
        # Los backups deben realizarse desde el servicio gestionado
        return None
    if not DB_PATH.exists():
        return None
    
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = BACKUP_DIR / f"postventa_backup_{timestamp}.db"
        
        # Copiar archivo de base de datos
        shutil.copy2(DB_PATH, backup_path)
        
        return str(backup_path)
    except Exception as e:
        print(f"Error al crear backup: {e}")
        return None

def restaurar_backup_db(backup_path: str):
    """
    Restaura la base de datos desde un backup.
    
    Args:
        backup_path: Ruta del archivo de backup
    
    Returns:
        bool: True si se restauró correctamente, False en caso contrario
    """
    if USE_POSTGRES:
        return False
    try:
        backup_file = Path(backup_path)
        if not backup_file.exists():
            return False
        
        # Hacer backup del archivo actual si existe
        if DB_PATH.exists():
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            old_backup = BACKUP_DIR / f"postventa_old_{timestamp}.db"
            shutil.copy2(DB_PATH, old_backup)
        
        # Restaurar desde backup
        shutil.copy2(backup_file, DB_PATH)
        
        return True
    except Exception as e:
        print(f"Error al restaurar backup: {e}")
        return False

def listar_backups():
    """
    Lista todos los backups disponibles.
    
    Returns:
        list: Lista de diccionarios con información de cada backup
    """
    backups = []
    
    if USE_POSTGRES:
        return backups
    
    if not BACKUP_DIR.exists():
        return backups
    
    for backup_file in sorted(BACKUP_DIR.glob("postventa_backup_*.db"), reverse=True):
        try:
            stat = backup_file.stat()
            backups.append({
                'nombre': backup_file.name,
                'ruta': str(backup_file),
                'fecha': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                'tamaño': stat.st_size
            })
        except Exception:
            continue
    
    return backups

def exportar_db_a_bytes():
    """
    Exporta la base de datos completa a bytes para descarga.
    
    Returns:
        bytes: Contenido de la base de datos, o None si falla
    """
    if USE_POSTGRES or not DB_PATH.exists():
        return None
    
    try:
        with open(DB_PATH, 'rb') as f:
            return f.read()
    except Exception as e:
        print(f"Error al exportar base de datos: {e}")
        return None

def importar_db_desde_bytes(db_bytes: bytes):
    """
    Importa una base de datos desde bytes.
    
    Args:
        db_bytes: Contenido de la base de datos en bytes
    
    Returns:
        bool: True si se importó correctamente, False en caso contrario
    """
    if USE_POSTGRES:
        return False
    try:
        # Hacer backup del archivo actual si existe
        if DB_PATH.exists():
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            old_backup = BACKUP_DIR / f"postventa_old_{timestamp}.db"
            shutil.copy2(DB_PATH, old_backup)
        
        # Escribir nueva base de datos
        with open(DB_PATH, 'wb') as f:
            f.write(db_bytes)
        
        return True
    except Exception as e:
        print(f"Error al importar base de datos: {e}")
        return False
