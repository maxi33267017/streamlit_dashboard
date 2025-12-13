#!/usr/bin/env python3
"""Migra los datos de postventa.db (SQLite) hacia PostgreSQL.

Uso:
    POSTGRES_URL="postgresql://usuario:pass@host:puerto/db?sslmode=require" \
        python scripts/migrate_sqlite_to_postgres.py

O define las variables individuales:
    export POSTGRES_HOST=...
    export POSTGRES_DB=...
    export POSTGRES_USER=...
    export POSTGRES_PASSWORD=...
    export POSTGRES_PORT=5432
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import psycopg2
import psycopg2.extras


def build_postgres_url() -> str | None:
    direct_url = os.environ.get("POSTGRES_URL") or os.environ.get("DATABASE_URL")
    if direct_url:
        return direct_url

    host = os.environ.get("POSTGRES_HOST")
    database = os.environ.get("POSTGRES_DB")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    port = os.environ.get("POSTGRES_PORT", "5432")
    sslmode = os.environ.get("POSTGRES_SSLMODE", "require")

    if host and database and user and password:
        return f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode={sslmode}"
    return None


POSTGRES_URL = build_postgres_url()
SQLITE_PATH = Path("postventa.db")

if not POSTGRES_URL:
    raise SystemExit("Define POSTGRES_URL (o variables POSTGRES_HOST/DB/USER/PASSWORD)")
if not SQLITE_PATH.exists():
    raise SystemExit("No se encontró postventa.db; cópialo junto al script antes de migrar.")

sqlite_conn = sqlite3.connect(SQLITE_PATH)
sqlite_conn.row_factory = sqlite3.Row
pg_conn = psycopg2.connect(POSTGRES_URL, cursor_factory=psycopg2.extras.RealDictCursor)
pg_conn.autocommit = False
pg_cur = pg_conn.cursor()

TABLES = [
    (
        "ventas",
        [
            "id",
            "mes",
            "fecha",
            "sucursal",
            "cliente",
            "pin",
            "comprobante",
            "tipo_comprobante",
            "trabajo",
            "n_comprobante",
            "tipo_re_se",
            "mano_obra",
            "asistencia",
            "repuestos",
            "terceros",
            "descuento",
            "total",
            "detalles",
            "archivo_comprobante",
            "created_at",
        ],
    ),
    (
        "gastos",
        [
            "id",
            "mes",
            "fecha",
            "sucursal",
            "area",
            "pct_postventa",
            "pct_servicios",
            "pct_repuestos",
            "tipo",
            "clasificacion",
            "proveedor",
            "total_pesos",
            "total_usd",
            "total_pct",
            "total_pct_se",
            "total_pct_re",
            "detalles",
            "created_at",
        ],
    ),
    (
        "plantillas_gastos",
        [
            "id",
            "nombre",
            "descripcion",
            "sucursal",
            "area",
            "pct_postventa",
            "pct_servicios",
            "pct_repuestos",
            "tipo",
            "clasificacion",
            "proveedor",
            "detalles",
            "activa",
            "created_at",
            "updated_at",
        ],
    ),
    (
        "historial_analisis_ia",
        [
            "id",
            "fecha_hora",
            "tipo_analisis",
            "fuente",
            "contenido",
            "metadata",
            "created_at",
        ],
    ),
]


def migrate_table(table: str, columns: list[str]):
    rows = sqlite_conn.execute(
        f"SELECT {', '.join(columns)} FROM {table} ORDER BY id"
    ).fetchall()
    if not rows:
        print(f"- {table}: sin datos, se omite")
        return

    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = (
        f"INSERT INTO {table} ({', '.join(columns)})"
        f" VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING"
    )

    payload = []
    for row in rows:
        values = []
        for col in columns:
            val = row[col]
            if col == "activa" and val is not None:
                values.append(bool(val))
            else:
                values.append(val)
        payload.append(tuple(values))

    psycopg2.extras.execute_batch(pg_cur, insert_sql, payload, page_size=500)
    _reset_sequence(table)
    pg_conn.commit()
    print(f"- {table}: insertados {len(rows)} registros")


def _reset_sequence(table: str):
    pg_cur.execute(
        f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
        f"COALESCE((SELECT MAX(id) FROM {table}), 1), true)"
    )


def main():
    try:
        for table, cols in TABLES:
            migrate_table(table, cols)
    except Exception as exc:
        pg_conn.rollback()
        raise
    finally:
        sqlite_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    main()
