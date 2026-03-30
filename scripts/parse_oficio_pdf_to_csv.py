#!/usr/bin/env python3
"""Parsea reportes PDF de Autologica (ventas repuestos por comprobante) a CSV.

Metodología alineada con control manual:

- Notas de crédito: importes ya vienen negativos en el PDF; se suman como restan.
- Comprobantes duplicados en el PDF: un solo registro por ``punto-venta + número`` (último
  bloque fusionado).
- Repuestos: primera cifra de ``Total repuestos`` = venta a **precio de lista** (misma base
  que el detalle del informe).
- RE / SE: canal mostrador vs servicio.
- Columna **Otros (\*)**: tercer número de ``Total comprobante`` cuando hay 5+ importes.
  ``total_sin_impuestos − venta_precio_lista`` incluye mano de obra + otros + segunda columna
  de repuestos del renglón; solo coincide con **Otros (\*)** cuando no hay más conceptos.
- IVA: para usuario **ERASJIDO** y sucursal **2** (Comodoro), ``total_con_impuestos_neto_iva21``
  = ``total_con_impuestos / 1,21`` (neto sin IVA; no es ``total × 0,79``). La línea **Total repuestos**
  debe usar la **misma** regla al importar con neto IVA; si no, ``total`` y ``repuestos`` quedarían
  en bases distintas (efecto tipo “IVA atrapado” en el desglose).

Uso:
    python scripts/parse_oficio_pdf_to_csv.py \
        --input oficio32.pdf \
        --out-normalizado imports/oficio32_normalizado.csv \
        --out-ventas imports/oficio32_ventas_import.csv
"""
from __future__ import annotations

import argparse
import re
from collections import defaultdict
from dataclasses import dataclass, asdict
from pathlib import Path

import pandas as pd
import pdfplumber


def parse_number(raw: str | None) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def normalize_ws(text: str) -> str:
    return re.sub(r"[ \t]+", " ", text).strip()


def extract_footer_grand_totals(path: Path) -> list[float] | None:
    """Lee la línea ``TOTAL`` del pie del reporte (última aparición). Columnas numéricas en orden."""
    with pdfplumber.open(path) as pdf:
        full_text = "\n".join((p.extract_text() or "") for p in pdf.pages)
    matches = list(re.finditer(r"TOTAL\s+([\d,\.\s\-]+)", full_text))
    if not matches:
        return None
    raw = matches[-1].group(1)
    nums = [parse_number(x) for x in re.findall(r"-?\d[\d,]*(?:\.\d+)?", raw)]
    nums = [n for n in nums if n is not None]
    return nums if nums else None


def map_sucursal(code: str | None, usuario: str | None) -> str | None:
    code = (code or "").strip()
    usuario_u = (usuario or "").upper()
    # Regla de negocio comentada: ERASJIDO se interpreta como Rio Gallegos
    if "ERASJIDO" in usuario_u:
        return "RIO GALLEGOS"
    if code == "1":
        return "RIO GRANDE"
    if code == "2":
        return "COMODORO"
    return None


def parse_total_comprobante_fields(
    raw: str,
) -> tuple[float | None, float | None, float | None, float | None]:
    """Devuelve (total_sin_impuestos, impuestos_total, total_con_impuestos, otros_columna).

    Con **5 o más** números en la línea ``Total comprobante``, el orden habitual es:
    ``repuestos (lista)``, ``repuestos (final)``, **Otros (\*)**, IVA, total con impuestos.
    El **tercer** número es la columna *Otros (\*)* del PDF (recargos, trabajos, etc.).
    Con **4** números suele faltar la columna Otros (p. ej. solo rep, rep, IVA, total).
    """
    nums = [parse_number(x) for x in re.findall(r"-?\d[\d,]*(?:\.\d+)?", raw or "")]
    nums = [n for n in nums if n is not None]
    if not nums:
        return None, None, None, None
    total_con_imp = nums[-1]
    otros = nums[2] if len(nums) >= 5 else None
    if len(nums) == 1:
        return total_con_imp, 0.0, total_con_imp, otros
    impuestos = nums[-2]
    total_sin_imp = total_con_imp - impuestos
    return total_sin_imp, impuestos, total_con_imp, otros


def total_comprobante_neto_sin_iva21(
    total_con_imp: float | None, usuario: str | None, sucursal_codigo: str | None
) -> float | None:
    """Importe llevado a neto sin IVA 21 % (÷ 1,21) donde aplica.

    Usado para el **total del comprobante** y, al importar en neto, para la línea **Total repuestos**,
    para que ambos queden en la misma base.

    Regla de negocio: facturas de usuario **ERASJIDO** (Río Gallegos) y **sucursal 2**
    (Comodoro) vienen con IVA en el importe del PDF; en neto se divide por 1,21.
    **No** es ``total * 0,79`` (eso no revierte el IVA 21 %).
    """
    if total_con_imp is None:
        return None
    u = (usuario or "").upper()
    if "ERASJIDO" in u or (sucursal_codigo or "").strip() == "2":
        return float(total_con_imp) / 1.21
    return float(total_con_imp)


@dataclass
class Row:
    fecha_emision: str | None
    comprobante_texto: str | None
    or_relacionada: str | None
    tipo_cuenta: str | None
    titular: str | None
    usuario: str | None
    sucursal_codigo: str | None
    sucursal_nombre: str | None
    total_repuestos_neto: float | None
    costo_fifo: float | None
    utilidad_sobre_ventas_monto: float | None
    utilidad_sobre_ventas_pct: float | None
    utilidad_sobre_costo_pct: float | None
    total_comprobante_campos_raw: str | None
    total_sin_impuestos_estimado: float | None
    impuestos_total_estimado: float | None
    total_con_impuestos_estimado: float | None
    venta_precio_lista: float | None
    otros_columna_pdf: float | None
    total_con_impuestos_neto_iva21: float | None
    page: int


def parse_segment(segment: str, page: int) -> Row | None:
    seg = normalize_ws(segment)

    # OR relacionada / Comprobante (el PDF puede venir en ambos órdenes)
    or_rel = None
    comprobante_texto = None
    m_comp_or = re.search(
        r"Comprobante:\s*(.*?)\s*OR relacionada:\s*(.*?)\s*Titular:",
        seg,
        flags=re.IGNORECASE,
    )
    if m_comp_or:
        comprobante_texto = m_comp_or.group(1).strip()
        or_rel = m_comp_or.group(2).strip()
    else:
        m_or_comp = re.search(
            r"OR relacionada:\s*(.*?)\s*Comprobante:\s*(.*?)\s*(?:Fecha contable|Titular:)",
            seg,
            flags=re.IGNORECASE,
        )
        if m_or_comp:
            or_rel = m_or_comp.group(1).strip()
            comprobante_texto = m_or_comp.group(2).strip()
    if not (or_rel or comprobante_texto):
        return None

    # Fecha
    m_fecha = re.search(r"Fecha de emisión:\s*(\d{1,2}/\d{1,2}/\d{4})", seg)
    fecha = m_fecha.group(1) if m_fecha else None

    # Tipo cuenta RE/SE
    m_tipo = re.search(r"Tipo de cuenta:\s*(SE|RE)\b", seg, flags=re.IGNORECASE)
    tipo = m_tipo.group(1).upper() if m_tipo else None

    # Titular / usuario / sucursal
    m_tit = re.search(r"Titular:\s*(.*?)\s*(?:Celular:|Concesionario:)", seg, flags=re.IGNORECASE)
    titular = m_tit.group(1).strip() if m_tit else None

    m_usr = re.search(r"Usuario:\s*(.*?)\s*(?:Descuentos|\(\*\)|$)", seg, flags=re.IGNORECASE)
    usuario = None
    if m_usr:
        usuario = (m_usr.group(1) or m_usr.group(2) or "").strip()

    m_suc = re.search(r"Sucursal:\s*(\d+)", seg, flags=re.IGNORECASE)
    suc_code = m_suc.group(1).strip() if m_suc else None
    suc_nombre = map_sucursal(suc_code, usuario)

    # Totales repuestos (si el PDF repite el comprobante, suele haber más de una línea: usar la última)
    tr_matches = list(re.finditer(r"Total repuestos\s*([^\n]+)", seg, flags=re.IGNORECASE))
    m_total_rep = tr_matches[-1] if tr_matches else None
    total_rep = None
    costo_fifo = None
    venta_precio_lista = None
    if m_total_rep:
        nums_rep = [parse_number(x) for x in re.findall(r"-?\d[\d,]*(?:\.\d+)?", m_total_rep.group(1))]
        nums_rep = [n for n in nums_rep if n is not None]
        if nums_rep:
            # Primera cifra del total = venta a precio de lista (informe detallado por comprobante)
            total_rep = nums_rep[0]
            venta_precio_lista = nums_rep[0]
        if len(nums_rep) >= 3:
            costo_fifo = nums_rep[2]

    # Bloque utilidades
    m_util = re.search(
        r"% Utilidad \(sobre costo\)\s*:\s*([-\d\.,]+)\s*"
        r"% Utilidad \(sobre ventas\)\s*:\s*([-\d\.,]+)\s*"
        r"Utilidad \(sobre ventas\)\s*:\s*([-\d\.,]+)",
        seg,
        flags=re.IGNORECASE,
    )
    util_monto = util_pct_ventas = util_pct_costo = None
    if m_util:
        util_pct_costo = parse_number(m_util.group(1))
        util_pct_ventas = parse_number(m_util.group(2))
        util_monto = parse_number(m_util.group(3))

    # Totales comprobante: última línea si hay varias (misma factura en otra página con Otros / ajuste)
    tc_matches = list(re.finditer(r"Total comprobante\s*([^\n]+)", seg, flags=re.IGNORECASE))
    m_total_comp = tc_matches[-1] if tc_matches else None
    total_comp_raw = normalize_ws(m_total_comp.group(1)) if m_total_comp else None
    total_sin_imp, impuestos, total_con_imp, otros_col = parse_total_comprobante_fields(total_comp_raw or "")

    # Fallback: cuando no viene "Total repuestos", usar total sin impuestos estimado.
    if total_rep is None and total_sin_imp is not None:
        total_rep = total_sin_imp
    if venta_precio_lista is None and total_rep is not None:
        venta_precio_lista = total_rep

    total_neto_iva21 = total_comprobante_neto_sin_iva21(total_con_imp, usuario, suc_code)

    # Fallback de costo FIFO usando utilidad sobre ventas% si está disponible.
    if costo_fifo is None and total_rep is not None and util_pct_ventas is not None:
        costo_fifo = total_rep * (1 - (util_pct_ventas / 100.0))

    return Row(
        fecha_emision=fecha,
        comprobante_texto=comprobante_texto,
        or_relacionada=or_rel,
        tipo_cuenta=tipo,
        titular=titular,
        usuario=usuario,
        sucursal_codigo=suc_code,
        sucursal_nombre=suc_nombre,
        total_repuestos_neto=total_rep,
        costo_fifo=costo_fifo,
        utilidad_sobre_ventas_monto=util_monto,
        utilidad_sobre_ventas_pct=util_pct_ventas,
        utilidad_sobre_costo_pct=util_pct_costo,
        total_comprobante_campos_raw=total_comp_raw,
        total_sin_impuestos_estimado=total_sin_imp,
        impuestos_total_estimado=impuestos,
        total_con_impuestos_estimado=total_con_imp,
        venta_precio_lista=venta_precio_lista,
        otros_columna_pdf=otros_col,
        total_con_impuestos_neto_iva21=total_neto_iva21,
        page=page,
    )


def _strip_page_markers(segment: str) -> str:
    return re.sub(r"__PAGE_\d+__\s*", "", segment)


def _clave_comprobante(segment: str) -> str | None:
    """Punto de venta + número de comprobante (ej. 0002-00011945). Sin duplicar por sucursal."""
    m = re.search(r"Comprobante:\s*([^\n]+?)\s*OR relacionada:", segment, flags=re.IGNORECASE)
    if not m:
        return None
    inner = re.search(r"(\d{4})\s*-\s*(\d{8})", m.group(1))
    if not inner:
        return None
    return f"{inner.group(1)}-{inner.group(2)}"


def _last_page_before(full_text: str, pos: int) -> int:
    """Página PDF donde empieza el comprobante: última marca __PAGE_N__ antes de pos."""
    before = full_text[:pos]
    markers = list(re.finditer(r"__PAGE_(\d+)__", before))
    if not markers:
        return 1
    return int(markers[-1].group(1))


def parse_pdf(path: Path) -> pd.DataFrame:
    """Une todas las páginas y parte por inicio de comprobante (evita cortes por paginación).

    El informe Autologica puede **repetir el mismo comprobante** en otra página (p. ej. solo
    Otros / totales). Se agrupa por ``punto-venta + n°`` y se **fusionan** los segmentos en
    orden de página; ``Total repuestos`` / ``Total comprobante`` se toman de la **última**
    aparición en el texto fusionado (total con otros conceptos).
    """
    rows: list[dict] = []
    with pdfplumber.open(path) as pdf:
        chunks: list[str] = []
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            chunks.append(f"\n__PAGE_{i}__\n{text}")
        full_text = "\n".join(chunks)

    # Solo inicio de comprobante real (no "Fecha de emisión entre" del filtro del reporte)
    split_re = re.compile(
        r"(?=Fecha de emisión:\s*\d{1,2}/\d{1,2}/\d{4}\s+Fecha contable)",
        flags=re.IGNORECASE,
    )
    boundaries = [m.start() for m in split_re.finditer(full_text)]
    starts = [0] + boundaries
    ends = boundaries + [len(full_text)]

    # Agrupar por clave de comprobante; mismo N° puede aparecer 2+ veces en el PDF
    por_clave: dict[str, list[tuple[int, int, str]]] = defaultdict(list)
    sin_clave = 0
    for start, end in zip(starts, ends):
        seg = full_text[start:end]
        if "Comprobante:" not in seg:
            continue
        clave = _clave_comprobante(seg)
        if not clave:
            sin_clave += 1
            clave = f"__sin_clave_{start}__"
        por_clave[clave].append((start, end, seg))

    segmentos_extra_por_clave = sum(len(v) - 1 for v in por_clave.values() if len(v) > 1)

    elegidos: list[tuple[int, str, str]] = []
    for clave, lista in por_clave.items():
        lista_sorted = sorted(lista, key=lambda x: x[0])
        start0 = lista_sorted[0][0]
        merged = "\n".join(x[2] for x in lista_sorted)
        elegidos.append((start0, merged, clave))
    elegidos.sort(key=lambda x: x[0])

    for start0, seg, clave in elegidos:
        page_start = _last_page_before(full_text, start0)
        seg_parse = normalize_ws(_strip_page_markers(seg))
        row = parse_segment(seg_parse, page_start)
        if row is not None:
            d = asdict(row)
            d["comprobante_clave"] = clave
            rows.append(d)

    df = pd.DataFrame(rows)
    if len(df):
        df.attrs["segmentos_con_comprobante"] = sum(len(por_clave[k]) for k in por_clave)
        df.attrs["comprobantes_unicos"] = len(por_clave)
        df.attrs["segmentos_fusionados"] = segmentos_extra_por_clave
        df.attrs["segmentos_sin_clave"] = sin_clave
    return df


def _tipo_comprobante_desde_texto(comprobante: str | None) -> str:
    u = (comprobante or "").upper()
    if "JD" in u and "CREDITO" in u:
        return "NOTA DE CREDITO JD"
    if "CREDITO" in u:
        return "NOTA CREDITO"
    return "FACTURA VENTA"


def to_app_sales_csv(df: pd.DataFrame, *, use_total_neto_iva21: bool = False) -> pd.DataFrame:
    """Genera CSV de apoyo para importación a tabla ventas.

    Si ``use_total_neto_iva21`` es True y existe la columna en el parseo, ``total`` pasa a ser
    el total neto sin IVA (÷ 1,21 en ERASJIDO y sucursal 2). La columna ``repuestos`` recibe
    la **misma** conversión para no mezclar neto en total y bruto en repuestos.
    """
    if df.empty:
        return pd.DataFrame()

    out = pd.DataFrame()
    out["fecha"] = pd.to_datetime(df["fecha_emision"], format="%d/%m/%Y", errors="coerce").dt.strftime("%Y-%m-%d")
    out["sucursal"] = df["sucursal_nombre"]
    out["cliente"] = df["titular"]
    out["n_comprobante"] = df["comprobante_texto"].astype(str).str.extract(r"(\d{4}\s*-\s*\d{8})", expand=False)
    out["tipo_re_se"] = df["tipo_cuenta"]
    out["repuestos"] = df["total_repuestos_neto"]
    # Costo FIFO (3.er número Total repuestos) o derivado por % utilidad sobre ventas en el parseo
    out["costo_repuestos"] = df["costo_fifo"]
    # Total facturación del comprobante (último valor en Total comprobante; suma = pie TOTAL del PDF)
    out["total"] = df["total_con_impuestos_estimado"].where(
        df["total_con_impuestos_estimado"].notna(), df["total_repuestos_neto"]
    )
    if "total_con_impuestos_neto_iva21" in df.columns:
        out["total_neto_iva21"] = df["total_con_impuestos_neto_iva21"]
    if use_total_neto_iva21 and "total_neto_iva21" in out.columns:
        out["total"] = out["total_neto_iva21"].where(out["total_neto_iva21"].notna(), out["total"])
        out["repuestos"] = df.apply(
            lambda r: (
                total_comprobante_neto_sin_iva21(
                    float(r["total_repuestos_neto"])
                    if r.get("total_repuestos_neto") is not None
                    and not pd.isna(r["total_repuestos_neto"])
                    else None,
                    r.get("usuario"),
                    r.get("sucursal_codigo"),
                )
            ),
            axis=1,
        )
    out["tipo_comprobante"] = df["comprobante_texto"].apply(_tipo_comprobante_desde_texto)
    out["comprobante"] = df["comprobante_texto"]
    out["detalles"] = (
        "OR: " + df["or_relacionada"].fillna("")
        + " | Usuario: " + df["usuario"].fillna("")
        + " | Costo FIFO: " + df["costo_fifo"].fillna(0).astype(str)
        + " | Util$ ventas: " + df["utilidad_sobre_ventas_monto"].fillna(0).astype(str)
        + " | Util% ventas: " + df["utilidad_sobre_ventas_pct"].fillna(0).astype(str)
    )
    # Servicio (no repuestos) para importar a ventas.servicio; RE = 0
    tre = out["tipo_re_se"].astype(str).str.upper()
    out["servicio"] = (out["total"] - out["repuestos"].fillna(0)).where(tre == "SE", 0.0)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Ruta al PDF de oficio")
    parser.add_argument("--out-normalizado", required=True, help="CSV normalizado de salida")
    parser.add_argument("--out-ventas", required=True, help="CSV adaptado para tabla ventas")
    args = parser.parse_args()

    input_path = Path(args.input)
    out_norm = Path(args.out_normalizado)
    out_ventas = Path(args.out_ventas)
    out_norm.parent.mkdir(parents=True, exist_ok=True)
    out_ventas.parent.mkdir(parents=True, exist_ok=True)

    df = parse_pdf(input_path)
    df.to_csv(out_norm, index=False)

    df_ventas = to_app_sales_csv(df)
    df_ventas.to_csv(out_ventas, index=False)

    print(f"PDF parseado: {input_path}")
    print(f"Registros (comprobantes únicos): {len(df)}")
    if len(df) and getattr(df, "attrs", None):
        sc = df.attrs.get("segmentos_con_comprobante")
        uq = df.attrs.get("comprobantes_unicos")
        fus = df.attrs.get("segmentos_fusionados")
        sk = df.attrs.get("segmentos_sin_clave")
        if sc is not None:
            print(
                f"Segmentos con encabezado de comprobante en el PDF: {sc} "
                f"| comprobantes únicos (clave): {uq} | segmentos fusionados por duplicado: {fus}"
            )
        if sk:
            print(f"Segmentos sin clave 0000-00000000 (clave sintética): {sk}")
    if len(df):
        print(f"Sin sucursal mapeada: {int(df['sucursal_nombre'].isna().sum())}")
        print(f"Sin costo FIFO: {int(df['costo_fifo'].isna().sum())}")
        print(f"Sin total repuestos: {int(df['total_repuestos_neto'].isna().sum())}")
        suma_fact = float(df["total_con_impuestos_estimado"].sum(skipna=True))
        print(f"Suma total facturación (comprobantes únicos, sin reimpresiones): {suma_fact:,.2f}")
        footer = extract_footer_grand_totals(input_path)
        if footer and len(footer) >= 6:
            pie = footer[-1]
            diff = abs(suma_fact - pie)
            print(f"Total pie de PDF (última columna): {pie:,.2f}")
            if diff > 0.02:
                print(
                    "NOTA: El pie suele coincidir con la suma de **todos** los bloques del PDF "
                    f"({df.attrs.get('segmentos_con_comprobante', '?')} segmentos), donde el mismo "
                    "comprobante puede aparecer varias veces; esa suma duplica importes. "
                    f"La suma deduplicada (~{suma_fact:,.0f}) es la coherente con {len(df)} comprobantes únicos."
                )
            else:
                print("OK: suma deduplicada coincide con el pie (caso excepcional).")
    print(f"CSV normalizado: {out_norm}")
    print(f"CSV para ventas: {out_ventas}")


if __name__ == "__main__":
    main()
