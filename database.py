"""
Módulo de gestión de base de datos SQLite
"""
import sqlite3
import pandas as pd
import re
from datetime import datetime
from pathlib import Path
import os

DB_PATH = Path("postventa.db")

def get_connection():
    """Obtiene una conexión a la base de datos"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    """Inicializa las tablas de la base de datos"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Tabla de ventas
    cursor.execute("""
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
            descuento REAL DEFAULT 0,
            total REAL NOT NULL,
            detalles TEXT,
            archivo_comprobante TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Agregar columna archivo_comprobante si no existe (para bases de datos existentes)
    try:
        cursor.execute("ALTER TABLE ventas ADD COLUMN archivo_comprobante TEXT")
    except sqlite3.OperationalError:
        pass  # La columna ya existe
    
    # Tabla de gastos
    cursor.execute("""
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
    """)
    
    # Tabla de plantillas de gastos
    cursor.execute("""
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
    """)
    
    conn.commit()
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
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    return df

def get_venta_by_id(venta_id):
    """Obtiene una venta por su ID"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM ventas WHERE id = ?", (venta_id,))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return dict(row)
    return None

def insert_venta(venta_data):
    """Inserta una nueva venta"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO ventas (
            mes, fecha, sucursal, cliente, pin, comprobante, tipo_comprobante,
            trabajo, n_comprobante, tipo_re_se, mano_obra, asistencia,
            repuestos, terceros, descuento, total, detalles, archivo_comprobante
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        venta_data.get('descuento', 0),
        venta_data.get('total', 0),
        venta_data.get('detalles'),
        venta_data.get('archivo_comprobante')
    ))
    
    venta_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    return venta_id

def update_venta(venta_id, venta_data):
    """Actualiza una venta existente"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE ventas SET
            mes = ?, fecha = ?, sucursal = ?, cliente = ?, pin = ?,
            comprobante = ?, tipo_comprobante = ?, trabajo = ?,
            n_comprobante = ?, tipo_re_se = ?, mano_obra = ?,
            asistencia = ?, repuestos = ?, terceros = ?, descuento = ?,
            total = ?, detalles = ?, archivo_comprobante = ?
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
        venta_data.get('descuento', 0),
        venta_data.get('total', 0),
        venta_data.get('detalles'),
        venta_data.get('archivo_comprobante'),
        venta_id
    ))
    
    conn.commit()
    conn.close()

def delete_venta(venta_id):
    """Elimina una venta"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Obtener información de la venta para eliminar archivo adjunto si existe
    cursor.execute("SELECT archivo_comprobante FROM ventas WHERE id = ?", (venta_id,))
    row = cursor.fetchone()
    
    if row and row[0]:
        archivo_path = Path(row[0])
        if archivo_path.exists():
            try:
                archivo_path.unlink()
            except:
                pass
    
    cursor.execute("DELETE FROM ventas WHERE id = ?", (venta_id,))
    conn.commit()
    conn.close()

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
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    return df

def get_gasto_by_id(gasto_id):
    """Obtiene un gasto por su ID"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM gastos WHERE id = ?", (gasto_id,))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return dict(row)
    return None

def insert_gasto(gasto_data):
    """Inserta un nuevo gasto"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO gastos (
            mes, fecha, sucursal, area, pct_postventa, pct_servicios,
            pct_repuestos, tipo, clasificacion, proveedor, total_pesos,
            total_usd, total_pct, total_pct_se, total_pct_re, detalles
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        gasto_data.get('detalles')
    ))
    
    gasto_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    return gasto_id

def update_gasto(gasto_id, gasto_data):
    """Actualiza un gasto existente"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
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
    cursor.execute("DELETE FROM gastos WHERE id = ?", (gasto_id,))
    conn.commit()
    conn.close()

def get_plantillas_gastos(activas_only=False):
    """Obtiene todas las plantillas de gastos"""
    conn = get_connection()
    
    query = "SELECT * FROM plantillas_gastos"
    if activas_only:
        query += " WHERE activa = 1"
    query += " ORDER BY nombre"
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df

def get_plantilla_gasto_by_id(plantilla_id):
    """Obtiene una plantilla de gasto por su ID"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM plantillas_gastos WHERE id = ?", (plantilla_id,))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return dict(row)
    return None

def insert_plantilla_gasto(plantilla_data):
    """Inserta una nueva plantilla de gasto"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO plantillas_gastos (
            nombre, descripcion, sucursal, area, pct_postventa, pct_servicios,
            pct_repuestos, tipo, clasificacion, proveedor, detalles, activa
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        plantilla_data.get('activa', 1)
    ))
    
    plantilla_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    return plantilla_id

def update_plantilla_gasto(plantilla_id, plantilla_data):
    """Actualiza una plantilla de gasto existente"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
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
    cursor.execute("DELETE FROM plantillas_gastos WHERE id = ?", (plantilla_id,))
    conn.commit()
    conn.close()

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
                
                tipo_comprobante = str(get_col_value(df, row, ['Tipo Comprobante', 'TIPO COMPROBANTE'], 'FACTURA VENTA')).strip() or 'FACTURA VENTA'
                total = _limpiar_valor_monetario(get_col_value(df, row, ['Total', 'TOTAL'], 0))
                
                # Si es nota de crédito (pero NO JD), convertir el total a negativo automáticamente
                # NOTA: "NOTA DE CREDITO JD" son pagos recibidos de John Deere, por lo que son POSITIVOS
                # Si el valor ya viene negativo del Excel, no hacer nada. Solo convertir si es positivo.
                es_nota_credito = tipo_comprobante and 'CREDITO' in tipo_comprobante.upper() and 'JD' not in tipo_comprobante.upper()
                if es_nota_credito and total > 0:
                    total = -total
                
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
                    'tipo_re_se': str(get_col_value(df, row, ['Tipo (RE o SE)', 'TIPO (RE o SE)'], 'SE')).strip() or 'SE',
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
                
                # Buscar Total USD (puede venir como texto "US $20,87")
                total_usd_val = get_col_value(df, row, ['Total USD', 'TOTAL USD', 'Total US$'], 0)
                total_usd = _limpiar_valor_monetario(total_usd_val)
                
                if total_usd == 0:
                    continue  # Saltar si no hay total USD
                
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
        cursor.execute("SELECT COUNT(*) FROM ventas")
        count_ventas = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM gastos")
        count_gastos = cursor.fetchone()[0]
        
        count_plantillas = 0
        if eliminar_plantillas:
            cursor.execute("SELECT COUNT(*) FROM plantillas_gastos")
            count_plantillas = cursor.fetchone()[0]
        
        # Eliminar registros
        cursor.execute("DELETE FROM ventas")
        cursor.execute("DELETE FROM gastos")
        
        if eliminar_plantillas:
            cursor.execute("DELETE FROM plantillas_gastos")
        
        # Resetear los autoincrement IDs (opcional, pero útil para empezar desde 1)
        cursor.execute("DELETE FROM sqlite_sequence WHERE name IN ('ventas', 'gastos', 'plantillas_gastos')")
        
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
