"""
Módulo de gestión de base de datos SQLite
Soporta backup automático y bases de datos persistentes para Streamlit Cloud
"""
import sqlite3
import pandas as pd
import re
import json
from datetime import datetime
from pathlib import Path
import os
import shutil

# Detectar si estamos en Streamlit Cloud
IS_STREAMLIT_CLOUD = os.environ.get('STREAMLIT_SERVER_ENVIRONMENT') == 'cloud'

# Configurar ruta de base de datos
# En Streamlit Cloud, intentar usar un directorio persistente si está disponible
if IS_STREAMLIT_CLOUD:
    # Streamlit Cloud tiene un directorio /mount que es persistente
    # Pero no siempre está disponible, así que usamos el directorio actual con backups
    DB_PATH = Path("postventa.db")
    BACKUP_DIR = Path("backups")
    BACKUP_DIR.mkdir(exist_ok=True)
else:
    DB_PATH = Path("postventa.db")
    BACKUP_DIR = Path("backups")
    BACKUP_DIR.mkdir(exist_ok=True)

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
    
    # Tabla de historial de análisis IA
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS historial_analisis_ia (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            tipo_analisis TEXT NOT NULL,
            fuente TEXT NOT NULL,
            contenido TEXT NOT NULL,
            metadata TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                    
                    # Si es nota de crédito (pero NO JD), convertir el total a negativo automáticamente
                    es_nota_credito = tipo_comprobante and 'CREDITO' in tipo_comprobante.upper() and 'JD' not in tipo_comprobante.upper()
                    if es_nota_credito and total > 0:
                        total = -total
                    
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
                        'detalles': str(row.get('detalles', '')).strip() if pd.notna(row.get('detalles')) else None
                    }
                else:
                    # Formato original: buscar columnas con nombres descriptivos
                    tipo_comprobante = str(get_col_value(df, row, ['Tipo Comprobante', 'TIPO COMPROBANTE'], 'FACTURA VENTA')).strip() or 'FACTURA VENTA'
                    total = _limpiar_valor_monetario(get_col_value(df, row, ['Total', 'TOTAL'], 0))
                    
                    # Si es nota de crédito (pero NO JD), convertir el total a negativo automáticamente
                    es_nota_credito = tipo_comprobante and 'CREDITO' in tipo_comprobante.upper() and 'JD' not in tipo_comprobante.upper()
                    if es_nota_credito and total > 0:
                        total = -total
                    
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
    
    cursor.execute("""
        INSERT INTO historial_analisis_ia (tipo_analisis, fuente, contenido, metadata)
        VALUES (?, ?, ?, ?)
    """, (tipo_analisis, fuente, contenido, metadata_json))
    
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
    
    df = pd.read_sql_query(query, conn, params=params)
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
    query = """
        SELECT * FROM historial_analisis_ia 
        WHERE strftime('%Y', fecha_hora) = ? 
        AND strftime('%m', fecha_hora) = ?
        ORDER BY fecha_hora DESC
    """
    
    df = pd.read_sql_query(query, conn, params=(str(año), f"{mes:02d}"))
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
    if not DB_PATH.exists():
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
