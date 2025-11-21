"""
Aplicación de Gestión de Postventa
Registro de ventas, gastos y reportes con KPIs
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO
from datetime import datetime, date
import os
import shutil
import base64
import json
import re
from pathlib import Path

# Imports opcionales para extracción de PDFs
try:
    import PyPDF2
    PDF2_AVAILABLE = True
except ImportError:
    PDF2_AVAILABLE = False

try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

try:
    from google.cloud import documentai
    from google.oauth2 import service_account
    GOOGLE_DOCAI_AVAILABLE = True
except ImportError:
    GOOGLE_DOCAI_AVAILABLE = False

from database import (
    init_database, insert_venta, insert_gasto,
    get_ventas, get_gastos,
    import_ventas_from_excel, import_gastos_from_excel,
    delete_venta, delete_gasto,
    get_venta_by_id, get_gasto_by_id,
    update_venta, update_gasto,
    get_plantillas_gastos, get_plantilla_gasto_by_id,
    insert_plantilla_gasto, update_plantilla_gasto, delete_plantilla_gasto,
    eliminar_todos_los_registros,
    exportar_plantillas_gastos, importar_plantillas_gastos
)
from gastos_automaticos import obtener_gastos_totales_con_automaticos
from calculos_financieros import (
    calcular_factor_absorcion_servicios,
    calcular_factor_absorcion_repuestos,
    calcular_factor_absorcion_postventa,
    calcular_punto_equilibrio
)
from ai_analysis import get_ai_summary
try:
    from ai_analysis import GEMINI_AVAILABLE
except ImportError:
    GEMINI_AVAILABLE = False

def analizar_texto_pdf(texto: str) -> dict:
    """
    Analiza el texto extraído de un PDF de comprobante y extrae datos estructurados
    usando patrones y reglas básicas (sin IA)
    """
    datos = {
        'cliente': None,
        'numero_comprobante': None,
        'fecha': None,
        'total': 0.0,
        'mano_obra': 0.0,
        'asistencia': 0.0,
        'repuestos': 0.0,
        'terceros': 0.0,
        'descuento': 0.0,
        'pin': None,
        'tipo_re_se': 'SE',  # Por defecto SE
        'sucursal': None,
        'tipo_comprobante': 'FACTURA VENTA',
        'items': []
    }
    
    texto_upper = texto.upper()
    texto_original = texto  # Mantener texto original para búsquedas más precisas
    
    # 1. Extraer CLIENTE (evitar confundirlo con PATAGONIA que es el proveedor y con direcciones)
    # Estrategia general:
    # - El cliente está después de los datos de la empresa (PATAGONIA, CUIT, etc.)
    # - El cliente está ANTES de la dirección (que tiene patrones como números, códigos postales, "Nro.:", etc.)
    # - El cliente es un nombre completo en mayúsculas (razón social o nombre completo)
    # - El cliente puede estar en la misma línea que "FACTURA DE VENTA" o en una línea separada
    
    # Palabras clave que indican datos de la empresa (NO es cliente)
    palabras_empresa = ['PATAGONIA', 'MAQUINARIAS', 'RUTA', 'TIERRA DEL FUEGO', 'ARGENTINA', 'CUIT', '02964', '999-129115', 'Ing. brutos', 'Página']
    
    # Patrones que indican dirección (NO es cliente)
    patrones_direccion = [
        r'Nro\.?\s*:?\s*\d+',  # "Nro.: 138" o "Nro 138"
        r'\(\d{4,5}\)',  # Códigos postales como "(9420)"
        r'\d{4,5}-\d{6,8}',  # Teléfonos como "02964-446019"
        r'CUIT\s*:?\s*\d{2}-\d{8}-\d{1}',  # CUIT del cliente
        r'Forma de pago',  # "Forma de pago CUENTA CORRIENTE"
        r'Entregar a:',  # "Entregar a: Vendedor"
    ]
    
    # Patrón 1: Buscar nombre ANTES de "FACTURA DE VENTA" en la misma línea
    # El cliente puede estar en la misma línea que "FACTURA DE VENTA"
    patron_cliente1 = re.search(r'^([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s]{5,50}?)\s+FACTURA\s+DE\s+VENTA', texto_original, re.MULTILINE | re.IGNORECASE)
    if patron_cliente1:
        cliente = patron_cliente1.group(1).strip()
        cliente = re.sub(r'\s+', ' ', cliente)
        # Verificar que NO sea datos de empresa
        if (len(cliente) > 5 and 
            not any(palabra in cliente.upper() for palabra in palabras_empresa) and
            'FACTURA' not in cliente.upper()):
            datos['cliente'] = cliente.title()
    
    # Patrón 2: Buscar nombre en línea completa que esté ANTES de patrones de dirección
    # Buscar líneas que terminen y luego tengan patrones de dirección en la siguiente línea
    if not datos['cliente']:
        # Buscar nombre que esté antes de una línea con patrones de dirección
        patron_direccion_combinado = '|'.join(patrones_direccion)
        patron_cliente2 = re.search(r'^([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s]{5,50}?)\s*\n\s*(?:' + patron_direccion_combinado + ')', texto_original, re.MULTILINE | re.IGNORECASE)
        if patron_cliente2:
            cliente = patron_cliente2.group(1).strip()
            cliente = re.sub(r'\s+', ' ', cliente)
            # Verificar que NO sea datos de empresa
            if (len(cliente) > 5 and 
                not any(palabra in cliente.upper() for palabra in palabras_empresa) and
                not any(re.search(patron, cliente, re.IGNORECASE) for patron in patrones_direccion)):
                datos['cliente'] = cliente.title()
    
    # Patrón 3: Buscar después de datos de empresa pero antes de "FACTURA DE VENTA" o direcciones
    # Buscar entre datos de empresa (CUIT, Ing. brutos, Página) y antes de "FACTURA" o patrones de dirección
    if not datos['cliente']:
        patron_direccion_combinado = '|'.join(patrones_direccion)
        patron_cliente3 = re.search(r'(?:CUIT:\s*\d{2}-\d{8}-\d{1}|Ing\.\s*brutos|Página:)[\s\S]{0,300}?^([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s]{5,50}?)\s*(?:FACTURA|' + patron_direccion_combinado + ')', texto_original, re.MULTILINE | re.IGNORECASE)
        if patron_cliente3:
            cliente = patron_cliente3.group(1).strip()
            cliente = re.sub(r'\s+', ' ', cliente)
            # Si contiene "FACTURA", extraer solo la parte antes de "FACTURA"
            if 'FACTURA' in cliente.upper():
                cliente = re.split(r'\s+FACTURA', cliente, flags=re.IGNORECASE)[0].strip()
            # Verificar que NO sea datos de empresa o dirección
            if (len(cliente) > 5 and 
                not any(palabra in cliente.upper() for palabra in palabras_empresa) and
                not any(re.search(patron, cliente, re.IGNORECASE) for patron in patrones_direccion)):
                datos['cliente'] = cliente.title()
    
    # Patrón 4: Buscar explícitamente "CLIENTE:"
    if not datos['cliente']:
        patron_cliente4 = re.search(r'CLIENTE\s*:?\s*([A-ZÁÉÍÓÚÑ\s]{5,}?)(?:\n|CUIT|Nro|Fecha)', texto_upper)
        if patron_cliente4:
            cliente = patron_cliente4.group(1).strip()
            cliente = re.sub(r'\s+', ' ', cliente)
            if len(cliente) > 3 and 'PATAGONIA' not in cliente.upper():
                datos['cliente'] = cliente.title()
    
    # 2. Extraer NÚMERO DE COMPROBANTE
    # Buscar patrones como "A 0002 - 00011587" o "# A 0002-00011587"
    patron_numero1 = re.search(r'(?:FACTURA\s+DE\s+VENTA\s+DOL\s*#?\s*|COMPROBANTE|#)\s*([A-Z]?\s*\d{4,6}\s*[-]?\s*\d{4,8})', texto_upper)
    if patron_numero1:
        numero = patron_numero1.group(1).strip()
        numero = re.sub(r'\s+', ' ', numero)
        datos['numero_comprobante'] = numero
    else:
        # Patrón alternativo: "Nro.: 138"
        patron_numero2 = re.search(r'(?:Nro\.?|Número)\s*:?\s*([A-Z]?\s*\d+[\s\-]?\d*)', texto_upper)
        if patron_numero2:
            numero = patron_numero2.group(1).strip()
            numero = re.sub(r'\s+', ' ', numero)
            datos['numero_comprobante'] = numero
    
    # 3. Extraer FECHA
    # Buscar patrones como "Noviembre 18, 2025" o "18/11/2025"
    patron_fecha1 = re.search(r'(\w+\s+\d{1,2},?\s+\d{4})', texto)
    patron_fecha2 = re.search(r'(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})', texto)
    if patron_fecha1:
        datos['fecha'] = patron_fecha1.group(1)
    elif patron_fecha2:
        datos['fecha'] = patron_fecha2.group(1)
    
    # 4. Extraer TOTAL
    # Buscar "TOTAL U$S 85.76" o "TOTAL U$S 2,321.81" (con comas como separador de miles)
    # Mejorar patrón para capturar mejor el total, incluso si hay espacios o saltos de línea
    patron_total = re.search(r'TOTAL\s*(?:U\$S|USD|\$)?\s*:?\s*([\d,\.]+)', texto_upper)
    if patron_total:
        total_str = patron_total.group(1).strip()
        # Manejar formato con comas como separador de miles: "2,321.81" -> "2321.81"
        # Si tiene comas Y punto, las comas son miles y el punto es decimal
        if ',' in total_str and '.' in total_str:
            # Formato: "2,321.81" -> quitar comas, mantener punto
            total_str = total_str.replace(',', '')
        # Si solo tiene comas, pueden ser miles o decimales (asumir miles si hay más de 3 dígitos)
        elif ',' in total_str and '.' not in total_str:
            # Si tiene más de 3 dígitos antes de la coma, probablemente es separador de miles
            if len(total_str.split(',')[0]) > 3:
                total_str = total_str.replace(',', '')
            else:
                # Si tiene pocos dígitos, la coma probablemente es decimal
                total_str = total_str.replace(',', '.')
        try:
            datos['total'] = float(total_str)
        except:
            pass
    else:
        # Patrón alternativo: buscar "TOTAL" en una línea y el monto en la siguiente o misma línea
        patron_total_alt = re.search(r'TOTAL\s*(?:U\$S|USD|\$)?\s*:?\s*[\s\n]+([\d,\.]+)', texto_upper, re.MULTILINE)
        if patron_total_alt:
            total_str = patron_total_alt.group(1).strip()
            if ',' in total_str and '.' in total_str:
                total_str = total_str.replace(',', '')
            elif ',' in total_str and '.' not in total_str:
                if len(total_str.split(',')[0]) > 3:
                    total_str = total_str.replace(',', '')
                else:
                    total_str = total_str.replace(',', '.')
            try:
                datos['total'] = float(total_str)
            except:
                pass
    
    # 5. Extraer PIN (si existe)
    patron_pin = re.search(r'PIN\s*:?\s*(\d+)', texto_upper)
    if patron_pin:
        datos['pin'] = patron_pin.group(1)
        datos['tipo_re_se'] = 'SE'  # Si hay PIN, probablemente es servicio
    
    # 6. Detectar SUCURSAL
    if 'RIO GRANDE' in texto_upper:
        datos['sucursal'] = 'RIO GRANDE'
    elif 'COMODORO' in texto_upper:
        datos['sucursal'] = 'COMODORO'
    elif 'RIO GALLEGOS' in texto_upper:
        datos['sucursal'] = 'RIO GALLEGOS'
    
    # 7. Analizar ITEMS y clasificarlos
    # Buscar todos los items en el texto usando regex más robusto
    # Patrón: código (letras+números) seguido de descripción, cantidad y precio
    # Ejemplo: "AT332908 FILTRO AIRE PRIM J 1.00 U$S 56.60" o "RE504836 FILTRO DE ACEITE RE541420 1.00 U$S 29.16"
    
    # Palabras clave para clasificar items
    palabras_repuestos = ['FILTRO', 'REPUESTO', 'PARTE', 'PIEZA', 'COMPONENTE', 'ACEITE', 'LUBRICANTE', 'AIRE', 'RETEN', 'ANILLO', 'ARANDELA', 'EMPAQUETADURA']
    palabras_mano_obra = ['HORA', 'HORAS', 'MANO DE OBRA', 'TRABAJO', 'REPARACIÓN', 'SERVICIO TÉCNICO', 'MECÁNICO']
    palabras_asistencia = ['ASISTENCIA', 'VISITA', 'ATENCIÓN', 'CONSULTA']
    palabras_terceros = ['FRETE', 'TRANSPORTE', 'FLETE', 'ENVÍO', 'TERCERO', 'EXTERNO']
    
    items_encontrados = []
    
    # 7.1. Buscar items especiales sin código (MANO DE OBRA, ASISTENCIA, etc.)
    # Patrón: "MANO DE OBRA U$S 1,260.00" o "ASISTENCIA U$S 500.00"
    # Mejorar patrón para ser más flexible con espacios y formato
    patron_especiales = re.finditer(r'(MANO\s+DE\s+OBRA|ASISTENCIA|TERCEROS?|FRETE|TRANSPORTE)\s+U\$S\s+([\d,\.]+)', texto_upper, re.MULTILINE)
    for match in patron_especiales:
        descripcion = match.group(1).strip()
        monto_str = match.group(2).strip()
        # Manejar formato con comas como separador de miles
        if ',' in monto_str and '.' in monto_str:
            monto_str = monto_str.replace(',', '')
        elif ',' in monto_str and '.' not in monto_str:
            if len(monto_str.split(',')[0]) > 3:
                monto_str = monto_str.replace(',', '')
            else:
                monto_str = monto_str.replace(',', '.')
        try:
            monto = float(monto_str)
            items_encontrados.append({
                'codigo': None,
                'descripcion': descripcion.title(),
                'cantidad': 1.0,
                'precio': monto,
                'monto': monto
            })
        except:
            pass
    
    # 7.1b. Buscar "MANO DE OBRA" en formato de tabla (puede estar en una línea separada o con espacios variables)
    # Ejemplo: "MANO DE OBRA U$S 1,260.00" o "MANO DE OBRA" en una línea y "U$S 1,260.00" en la siguiente
    # Hacer el patrón más flexible para capturar diferentes formatos
    patron_mano_obra_variantes = [
        r'MANO\s+DE\s+OBRA\s+U\$S\s*([\d,\.]+)',  # Misma línea: "MANO DE OBRA U$S 1,260.00"
        r'MANO\s+DE\s+OBRA\s*\n\s*U\$S\s*([\d,\.]+)',  # Líneas separadas: "MANO DE OBRA\nU$S 1,260.00"
        r'MANO\s+DE\s+OBRA\s+([\d,\.]+)\s*U\$S',  # Formato alternativo: "MANO DE OBRA 1,260.00 U$S"
    ]
    
    for patron_variante in patron_mano_obra_variantes:
        match_mano_obra = re.search(patron_variante, texto_upper, re.MULTILINE | re.DOTALL)
        if match_mano_obra:
            monto_str = match_mano_obra.group(1).strip()
            if ',' in monto_str and '.' in monto_str:
                monto_str = monto_str.replace(',', '')
            elif ',' in monto_str and '.' not in monto_str:
                if len(monto_str.split(',')[0]) > 3:
                    monto_str = monto_str.replace(',', '')
                else:
                    monto_str = monto_str.replace(',', '.')
            try:
                monto = float(monto_str)
                # Verificar que no esté ya en items_encontrados
                if not any(item.get('descripcion', '').upper() == 'MANO DE OBRA' for item in items_encontrados):
                    items_encontrados.append({
                        'codigo': None,
                        'descripcion': 'Mano De Obra',
                        'cantidad': 1.0,
                        'precio': monto,
                        'monto': monto
                    })
                    break  # Si encontramos uno, no buscar más variantes
            except:
                continue
    
    # Buscar items usando regex en todo el texto
    # Patrón mejorado: código (2-10 letras + 4-10 números), descripción, cantidad, U$S, precio
    # Ejemplo: "AT332908 FILTRO AIRE PRIM J 1.00 U$S 56.60" o "RE504836 FILTRO DE ACEITE RE541420 1.00 U$S 29.16"
    # El patrón debe ser más flexible para capturar variaciones en el formato
    
    # 7.2. Buscar items normales con código (repuestos, etc.)
    # Patrón principal: código seguido de descripción (puede tener códigos adicionales), cantidad, U$S, precio
    # Mejorar el patrón para manejar precios con comas como separador de miles: "33.01" o "1,260.00"
    # Hacer el patrón más flexible para códigos que pueden tener letras al final (ej: "AJM2026LITRO") o números al principio (ej: "40M7048")
    patron_items = re.finditer(r'([A-Z0-9]{4,15})\s+([A-ZÁÉÍÓÚÑ0-9\s]{5,80}?)\s+(\d+[,\d]*\.?\d*)\s+U\$S\s+([\d,\.]+)', texto_upper)
    
    for match in patron_items:
        codigo = match.group(1).strip()
        descripcion = match.group(2).strip()
        # Limpiar descripción: quitar códigos adicionales al final (como "RE541420")
        # También quitar números sueltos que puedan ser parte de otro código
        descripcion = re.sub(r'\s+[A-Z]{2,10}\d{4,10}$', '', descripcion).strip()
        descripcion = re.sub(r'\s+\d{4,10}$', '', descripcion).strip()
        # Limpiar espacios múltiples
        descripcion = ' '.join(descripcion.split())
        
        # Procesar cantidad (puede tener comas como separador de miles)
        cantidad_str = match.group(3).replace(',', '')
        cantidad = float(cantidad_str)
        
        # Procesar precio (puede tener comas como separador de miles)
        precio_str = match.group(4).strip()
        if ',' in precio_str and '.' in precio_str:
            precio_str = precio_str.replace(',', '')
        elif ',' in precio_str and '.' not in precio_str:
            if len(precio_str.split(',')[0]) > 3:
                precio_str = precio_str.replace(',', '')
            else:
                precio_str = precio_str.replace(',', '.')
        precio = float(precio_str)
        monto = cantidad * precio
        
        # Solo agregar si la descripción tiene sentido (más de 3 caracteres después de limpiar)
        if len(descripcion) > 3:
            items_encontrados.append({
                'codigo': codigo,
                'descripcion': descripcion.title(),
                'cantidad': cantidad,
                'precio': precio,
                'monto': monto
            })
    
    # 7.3. Si no se encontraron items con el patrón principal, buscar patrón alternativo
    if len(items_encontrados) == 0:
        # Patrón alternativo: buscar líneas con código y precio al final
        lineas = texto.split('\n')
        for linea in lineas:
            linea_upper = linea.upper()
            # Buscar: código, descripción, U$S, precio
            patron_alt = re.search(r'([A-Z0-9]{6,})\s+([A-ZÁÉÍÓÚÑ0-9\s]{5,}?)\s+U\$S\s+([\d,\.]+)', linea_upper)
            if patron_alt:
                codigo = patron_alt.group(1).strip()
                descripcion = patron_alt.group(2).strip()
                # Limpiar descripción
                descripcion = re.sub(r'\s+[A-Z]{2,10}\d{4,10}$', '', descripcion).strip()
                # Procesar monto (puede tener comas como separador de miles)
                monto_str = patron_alt.group(3).strip()
                if ',' in monto_str and '.' in monto_str:
                    monto_str = monto_str.replace(',', '')
                elif ',' in monto_str and '.' not in monto_str:
                    if len(monto_str.split(',')[0]) > 3:
                        monto_str = monto_str.replace(',', '')
                    else:
                        monto_str = monto_str.replace(',', '.')
                monto = float(monto_str)
                
                items_encontrados.append({
                    'codigo': codigo,
                    'descripcion': descripcion.title(),
                    'cantidad': 1.0,
                    'precio': monto,
                    'monto': monto
                })
    
    # Guardar items en datos
    datos['items'] = items_encontrados
    
    # Clasificar items encontrados
    for item in items_encontrados:
        desc = item['descripcion'].upper()
        monto = item['monto']
        
        # Clasificar por palabras clave
        if any(palabra in desc for palabra in palabras_repuestos):
            datos['repuestos'] += monto
        elif any(palabra in desc for palabra in palabras_mano_obra):
            datos['mano_obra'] += monto
        elif any(palabra in desc for palabra in palabras_asistencia):
            datos['asistencia'] += monto
        elif any(palabra in desc for palabra in palabras_terceros):
            datos['terceros'] += monto
        else:
            # Si no se puede clasificar, asumir repuesto por defecto
            datos['repuestos'] += monto
    
    # Detectar tipo RE/SE basado en los items encontrados
    # Si hay mano de obra o asistencia, es SE (servicio)
    if datos['mano_obra'] > 0 or datos['asistencia'] > 0 or datos['pin']:
        datos['tipo_re_se'] = 'SE'
    # Si solo hay repuestos y no hay PIN, probablemente es RE
    elif datos['repuestos'] > 0 and datos['mano_obra'] == 0 and datos['asistencia'] == 0 and not datos['pin']:
        datos['tipo_re_se'] = 'RE'
    
    # 8. Buscar DESCUENTO
    patron_descuento = re.search(r'DESCUENTO\s*:?\s*([\d,\.]+)', texto_upper)
    if patron_descuento:
        descuento_str = patron_descuento.group(1).strip()
        # Manejar formato con comas como separador de miles
        if ',' in descuento_str and '.' in descuento_str:
            descuento_str = descuento_str.replace(',', '')
        elif ',' in descuento_str and '.' not in descuento_str:
            if len(descuento_str.split(',')[0]) > 3:
                descuento_str = descuento_str.replace(',', '')
            else:
                descuento_str = descuento_str.replace(',', '.')
        try:
            datos['descuento'] = float(descuento_str)
        except:
            pass
    
    # 9. Detectar TIPO DE COMPROBANTE
    if 'NOTA DE CREDITO' in texto_upper or 'NOTA DE CRÉDITO' in texto_upper:
        datos['tipo_comprobante'] = 'NOTA DE CREDITO'
    elif 'FACTURA' in texto_upper:
        datos['tipo_comprobante'] = 'FACTURA VENTA'
    
    return datos

def formatear_moneda(valor: float) -> str:
    """Formatea un valor numérico como moneda: $22.423,45"""
    try:
        if pd.isna(valor) or valor is None:
            return "$0,00"
        valor = float(valor)
        # Formatear con punto como separador de miles y coma como decimal
        valor_str = f"{valor:,.2f}"
        # Reemplazar separadores: . por , y , por .
        partes = valor_str.split('.')
        if len(partes) == 2:
            return f"${partes[0].replace(',', '.')},{partes[1]}"
        return f"${valor_str.replace(',', '.')},00"
    except:
        return "$0,00"

# Crear carpeta para almacenar comprobantes
COMPROBANTES_DIR = Path("comprobantes")
COMPROBANTES_DIR.mkdir(exist_ok=True)

# Configuración de la página
st.set_page_config(
    page_title="Gestión Postventa",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== AUTENTICACIÓN ====================
CLAVE_ACCESO = "2815"

def verificar_autenticacion():
    """Verifica si el usuario está autenticado"""
    if 'autenticado' not in st.session_state:
        st.session_state['autenticado'] = False
    
    return st.session_state['autenticado']

def mostrar_pantalla_login():
    """Muestra la pantalla de login"""
    # Centrar el contenido
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
        <div style='text-align: center; padding: 50px 0;'>
            <h1 style='color: #1f77b4; margin-bottom: 30px;'>🔐 Acceso al Sistema</h1>
            <p style='font-size: 18px; color: #666; margin-bottom: 40px;'>
                Sistema de Gestión de Postventa
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        with st.form("login_form"):
            clave = st.text_input("🔑 Ingrese la clave de acceso", type="password", placeholder="Clave de acceso")
            submit = st.form_submit_button("🚪 Ingresar", use_container_width=True)
            
            if submit:
                if clave == CLAVE_ACCESO:
                    st.session_state['autenticado'] = True
                    st.rerun()
                else:
                    st.error("❌ Clave incorrecta. Intente nuevamente.")
        
        st.markdown("""
        <div style='text-align: center; margin-top: 50px; color: #999; font-size: 12px;'>
            Sistema de Gestión de Postventa © 2025
        </div>
        """, unsafe_allow_html=True)

# Verificar autenticación antes de mostrar el contenido
if not verificar_autenticacion():
    mostrar_pantalla_login()
    st.stop()  # Detener la ejecución si no está autenticado

# Inicializar base de datos
if 'db_initialized' not in st.session_state:
    init_database()
    st.session_state.db_initialized = True

# Sidebar navigation
st.sidebar.title("📊 Gestión Postventa")

# Botón para cerrar sesión en el sidebar
if st.sidebar.button("🚪 Cerrar Sesión", use_container_width=True, key="cerrar_sesion_btn"):
    st.session_state['autenticado'] = False
    st.rerun()
page = st.sidebar.radio(
    "Navegación",
    ["🏠 Dashboard", "💰 Registrar Venta", "💸 Registrar Gasto", "⚙️ Plantillas Gastos", "📥 Importar Excel", "📋 Ver Registros", "📈 Reportes", "🤖 Análisis IA", "🔍 Probar Extracción PDF"]
)

# ==================== DASHBOARD ====================
if page == "🏠 Dashboard":
    st.title("🏠 Dashboard - KPIs y Métricas")
    
    # Filtros de fecha
    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio = st.date_input("Fecha Inicio", value=date(2025, 11, 1))
    with col2:
        fecha_fin = st.date_input("Fecha Fin", value=date.today())
    
    # Obtener datos
    df_ventas = get_ventas(str(fecha_inicio), str(fecha_fin))
    df_gastos = get_gastos(str(fecha_inicio), str(fecha_fin))
    
    if len(df_ventas) == 0 and len(df_gastos) == 0:
        st.info("📊 No hay datos para el período seleccionado. Importa datos desde Excel o registra nuevas ventas/gastos.")
    else:
        # KPIs principales
        st.subheader("📊 Indicadores Principales")
        
        # Calcular métricas
        total_ingresos = df_ventas['total'].sum() if len(df_ventas) > 0 else 0
        ingresos_servicios = df_ventas[df_ventas['tipo_re_se'] == 'SE']['total'].sum() if len(df_ventas) > 0 else 0
        ingresos_repuestos = df_ventas[df_ventas['tipo_re_se'] == 'RE']['total'].sum() if len(df_ventas) > 0 else 0
        
        # Obtener gastos incluyendo los calculados automáticamente
        gastos_totales = obtener_gastos_totales_con_automaticos(str(fecha_inicio), str(fecha_fin))
        
        # Gastos totales (todos los períodos)
        gastos_totales_todos = obtener_gastos_totales_con_automaticos()
        gastos_postventa_total = gastos_totales_todos['gastos_postventa_total']
        gastos_se_todos = gastos_totales_todos['gastos_se_total']
        gastos_re_todos = gastos_totales_todos['gastos_re_total']
        
        # Gastos del período filtrado
        gastos_se = gastos_totales['gastos_se_total']
        gastos_re = gastos_totales['gastos_re_total']
        gastos_postventa = gastos_totales['gastos_postventa_total']
        
        resultado_total = total_ingresos - gastos_postventa_total
        margen_total = (resultado_total / total_ingresos * 100) if total_ingresos > 0 else 0
        
        resultado_servicios = ingresos_servicios - gastos_se_todos
        resultado_repuestos = ingresos_repuestos - gastos_re_todos
        margen_servicios = (resultado_servicios / ingresos_servicios * 100) if ingresos_servicios > 0 else 0
        margen_repuestos = (resultado_repuestos / ingresos_repuestos * 100) if ingresos_repuestos > 0 else 0
        
        # Mostrar KPIs
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        
        with kpi1:
            st.metric("💰 Total Ingresos", f"{formatear_moneda(total_ingresos)} USD")
        
        with kpi2:
            # Calcular gastos registrados (sin automáticos) y automáticos por separado
            gastos_registrados_total = gastos_totales_todos['gastos_registrados']['total_pct_se'].sum() + gastos_totales_todos['gastos_registrados']['total_pct_re'].sum() if len(gastos_totales_todos['gastos_registrados']) > 0 else 0
            gastos_automaticos_total = gastos_totales_todos['gastos_automaticos']['total_pct_se'].sum() + gastos_totales_todos['gastos_automaticos']['total_pct_re'].sum() if len(gastos_totales_todos['gastos_automaticos']) > 0 else 0
            
            st.metric("💸 Gastos Postventa", f"{formatear_moneda(gastos_postventa_total)} USD", 
                     delta=f"Período: {formatear_moneda(gastos_postventa)}")
            st.caption(f"📝 Registrados: {formatear_moneda(gastos_registrados_total)} | 🤖 Automáticos: {formatear_moneda(gastos_automaticos_total)}")
        
        with kpi3:
            st.metric("📈 Resultado Neto", f"{formatear_moneda(resultado_total)} USD", 
                     delta=f"{margen_total:.1f}%")
        
        with kpi4:
            st.metric("📊 Total Registros", f"{len(df_ventas) + len(df_gastos)}")
        
        # Objetivo de ventas de repuestos FY26
        st.divider()
        st.subheader("🎯 Objetivo de Ventas de Repuestos - FY26")
        
        # Objetivo: $1,300,000 USD para el año fiscal (Nov 2025 - Oct 2026)
        OBJETIVO_REPUESTOS_FY26 = 1300000.0
        
        # Calcular total de repuestos vendidos desde inicio del año fiscal (Nov 2025)
        fecha_inicio_fy26 = date(2025, 11, 1)
        fecha_fin_actual = fecha_fin if fecha_fin else date.today()
        
        # Obtener todas las ventas desde inicio del año fiscal
        df_ventas_fy26 = get_ventas(str(fecha_inicio_fy26), str(fecha_fin_actual))
        
        if len(df_ventas_fy26) > 0:
            # Calcular repuestos vendidos (RE + repuestos en SE)
            ventas_re_fy26 = df_ventas_fy26[df_ventas_fy26['tipo_re_se'] == 'RE']
            ventas_se_fy26 = df_ventas_fy26[df_ventas_fy26['tipo_re_se'] == 'SE']
            
            total_re_fy26 = ventas_re_fy26['total'].sum() if len(ventas_re_fy26) > 0 else 0
            repuestos_se_fy26 = ventas_se_fy26['repuestos'].sum() if len(ventas_se_fy26) > 0 and 'repuestos' in ventas_se_fy26.columns else 0
            total_repuestos_vendidos_fy26 = total_re_fy26 + repuestos_se_fy26
            
            # Calcular porcentaje completado
            porcentaje_completado = (total_repuestos_vendidos_fy26 / OBJETIVO_REPUESTOS_FY26 * 100) if OBJETIVO_REPUESTOS_FY26 > 0 else 0
            monto_restante = OBJETIVO_REPUESTOS_FY26 - total_repuestos_vendidos_fy26
            
            # Mostrar métricas
            col_obj1, col_obj2, col_obj3 = st.columns(3)
            
            with col_obj1:
                st.metric(
                    "💰 Vendido hasta ahora",
                    formatear_moneda(total_repuestos_vendidos_fy26),
                    help="Total de repuestos vendidos desde Noviembre 2025"
                )
            
            with col_obj2:
                st.metric(
                    "🎯 Objetivo FY26",
                    formatear_moneda(OBJETIVO_REPUESTOS_FY26),
                    delta=f"{porcentaje_completado:.1f}% completado"
                )
            
            with col_obj3:
                color_delta = "normal" if monto_restante > 0 else "inverse"
                st.metric(
                    "📊 Restante",
                    formatear_moneda(abs(monto_restante)),
                    delta=f"{'Faltan' if monto_restante > 0 else 'Superado por'}: {formatear_moneda(abs(monto_restante))}"
                )
            
            # Barra de progreso visual
            st.write("**Progreso del Objetivo**")
            
            # Crear barra de progreso con colores
            if porcentaje_completado >= 100:
                color_barra = "🟢"  # Verde si se alcanzó o superó
                porcentaje_mostrar = 100
            elif porcentaje_completado >= 75:
                color_barra = "🟡"  # Amarillo si está cerca
                porcentaje_mostrar = porcentaje_completado
            else:
                color_barra = "🔴"  # Rojo si está lejos
                porcentaje_mostrar = porcentaje_completado
            
            # Usar st.progress para la barra visual
            st.progress(min(porcentaje_completado / 100, 1.0))
            
            # Mostrar información adicional
            col_info1, col_info2 = st.columns(2)
            with col_info1:
                st.caption(f"📅 Período: {fecha_inicio_fy26.strftime('%d/%m/%Y')} - {fecha_fin_actual.strftime('%d/%m/%Y')}")
                st.caption(f"📦 Ventas RE: {formatear_moneda(total_re_fy26)}")
            with col_info2:
                st.caption(f"🔩 Repuestos en SE: {formatear_moneda(repuestos_se_fy26)}")
                st.caption(f"⏱️ Tiempo transcurrido: {((fecha_fin_actual - fecha_inicio_fy26).days / 365) * 100:.1f}% del año fiscal")
        else:
            st.info("📊 No hay ventas registradas desde el inicio del año fiscal (Noviembre 2025)")
        
        st.divider()
        
        # Gráficos
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("💰 Ingresos por Tipo")
            if len(df_ventas) > 0:
                ingresos_tipo = df_ventas.groupby('tipo_re_se')['total'].sum()
                fig = px.pie(values=ingresos_tipo.values, names=ingresos_tipo.index, 
                           title="Distribución de Ingresos")
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("🏢 Ingresos por Sucursal")
            if len(df_ventas) > 0:
                ingresos_sucursal = df_ventas.groupby('sucursal')['total'].sum()
                fig = px.bar(x=ingresos_sucursal.index, y=ingresos_sucursal.values,
                           title="Ingresos por Sucursal")
                st.plotly_chart(fig, use_container_width=True)
        
        # Resumen por segmento
        st.subheader("📊 Resumen por Segmento")
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Servicios (SE)**")
            st.metric("Ingresos", formatear_moneda(ingresos_servicios))
            
            # Desglose de gastos SE
            gastos_se_registrados = gastos_totales_todos['gastos_registrados']['total_pct_se'].sum() if len(gastos_totales_todos['gastos_registrados']) > 0 else 0
            gastos_se_automaticos = gastos_totales_todos['gastos_automaticos']['total_pct_se'].sum() if len(gastos_totales_todos['gastos_automaticos']) > 0 else 0
            
            st.metric("Gastos", formatear_moneda(gastos_se_todos))
            st.caption(f"📝 Registrados: {formatear_moneda(gastos_se_registrados)} | 🤖 Automáticos: {formatear_moneda(gastos_se_automaticos)}")
            
            st.metric("Resultado", formatear_moneda(resultado_servicios), 
                     delta=f"{margen_servicios:.1f}%")
        
        with col2:
            st.write("**Repuestos (RE)**")
            st.metric("Ingresos", formatear_moneda(ingresos_repuestos))
            
            # Desglose de gastos RE
            gastos_re_registrados = gastos_totales_todos['gastos_registrados']['total_pct_re'].sum() if len(gastos_totales_todos['gastos_registrados']) > 0 else 0
            gastos_re_automaticos = gastos_totales_todos['gastos_automaticos']['total_pct_re'].sum() if len(gastos_totales_todos['gastos_automaticos']) > 0 else 0
            
            st.metric("Gastos", formatear_moneda(gastos_re_todos))
            st.caption(f"📝 Registrados: {formatear_moneda(gastos_re_registrados)} | 🤖 Automáticos: {formatear_moneda(gastos_re_automaticos)}")
            
            st.metric("Resultado", formatear_moneda(resultado_repuestos), 
                     delta=f"{margen_repuestos:.1f}%")

# ==================== REGISTRAR VENTA ====================
elif page == "💰 Registrar Venta":
    st.title("💰 Registrar Nueva Venta")
    
    # Verificar si hay datos de PDF para prellenar
    datos_pdf = st.session_state.get('venta_desde_pdf', None)
    archivo_pdf_venta = st.session_state.get('archivo_pdf_venta', None)
    
    if datos_pdf:
        st.success("📄 Datos detectados desde PDF! Los campos se prellenarán automáticamente.")
        if st.button("❌ Limpiar datos del PDF"):
            del st.session_state['venta_desde_pdf']
            if 'archivo_pdf_venta' in st.session_state:
                del st.session_state['archivo_pdf_venta']
            st.rerun()
    
    # Inicializar valores en session_state si no existen o usar datos del PDF
    if datos_pdf:
        st.session_state.venta_mano_obra = datos_pdf.get('mano_obra', 0.0)
        st.session_state.venta_asistencia = datos_pdf.get('asistencia', 0.0)
        st.session_state.venta_repuestos = datos_pdf.get('repuestos', 0.0)
        st.session_state.venta_terceros = datos_pdf.get('terceros', 0.0)
        st.session_state.venta_descuento = datos_pdf.get('descuento', 0.0)
    else:
        if 'venta_mano_obra' not in st.session_state:
            st.session_state.venta_mano_obra = 0.0
        if 'venta_asistencia' not in st.session_state:
            st.session_state.venta_asistencia = 0.0
        if 'venta_repuestos' not in st.session_state:
            st.session_state.venta_repuestos = 0.0
        if 'venta_terceros' not in st.session_state:
            st.session_state.venta_terceros = 0.0
        if 'venta_descuento' not in st.session_state:
            st.session_state.venta_descuento = 0.0
    
    # Campos numéricos FUERA del formulario para que se actualicen en tiempo real
    st.subheader("💰 Montos de la Venta")
    col_montos1, col_montos2 = st.columns(2)
    
    with col_montos1:
        mano_obra = st.number_input(
            "Mano de Obra (USD)", 
            min_value=0.0, 
            value=st.session_state.venta_mano_obra, 
            step=0.01,
            key="input_mano_obra"
        )
        repuestos = st.number_input(
            "Repuestos (USD)", 
            min_value=0.0, 
            value=st.session_state.venta_repuestos, 
            step=0.01,
            key="input_repuestos"
        )
        descuento = st.number_input(
            "Descuento (USD)", 
            min_value=0.0, 
            value=st.session_state.venta_descuento, 
            step=0.01,
            key="input_descuento"
        )
    
    with col_montos2:
        asistencia = st.number_input(
            "Asistencia (USD)", 
            min_value=0.0, 
            value=st.session_state.venta_asistencia, 
            step=0.01,
            key="input_asistencia"
        )
        terceros = st.number_input(
            "Terceros (USD)", 
            min_value=0.0, 
            value=st.session_state.venta_terceros, 
            step=0.01,
            key="input_terceros"
        )
    
    # Calcular total en tiempo real (usar valores actuales de los inputs)
    total_calculado = mano_obra + asistencia + repuestos + terceros - descuento
    
    # Actualizar session_state con los valores actuales para mantenerlos
    st.session_state.venta_mano_obra = mano_obra
    st.session_state.venta_asistencia = asistencia
    st.session_state.venta_repuestos = repuestos
    st.session_state.venta_terceros = terceros
    st.session_state.venta_descuento = descuento
    
    # Mostrar total calculado
    st.metric("💰 Total Calculado", f"{formatear_moneda(total_calculado)} USD", 
             delta=f"Mano Obra: {formatear_moneda(mano_obra)} + Asistencia: {formatear_moneda(asistencia)} + Repuestos: {formatear_moneda(repuestos)} + Terceros: {formatear_moneda(terceros)} - Descuento: {formatear_moneda(descuento)}")
    
    st.info("ℹ️ **Nota:** Si seleccionas 'NOTA DE CREDITO' como tipo de comprobante, el total se convertirá automáticamente a negativo al guardar.")
    
    st.divider()
    
    # Formulario para el resto de los campos
    with st.form("form_venta"):
        col1, col2 = st.columns(2)
        
        # Valores por defecto desde PDF si existen
        fecha_default = date.today()
        if datos_pdf and datos_pdf.get('fecha'):
            try:
                # Intentar parsear fecha del PDF
                fecha_str = datos_pdf['fecha']
                if 'Noviembre' in fecha_str or 'Noviembre' in fecha_str:
                    # Formato: "Noviembre 18, 2025"
                    meses = {'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,
                            'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12}
                    partes = fecha_str.replace(',', '').split()
                    if len(partes) >= 3:
                        mes_nombre = partes[0].lower()
                        dia = int(partes[1])
                        año = int(partes[2])
                        if mes_nombre in meses:
                            fecha_default = date(año, meses[mes_nombre], dia)
            except:
                pass
        
        sucursal_default = datos_pdf.get('sucursal', 'COMODORO') if datos_pdf else 'COMODORO'
        cliente_default = datos_pdf.get('cliente', '') if datos_pdf else ''
        pin_default = datos_pdf.get('pin', '') if datos_pdf else ''
        n_comprobante_default = datos_pdf.get('numero_comprobante', '') if datos_pdf else ''
        tipo_re_se_default = datos_pdf.get('tipo_re_se', 'SE') if datos_pdf else 'SE'
        tipo_comprobante_default = datos_pdf.get('tipo_comprobante', 'FACTURA VENTA') if datos_pdf else 'FACTURA VENTA'
        
        with col1:
            fecha = st.date_input("Fecha *", value=fecha_default)
            sucursal = st.selectbox("Sucursal", ["COMODORO", "RIO GRANDE", "RIO GALLEGOS"],
                                   index=["COMODORO", "RIO GRANDE", "RIO GALLEGOS"].index(sucursal_default) 
                                   if sucursal_default in ["COMODORO", "RIO GRANDE", "RIO GALLEGOS"] else 0)
            cliente = st.text_input("Cliente *", value=cliente_default)
            pin = st.text_input("PIN", value=pin_default)
            comprobante = st.text_input("Comprobante")
            tipo_comprobante = st.selectbox(
                "Tipo Comprobante",
                ["FACTURA VENTA", "NOTA DE CREDITO", "NOTA DE CREDITO JD", 
                 "CITA INTERNA", "FACTURA AFIP", "NOTA DE CRETIDO"],
                index=["FACTURA VENTA", "NOTA DE CREDITO", "NOTA DE CREDITO JD", 
                       "CITA INTERNA", "FACTURA AFIP", "NOTA DE CRETIDO"].index(tipo_comprobante_default)
                if tipo_comprobante_default in ["FACTURA VENTA", "NOTA DE CREDITO", "NOTA DE CREDITO JD", 
                                                 "CITA INTERNA", "FACTURA AFIP", "NOTA DE CRETIDO"] else 0
            )
            trabajo = st.selectbox("Trabajo", ["EXTERNO", "GARANTIA", "INTERNO"])
        
        with col2:
            n_comprobante = st.text_input("N° Comprobante", value=n_comprobante_default)
            tipo_re_se = st.selectbox("Tipo (RE o SE) *", ["SE", "RE"],
                                     index=0 if tipo_re_se_default == 'SE' else 1)
            detalles = st.text_area("Detalles")
        
        # Campo para adjuntar comprobante
        st.subheader("📎 Adjuntar Comprobante (Opcional)")
        
        # Si hay archivo PDF del análisis, usarlo por defecto
        if archivo_pdf_venta:
            st.info(f"📄 Archivo PDF detectado: {archivo_pdf_venta.name}")
            st.write("💡 El archivo se adjuntará automáticamente al guardar.")
        
        archivo_comprobante = st.file_uploader(
            "Subir comprobante (PDF o Imagen)",
            type=['pdf', 'png', 'jpg', 'jpeg'],
            help="Puedes adjuntar un PDF o imagen del comprobante. Si ya analizaste un PDF, se usará ese."
        )
        
        if archivo_comprobante is not None:
            # Mostrar preview si es imagen
            if archivo_comprobante.type.startswith('image/'):
                st.image(archivo_comprobante, width=300)
            else:
                st.info(f"📄 Archivo PDF: {archivo_comprobante.name}")
        
        submitted = st.form_submit_button("💾 Guardar Venta")
        
        # Guardar valores del formulario en session_state para usarlos después
        if submitted:
            # Verificar que no se haya procesado ya (evitar duplicados)
            if not st.session_state.get('form_processing', False):
                st.session_state.form_processing = True
                st.session_state.form_fecha = fecha
                st.session_state.form_sucursal = sucursal
                st.session_state.form_cliente = cliente
                st.session_state.form_pin = pin
                st.session_state.form_comprobante = comprobante
                st.session_state.form_tipo_comprobante = tipo_comprobante
                st.session_state.form_trabajo = trabajo
                st.session_state.form_n_comprobante = n_comprobante
                st.session_state.form_tipo_re_se = tipo_re_se
                st.session_state.form_detalles = detalles
                st.session_state.form_archivo_comprobante = archivo_comprobante
                st.session_state.form_submitted = True
                st.rerun()
    
    # Procesar el envío del formulario FUERA del form para tener acceso a los valores numéricos
    if st.session_state.get('form_submitted', False) and st.session_state.get('form_processing', False):
        # Obtener valores del formulario desde session_state
        fecha = st.session_state.get('form_fecha', date.today())
        sucursal = st.session_state.get('form_sucursal', 'COMODORO')
        cliente = st.session_state.get('form_cliente', '')
        pin = st.session_state.get('form_pin', '')
        comprobante = st.session_state.get('form_comprobante', '')
        tipo_comprobante = st.session_state.get('form_tipo_comprobante', 'FACTURA VENTA')
        trabajo = st.session_state.get('form_trabajo', 'EXTERNO')
        n_comprobante = st.session_state.get('form_n_comprobante', '')
        tipo_re_se = st.session_state.get('form_tipo_re_se', 'SE')
        detalles = st.session_state.get('form_detalles', '')
        archivo_comprobante = st.session_state.get('form_archivo_comprobante', None)
        
        # Obtener valores actuales de los campos numéricos (están fuera del form)
        mano_obra_actual = st.session_state.get('input_mano_obra', st.session_state.venta_mano_obra)
        asistencia_actual = st.session_state.get('input_asistencia', st.session_state.venta_asistencia)
        repuestos_actual = st.session_state.get('input_repuestos', st.session_state.venta_repuestos)
        terceros_actual = st.session_state.get('input_terceros', st.session_state.venta_terceros)
        descuento_actual = st.session_state.get('input_descuento', st.session_state.venta_descuento)
        
        total = mano_obra_actual + asistencia_actual + repuestos_actual + terceros_actual - descuento_actual
        
        # Si es nota de crédito (pero NO JD), convertir el total a negativo automáticamente
        # NOTA: "NOTA DE CREDITO JD" son pagos recibidos de John Deere, por lo que son POSITIVOS
        es_nota_credito = tipo_comprobante and 'NOTA DE CREDITO' in tipo_comprobante.upper() and 'JD' not in tipo_comprobante.upper()
        if es_nota_credito and total > 0:
            total = -total  # Convertir a negativo
        
        # Resetear flags ANTES de procesar para evitar loops
        st.session_state.form_submitted = False
        st.session_state.form_processing = False
        
        if not cliente:
            st.error("⚠️ Por favor completa el campo obligatorio (Cliente)")
        elif total == 0:
            st.error("⚠️ El total no puede ser 0. Verifica los valores ingresados.")
        else:
                # Guardar archivo adjunto si existe (priorizar el del PDF analizado)
                ruta_archivo = None
                if archivo_pdf_venta and not archivo_comprobante:
                    # Usar el archivo PDF que se analizó
                    try:
                        extension = Path(archivo_pdf_venta.name).suffix
                        nombre_archivo = f"venta_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{archivo_pdf_venta.name}"
                        ruta_archivo = COMPROBANTES_DIR / nombre_archivo
                        
                        # Guardar el archivo
                        with open(ruta_archivo, "wb") as f:
                            f.write(archivo_pdf_venta.getbuffer())
                        
                        ruta_archivo = str(ruta_archivo)
                    except Exception as e:
                        st.warning(f"⚠️ Error al guardar el archivo PDF: {e}")
                
                if archivo_comprobante is not None:
                    try:
                        # El archivo puede ser un objeto UploadedFile o un path guardado
                        if hasattr(archivo_comprobante, 'getbuffer'):
                            # Es un objeto UploadedFile
                            extension = Path(archivo_comprobante.name).suffix
                            nombre_archivo = f"venta_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{archivo_comprobante.name}"
                            ruta_archivo = COMPROBANTES_DIR / nombre_archivo
                            
                            # Guardar el archivo
                            with open(ruta_archivo, "wb") as f:
                                f.write(archivo_comprobante.getbuffer())
                            
                            ruta_archivo = str(ruta_archivo)
                        elif isinstance(archivo_comprobante, str):
                            # Ya es un path guardado
                            ruta_archivo = archivo_comprobante
                    except Exception as e:
                        st.warning(f"⚠️ Error al guardar el archivo: {e}")
                
                venta_data = {
                    'mes': fecha.strftime("%B%y") if isinstance(fecha, date) else fecha,
                    'fecha': fecha,
                    'sucursal': sucursal,
                    'cliente': cliente,
                    'pin': pin if pin else None,
                    'comprobante': comprobante if comprobante else None,
                    'tipo_comprobante': tipo_comprobante,
                    'trabajo': trabajo,
                    'n_comprobante': n_comprobante if n_comprobante else None,
                    'tipo_re_se': tipo_re_se,
                    'mano_obra': mano_obra_actual,
                    'asistencia': asistencia_actual,
                    'repuestos': repuestos_actual,
                    'terceros': terceros_actual,
                    'descuento': descuento_actual,
                    'total': total,
                    'detalles': detalles if detalles else None,
                    'archivo_comprobante': ruta_archivo
                }
                
                try:
                    venta_id = insert_venta(venta_data)
                    st.success(f"✅ Venta registrada exitosamente! ID: {venta_id}")
                    if ruta_archivo:
                        st.success(f"📎 Comprobante guardado: {Path(ruta_archivo).name}")
                    
                    # Limpiar valores del formulario
                    st.session_state.venta_mano_obra = 0.0
                    st.session_state.venta_asistencia = 0.0
                    st.session_state.venta_repuestos = 0.0
                    st.session_state.venta_terceros = 0.0
                    st.session_state.venta_descuento = 0.0
                    # Limpiar también los keys de los inputs
                    if 'input_mano_obra' in st.session_state:
                        del st.session_state.input_mano_obra
                    if 'input_asistencia' in st.session_state:
                        del st.session_state.input_asistencia
                    if 'input_repuestos' in st.session_state:
                        del st.session_state.input_repuestos
                    if 'input_terceros' in st.session_state:
                        del st.session_state.input_terceros
                    if 'input_descuento' in st.session_state:
                        del st.session_state.input_descuento
                    # Limpiar valores del formulario
                    for key in ['form_fecha', 'form_sucursal', 'form_cliente', 'form_pin', 'form_comprobante', 
                               'form_tipo_comprobante', 'form_trabajo', 'form_n_comprobante', 'form_tipo_re_se', 
                               'form_detalles', 'form_archivo_comprobante', 'form_submitted', 'form_processing',
                               'venta_desde_pdf', 'archivo_pdf_venta']:
                        if key in st.session_state:
                            del st.session_state[key]
                    
                    st.balloons()
                    # NO hacer rerun aquí, dejar que el mensaje se muestre
                except Exception as e:
                    st.error(f"❌ Error al guardar: {e}")
                    # Si hay error, resetear el flag de procesamiento para permitir reintentar
                    st.session_state.form_processing = False

# ==================== REGISTRAR GASTO ====================
elif page == "💸 Registrar Gasto":
    st.title("💸 Registrar Nuevo Gasto")
    
    # Obtener plantillas disponibles
    df_plantillas = get_plantillas_gastos(activas_only=True)
    
    # Sección de plantillas con búsqueda
    st.subheader("📋 Usar Plantilla (Opcional)")
    
    # Campo de búsqueda para filtrar plantillas
    buscar_plantilla = st.text_input("🔍 Buscar plantilla", placeholder="Escribe para buscar (clasificación, proveedor, sucursal, área, tipo)...", key="buscar_plantilla_gasto")
    
    # Filtrar plantillas según búsqueda
    df_plantillas_filtrado = df_plantillas.copy()
    if buscar_plantilla and buscar_plantilla.strip():
        texto_busqueda = buscar_plantilla.lower().strip()
        mask = (
            df_plantillas_filtrado['clasificacion'].astype(str).str.contains(texto_busqueda, case=False, na=False) |
            df_plantillas_filtrado['proveedor'].astype(str).str.contains(texto_busqueda, case=False, na=False) |
            df_plantillas_filtrado['sucursal'].astype(str).str.contains(texto_busqueda, case=False, na=False) |
            df_plantillas_filtrado['area'].astype(str).str.contains(texto_busqueda, case=False, na=False) |
            df_plantillas_filtrado['tipo'].astype(str).str.contains(texto_busqueda, case=False, na=False)
        )
        df_plantillas_filtrado = df_plantillas_filtrado[mask]
        
        # Mostrar mensaje según resultados
        if len(df_plantillas_filtrado) == 0:
            st.warning(f"⚠️ No se encontraron plantillas que coincidan con '{buscar_plantilla}'. Mostrando todas las plantillas.")
            df_plantillas_filtrado = df_plantillas.copy()
        else:
            st.info(f"✅ {len(df_plantillas_filtrado)} plantilla(s) encontrada(s)")
    
    # Función para crear texto de opción
    def crear_texto_opcion(row):
        clasificacion = str(row.get('clasificacion', '') or 'Sin clasificación').strip()
        proveedor = str(row.get('proveedor', '') or 'Sin proveedor').strip()
        sucursal = str(row['sucursal'] or 'COMPARTIDOS').strip()
        area = str(row.get('area', '') or 'Sin área').strip()
        tipo = str(row.get('tipo', '') or 'Sin tipo').strip()
        return f"{clasificacion} - {proveedor} - {sucursal} - {area} - {tipo}"
    
    # Crear opciones con formato: CLASIFICACION - PROVEEDOR - SUCURSAL - AREA - TIPO
    plantillas_opciones = ['Crear desde cero']
    plantillas_dict = {}
    
    # Agregar plantillas filtradas a las opciones
    for _, row in df_plantillas_filtrado.iterrows():
        texto_opcion = crear_texto_opcion(row)
        plantillas_opciones.append(texto_opcion)
        plantillas_dict[texto_opcion] = row.to_dict()
    
    # Selector de plantilla
    plantilla_seleccionada = st.selectbox(
        "Selecciona una plantilla:",
        plantillas_opciones,
        help="Selecciona una plantilla para llenar automáticamente los campos, o 'Crear desde cero' para empezar vacío."
    )
    
    # Valores iniciales desde plantilla
    valores_iniciales = {
        'sucursal': "COMPARTIDOS",
        'area': "POSTVENTA",
        'tipo': "FIJO",
        'clasificacion': '',
        'proveedor': '',
        'pct_postventa': 0.0,
        'pct_servicios': 0.0,
        'pct_repuestos': 0.0,
        'detalles': ''
    }
    
    if plantilla_seleccionada != 'Crear desde cero':
        plantilla = plantillas_dict[plantilla_seleccionada]
        valores_iniciales = {
            'sucursal': plantilla.get('sucursal', 'COMPARTIDOS'),
            'area': plantilla.get('area', 'POSTVENTA'),
            'tipo': plantilla.get('tipo', 'FIJO'),
            'clasificacion': plantilla.get('clasificacion', ''),
            'proveedor': plantilla.get('proveedor', ''),
            'pct_postventa': plantilla.get('pct_postventa', 0.0),
            'pct_servicios': plantilla.get('pct_servicios', 0.0),
            'pct_repuestos': plantilla.get('pct_repuestos', 0.0),
            'detalles': plantilla.get('detalles', '')
        }
        
        # Mostrar información de la plantilla
        st.info(f"""
        **Plantilla seleccionada:** {plantilla['nombre']}
        - Sucursal: {valores_iniciales['sucursal']}
        - Área: {valores_iniciales['area']}
        - Tipo: {valores_iniciales['tipo']}
        - Clasificación: {valores_iniciales['clasificacion']}
        - Proveedor: {valores_iniciales['proveedor'] if valores_iniciales['proveedor'] else 'N/A'}
        - % Postventa: {valores_iniciales['pct_postventa']*100:.0f}%
        - % Servicios: {valores_iniciales['pct_servicios']*100:.0f}%
        - % Repuestos: {valores_iniciales['pct_repuestos']*100:.0f}%
        """)
    
    with st.form("form_gasto"):
        col1, col2 = st.columns(2)
        
        with col1:
            fecha = st.date_input("Fecha *", value=date.today())
            sucursal = st.selectbox("Sucursal", ["COMODORO", "RIO GRANDE", "RIO GALLEGOS", "COMPARTIDOS"],
                                   index=["COMODORO", "RIO GRANDE", "RIO GALLEGOS", "COMPARTIDOS"].index(valores_iniciales['sucursal']) 
                                   if valores_iniciales['sucursal'] in ["COMODORO", "RIO GRANDE", "RIO GALLEGOS", "COMPARTIDOS"] else 3)
            area = st.selectbox("Área", ["POSTVENTA", "SERVICIO", "REPUESTOS"],
                               index=["POSTVENTA", "SERVICIO", "REPUESTOS"].index(valores_iniciales['area']) 
                               if valores_iniciales['area'] in ["POSTVENTA", "SERVICIO", "REPUESTOS"] else 0)
            tipo = st.selectbox("Tipo", ["FIJO", "VARIABLE"],
                               index=0 if valores_iniciales['tipo'] == 'FIJO' else 1)
            clasificacion = st.text_input("Clasificación *", value=valores_iniciales['clasificacion'])
            proveedor = st.text_input("Proveedor", value=valores_iniciales['proveedor'])
            total_pesos = st.number_input("Total Pesos (Opcional)", min_value=0.0, value=0.0, step=0.01)
        
        with col2:
            total_usd = st.number_input("Total USD *", min_value=0.0, value=0.0, step=0.01)
            pct_postventa = st.number_input("% Postventa", min_value=0.0, max_value=1.0, 
                                           value=valores_iniciales['pct_postventa'], step=0.01)
            pct_servicios = st.number_input("% Servicios", min_value=0.0, max_value=1.0, 
                                          value=valores_iniciales['pct_servicios'], step=0.01)
            pct_repuestos = st.number_input("% Repuestos", min_value=0.0, max_value=1.0, 
                                          value=valores_iniciales['pct_repuestos'], step=0.01)
            detalles = st.text_area("Detalles", value=valores_iniciales['detalles'])
        
        # Calcular totales
        total_pct = total_usd * pct_postventa if pct_postventa > 0 else 0
        total_pct_se = total_pct * pct_servicios if pct_servicios > 0 else 0
        total_pct_re = total_pct * pct_repuestos if pct_repuestos > 0 else 0
        
        st.info(f"📊 Total %: {formatear_moneda(total_pct)} | %SE: {formatear_moneda(total_pct_se)} | %RE: {formatear_moneda(total_pct_re)}")
        
        submitted = st.form_submit_button("💾 Guardar Gasto")
        
        if submitted:
            if not clasificacion:
                st.error("⚠️ Por favor completa el campo obligatorio (Clasificación)")
            elif total_usd <= 0:
                st.error("⚠️ El total USD debe ser mayor a 0")
            else:
                gasto_data = {
                    'mes': fecha.strftime("%B"),
                    'fecha': fecha,
                    'sucursal': sucursal,
                    'area': area,
                    'pct_postventa': pct_postventa,
                    'pct_servicios': pct_servicios,
                    'pct_repuestos': pct_repuestos,
                    'tipo': tipo,
                    'clasificacion': clasificacion,
                    'proveedor': proveedor if proveedor else None,
                    'total_pesos': total_pesos if total_pesos > 0 else None,
                    'total_usd': total_usd,
                    'total_pct': total_pct,
                    'total_pct_se': total_pct_se,
                    'total_pct_re': total_pct_re,
                    'detalles': detalles if detalles else None
                }
                
                try:
                    gasto_id = insert_gasto(gasto_data)
                    st.success(f"✅ Gasto registrado exitosamente! ID: {gasto_id}")
                    st.balloons()
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error al guardar: {e}")

# ==================== PLANTILLAS GASTOS ====================
elif page == "⚙️ Plantillas Gastos":
    st.title("⚙️ Gestión de Plantillas de Gastos")
    
    tab1, tab2, tab3 = st.tabs(["📋 Ver Plantillas", "➕ Crear/Editar Plantilla", "📥 Crear desde Gasto Existente"])
    
    with tab1:
        st.subheader("Plantillas Existentes")
        df_plantillas = get_plantillas_gastos()
        
        # Sección de Exportar/Importar Plantillas
        st.divider()
        st.subheader("💾 Exportar/Importar Plantillas")
        st.info("💡 **Exporta tus plantillas** para transferirlas a otra instancia (ej: Streamlit Cloud) o **importa plantillas** desde un archivo JSON.")
        
        col_exp1, col_exp2 = st.columns(2)
        
        with col_exp1:
            st.markdown("#### 📤 Exportar Plantillas")
            if len(df_plantillas) > 0:
                # Exportar a JSON
                plantillas_data = exportar_plantillas_gastos()
                json_data = json.dumps(plantillas_data, indent=2, ensure_ascii=False)
                
                st.download_button(
                    label="📥 Descargar Plantillas (JSON)",
                    data=json_data,
                    file_name=f"plantillas_gastos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json",
                    help=f"Descarga {len(plantillas_data)} plantillas en formato JSON"
                )
                st.caption(f"Total: {len(plantillas_data)} plantillas disponibles para exportar")
            else:
                st.info("No hay plantillas para exportar")
        
        with col_exp2:
            st.markdown("#### 📥 Importar Plantillas")
            archivo_json = st.file_uploader("Subir archivo JSON de plantillas", type=['json'], key="import_plantillas")
            
            if archivo_json is not None:
                sobrescribir = st.checkbox("Sobrescribir plantillas existentes con el mismo nombre", value=False)
                
                if st.button("📥 Importar Plantillas", key="btn_importar_plantillas"):
                    try:
                        # Leer JSON
                        contenido = archivo_json.read().decode('utf-8')
                        plantillas_data = json.loads(contenido)
                        
                        if not isinstance(plantillas_data, list):
                            st.error("❌ El archivo JSON debe contener una lista de plantillas")
                        else:
                            with st.spinner(f"Importando {len(plantillas_data)} plantillas..."):
                                resultado = importar_plantillas_gastos(plantillas_data, sobrescribir=sobrescribir)
                                
                                if resultado['importadas'] > 0 or resultado['actualizadas'] > 0:
                                    st.success(f"✅ Importación completada:")
                                    if resultado['importadas'] > 0:
                                        st.write(f"- ✅ {resultado['importadas']} plantillas importadas")
                                    if resultado['actualizadas'] > 0:
                                        st.write(f"- 🔄 {resultado['actualizadas']} plantillas actualizadas")
                                    if resultado['omitidas'] > 0:
                                        st.write(f"- ⏭️ {resultado['omitidas']} plantillas omitidas (ya existían)")
                                    if resultado['errores']:
                                        st.warning(f"⚠️ {len(resultado['errores'])} errores:")
                                        for error in resultado['errores'][:5]:
                                            st.write(f"  - {error}")
                                    st.rerun()
                                else:
                                    st.warning("⚠️ No se importaron plantillas. Verifica el archivo JSON.")
                    except json.JSONDecodeError as e:
                        st.error(f"❌ Error al leer el archivo JSON: {e}")
                    except Exception as e:
                        st.error(f"❌ Error al importar: {e}")
        
        st.divider()
        
        if len(df_plantillas) > 0:
            buscar_plantilla = st.text_input("🔍 Buscar plantilla", placeholder="Buscar por clasificación, proveedor, sucursal, área, tipo...")
            df_filtrado = df_plantillas.copy()
            
            if buscar_plantilla:
                # Buscar en múltiples columnas
                texto_busqueda = buscar_plantilla.lower()
                mask = (
                    df_filtrado['clasificacion'].astype(str).str.contains(texto_busqueda, case=False, na=False) |
                    df_filtrado['proveedor'].astype(str).str.contains(texto_busqueda, case=False, na=False) |
                    df_filtrado['sucursal'].astype(str).str.contains(texto_busqueda, case=False, na=False) |
                    df_filtrado['area'].astype(str).str.contains(texto_busqueda, case=False, na=False) |
                    df_filtrado['tipo'].astype(str).str.contains(texto_busqueda, case=False, na=False) |
                    df_filtrado['nombre'].astype(str).str.contains(texto_busqueda, case=False, na=False)
                )
                df_filtrado = df_filtrado[mask]
            
            # Mostrar plantillas
            for idx, row in df_filtrado.iterrows():
                # Formato del título: CLASIFICACION - PROVEEDOR - SUCURSAL - AREA - TIPO
                clasificacion = row.get('clasificacion', '') or 'Sin clasificación'
                proveedor = row.get('proveedor', '') or 'Sin proveedor'
                sucursal = row['sucursal'] or 'COMPARTIDOS'
                area = row.get('area', '') or 'Sin área'
                tipo = row.get('tipo', '') or 'Sin tipo'
                
                titulo_expander = f"{'✅' if row['activa'] else '❌'} {clasificacion} - {proveedor} - {sucursal} - {area} - {tipo}"
                
                with st.expander(titulo_expander):
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.write(f"**Descripción:** {row['descripcion'] or 'Sin descripción'}")
                        st.write(f"**Área:** {row['area']} | **Tipo:** {row['tipo']}")
                        st.write(f"**Clasificación:** {row['clasificacion']}")
                        if row['proveedor']:
                            st.write(f"**Proveedor:** {row['proveedor']}")
                        st.write(f"**% Postventa:** {row['pct_postventa']*100:.0f}% | **% Servicios:** {row['pct_servicios']*100:.0f}% | **% Repuestos:** {row['pct_repuestos']*100:.0f}%")
                        if row['detalles']:
                            st.write(f"**Detalles:** {row['detalles']}")
                    
                    with col2:
                        if st.button("✏️ Editar", key=f"edit_{row['id']}"):
                            st.session_state['editar_plantilla_id'] = row['id']
                            st.success(f"✅ Plantilla seleccionada para editar. Ve a la pestaña '➕ Crear/Editar Plantilla' para editarla.")
                            st.rerun()
                        if st.button("🗑️ Eliminar", key=f"delete_{row['id']}"):
                            try:
                                delete_plantilla_gasto(row['id'])
                                st.success(f"✅ Plantilla '{row['nombre']}' eliminada")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error: {e}")
        else:
            st.info("No hay plantillas creadas. Crea una nueva en la pestaña 'Crear/Editar Plantilla'")
    
    with tab2:
        # Verificar si hay una plantilla para editar
        plantilla_editar_id = st.session_state.get('editar_plantilla_id', None)
        
        if plantilla_editar_id:
            try:
                plantilla_editar = get_plantilla_gasto_by_id(plantilla_editar_id)
                if plantilla_editar and len(plantilla_editar) > 0:
                    st.subheader(f"✏️ Editar Plantilla: {plantilla_editar.get('nombre', 'Sin nombre')}")
                    st.info("📝 Estás editando una plantilla existente. Modifica los campos y guarda los cambios.")
                else:
                    st.warning("⚠️ La plantilla seleccionada no se encontró. Creando nueva plantilla.")
                    plantilla_editar = None
                    if 'editar_plantilla_id' in st.session_state:
                        del st.session_state['editar_plantilla_id']
                    st.subheader("➕ Crear Nueva Plantilla")
            except Exception as e:
                st.error(f"❌ Error al cargar la plantilla: {e}")
                import traceback
                st.code(traceback.format_exc())
                plantilla_editar = None
                if 'editar_plantilla_id' in st.session_state:
                    del st.session_state['editar_plantilla_id']
                st.subheader("➕ Crear Nueva Plantilla")
        else:
            plantilla_editar = None
            st.subheader("➕ Crear Nueva Plantilla")
        
        with st.form("form_plantilla"):
            # Valores por defecto seguros
            nombre_default = plantilla_editar.get('nombre', '') if plantilla_editar else ''
            descripcion_default = plantilla_editar.get('descripcion', '') if plantilla_editar else ''
            clasificacion_default = plantilla_editar.get('clasificacion', '') if plantilla_editar else ''
            proveedor_default = plantilla_editar.get('proveedor', '') if plantilla_editar else ''
            detalles_default = plantilla_editar.get('detalles', '') if plantilla_editar else ''
            pct_postventa_default = float(plantilla_editar.get('pct_postventa', 0.0)) if plantilla_editar else 0.0
            pct_servicios_default = float(plantilla_editar.get('pct_servicios', 0.0)) if plantilla_editar else 0.0
            pct_repuestos_default = float(plantilla_editar.get('pct_repuestos', 0.0)) if plantilla_editar else 0.0
            activa_default = bool(plantilla_editar.get('activa', True)) if plantilla_editar else True
            
            nombre = st.text_input("Nombre *", value=nombre_default)
            descripcion = st.text_area("Descripción", value=descripcion_default)
            
            col1, col2 = st.columns(2)
            with col1:
                # Sucursal
                sucursal_opciones = ["COMODORO", "RIO GRANDE", "RIO GALLEGOS", None]
                sucursal_default = plantilla_editar.get('sucursal') if plantilla_editar else None
                sucursal_index = 0
                if sucursal_default and sucursal_default in sucursal_opciones:
                    sucursal_index = sucursal_opciones.index(sucursal_default)
                elif sucursal_default is None:
                    sucursal_index = 3
                sucursal = st.selectbox("Sucursal", sucursal_opciones, index=sucursal_index)
                
                # Área
                area_opciones = ["POSTVENTA", "SERVICIO", "REPUESTOS"]
                area_default = plantilla_editar.get('area', 'POSTVENTA') if plantilla_editar else 'POSTVENTA'
                area_index = area_opciones.index(area_default) if area_default in area_opciones else 0
                area = st.selectbox("Área", area_opciones, index=area_index)
                
                # Tipo
                tipo_default = plantilla_editar.get('tipo', 'FIJO') if plantilla_editar else 'FIJO'
                tipo_index = 0 if tipo_default == 'FIJO' else 1
                tipo = st.selectbox("Tipo", ["FIJO", "VARIABLE"], index=tipo_index)
                
                clasificacion = st.text_input("Clasificación", value=clasificacion_default)
            
            with col2:
                proveedor = st.text_input("Proveedor", value=proveedor_default)
                pct_postventa = st.number_input("% Postventa", min_value=0.0, max_value=1.0, 
                                               value=pct_postventa_default, step=0.01)
                pct_servicios = st.number_input("% Servicios", min_value=0.0, max_value=1.0, 
                                               value=pct_servicios_default, step=0.01)
                pct_repuestos = st.number_input("% Repuestos", min_value=0.0, max_value=1.0, 
                                               value=pct_repuestos_default, step=0.01)
            
            detalles = st.text_area("Detalles", value=detalles_default)
            activa = st.checkbox("Activa", value=activa_default)
            
            submitted = st.form_submit_button("💾 Guardar Plantilla")
            
            if submitted:
                if not nombre:
                    st.error("⚠️ El nombre es obligatorio")
                else:
                    # Verificar si el nombre ya existe (excepto si estamos editando la misma plantilla)
                    df_plantillas_existentes = get_plantillas_gastos()
                    nombre_duplicado = False
                    if len(df_plantillas_existentes) > 0:
                        if plantilla_editar_id:
                            # Al editar, excluir la plantilla actual de la verificación
                            nombres_existentes = df_plantillas_existentes[df_plantillas_existentes['id'] != plantilla_editar_id]
                        else:
                            nombres_existentes = df_plantillas_existentes
                        nombres_existentes_set = set(nombres_existentes['nombre'].str.lower())
                        nombre_duplicado = nombre.lower() in nombres_existentes_set
                    
                    if nombre_duplicado:
                        st.error(f"❌ Ya existe una plantilla con el nombre '{nombre}'. Por favor, elige un nombre diferente.")
                    else:
                        plantilla_data = {
                            'nombre': nombre,
                            'descripcion': descripcion,
                            'sucursal': sucursal if sucursal else None,
                            'area': area,
                            'tipo': tipo,
                            'clasificacion': clasificacion,
                            'proveedor': proveedor if proveedor else None,
                            'pct_postventa': pct_postventa,
                            'pct_servicios': pct_servicios,
                            'pct_repuestos': pct_repuestos,
                            'detalles': detalles if detalles else None,
                            'activa': 1 if activa else 0
                        }
                        
                        try:
                            if plantilla_editar_id:
                                update_plantilla_gasto(plantilla_editar_id, plantilla_data)
                                st.success(f"✅ Plantilla '{nombre}' actualizada")
                                if 'editar_plantilla_id' in st.session_state:
                                    del st.session_state['editar_plantilla_id']
                            else:
                                insert_plantilla_gasto(plantilla_data)
                                st.success(f"✅ Plantilla '{nombre}' creada")
                            st.rerun()
                        except Exception as e:
                            if "UNIQUE constraint" in str(e) and "nombre" in str(e):
                                st.error(f"❌ Ya existe una plantilla con el nombre '{nombre}'. Por favor, elige un nombre diferente.")
                            else:
                                st.error(f"❌ Error: {e}")
    
    with tab3:
        st.subheader("📥 Crear Plantilla desde Gasto Existente")
        df_gastos = get_gastos()
        
        if len(df_gastos) > 0:
            # Filtros
            col1, col2 = st.columns(2)
            with col1:
                sucursales = ['Todas'] + list(df_gastos['sucursal'].dropna().unique())
                filtro_sucursal = st.selectbox("Filtrar por Sucursal", sucursales)
            with col2:
                areas = ['Todas'] + list(df_gastos['area'].dropna().unique())
                filtro_area = st.selectbox("Filtrar por Área", areas)
            
            # Aplicar filtros
            df_filtrado = df_gastos.copy()
            if filtro_sucursal != 'Todas':
                df_filtrado = df_filtrado[df_filtrado['sucursal'] == filtro_sucursal]
            if filtro_area != 'Todas':
                df_filtrado = df_filtrado[df_filtrado['area'] == filtro_area]
            
            # Crear opciones descriptivas
            def format_gasto_option(row):
                clasif = row.get('clasificacion', '') or 'Sin clasificación'
                area = row.get('area', '') or 'Sin área'
                sucursal = row.get('sucursal', '') or 'Sin sucursal'
                proveedor = row.get('proveedor', '') or 'Sin proveedor'
                tipo = row.get('tipo', '') or 'Sin tipo'
                return f"ID {row['id']}: {clasif} - {area} - {sucursal} - {proveedor} - {tipo}"
            
            # Pre-cargar todos los datos necesarios en un dict para evitar múltiples llamadas
            gastos_dict = {}
            opciones_gastos = []
            for idx, row in df_filtrado.iterrows():
                gasto_id = row['id']
                gastos_dict[gasto_id] = row.to_dict()
                opciones_gastos.append((gasto_id, format_gasto_option(row)))
            
            if opciones_gastos:
                gasto_seleccionado_id = st.selectbox(
                    "Selecciona un gasto para crear plantilla",
                    [None] + [g[0] for g in opciones_gastos],
                    format_func=lambda x: dict(opciones_gastos)[x] if x and x in dict(opciones_gastos) else "Seleccionar...",
                    key="select_gasto_plantilla"
                )
                
                if gasto_seleccionado_id:
                    gasto_seleccionado = gastos_dict[gasto_seleccionado_id]
                    
                    with st.form("form_plantilla_desde_gasto"):
                        # Generar nombre sugerido más único
                        clasificacion = gasto_seleccionado.get('clasificacion', 'Nueva Plantilla')
                        sucursal = gasto_seleccionado.get('sucursal', '')
                        proveedor = gasto_seleccionado.get('proveedor', '')
                        
                        # Crear nombre sugerido: CLASIFICACION - SUCURSAL (o con proveedor si existe)
                        nombre_sugerido = f"{clasificacion} - {sucursal}" if sucursal else clasificacion
                        if proveedor:
                            nombre_sugerido = f"{clasificacion} - {sucursal} - {proveedor}" if sucursal else f"{clasificacion} - {proveedor}"
                        
                        # Verificar si el nombre ya existe
                        df_plantillas_existentes = get_plantillas_gastos()
                        nombres_existentes = set(df_plantillas_existentes['nombre'].str.lower() if len(df_plantillas_existentes) > 0 else set())
                        
                        # Si el nombre sugerido ya existe, agregar un sufijo
                        nombre_final = nombre_sugerido
                        contador = 1
                        while nombre_final.lower() in nombres_existentes:
                            nombre_final = f"{nombre_sugerido} ({contador})"
                            contador += 1
                        
                        nombre_plantilla = st.text_input("Nombre de la Plantilla *", value=nombre_final)
                        
                        # Mostrar advertencia si el nombre ya existe
                        if nombre_plantilla and nombre_plantilla.lower() in nombres_existentes:
                            st.warning(f"⚠️ Ya existe una plantilla con el nombre '{nombre_plantilla}'. Por favor, usa un nombre diferente.")
                        
                        descripcion_plantilla = st.text_area("Descripción", 
                                                            value=f"Plantilla creada desde gasto ID {gasto_seleccionado_id}")
                        
                        # Mostrar datos del gasto seleccionado
                        st.info(f"""
                        **Datos del gasto seleccionado:**
                        - Clasificación: {gasto_seleccionado.get('clasificacion', 'N/A')}
                        - Área: {gasto_seleccionado.get('area', 'N/A')}
                        - Sucursal: {gasto_seleccionado.get('sucursal', 'N/A')}
                        - Proveedor: {gasto_seleccionado.get('proveedor', 'N/A')}
                        - Tipo: {gasto_seleccionado.get('tipo', 'N/A')}
                        - % Postventa: {gasto_seleccionado.get('pct_postventa', 0)*100:.0f}%
                        - % Servicios: {gasto_seleccionado.get('pct_servicios', 0)*100:.0f}%
                        - % Repuestos: {gasto_seleccionado.get('pct_repuestos', 0)*100:.0f}%
                        """)
                        
                        submitted = st.form_submit_button("💾 Crear Plantilla")
                        
                        if submitted:
                            if not nombre_plantilla:
                                st.error("⚠️ El nombre es obligatorio")
                            elif nombre_plantilla.lower() in nombres_existentes:
                                st.error(f"❌ Ya existe una plantilla con el nombre '{nombre_plantilla}'. Por favor, elige un nombre diferente.")
                            else:
                                plantilla_data = {
                                    'nombre': nombre_plantilla,
                                    'descripcion': descripcion_plantilla,
                                    'sucursal': gasto_seleccionado.get('sucursal'),
                                    'area': gasto_seleccionado.get('area'),
                                    'tipo': gasto_seleccionado.get('tipo'),
                                    'clasificacion': gasto_seleccionado.get('clasificacion'),
                                    'proveedor': gasto_seleccionado.get('proveedor'),
                                    'pct_postventa': gasto_seleccionado.get('pct_postventa', 0),
                                    'pct_servicios': gasto_seleccionado.get('pct_servicios', 0),
                                    'pct_repuestos': gasto_seleccionado.get('pct_repuestos', 0),
                                    'detalles': gasto_seleccionado.get('detalles'),
                                    'activa': 1
                                }
                                
                                try:
                                    insert_plantilla_gasto(plantilla_data)
                                    st.success(f"✅ Plantilla '{nombre_plantilla}' creada desde gasto ID {gasto_seleccionado_id}")
                                    st.rerun()
                                except Exception as e:
                                    if "UNIQUE constraint" in str(e) and "nombre" in str(e):
                                        st.error(f"❌ Ya existe una plantilla con el nombre '{nombre_plantilla}'. Por favor, elige un nombre diferente.")
                                    else:
                                        st.error(f"❌ Error: {e}")
            else:
                st.info("No hay gastos que coincidan con los filtros seleccionados")
        else:
            st.info("No hay gastos registrados. Primero registra algunos gastos.")

# ==================== IMPORTAR EXCEL ====================
elif page == "📥 Importar Excel":
    st.title("📥 Importar Datos desde Excel")
    
    # Sección para eliminar todos los registros
    st.subheader("🗑️ Limpiar Base de Datos")
    st.warning("⚠️ **ADVERTENCIA:** Esta acción eliminará TODOS los registros de ventas y gastos. Esta acción NO se puede deshacer.")
    
    col_eliminar1, col_eliminar2 = st.columns(2)
    
    with col_eliminar1:
        eliminar_plantillas = st.checkbox("También eliminar plantillas de gastos", value=False)
    
    with col_eliminar2:
        if st.button("🗑️ Eliminar Todos los Registros", type="primary"):
            with st.spinner("Eliminando registros..."):
                resultado = eliminar_todos_los_registros(eliminar_plantillas=eliminar_plantillas)
                if resultado['exito']:
                    st.success(f"✅ Registros eliminados exitosamente:")
                    st.write(f"- Ventas eliminadas: {resultado['ventas_eliminadas']}")
                    st.write(f"- Gastos eliminados: {resultado['gastos_eliminados']}")
                    if eliminar_plantillas:
                        st.write(f"- Plantillas eliminadas: {resultado['plantillas_eliminadas']}")
                    st.rerun()
                else:
                    st.error(f"❌ Error al eliminar: {resultado.get('error', 'Error desconocido')}")
    
    st.divider()
    
    st.subheader("📥 Importar desde Excel")
    st.info("""
    **Instrucciones:**
    - El archivo Excel debe tener las hojas: "REGISTRO VENTAS" y "REGISTRO GASTOS"
    - Las columnas deben coincidir con los nombres esperados
    - Los datos se agregarán a la base de datos existente
    """)
    
    archivo_excel = st.file_uploader("Subir archivo Excel", type=['xlsx', 'xls'])
    
    if archivo_excel is not None:
        try:
            # Guardar archivo temporalmente
            temp_path = Path(f"temp_{archivo_excel.name}")
            with open(temp_path, "wb") as f:
                f.write(archivo_excel.getbuffer())
            
            col1, col2 = st.columns(2)
            
            # Mostrar información del archivo
            try:
                excel_file = pd.ExcelFile(str(temp_path))
                st.info(f"📋 Hojas encontradas en el Excel: {', '.join(excel_file.sheet_names)}")
            except Exception as e:
                st.warning(f"⚠️ No se pudo leer el archivo Excel: {e}")
            
            with col1:
                if st.button("📥 Importar Ventas"):
                    with st.spinner("Importando ventas..."):
                        try:
                            count = import_ventas_from_excel(str(temp_path))
                            if count > 0:
                                st.success(f"✅ {count} ventas importadas exitosamente")
                            else:
                                st.warning(f"⚠️ No se importaron ventas. Verifica que la hoja 'REGISTRO VENTAS' tenga datos válidos.")
                        except Exception as e:
                            st.error(f"❌ Error al importar ventas: {e}")
                        finally:
                            if temp_path.exists():
                                temp_path.unlink()
            
            with col2:
                if st.button("📥 Importar Gastos"):
                    with st.spinner("Importando gastos..."):
                        try:
                            count = import_gastos_from_excel(str(temp_path))
                            if count > 0:
                                st.success(f"✅ {count} gastos importados exitosamente")
                            else:
                                st.warning(f"⚠️ No se importaron gastos. Verifica que la hoja 'REGISTRO GASTOS' tenga datos válidos con 'Total USD' > 0.")
                        except Exception as e:
                            st.error(f"❌ Error al importar gastos: {e}")
                        finally:
                            if temp_path.exists():
                                temp_path.unlink()
            
            if st.button("📥 Importar Todo"):
                with st.spinner("Importando todos los datos..."):
                    try:
                        count_ventas = import_ventas_from_excel(str(temp_path))
                        count_gastos = import_gastos_from_excel(str(temp_path))
                        if count_ventas > 0 or count_gastos > 0:
                            st.success(f"✅ {count_ventas} ventas y {count_gastos} gastos importados")
                        else:
                            st.warning(f"⚠️ No se importaron datos. Verifica que las hojas tengan datos válidos.")
                        if temp_path.exists():
                            temp_path.unlink()
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error al importar: {e}")
                        if temp_path.exists():
                            temp_path.unlink()
        except Exception as e:
            st.error(f"❌ Error al importar: {e}")

# ==================== VER REGISTROS ====================
elif page == "📋 Ver Registros":
    st.title("📋 Ver y Gestionar Registros")
    
    # Filtros de fecha globales
    st.subheader("📅 Filtros de Fecha")
    col_fecha1, col_fecha2, col_fecha3 = st.columns([2, 2, 1])
    with col_fecha1:
        fecha_inicio_ver = st.date_input("Fecha Inicio", value=date(2025, 11, 1), key="fecha_inicio_ver")
    with col_fecha2:
        fecha_fin_ver = st.date_input("Fecha Fin", value=date.today(), key="fecha_fin_ver")
    with col_fecha3:
        st.write("")  # Espacio vacío para alineación
        st.write("")  # Espacio vacío para alineación
    
    # Botón de exportar a Excel (arriba de los tabs)
    col_export1, col_export2 = st.columns([3, 1])
    with col_export1:
        st.write("")  # Espacio
    with col_export2:
        # Obtener datos filtrados por fecha para el export
        df_ventas_export = get_ventas(str(fecha_inicio_ver), str(fecha_fin_ver))
        df_gastos_export = get_gastos(str(fecha_inicio_ver), str(fecha_fin_ver))
        
        # Crear Excel en memoria
        try:
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # Hoja de Ventas
                df_ventas_export.to_excel(writer, sheet_name='REGISTRO VENTAS', index=False)
                
                # Hoja de Gastos
                df_gastos_export.to_excel(writer, sheet_name='REGISTRO GASTOS', index=False)
            
            output.seek(0)
            
            # Nombre del archivo con fechas
            nombre_archivo = f"Registros_{fecha_inicio_ver.strftime('%Y%m%d')}_{fecha_fin_ver.strftime('%Y%m%d')}.xlsx"
            
            st.download_button(
                label="📥 Exportar a Excel",
                data=output.getvalue(),
                file_name=nombre_archivo,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help=f"Exporta {len(df_ventas_export)} ventas y {len(df_gastos_export)} gastos del período seleccionado"
            )
        except Exception as e:
            st.error(f"❌ Error al generar Excel: {e}")
    
    st.divider()
    
    tab1, tab2 = st.tabs(["💰 Ventas", "💸 Gastos"])
    
    with tab1:
        st.subheader("Registro de Ventas")
        df_ventas = get_ventas(str(fecha_inicio_ver), str(fecha_fin_ver))
        
        if len(df_ventas) > 0:
            st.write(f"Total de registros: {len(df_ventas)}")
            
            # Filtros
            col1, col2, col3 = st.columns(3)
            with col1:
                sucursales = ['Todas'] + list(df_ventas['sucursal'].dropna().unique())
                filtro_sucursal = st.selectbox("Filtrar por Sucursal", sucursales)
            with col2:
                tipos = ['Todos'] + list(df_ventas['tipo_re_se'].dropna().unique())
                filtro_tipo = st.selectbox("Filtrar por Tipo", tipos)
            with col3:
                buscar = st.text_input("Buscar cliente")
            
            # Aplicar filtros
            df_filtrado = df_ventas.copy()
            if filtro_sucursal != 'Todas':
                df_filtrado = df_filtrado[df_filtrado['sucursal'] == filtro_sucursal]
            if filtro_tipo != 'Todos':
                df_filtrado = df_filtrado[df_filtrado['tipo_re_se'] == filtro_tipo]
            if buscar:
                df_filtrado = df_filtrado[df_filtrado['cliente'].str.contains(buscar, case=False, na=False)]
            
            # Seleccionar registro para editar/eliminar
            st.subheader("Editar o Eliminar Registro")
            venta_ids = ['Seleccionar...'] + [int(x) for x in df_filtrado['id'].tolist()]
            venta_seleccionada = st.selectbox("Selecciona una venta por ID", venta_ids, key="select_venta")
            
            if venta_seleccionada != 'Seleccionar...':
                venta_data = get_venta_by_id(venta_seleccionada)
                
                if venta_data:
                    tab_ver, tab_editar, tab_eliminar = st.tabs(["👁️ Ver", "✏️ Editar", "🗑️ Eliminar"])
                    
                    with tab_ver:
                        st.write(f"**ID:** {venta_data['id']}")
                        st.write(f"**Fecha:** {venta_data['fecha']}")
                        st.write(f"**Sucursal:** {venta_data['sucursal']}")
                        st.write(f"**Cliente:** {venta_data['cliente']}")
                        st.write(f"**Tipo:** {venta_data['tipo_re_se']}")
                        st.write(f"**Total:** {formatear_moneda(venta_data['total'])} USD")
                        if venta_data.get('archivo_comprobante') and os.path.exists(venta_data['archivo_comprobante']):
                            archivo_path = Path(venta_data['archivo_comprobante'])
                            
                            # Leer archivo una sola vez
                            with open(archivo_path, "rb") as f:
                                archivo_bytes = f.read()
                            
                            # Botones de acción
                            col_btn1, col_btn2 = st.columns(2)
                            
                            with col_btn1:
                                if archivo_path.suffix.lower() == '.pdf':
                                    st.download_button("📄 Descargar PDF", archivo_bytes, file_name=archivo_path.name, mime="application/pdf")
                                else:
                                    st.download_button("🖼️ Descargar Imagen", archivo_bytes, file_name=archivo_path.name, mime=f"image/{archivo_path.suffix[1:]}")
                            
                            with col_btn2:
                                if archivo_path.suffix.lower() == '.pdf':
                                    preview_key = f'preview_pdf_{venta_seleccionada}'
                                    if st.button("👁️ Previsualizar PDF" if not st.session_state.get(preview_key, False) else "❌ Cerrar Previsualización"):
                                        st.session_state[preview_key] = not st.session_state.get(preview_key, False)
                                        st.rerun()
                                else:
                                    # Para imágenes, mostrar directamente
                                    st.image(str(archivo_path), width=400)
                            
                            # Mostrar preview del PDF si está activado
                            if archivo_path.suffix.lower() == '.pdf' and st.session_state.get(f'preview_pdf_{venta_seleccionada}', False):
                                st.subheader("📄 Previsualización del PDF")
                                
                                # Convertir PDF a base64
                                base64_pdf = base64.b64encode(archivo_bytes).decode('utf-8')
                                
                                # Verificar tamaño del PDF (data URIs tienen límites ~2MB en algunos navegadores)
                                pdf_size_mb = len(archivo_bytes) / (1024 * 1024)
                                
                                if pdf_size_mb > 2:
                                    st.warning(f"⚠️ El PDF es grande ({pdf_size_mb:.1f} MB). La previsualización puede tardar o no funcionar. Usa el botón de descarga si hay problemas.")
                                
                                # Usar st.components.v1.html para mejor compatibilidad en Streamlit Cloud
                                # Intentar con iframe primero, con fallback a embed
                                pdf_html = f"""
                                <div style="width: 100%; height: 600px; border: 1px solid #ccc;">
                                    <iframe 
                                        src="data:application/pdf;base64,{base64_pdf}" 
                                        width="100%" 
                                        height="100%" 
                                        type="application/pdf"
                                        style="border: none;"
                                        onerror="this.style.display='none'; this.nextElementSibling.style.display='block';">
                                    </iframe>
                                    <embed 
                                        src="data:application/pdf;base64,{base64_pdf}" 
                                        type="application/pdf" 
                                        width="100%" 
                                        height="100%"
                                        style="display: none; border: none;"
                                        onerror="this.parentElement.innerHTML='<p style=\\'padding: 20px; text-align: center; color: #666;\\'>⚠️ No se pudo cargar la previsualización. Por favor, usa el botón de descarga.</p>'">
                                    </embed>
                                </div>
                                """
                                
                                try:
                                    import streamlit.components.v1 as components
                                    components.html(pdf_html, height=600)
                                except Exception as e:
                                    # Fallback a markdown si components.html falla
                                    st.markdown(f'<iframe src="data:application/pdf;base64,{base64_pdf}" width="100%" height="600px" type="application/pdf"></iframe>', unsafe_allow_html=True)
                                    st.warning(f"⚠️ Error al cargar previsualización: {e}. Usa el botón de descarga.")
                    
                    with tab_editar:
                        with st.form(f"form_edit_venta_{venta_seleccionada}"):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                fecha_edit = st.date_input("Fecha", value=pd.to_datetime(venta_data['fecha']).date() if pd.notna(venta_data['fecha']) else date.today())
                                sucursal_edit = st.selectbox("Sucursal", ["COMODORO", "RIO GRANDE", "RIO GALLEGOS"], 
                                                             index=["COMODORO", "RIO GRANDE", "RIO GALLEGOS"].index(venta_data['sucursal']) if venta_data['sucursal'] in ["COMODORO", "RIO GRANDE", "RIO GALLEGOS"] else 0)
                                cliente_edit = st.text_input("Cliente", value=venta_data['cliente'] or '')
                                pin_edit = st.text_input("PIN", value=venta_data['pin'] or '')
                                comprobante_edit = st.text_input("Comprobante", value=venta_data['comprobante'] or '')
                                tipos_comprobante_list = ["FACTURA VENTA", "NOTA DE CREDITO", "NOTA DE CREDITO JD", "CITA INTERNA", "FACTURA AFIP", "NOTA DE CRETIDO"]
                                tipo_comprobante_actual = venta_data.get('tipo_comprobante', 'FACTURA VENTA')
                                index_tipo = tipos_comprobante_list.index(tipo_comprobante_actual) if tipo_comprobante_actual in tipos_comprobante_list else 0
                                tipo_comprobante_edit = st.selectbox("Tipo Comprobante", 
                                                                     tipos_comprobante_list,
                                                                     index=index_tipo)
                                trabajo_edit = st.selectbox("Trabajo", ["EXTERNO", "GARANTIA", "INTERNO"],
                                                            index=["EXTERNO", "GARANTIA", "INTERNO"].index(venta_data['trabajo']) if venta_data['trabajo'] in ["EXTERNO", "GARANTIA", "INTERNO"] else 0)
                            
                            with col2:
                                n_comprobante_edit = st.text_input("N° Comprobante", value=venta_data['n_comprobante'] or '')
                                tipo_re_se_edit = st.selectbox("Tipo (RE o SE)", ["SE", "RE"],
                                                              index=0 if venta_data['tipo_re_se'] == 'SE' else 1)
                                mano_obra_edit = st.number_input("Mano de Obra", value=float(venta_data['mano_obra']) or 0.0, step=0.01)
                                asistencia_edit = st.number_input("Asistencia", value=float(venta_data['asistencia']) or 0.0, step=0.01)
                                repuestos_edit = st.number_input("Repuestos", value=float(venta_data['repuestos']) or 0.0, step=0.01)
                                terceros_edit = st.number_input("Terceros", value=float(venta_data['terceros']) or 0.0, step=0.01)
                                descuento_edit = st.number_input("Descuento", value=float(venta_data['descuento']) or 0.0, step=0.01)
                                detalles_edit = st.text_area("Detalles", value=venta_data['detalles'] or '')
                            
                            # Calcular total automáticamente
                            total_edit = mano_obra_edit + asistencia_edit + repuestos_edit + terceros_edit - descuento_edit
                            
                            # Si es nota de crédito (pero NO JD), convertir el total a negativo automáticamente
                            # NOTA: "NOTA DE CREDITO JD" son pagos recibidos de John Deere, por lo que son POSITIVOS
                            es_nota_credito_edit = tipo_comprobante_edit and 'NOTA DE CREDITO' in tipo_comprobante_edit.upper() and 'JD' not in tipo_comprobante_edit.upper()
                            total_edit_mostrar = total_edit
                            if es_nota_credito_edit and total_edit > 0:
                                total_edit_mostrar = -total_edit  # Mostrar como negativo
                            
                            st.metric("💰 Total Calculado", f"{formatear_moneda(total_edit_mostrar)} USD",
                                     delta=f"Mano Obra: {formatear_moneda(mano_obra_edit)} + Asistencia: {formatear_moneda(asistencia_edit)} + Repuestos: {formatear_moneda(repuestos_edit)} + Terceros: {formatear_moneda(terceros_edit)} - Descuento: {formatear_moneda(descuento_edit)}")
                            
                            # Sección para adjuntar/cambiar comprobante
                            st.subheader("📎 Comprobante Adjunto")
                            
                            # Mostrar comprobante actual si existe
                            archivo_actual = venta_data.get('archivo_comprobante')
                            if archivo_actual and os.path.exists(archivo_actual):
                                archivo_path_actual = Path(archivo_actual)
                                st.info(f"📄 Comprobante actual: {archivo_path_actual.name}")
                                
                                # Mostrar preview si es imagen
                                if archivo_path_actual.suffix.lower() in ['.png', '.jpg', '.jpeg']:
                                    st.image(str(archivo_path_actual), width=300)
                                else:
                                    st.write("📄 Archivo PDF")
                                
                                # Opción para eliminar comprobante actual
                                eliminar_comprobante = st.checkbox("🗑️ Eliminar comprobante actual")
                            else:
                                eliminar_comprobante = False
                                st.info("No hay comprobante adjunto")
                            
                            # Campo para subir nuevo comprobante
                            archivo_comprobante_edit = st.file_uploader(
                                "Subir nuevo comprobante (PDF o Imagen)",
                                type=['pdf', 'png', 'jpg', 'jpeg'],
                                help="Puedes adjuntar un PDF o imagen del comprobante. Si subes uno nuevo, reemplazará el actual."
                            )
                            
                            if archivo_comprobante_edit is not None:
                                # Mostrar preview si es imagen
                                if archivo_comprobante_edit.type.startswith('image/'):
                                    st.image(archivo_comprobante_edit, width=300)
                                else:
                                    st.info(f"📄 Archivo PDF: {archivo_comprobante_edit.name}")
                            
                            if st.form_submit_button("💾 Guardar Cambios"):
                                # Si es nota de crédito, convertir el total a negativo automáticamente
                                total_edit_final = total_edit
                                if es_nota_credito_edit and total_edit > 0:
                                    total_edit_final = -total_edit  # Convertir a negativo
                                
                                # Validar que el total no sea 0
                                if total_edit_final == 0:
                                    st.error("⚠️ El total no puede ser 0. Verifica los valores ingresados.")
                                else:
                                    # Manejar archivo de comprobante
                                    ruta_archivo_edit = venta_data.get('archivo_comprobante')
                                    
                                    # Si se marca eliminar, borrar el archivo actual
                                    if eliminar_comprobante and ruta_archivo_edit and os.path.exists(ruta_archivo_edit):
                                        try:
                                            os.unlink(ruta_archivo_edit)
                                            ruta_archivo_edit = None
                                        except Exception as e:
                                            st.warning(f"⚠️ Error al eliminar archivo: {e}")
                                    
                                    # Si se sube un nuevo archivo
                                    if archivo_comprobante_edit is not None:
                                        try:
                                            # Eliminar archivo anterior si existe
                                            if ruta_archivo_edit and os.path.exists(ruta_archivo_edit):
                                                try:
                                                    os.unlink(ruta_archivo_edit)
                                                except:
                                                    pass
                                            
                                            # Guardar nuevo archivo
                                            extension = Path(archivo_comprobante_edit.name).suffix
                                            nombre_archivo = f"venta_{venta_seleccionada}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{archivo_comprobante_edit.name}"
                                            ruta_archivo_edit = COMPROBANTES_DIR / nombre_archivo
                                            
                                            with open(ruta_archivo_edit, "wb") as f:
                                                f.write(archivo_comprobante_edit.getbuffer())
                                            
                                            ruta_archivo_edit = str(ruta_archivo_edit)
                                        except Exception as e:
                                            st.warning(f"⚠️ Error al guardar el archivo: {e}")
                                            ruta_archivo_edit = venta_data.get('archivo_comprobante')  # Mantener el anterior si hay error
                                    
                                    venta_actualizada = {
                                        'mes': fecha_edit.strftime("%B%y"),
                                        'fecha': fecha_edit,
                                        'sucursal': sucursal_edit,
                                        'cliente': cliente_edit,
                                        'pin': pin_edit if pin_edit else None,
                                        'comprobante': comprobante_edit if comprobante_edit else None,
                                        'tipo_comprobante': tipo_comprobante_edit,
                                        'trabajo': trabajo_edit,
                                        'n_comprobante': n_comprobante_edit if n_comprobante_edit else None,
                                        'tipo_re_se': tipo_re_se_edit,
                                        'mano_obra': mano_obra_edit,
                                        'asistencia': asistencia_edit,
                                        'repuestos': repuestos_edit,
                                        'terceros': terceros_edit,
                                        'descuento': descuento_edit,
                                        'total': total_edit_final,  # Total calculado automáticamente (negativo si es nota de crédito)
                                        'detalles': detalles_edit if detalles_edit else None,
                                        'archivo_comprobante': ruta_archivo_edit
                                    }
                                    
                                    try:
                                        update_venta(venta_seleccionada, venta_actualizada)
                                        st.success(f"✅ Venta {venta_seleccionada} actualizada exitosamente!")
                                        if archivo_comprobante_edit:
                                            st.success(f"📎 Nuevo comprobante guardado: {Path(ruta_archivo_edit).name}")
                                        elif eliminar_comprobante:
                                            st.success("🗑️ Comprobante eliminado")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"❌ Error al actualizar: {e}")
                    
                    with tab_eliminar:
                        st.warning(f"⚠️ Estás a punto de eliminar la venta ID: {venta_seleccionada}")
                        st.write(f"Cliente: {venta_data['cliente']}")
                        st.write(f"Total: {formatear_moneda(venta_data['total'])} USD")
                        
                        if st.button("🗑️ Confirmar Eliminación", type="primary"):
                            try:
                                delete_venta(venta_seleccionada)
                                st.success(f"✅ Venta {venta_seleccionada} eliminada")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error: {e}")
            
            # Tabla resumen - Mostrar últimos 20 registros ordenados por ID descendente
            st.subheader("Resumen de Registros (Últimos 20)")
            df_resumen = df_filtrado.sort_values('id', ascending=False).head(20)
            st.dataframe(df_resumen[['id', 'fecha', 'sucursal', 'cliente', 'tipo_re_se', 'total']], 
                        use_container_width=True)
        else:
            st.info("No hay ventas registradas")
    
    with tab2:
        st.subheader("Registro de Gastos")
        df_gastos = get_gastos(str(fecha_inicio_ver), str(fecha_fin_ver))
        
        if len(df_gastos) > 0:
            st.write(f"Total de registros: {len(df_gastos)}")
            
            # Filtros
            col1, col2 = st.columns(2)
            with col1:
                sucursales = ['Todas'] + list(df_gastos['sucursal'].dropna().unique())
                filtro_sucursal = st.selectbox("Filtrar por Sucursal (Gastos)", sucursales)
            with col2:
                areas = ['Todas'] + list(df_gastos['area'].dropna().unique())
                filtro_area = st.selectbox("Filtrar por Área", areas)
            
            # Aplicar filtros
            df_filtrado = df_gastos.copy()
            if filtro_sucursal != 'Todas':
                df_filtrado = df_filtrado[df_filtrado['sucursal'] == filtro_sucursal]
            if filtro_area != 'Todas':
                df_filtrado = df_filtrado[df_filtrado['area'] == filtro_area]
            
            # Seleccionar registro para editar/eliminar
            st.subheader("Editar o Eliminar Registro")
            gasto_ids = ['Seleccionar...'] + [int(x) for x in df_filtrado['id'].tolist()]
            gasto_seleccionado = st.selectbox("Selecciona un gasto por ID", gasto_ids, key="select_gasto")
            
            if gasto_seleccionado != 'Seleccionar...':
                gasto_data = get_gasto_by_id(gasto_seleccionado)
                
                if gasto_data:
                    tab_ver, tab_editar, tab_eliminar = st.tabs(["👁️ Ver", "✏️ Editar", "🗑️ Eliminar"])
                    
                    with tab_ver:
                        st.write(f"**ID:** {gasto_data['id']}")
                        st.write(f"**Fecha:** {gasto_data['fecha']}")
                        st.write(f"**Sucursal:** {gasto_data['sucursal']}")
                        st.write(f"**Área:** {gasto_data['area']}")
                        st.write(f"**Clasificación:** {gasto_data['clasificacion']}")
                        st.write(f"**Total USD:** {formatear_moneda(gasto_data['total_usd'])}")
                        st.write(f"**%SE:** {formatear_moneda(gasto_data['total_pct_se'])}")
                        st.write(f"**%RE:** {formatear_moneda(gasto_data['total_pct_re'])}")
                    
                    with tab_editar:
                        with st.form(f"form_edit_gasto_{gasto_seleccionado}"):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                fecha_edit = st.date_input("Fecha", value=pd.to_datetime(gasto_data['fecha']).date() if pd.notna(gasto_data['fecha']) else date.today())
                                sucursal_edit = st.selectbox("Sucursal", ["COMODORO", "RIO GRANDE", "RIO GALLEGOS", "COMPARTIDOS"],
                                                            index=["COMODORO", "RIO GRANDE", "RIO GALLEGOS", "COMPARTIDOS"].index(gasto_data['sucursal']) if gasto_data['sucursal'] in ["COMODORO", "RIO GRANDE", "RIO GALLEGOS", "COMPARTIDOS"] else 0)
                                area_edit = st.selectbox("Área", ["POSTVENTA", "SERVICIO", "REPUESTOS"],
                                                        index=["POSTVENTA", "SERVICIO", "REPUESTOS"].index(gasto_data['area']) if gasto_data['area'] in ["POSTVENTA", "SERVICIO", "REPUESTOS"] else 0)
                                tipo_edit = st.selectbox("Tipo", ["FIJO", "VARIABLE"],
                                                        index=0 if gasto_data['tipo'] == 'FIJO' else 1)
                                clasificacion_edit = st.text_input("Clasificación", value=gasto_data['clasificacion'] or '')
                                proveedor_edit = st.text_input("Proveedor", value=gasto_data['proveedor'] or '')
                                total_pesos_edit = st.number_input("Total Pesos", value=float(gasto_data['total_pesos']) if gasto_data['total_pesos'] else 0.0, step=0.01)
                            
                            with col2:
                                total_usd_edit = st.number_input("Total USD", value=float(gasto_data['total_usd']) or 0.0, step=0.01)
                                pct_postventa_edit = st.number_input("% Postventa", min_value=0.0, max_value=1.0, value=float(gasto_data['pct_postventa']) or 0.0, step=0.01)
                                pct_servicios_edit = st.number_input("% Servicios", min_value=0.0, max_value=1.0, value=float(gasto_data['pct_servicios']) or 0.0, step=0.01)
                                pct_repuestos_edit = st.number_input("% Repuestos", min_value=0.0, max_value=1.0, value=float(gasto_data['pct_repuestos']) or 0.0, step=0.01)
                                detalles_edit = st.text_area("Detalles", value=gasto_data['detalles'] or '')
                            
                            total_pct_edit = total_usd_edit * pct_postventa_edit if pct_postventa_edit > 0 else 0
                            total_pct_se_edit = total_pct_edit * pct_servicios_edit if pct_servicios_edit > 0 else 0
                            total_pct_re_edit = total_pct_edit * pct_repuestos_edit if pct_repuestos_edit > 0 else 0
                            
                            st.info(f"📊 Total %: {formatear_moneda(total_pct_edit)} | %SE: {formatear_moneda(total_pct_se_edit)} | %RE: {formatear_moneda(total_pct_re_edit)}")
                            
                            if st.form_submit_button("💾 Guardar Cambios"):
                                gasto_actualizado = {
                                    'mes': fecha_edit.strftime("%B"),
                                    'fecha': fecha_edit,
                                    'sucursal': sucursal_edit,
                                    'area': area_edit,
                                    'pct_postventa': pct_postventa_edit,
                                    'pct_servicios': pct_servicios_edit,
                                    'pct_repuestos': pct_repuestos_edit,
                                    'tipo': tipo_edit,
                                    'clasificacion': clasificacion_edit,
                                    'proveedor': proveedor_edit if proveedor_edit else None,
                                    'total_pesos': total_pesos_edit if total_pesos_edit > 0 else None,
                                    'total_usd': total_usd_edit,
                                    'total_pct': total_pct_edit,
                                    'total_pct_se': total_pct_se_edit,
                                    'total_pct_re': total_pct_re_edit,
                                    'detalles': detalles_edit if detalles_edit else None
                                }
                                
                                try:
                                    update_gasto(gasto_seleccionado, gasto_actualizado)
                                    st.success(f"✅ Gasto {gasto_seleccionado} actualizado exitosamente!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Error al actualizar: {e}")
                    
                    with tab_eliminar:
                        st.warning(f"⚠️ Estás a punto de eliminar el gasto ID: {gasto_seleccionado}")
                        st.write(f"Clasificación: {gasto_data['clasificacion']}")
                        st.write(f"Total USD: {formatear_moneda(gasto_data['total_usd'])}")
                        
                        if st.button("🗑️ Confirmar Eliminación", type="primary"):
                            try:
                                delete_gasto(gasto_seleccionado)
                                st.success(f"✅ Gasto {gasto_seleccionado} eliminado")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error: {e}")
            
            # Tabla resumen - Mostrar últimos 20 registros ordenados por ID descendente
            st.subheader("Resumen de Registros (Últimos 20)")
            df_resumen = df_filtrado.sort_values('id', ascending=False).head(20)
            st.dataframe(df_resumen[['id', 'fecha', 'sucursal', 'area', 'clasificacion', 'total_usd', 'total_pct_se', 'total_pct_re']], 
                        use_container_width=True)
        else:
            st.info("No hay gastos registrados")


# ==================== REPORTES ====================
elif page == "📈 Reportes":
    st.title("📈 Reportes Detallados")
    
    # Filtros de fecha
    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio = st.date_input("Fecha Inicio (Reportes)", value=date(2025, 11, 1))
    with col2:
        fecha_fin = st.date_input("Fecha Fin (Reportes)", value=date.today())
    
    df_ventas = get_ventas(str(fecha_inicio), str(fecha_fin))
    df_gastos = get_gastos(str(fecha_inicio), str(fecha_fin))
    
    if len(df_ventas) == 0 and len(df_gastos) == 0:
        st.info("No hay datos para el período seleccionado")
    else:
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Resumen", "💰 Ventas", "💸 Gastos", "📈 Análisis", "📊 KPIs Financieros"])
        
        with tab1:
            st.subheader("Resumen Ejecutivo")
            
            total_ingresos = df_ventas['total'].sum() if len(df_ventas) > 0 else 0
            # Calcular ingresos netos descontando 4.5% de IIBB
            porcentaje_iibb = 0.045  # 4.5%
            descuento_iibb = total_ingresos * porcentaje_iibb
            ingresos_netos = total_ingresos - descuento_iibb
            
            # Obtener gastos incluyendo automáticos
            gastos_totales = obtener_gastos_totales_con_automaticos(str(fecha_inicio), str(fecha_fin))
            gastos_postventa = gastos_totales['gastos_postventa_total']
            resultado = ingresos_netos - gastos_postventa
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Ingresos Totales (Bruto)", f"{formatear_moneda(total_ingresos)} USD")
                st.caption(f"Descuento IIBB (4.5%): {formatear_moneda(descuento_iibb)} USD")
                st.metric("Ingresos Netos", f"{formatear_moneda(ingresos_netos)} USD", 
                         delta=f"-{formatear_moneda(descuento_iibb)} USD")
            
            with col2:
                st.metric("Gastos Postventa", f"{formatear_moneda(gastos_postventa)} USD")
                st.metric("Resultado Neto", f"{formatear_moneda(resultado)} USD")
            
            # Comparación por Sucursal
            st.divider()
            st.subheader("📊 Comparación por Sucursal")
            
            # Obtener gastos totales con automáticos (esto ya filtra los automáticos de los registrados)
            gastos_totales = obtener_gastos_totales_con_automaticos(str(fecha_inicio), str(fecha_fin))
            
            # Obtener todas las sucursales únicas
            sucursales_ventas = df_ventas['sucursal'].unique() if len(df_ventas) > 0 else []
            df_gastos_registrados = gastos_totales['gastos_registrados']
            df_gastos_automaticos = gastos_totales['gastos_automaticos']
            sucursales_gastos_reg = df_gastos_registrados['sucursal'].unique() if len(df_gastos_registrados) > 0 else []
            sucursales_gastos_auto = df_gastos_automaticos['sucursal'].unique() if len(df_gastos_automaticos) > 0 else []
            todas_sucursales = sorted(set(list(sucursales_ventas) + list(sucursales_gastos_reg) + list(sucursales_gastos_auto)))
            
            if len(todas_sucursales) > 0:
                # Crear DataFrame para comparación
                datos_comparacion = []
                
                for sucursal in todas_sucursales:
                    # Ingresos por sucursal
                    ingresos_sucursal = df_ventas[df_ventas['sucursal'] == sucursal]['total'].sum() if len(df_ventas) > 0 else 0
                    descuento_iibb_sucursal = ingresos_sucursal * porcentaje_iibb
                    ingresos_netos_sucursal = ingresos_sucursal - descuento_iibb_sucursal
                    
                    # Gastos por sucursal (incluyendo automáticos)
                    # Gastos registrados por sucursal (usar los ya filtrados, sin automáticos)
                    gastos_registrados_sucursal = 0
                    if len(df_gastos_registrados) > 0:
                        gastos_registrados_sucursal = (df_gastos_registrados[df_gastos_registrados['sucursal'] == sucursal]['total_pct_se'].sum() + 
                                                       df_gastos_registrados[df_gastos_registrados['sucursal'] == sucursal]['total_pct_re'].sum())
                    
                    # Gastos automáticos por sucursal
                    gastos_automaticos_sucursal = 0
                    if len(df_gastos_automaticos) > 0:
                        gastos_automaticos_sucursal = (df_gastos_automaticos[df_gastos_automaticos['sucursal'] == sucursal]['total_pct_se'].sum() + 
                                                       df_gastos_automaticos[df_gastos_automaticos['sucursal'] == sucursal]['total_pct_re'].sum())
                    
                    gastos_totales_sucursal = gastos_registrados_sucursal + gastos_automaticos_sucursal
                    
                    # Resultado por sucursal
                    resultado_sucursal = ingresos_netos_sucursal - gastos_totales_sucursal
                    
                    datos_comparacion.append({
                        'Sucursal': sucursal if sucursal else 'Sin Sucursal',
                        'Ingresos Brutos': ingresos_sucursal,
                        'Descuento IIBB': descuento_iibb_sucursal,
                        'Ingresos Netos': ingresos_netos_sucursal,
                        'Gastos Totales': gastos_totales_sucursal,
                        'Resultado': resultado_sucursal
                    })
                
                # Crear DataFrame y formatear valores para mostrar
                df_comparacion = pd.DataFrame(datos_comparacion)
                
                # Crear copia para mostrar con valores formateados
                df_comparacion_display = df_comparacion.copy()
                df_comparacion_display['Ingresos Brutos'] = df_comparacion_display['Ingresos Brutos'].apply(lambda x: formatear_moneda(x))
                df_comparacion_display['Descuento IIBB'] = df_comparacion_display['Descuento IIBB'].apply(lambda x: formatear_moneda(x))
                df_comparacion_display['Ingresos Netos'] = df_comparacion_display['Ingresos Netos'].apply(lambda x: formatear_moneda(x))
                df_comparacion_display['Gastos Totales'] = df_comparacion_display['Gastos Totales'].apply(lambda x: formatear_moneda(x))
                df_comparacion_display['Resultado'] = df_comparacion_display['Resultado'].apply(lambda x: f"{'✅' if x >= 0 else '❌'} {formatear_moneda(x)}")
                
                # Mostrar tabla
                st.dataframe(
                    df_comparacion_display,
                    use_container_width=True,
                    hide_index=True
                )
                
                # Mostrar también en columnas para mejor visualización
                st.markdown("#### 📈 Detalle por Sucursal")
                cols = st.columns(len(todas_sucursales))
                
                for idx, sucursal in enumerate(todas_sucursales):
                    with cols[idx]:
                        datos = datos_comparacion[idx]
                        st.markdown(f"**{datos['Sucursal']}**")
                        st.metric("Ingresos Netos", formatear_moneda(datos['Ingresos Netos']), 
                                 delta=f"Bruto: {formatear_moneda(datos['Ingresos Brutos'])}")
                        st.metric("Gastos Totales", formatear_moneda(datos['Gastos Totales']))
                        resultado_color = "✅" if datos['Resultado'] >= 0 else "❌"
                        st.metric("Resultado", f"{resultado_color} {formatear_moneda(datos['Resultado'])}")
            else:
                st.info("No hay datos de sucursales para mostrar")
        
        with tab2:
            st.subheader("Análisis de Ventas")
            
            if len(df_ventas) > 0:
                # Calcular total de repuestos vendidos (RE + repuestos en SE)
                ventas_re = df_ventas[df_ventas['tipo_re_se'] == 'RE']
                ventas_se = df_ventas[df_ventas['tipo_re_se'] == 'SE']
                
                total_ventas_re = ventas_re['total'].sum() if len(ventas_re) > 0 else 0
                repuestos_en_se = ventas_se['repuestos'].sum() if len(ventas_se) > 0 and 'repuestos' in ventas_se.columns else 0
                total_repuestos_vendidos = total_ventas_re + repuestos_en_se
                
                # Calcular otros componentes de ventas
                total_mano_obra = df_ventas['mano_obra'].sum() if 'mano_obra' in df_ventas.columns else 0
                total_asistencia = df_ventas['asistencia'].sum() if 'asistencia' in df_ventas.columns else 0
                total_terceros = df_ventas['terceros'].sum() if 'terceros' in df_ventas.columns else 0
                
                # Mostrar métricas de repuestos vendidos
                st.write("**🔧 Repuestos Vendidos**")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("🔧 Total Repuestos Vendidos", 
                             formatear_moneda(total_repuestos_vendidos),
                             help="Suma de ventas RE + repuestos vendidos en servicios SE")
                with col2:
                    st.metric("📦 Ventas RE (Repuestos)", 
                             formatear_moneda(total_ventas_re))
                with col3:
                    st.metric("🔩 Repuestos en Servicios SE", 
                             formatear_moneda(repuestos_en_se))
                
                st.divider()
                
                # Mostrar métricas de otros componentes
                st.write("**💰 Otros Componentes de Ventas**")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("🔨 Mano de Obra", 
                             formatear_moneda(total_mano_obra))
                with col2:
                    st.metric("🛠️ Asistencias", 
                             formatear_moneda(total_asistencia))
                with col3:
                    st.metric("👥 Terceros", 
                             formatear_moneda(total_terceros))
                
                st.divider()
                
                # Por sucursal
                st.write("**Ventas por Sucursal**")
                ventas_sucursal = df_ventas.groupby('sucursal')['total'].sum().sort_values(ascending=False)
                st.bar_chart(ventas_sucursal)
                
                # Por cliente
                st.write("**Top 10 Clientes**")
                top_clientes = df_ventas.groupby('cliente')['total'].sum().sort_values(ascending=False).head(10).reset_index()
                # Formatear total como moneda
                top_clientes['total'] = top_clientes['total'].apply(lambda x: formatear_moneda(x))
                st.dataframe(top_clientes)
                
                # Por tipo
                st.write("**Ventas por Tipo**")
                ventas_tipo = df_ventas.groupby('tipo_re_se')['total'].sum().reset_index()
                # Formatear total como moneda
                ventas_tipo['total'] = ventas_tipo['total'].apply(lambda x: formatear_moneda(x))
                st.dataframe(ventas_tipo)
            else:
                st.info("No hay datos de ventas")
        
        with tab3:
            st.subheader("Análisis de Gastos")
            
            if len(df_gastos) > 0:
                # Por clasificación
                st.write("**Gastos por Clasificación**")
                gastos_clasif = df_gastos.groupby('clasificacion')['total_usd'].sum().sort_values(ascending=False)
                st.bar_chart(gastos_clasif.head(10))
                
                # Por área
                st.write("**Gastos por Área**")
                gastos_area = df_gastos.groupby('area')['total_usd'].sum().reset_index()
                # Formatear total_usd como moneda
                gastos_area['total_usd'] = gastos_area['total_usd'].apply(lambda x: formatear_moneda(x))
                st.dataframe(gastos_area)
                
                # Por tipo (fijo/variable)
                st.write("**Gastos Fijos vs Variables**")
                gastos_tipo = df_gastos.groupby('tipo')['total_usd'].sum().reset_index()
                # Formatear total_usd como moneda
                gastos_tipo['total_usd'] = gastos_tipo['total_usd'].apply(lambda x: formatear_moneda(x))
                st.dataframe(gastos_tipo)
                
                # Por sucursal (incluyendo gastos automáticos)
                st.write("**Gastos por Sucursal**")
                
                # Obtener gastos totales con automáticos (esto ya filtra los automáticos de los registrados)
                gastos_totales = obtener_gastos_totales_con_automaticos(str(fecha_inicio), str(fecha_fin))
                
                # Gastos registrados por sucursal (usar los ya filtrados, sin automáticos)
                df_gastos_registrados = gastos_totales['gastos_registrados']
                if len(df_gastos_registrados) > 0:
                    # Calcular total_usd como suma de total_pct_se + total_pct_re para cada gasto
                    df_gastos_registrados = df_gastos_registrados.copy()
                    df_gastos_registrados['total_usd'] = df_gastos_registrados['total_pct_se'] + df_gastos_registrados['total_pct_re']
                    gastos_registrados_sucursal = df_gastos_registrados.groupby('sucursal')['total_usd'].sum().reset_index()
                    gastos_registrados_sucursal.columns = ['sucursal', 'gastos_registrados']
                else:
                    gastos_registrados_sucursal = pd.DataFrame(columns=['sucursal', 'gastos_registrados'])
                
                # Gastos automáticos por sucursal
                df_gastos_automaticos = gastos_totales['gastos_automaticos']
                gastos_automaticos_sucursal = pd.DataFrame(columns=['sucursal', 'gastos_automaticos'])
                if len(df_gastos_automaticos) > 0:
                    # Sumar total_pct_se y total_pct_re por sucursal
                    gastos_auto_se = df_gastos_automaticos.groupby('sucursal')['total_pct_se'].sum().reset_index()
                    gastos_auto_re = df_gastos_automaticos.groupby('sucursal')['total_pct_re'].sum().reset_index()
                    
                    # Combinar y sumar
                    gastos_automaticos_sucursal = gastos_auto_se.merge(
                        gastos_auto_re, 
                        on='sucursal', 
                        how='outer'
                    ).fillna(0)
                    gastos_automaticos_sucursal['gastos_automaticos'] = (
                        gastos_automaticos_sucursal['total_pct_se'] + 
                        gastos_automaticos_sucursal['total_pct_re']
                    )
                    gastos_automaticos_sucursal = gastos_automaticos_sucursal[['sucursal', 'gastos_automaticos']]
                
                # Combinar gastos registrados y automáticos
                gastos_por_sucursal = gastos_registrados_sucursal.merge(
                    gastos_automaticos_sucursal, 
                    on='sucursal', 
                    how='outer'
                ).fillna(0)
                
                # Calcular totales
                gastos_por_sucursal['gastos_totales'] = (
                    gastos_por_sucursal['gastos_registrados'] + 
                    gastos_por_sucursal['gastos_automaticos']
                )
                
                # Ordenar por totales descendente
                gastos_por_sucursal = gastos_por_sucursal.sort_values('gastos_totales', ascending=False)
                
                # Formatear valores como moneda
                gastos_por_sucursal['gastos_registrados'] = gastos_por_sucursal['gastos_registrados'].apply(lambda x: formatear_moneda(x))
                gastos_por_sucursal['gastos_automaticos'] = gastos_por_sucursal['gastos_automaticos'].apply(lambda x: formatear_moneda(x))
                gastos_por_sucursal['gastos_totales'] = gastos_por_sucursal['gastos_totales'].apply(lambda x: formatear_moneda(x))
                
                # Renombrar columnas para mejor visualización
                gastos_por_sucursal.columns = ['Sucursal', '📝 Registrados', '🤖 Automáticos', '💰 Total']
                
                # Mostrar tabla
                st.dataframe(gastos_por_sucursal, use_container_width=True, hide_index=True)
                
                # Mostrar gráfico de barras
                # Crear DataFrame numérico para el gráfico (antes de formatear)
                # Usar los DataFrames originales antes de formatear
                gastos_por_sucursal_grafico = gastos_registrados_sucursal.merge(
                    gastos_automaticos_sucursal, 
                    on='sucursal', 
                    how='outer'
                ).fillna(0)
                gastos_por_sucursal_grafico['gastos_totales'] = (
                    gastos_por_sucursal_grafico['gastos_registrados'] + 
                    gastos_por_sucursal_grafico['gastos_automaticos']
                )
                gastos_por_sucursal_grafico = gastos_por_sucursal_grafico.sort_values('gastos_totales', ascending=False)
                
                # Gráfico de barras apiladas
                fig = go.Figure()
                
                # Barras de gastos registrados
                fig.add_trace(go.Bar(
                    name='📝 Registrados',
                    x=gastos_por_sucursal_grafico['sucursal'],
                    y=gastos_por_sucursal_grafico['gastos_registrados'],
                    marker_color='#1f77b4'
                ))
                
                # Barras de gastos automáticos
                fig.add_trace(go.Bar(
                    name='🤖 Automáticos',
                    x=gastos_por_sucursal_grafico['sucursal'],
                    y=gastos_por_sucursal_grafico['gastos_automaticos'],
                    marker_color='#ff7f0e'
                ))
                
                fig.update_layout(
                    barmode='stack',
                    title='Gastos por Sucursal (Registrados + Automáticos)',
                    xaxis_title='Sucursal',
                    yaxis_title='Gastos (USD)',
                    height=400
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No hay datos de gastos")
        
        with tab4:
            st.subheader("Análisis Avanzado")
            
            if len(df_ventas) > 0 and len(df_gastos) > 0:
                ingresos_servicios = df_ventas[df_ventas['tipo_re_se'] == 'SE']['total'].sum()
                ingresos_repuestos = df_ventas[df_ventas['tipo_re_se'] == 'RE']['total'].sum()
                gastos_se = df_gastos['total_pct_se'].sum()
                gastos_re = df_gastos['total_pct_re'].sum()
                
                st.write("**Análisis por Segmento**")
                
                analisis_data = {
                    'Segmento': ['Servicios', 'Repuestos', 'Total'],
                    'Ingresos': [ingresos_servicios, ingresos_repuestos, ingresos_servicios + ingresos_repuestos],
                    'Gastos': [gastos_se, gastos_re, gastos_se + gastos_re],
                    'Resultado': [
                        ingresos_servicios - gastos_se,
                        ingresos_repuestos - gastos_re,
                        (ingresos_servicios + ingresos_repuestos) - (gastos_se + gastos_re)
                    ]
                }
                
                df_analisis = pd.DataFrame(analisis_data)
                df_analisis['Margen %'] = (df_analisis['Resultado'] / df_analisis['Ingresos'] * 100).round(2)
                # Formatear columnas monetarias
                df_analisis['Ingresos'] = df_analisis['Ingresos'].apply(lambda x: formatear_moneda(x))
                df_analisis['Gastos'] = df_analisis['Gastos'].apply(lambda x: formatear_moneda(x))
                df_analisis['Resultado'] = df_analisis['Resultado'].apply(lambda x: formatear_moneda(x))
                st.dataframe(df_analisis, use_container_width=True)
            else:
                st.info("Se necesitan datos de ventas y gastos para el análisis")
        
        with tab5:
            st.subheader("📊 KPIs Financieros")
            
            st.info("""
            **Explicación de los KPIs:**
            - **Factor de Absorción (%)**: Ingresos / Gastos Fijos × 100. Muestra qué porcentaje representan los ingresos respecto a los gastos fijos.
              Ejemplo: 1165% significa que los ingresos son 11.65 veces los gastos fijos.
            - **Margen $**: Ingresos - Gastos Variables. Es la ganancia antes de descontar los gastos fijos.
            - **Resultado Operativo**: Margen - Gastos Fijos. Es la ganancia neta después de todos los gastos.
            - **Punto de Equilibrio**: Es el nivel mínimo de ingresos necesario para cubrir todos los gastos.
              Si los ingresos actuales son mayores, hay ganancia (✅). Si son menores, hay pérdida (❌).
            """)
            
            # Calcular factores de absorción
            factor_servicios = calcular_factor_absorcion_servicios(str(fecha_inicio), str(fecha_fin))
            factor_repuestos = calcular_factor_absorcion_repuestos(str(fecha_inicio), str(fecha_fin))
            factor_postventa = calcular_factor_absorcion_postventa(str(fecha_inicio), str(fecha_fin))
            punto_equilibrio = calcular_punto_equilibrio(str(fecha_inicio), str(fecha_fin))
            
            # Totales
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "Factor Absorción Servicios",
                    f"{factor_servicios['factor_absorcion']:.1f}%",
                    delta=f"Margen: {formatear_moneda(factor_servicios['margen'])}"
                )
                st.caption(f"💰 Ingresos: {formatear_moneda(factor_servicios['ingresos_servicios'])}")
                st.caption(f"💸 Gastos Variables: {formatear_moneda(factor_servicios['gastos_variables'])}")
                st.caption(f"📊 Gastos Fijos: {formatear_moneda(factor_servicios['gastos_fijos'])}")
                st.caption(f"💵 Margen: {formatear_moneda(factor_servicios['margen'])}")
                st.caption(f"✅ Resultado Operativo: {formatear_moneda(factor_servicios['resultado_operativo'])}")
            
            with col2:
                st.metric(
                    "Factor Absorción Repuestos",
                    f"{factor_repuestos['factor_absorcion']:.1f}%",
                    delta=f"Margen: {formatear_moneda(factor_repuestos['margen'])}"
                )
                st.caption(f"💰 Ingresos: {formatear_moneda(factor_repuestos['ingresos_repuestos'])}")
                st.caption(f"💸 Gastos Variables: {formatear_moneda(factor_repuestos['gastos_variables'])}")
                st.caption(f"📊 Gastos Fijos: {formatear_moneda(factor_repuestos['gastos_fijos'])}")
                st.caption(f"💵 Margen: {formatear_moneda(factor_repuestos['margen'])}")
                st.caption(f"✅ Resultado Operativo: {formatear_moneda(factor_repuestos['resultado_operativo'])}")
            
            with col3:
                st.metric(
                    "Factor Absorción Postventa",
                    f"{factor_postventa['factor_absorcion']:.1f}%",
                    delta=f"Margen: {formatear_moneda(factor_postventa['margen'])}"
                )
                st.caption(f"💰 Ingresos: {formatear_moneda(factor_postventa['ingresos_totales'])}")
                st.caption(f"💸 Gastos Variables: {formatear_moneda(factor_postventa['gastos_variables'])}")
                st.caption(f"📊 Gastos Fijos: {formatear_moneda(factor_postventa['gastos_fijos'])}")
                st.caption(f"💵 Margen: {formatear_moneda(factor_postventa['margen'])}")
                st.caption(f"✅ Resultado Operativo: {formatear_moneda(factor_postventa['resultado_operativo'])}")
            
            with col4:
                diferencia_pe = punto_equilibrio['diferencia']
                st.metric(
                    "Punto de Equilibrio",
                    formatear_moneda(punto_equilibrio['punto_equilibrio']),
                    delta=formatear_moneda(diferencia_pe) if diferencia_pe != 0 else "$0,00"
                )
                st.caption(f"Ingresos Actuales: {formatear_moneda(punto_equilibrio['ingresos_actuales'])}")
                st.caption(f"Gastos Totales: {formatear_moneda(punto_equilibrio['gastos_totales'])}")
            
            # Por sucursal
            st.subheader("📊 KPIs por Sucursal")
            
            factor_servicios_suc = calcular_factor_absorcion_servicios(str(fecha_inicio), str(fecha_fin), por_sucursal=True)
            factor_repuestos_suc = calcular_factor_absorcion_repuestos(str(fecha_inicio), str(fecha_fin), por_sucursal=True)
            factor_postventa_suc = calcular_factor_absorcion_postventa(str(fecha_inicio), str(fecha_fin), por_sucursal=True)
            punto_equilibrio_suc = calcular_punto_equilibrio(str(fecha_inicio), str(fecha_fin), por_sucursal=True)
            
            for sucursal in factor_servicios_suc.keys():
                st.write(f"### {sucursal}")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.write("**Servicios**")
                    st.write(f"Factor: **{factor_servicios_suc[sucursal]['factor_absorcion']:.1f}%**")
                    st.caption(f"💰 Ingresos: {formatear_moneda(factor_servicios_suc[sucursal]['ingresos_servicios'])}")
                    st.caption(f"💸 Gastos Variables: {formatear_moneda(factor_servicios_suc[sucursal]['gastos_variables'])}")
                    st.caption(f"📊 Gastos Fijos: {formatear_moneda(factor_servicios_suc[sucursal]['gastos_fijos'])}")
                    st.caption(f"💵 Margen: {formatear_moneda(factor_servicios_suc[sucursal]['margen'])}")
                    st.caption(f"✅ Resultado Operativo: {formatear_moneda(factor_servicios_suc[sucursal]['resultado_operativo'])}")
                
                with col2:
                    st.write("**Repuestos**")
                    st.write(f"Factor: **{factor_repuestos_suc[sucursal]['factor_absorcion']:.1f}%**")
                    st.caption(f"💰 Ingresos: {formatear_moneda(factor_repuestos_suc[sucursal]['ingresos_repuestos'])}")
                    st.caption(f"💸 Gastos Variables: {formatear_moneda(factor_repuestos_suc[sucursal]['gastos_variables'])}")
                    st.caption(f"📊 Gastos Fijos: {formatear_moneda(factor_repuestos_suc[sucursal]['gastos_fijos'])}")
                    st.caption(f"💵 Margen: {formatear_moneda(factor_repuestos_suc[sucursal]['margen'])}")
                    st.caption(f"✅ Resultado Operativo: {formatear_moneda(factor_repuestos_suc[sucursal]['resultado_operativo'])}")
                
                with col3:
                    st.write("**Postventa**")
                    st.write(f"Factor: **{factor_postventa_suc[sucursal]['factor_absorcion']:.1f}%**")
                    st.caption(f"💰 Ingresos: {formatear_moneda(factor_postventa_suc[sucursal]['ingresos_totales'])}")
                    st.caption(f"💸 Gastos Variables: {formatear_moneda(factor_postventa_suc[sucursal]['gastos_variables'])}")
                    st.caption(f"📊 Gastos Fijos: {formatear_moneda(factor_postventa_suc[sucursal]['gastos_fijos'])}")
                    st.caption(f"💵 Margen: {formatear_moneda(factor_postventa_suc[sucursal]['margen'])}")
                    st.caption(f"✅ Resultado Operativo: {formatear_moneda(factor_postventa_suc[sucursal]['resultado_operativo'])}")
                
                with col4:
                    st.write("**Punto Equilibrio**")
                    st.write(formatear_moneda(punto_equilibrio_suc[sucursal]['punto_equilibrio']))
                    diferencia = punto_equilibrio_suc[sucursal]['diferencia']
                    if diferencia > 0:
                        st.success(f"✅ +{formatear_moneda(diferencia)}")
                    elif diferencia < 0:
                        st.error(f"❌ {formatear_moneda(diferencia)}")
                    else:
                        st.info("⚖️ Equilibrado")
                
                st.divider()

# ==================== ANÁLISIS CON IA ====================
elif page == "🤖 Análisis IA":
    st.title("🤖 Análisis Inteligente con IA")
    st.markdown("**Análisis avanzado, predicciones y recomendaciones basadas en tus datos**")
    
    # Configuración de Google Gemini (opcional)
    st.sidebar.divider()
    st.sidebar.subheader("⚙️ Configuración de IA")
    
    # Inicializar API key por defecto si no existe
    if 'gemini_api_key' not in st.session_state:
        # API key proporcionada por el usuario
        st.session_state['gemini_api_key'] = 'AIzaSyAIsktRR9lhw_cdrK6_PMf-aSk88i06CQk'
    
    usar_gemini = st.sidebar.checkbox("Usar Google Gemini API (Análisis Avanzado)", value=True)
    gemini_api_key = None
    
    if usar_gemini:
        gemini_api_key_input = st.sidebar.text_input(
            "🔑 Google Gemini API Key",
            value=st.session_state['gemini_api_key'],
            type="password",
            help="Obtén tu API key gratuita en: https://aistudio.google.com/apikey"
        )
        
        if gemini_api_key_input:
            st.session_state['gemini_api_key'] = gemini_api_key_input
            gemini_api_key = gemini_api_key_input
        
        # Debug info
        with st.sidebar.expander("🔍 Debug Info", expanded=False):
            st.write(f"**usar_gemini:** {usar_gemini}")
            st.write(f"**API key presente:** {bool(gemini_api_key)}")
            st.write(f"**GEMINI_AVAILABLE:** {GEMINI_AVAILABLE}")
            st.write(f"**API key (primeros 10):** {gemini_api_key[:10] if gemini_api_key else 'N/A'}...")
        
        if not gemini_api_key:
            st.sidebar.warning("⚠️ Ingresa tu API key para usar análisis avanzado con Gemini")
        else:
            st.sidebar.success("✅ Gemini API configurada")
            
            # Botón para probar conexión
            if st.sidebar.button("🧪 Probar Conexión Gemini", use_container_width=True, key="test_gemini"):
                with st.sidebar:
                    with st.spinner("Probando conexión..."):
                        try:
                            from ai_analysis import test_gemini_connection
                            test_result = test_gemini_connection(gemini_api_key)
                            if test_result['success']:
                                st.success(f"✅ {test_result['message']}")
                                st.caption(f"Modelo: {test_result.get('model', 'N/A')}")
                                if test_result.get('modelos_disponibles'):
                                    with st.expander("Ver modelos disponibles"):
                                        for m in test_result['modelos_disponibles']:
                                            st.write(f"- {m}")
                            else:
                                error_msg = test_result.get('error', 'Desconocido')
                                st.error(f"❌ Error: {error_msg}")
                                if 'quota' in error_msg.lower() or '429' in error_msg:
                                    st.warning("⚠️ Cuota agotada. Espera unos minutos o verifica tu plan en https://ai.dev/usage")
                                elif 'Librería' in error_msg:
                                    st.info("💡 Instala la librería: `pip install google-generativeai`")
                                elif test_result.get('modelos_disponibles'):
                                    st.info(f"💡 Modelos disponibles: {', '.join(test_result['modelos_disponibles'][:3])}")
                        except ImportError:
                            st.error("❌ Función de prueba no disponible")
                        except Exception as e:
                            st.error(f"❌ Error al probar conexión: {str(e)}")
            
            if st.sidebar.button("🗑️ Eliminar API Key", key="eliminar_gemini_key"):
                st.session_state['gemini_api_key'] = ''
                st.rerun()
    
    # Configuración de auto-actualización
    auto_refresh = st.sidebar.checkbox("🔄 Auto-actualización", value=False, help="Actualiza el análisis automáticamente cada X minutos")
    refresh_interval = None
    if auto_refresh:
        refresh_interval = st.sidebar.selectbox(
            "Intervalo de actualización",
            [5, 10, 15, 30, 60],
            index=1,
            format_func=lambda x: f"{x} minutos"
        )
    
    # Filtros de fecha
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        fecha_inicio = st.date_input("Fecha Inicio (IA)", value=date(2025, 11, 1))
    with col2:
        fecha_fin = st.date_input("Fecha Fin (IA)", value=date.today())
    with col3:
        st.write("")  # Espacio
        st.write("")  # Espacio
        if st.button("🔄 Actualizar Análisis", use_container_width=True, type="primary"):
            st.rerun()
    
    # Auto-refresh usando st.rerun con time.sleep (simulado con placeholder)
    if auto_refresh and refresh_interval:
        st.sidebar.info(f"⏱️ Próxima actualización en {refresh_interval} minutos")
        # Nota: En producción, esto requeriría un mecanismo más sofisticado
        # Streamlit no tiene auto-refresh nativo, pero se puede usar st.rerun() con time.sleep
    
    # Obtener datos
    df_ventas = get_ventas(str(fecha_inicio), str(fecha_fin))
    df_gastos = get_gastos(str(fecha_inicio), str(fecha_fin))
    
    if len(df_ventas) == 0 and len(df_gastos) == 0:
        st.info("📊 No hay datos para analizar. Importa datos desde Excel o registra nuevas ventas/gastos.")
    else:
        # Ejecutar análisis con IA
        mensaje_analisis = "🤖 Analizando datos con IA avanzada (Gemini)..." if gemini_api_key else "🤖 Analizando datos con IA..."
        with st.spinner(mensaje_analisis):
            summary = get_ai_summary(df_ventas, df_gastos, gemini_api_key=gemini_api_key)
        
        # Mostrar timestamp del análisis
        if summary.get('timestamp_analisis'):
            st.caption(f"📅 Última actualización: {summary['timestamp_analisis']}")
        
        # Mostrar alertas críticas primero (si existen)
        if summary.get('alertas_criticas') and len(summary['alertas_criticas']) > 0:
            st.error("🚨 **ALERTAS CRÍTICAS DETECTADAS**")
            for alerta in summary['alertas_criticas']:
                if alerta['severidad'] == 'ALTA':
                    st.error(f"**{alerta['titulo']}** - {alerta['descripcion']}")
                else:
                    st.warning(f"**{alerta['titulo']}** - {alerta['descripcion']}")
                st.caption(f"Detectado: {alerta['fecha_deteccion']}")
            st.divider()
        
        # Mostrar indicador de método usado y estado de Gemini
        gemini_status = summary.get('gemini_status', {})
        
        # Panel de estado detallado de Gemini
        with st.expander("🔍 Estado Detallado de Gemini API", expanded=True):
            if summary.get('usando_ia'):
                if gemini_status.get('activo'):
                    st.success("✅ **Gemini API está ACTIVO y funcionando**")
                    st.write(f"📊 **Insights agregados:**")
                    st.write(f"- Tendencias: {gemini_status.get('tendencias_agregadas', 0)}")
                    st.write(f"- Alertas: {gemini_status.get('alertas_agregadas', 0)}")
                    st.write(f"- Recomendaciones: {gemini_status.get('recomendaciones_agregadas', 0)}")
                    st.write(f"- **Total:** {gemini_status.get('insights_agregados', 0)} insights")
                    if gemini_status.get('debug_info'):
                        st.caption(f"ℹ️ {gemini_status['debug_info']}")
                elif gemini_status.get('error'):
                    st.error(f"❌ **Error con Gemini API**: {gemini_status['error']}")
                    if gemini_status.get('debug_info'):
                        st.code(gemini_status['debug_info'], language='text')
                    st.info("ℹ️ Continuando con análisis estadístico local")
                else:
                    st.warning("⚠️ Gemini API configurada pero no se pudo conectar. Verifica tu API key.")
                    if gemini_status.get('debug_info'):
                        st.caption(f"ℹ️ {gemini_status['debug_info']}")
            else:
                st.info("ℹ️ Gemini API no está activado. Actívalo en el sidebar para análisis avanzado.")
        
        # Mensaje principal más visible
        if summary.get('usando_ia') and gemini_status.get('activo'):
            st.success("✅ **Análisis mejorado con Google Gemini API** - Funcionando correctamente")
            if gemini_status.get('insights_agregados', 0) > 0:
                st.caption(f"📊 Gemini agregó {gemini_status['insights_agregados']} insights adicionales (Tendencias: {gemini_status.get('tendencias_agregadas', 0)}, Alertas: {gemini_status.get('alertas_agregadas', 0)}, Recomendaciones: {gemini_status.get('recomendaciones_agregadas', 0)})")
            else:
                st.warning("⚠️ Gemini está activo pero no agregó insights. Revisa los logs en la consola.")
        elif summary.get('usando_ia') and gemini_status.get('error'):
            st.error(f"❌ **Error con Gemini API**: {gemini_status['error']}")
        elif not summary.get('usando_ia'):
            st.info("ℹ️ Análisis estadístico local (activa Gemini API en el sidebar para análisis avanzado)")
        
        # Mostrar insights
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Insights", "🔮 Predicciones", "⚠️ Anomalías", "💡 Recomendaciones", "🚨 Alertas Críticas"])
        
        with tab1:
            st.subheader("📊 Tendencias e Insights")
            
            if summary['insights']['tendencias']:
                st.success("**Tendencias Identificadas:**")
                for tendencia in summary['insights']['tendencias']:
                    st.write(f"  {tendencia}")
            else:
                st.info("No se identificaron tendencias significativas")
            
            if summary['insights']['alertas']:
                st.warning("**Alertas:**")
                for alerta in summary['insights']['alertas']:
                    st.write(f"  {alerta}")
        
        with tab2:
            st.subheader("🔮 Predicción del Próximo Mes")
            
            if summary['prediccion']['prediccion']:
                prediccion = summary['prediccion']['prediccion']
                confianza = summary['prediccion']['confianza']
                mensaje = summary['prediccion']['mensaje']
                metodo = summary['prediccion'].get('metodo', 'Promedio Simple')
                
                # Mostrar método usado
                st.info(f"📊 Método: {metodo}")
                
                # Si hay predicciones de múltiples modelos, mostrarlas
                if 'predicciones_individuales' in summary['prediccion']:
                    with st.expander("📈 Ver predicciones por modelo"):
                        for modelo, pred_valor in summary['prediccion']['predicciones_individuales'].items():
                            st.write(f"**{modelo}**: {formatear_moneda(pred_valor)} USD")
                
                # Mostrar predicción con color según confianza
                if confianza == 'Alta':
                    st.success(f"**Predicción: {formatear_moneda(prediccion)} USD**")
                elif confianza == 'Media':
                    st.warning(f"**Predicción: {formatear_moneda(prediccion)} USD**")
                else:
                    st.info(f"**Predicción: {formatear_moneda(prediccion)} USD**")
                
                # Explicación del nivel de confianza
                st.write(f"**Nivel de confianza:** {confianza}")
                
                if confianza == 'Alta':
                    st.success("✅ **Alta confianza**: Las ventas diarias son muy consistentes (baja variabilidad). La predicción es más confiable.")
                elif confianza == 'Media':
                    st.warning("⚠️ **Confianza media**: Las ventas diarias tienen variabilidad moderada. La predicción puede tener desviaciones.")
                else:
                    st.error("❌ **Baja confianza**: Las ventas diarias tienen alta variabilidad (días con ventas muy altas y otros muy bajos). La predicción es menos confiable y puede diferir significativamente del resultado real.")
                
                st.write(f"**{mensaje}**")
                
                st.info("💡 **Nota**: La predicción se basa en el promedio diario de las últimas 2 semanas. Si hay mucha variabilidad entre días, la confianza será menor.")
                
                # Gráfico de predicción
                if len(df_ventas) > 0:
                    df_ventas['fecha'] = pd.to_datetime(df_ventas['fecha'])
                    ventas_mensuales = df_ventas.groupby(df_ventas['fecha'].dt.to_period('M'))['total'].sum()
                    
                    # Crear gráfico con predicción
                    fig = go.Figure()
                    
                    # Datos históricos
                    fechas_historicas = [str(p) for p in ventas_mensuales.index]
                    fig.add_trace(go.Scatter(
                        x=fechas_historicas,
                        y=ventas_mensuales.values,
                        mode='lines+markers',
                        name='Histórico',
                        line=dict(color='blue')
                    ))
                    
                    # Predicción
                    ultima_fecha = ventas_mensuales.index[-1]
                    siguiente_mes = str(ultima_fecha + 1)
                    fig.add_trace(go.Scatter(
                        x=[siguiente_mes],
                        y=[prediccion],
                        mode='markers',
                        name='Predicción',
                        marker=dict(color='red', size=15, symbol='diamond')
                    ))
                    
                    fig.update_layout(
                        title="Predicción de Ingresos - Próximo Mes",
                        xaxis_title="Mes",
                        yaxis_title="Ingresos (USD)",
                        hovermode='x unified'
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info(summary['prediccion']['mensaje'])
        
        with tab3:
            st.subheader("⚠️ Anomalías Detectadas")
            
            if summary['anomalias']:
                # Agrupar por categoría
                categorias = {}
                for anomalia in summary['anomalias']:
                    categoria = anomalia.get('categoria', 'General')
                    if categoria not in categorias:
                        categorias[categoria] = []
                    categorias[categoria].append(anomalia)
                
                for categoria, anomalias_cat in categorias.items():
                    with st.expander(f"📂 {categoria} ({len(anomalias_cat)} anomalías)"):
                        for anomalia in anomalias_cat:
                            st.warning(f"**{anomalia['tipo']}** - {anomalia['descripcion']}")
                            st.caption(f"Fecha: {anomalia['fecha']} | Valor: {formatear_moneda(anomalia['valor']) if isinstance(anomalia['valor'], (int, float)) else anomalia['valor']}")
            else:
                st.success("✅ No se detectaron anomalías significativas en los datos")
        
        with tab4:
            st.subheader("💡 Recomendaciones Inteligentes")
            
            # Separar recomendaciones de Gemini vs estadísticas
            gemini_status = summary.get('gemini_status', {})
            hay_gemini = gemini_status.get('activo', False)
            
            # Obtener recomendaciones de Gemini (si las hay)
            recomendaciones_gemini = []
            if hay_gemini and gemini_status.get('recomendaciones_agregadas', 0) > 0:
                # Las recomendaciones de Gemini están al final de la lista (se agregaron después)
                # Contar cuántas agregó Gemini
                num_gemini = gemini_status.get('recomendaciones_agregadas', 0)
                todas_recomendaciones = summary['insights']['recomendaciones'] + summary['recomendaciones']
                if len(todas_recomendaciones) >= num_gemini:
                    recomendaciones_gemini = todas_recomendaciones[-num_gemini:]
                    recomendaciones_estadisticas = todas_recomendaciones[:-num_gemini] if num_gemini < len(todas_recomendaciones) else []
                else:
                    recomendaciones_estadisticas = todas_recomendaciones
            else:
                todas_recomendaciones = summary['insights']['recomendaciones'] + summary['recomendaciones']
                recomendaciones_estadisticas = todas_recomendaciones
            
            if recomendaciones_gemini or recomendaciones_estadisticas:
                if recomendaciones_gemini:
                    st.success("🤖 **Recomendaciones de Google Gemini AI:**")
                    for i, recomendacion in enumerate(recomendaciones_gemini, 1):
                        st.success(f"🤖 {i}. {recomendacion}")
                    st.divider()
                
                if recomendaciones_estadisticas:
                    st.info("📊 **Recomendaciones del Análisis Estadístico:**")
                    for i, recomendacion in enumerate(recomendaciones_estadisticas, 1):
                        st.info(f"{i}. {recomendacion}")
            else:
                st.info("No hay recomendaciones específicas en este momento")
            
            # Mostrar estado de Gemini si está activo
            if hay_gemini:
                st.divider()
                if gemini_status.get('insights_agregados', 0) > 0:
                    st.success(f"✅ **Google Gemini AI** está activo y agregó {gemini_status['insights_agregados']} insights")
                else:
                    st.warning("⚠️ **Google Gemini AI** está activo pero no agregó insights. Revisa los logs para más detalles.")
        
        with tab5:
            st.subheader("🚨 Alertas Críticas en Tiempo Real")
            
            if summary.get('alertas_criticas') and len(summary['alertas_criticas']) > 0:
                # Agrupar por severidad
                alertas_altas = [a for a in summary['alertas_criticas'] if a['severidad'] == 'ALTA']
                alertas_medias = [a for a in summary['alertas_criticas'] if a['severidad'] == 'MEDIA']
                
                if alertas_altas:
                    st.error(f"**🔴 Alertas de Alta Severidad ({len(alertas_altas)})**")
                    for alerta in alertas_altas:
                        with st.container():
                            st.error(f"**{alerta['titulo']}**")
                            st.write(alerta['descripcion'])
                            st.caption(f"⏰ Detectado: {alerta['fecha_deteccion']}")
                            st.divider()
                
                if alertas_medias:
                    st.warning(f"**🟡 Alertas de Severidad Media ({len(alertas_medias)})**")
                    for alerta in alertas_medias:
                        with st.container():
                            st.warning(f"**{alerta['titulo']}**")
                            st.write(alerta['descripcion'])
                            st.caption(f"⏰ Detectado: {alerta['fecha_deteccion']}")
                            st.divider()
            else:
                st.success("✅ No hay alertas críticas en este momento. El negocio está funcionando normalmente.")
        
        # Resumen ejecutivo
        st.divider()
        st.subheader("📋 Resumen Ejecutivo")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "Tendencias",
                len(summary['insights']['tendencias']),
                delta=f"{len(summary['insights']['alertas'])} alertas"
            )
        
        with col2:
            confianza_icon = "🟢" if summary['prediccion']['confianza'] == 'Alta' else "🟡" if summary['prediccion']['confianza'] == 'Media' else "🔴"
            st.metric(
                "Predicción",
                confianza_icon,
                delta=summary['prediccion']['confianza']
            )
        
        with col3:
            st.metric(
                "Anomalías",
                len(summary['anomalias']),
                delta=f"{len(todas_recomendaciones)} recomendaciones"
            )

# ==================== PROBAR EXTRACCIÓN PDF ====================
elif page == "🔍 Probar Extracción PDF":
    st.title("🔍 Probar Extracción de Datos de PDF")
    st.info("""
    **Extrae datos de comprobantes PDF usando pdfplumber:**
    - Extracción automática de texto del PDF
    - Detección de cliente, número de comprobante, fecha, total
    - Clasificación automática de items (Repuestos, Mano de Obra, Asistencia, Terceros)
    """)
    
    archivo_pdf = st.file_uploader("Subir PDF para analizar", type=['pdf'])
    
    if archivo_pdf is not None:
        # Guardar temporalmente
        temp_pdf = Path(f"temp_{archivo_pdf.name}")
        with open(temp_pdf, "wb") as f:
            f.write(archivo_pdf.getbuffer())
        
        if st.button("🔍 Extraer Datos"):
            with st.spinner("Extrayendo datos del PDF..."):
                datos_extraidos = {}
                
                # Método local con pdfplumber
                try:
                    if PDFPLUMBER_AVAILABLE:
                        with pdfplumber.open(temp_pdf) as pdf:
                            texto_completo = ""
                            for page in pdf.pages:
                                texto_completo += page.extract_text() or ""
                        datos_extraidos['texto'] = texto_completo
                        datos_extraidos['metodo'] = 'pdfplumber'
                    elif PDF2_AVAILABLE:
                        with open(temp_pdf, 'rb') as f:
                            pdf_reader = PyPDF2.PdfReader(f)
                            texto_completo = ""
                            for page in pdf_reader.pages:
                                texto_completo += page.extract_text()
                        datos_extraidos['texto'] = texto_completo
                        datos_extraidos['metodo'] = 'PyPDF2'
                    else:
                        st.error("❌ No hay librerías de PDF instaladas. Instala PyPDF2 o pdfplumber.")
                except Exception as e:
                    st.error(f"❌ Error en extracción: {e}")
                
                # Analizar texto con función local
                if 'texto' in datos_extraidos:
                    datos_estructurados = analizar_texto_pdf(datos_extraidos['texto'])
                    
                    # Mostrar texto completo (colapsable)
                    with st.expander("📄 Ver texto completo extraído"):
                        st.text_area("Texto completo", datos_extraidos['texto'], height=300, key="texto_completo", disabled=True)
                    
                    # Mostrar datos estructurados
                    st.subheader("📊 Datos Estructurados")
                    st.info("💡 Estos datos se extrajeron automáticamente usando patrones. Revísalos y corrígelos si es necesario.")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Información Básica**")
                        st.write(f"**Cliente:** {datos_estructurados['cliente'] or '❌ No detectado'}")
                        st.write(f"**N° Comprobante:** {datos_estructurados['numero_comprobante'] or '❌ No detectado'}")
                        st.write(f"**Fecha:** {datos_estructurados['fecha'] or '❌ No detectada'}")
                        st.write(f"**PIN:** {datos_estructurados['pin'] or '❌ No detectado'}")
                        st.write(f"**Sucursal:** {datos_estructurados['sucursal'] or '❌ No detectada'}")
                        st.write(f"**Tipo:** {datos_estructurados['tipo_re_se']}")
                        st.write(f"**Tipo Comprobante:** {datos_estructurados['tipo_comprobante']}")
                    
                    with col2:
                        st.write("**Montos Detectados**")
                        st.write(f"**Mano de Obra:** {formatear_moneda(datos_estructurados['mano_obra'])}")
                        st.write(f"**Asistencia:** {formatear_moneda(datos_estructurados['asistencia'])}")
                        st.write(f"**Repuestos:** {formatear_moneda(datos_estructurados['repuestos'])}")
                        st.write(f"**Terceros:** {formatear_moneda(datos_estructurados['terceros'])}")
                        st.write(f"**Descuento:** {formatear_moneda(datos_estructurados['descuento'])}")
                        
                        # Calcular total
                        total_calculado = (datos_estructurados['mano_obra'] + 
                                         datos_estructurados['asistencia'] + 
                                         datos_estructurados['repuestos'] + 
                                         datos_estructurados['terceros'] - 
                                         datos_estructurados['descuento'])
                        st.write(f"**Total Calculado:** {formatear_moneda(total_calculado)}")
                        st.write(f"**Total en PDF:** {formatear_moneda(datos_estructurados['total'])}")
                        
                        if abs(total_calculado - datos_estructurados['total']) > 0.01:
                            st.warning("⚠️ El total calculado no coincide con el total del PDF. Revisa los items.")
                    
                    # Mostrar items encontrados
                    if datos_estructurados.get('items'):
                        st.subheader("📋 Items Encontrados")
                        for i, item in enumerate(datos_estructurados['items'], 1):
                            st.write(f"{i}. **{item.get('descripcion', 'Sin descripción')}** - Código: {item.get('codigo', 'N/A')} - Cantidad: {item.get('cantidad', 0)} - Precio: {formatear_moneda(item.get('precio', 0))} - **Total: {formatear_moneda(item.get('monto', 0))}**")
                    
                    # Botón para usar estos datos en el formulario de venta
                    st.divider()
                    if st.button("✅ Usar estos datos para crear venta", type="primary"):
                        # Guardar datos en session_state para usar en el formulario
                        st.session_state['venta_desde_pdf'] = datos_estructurados
                        st.session_state['archivo_pdf_venta'] = archivo_pdf
                        st.success("✅ Datos guardados! Ve a '💰 Registrar Venta' para completar y guardar.")
                        st.info("💡 Los datos estarán prellenados en el formulario. Revísalos y ajusta lo necesario.")
        
        # Limpiar archivo temporal
        if temp_pdf.exists():
            try:
                temp_pdf.unlink()
            except:
                pass
