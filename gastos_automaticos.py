"""
Módulo para calcular gastos automáticos basados en ventas
"""
import pandas as pd
from datetime import datetime
from database import get_ventas, get_gastos

# Porcentaje de costo sobre ventas de repuestos
COSTO_PORCENTAJE = 0.65  # 65% del valor facturado

def obtener_gastos_automaticos(fecha_inicio: str = None, fecha_fin: str = None) -> pd.DataFrame:
    """
    Calcula gastos automáticos basados en las ventas registradas
    Estos gastos se generan automáticamente y no deben registrarse manualmente
    """
    df_ventas = get_ventas(fecha_inicio, fecha_fin)
    
    if len(df_ventas) == 0:
        return pd.DataFrame()
    
    # Asegurar que la columna fecha esté en formato datetime; coerciar inválidos
    if 'fecha' in df_ventas.columns:
        df_ventas['fecha'] = pd.to_datetime(df_ventas['fecha'], errors='coerce')
        df_ventas = df_ventas.dropna(subset=['fecha'])
    
    gastos_automaticos = []
    
    # Agrupar por sucursal
    sucursales = df_ventas['sucursal'].dropna().unique()
    
    for sucursal in sucursales:
        df_sucursal = df_ventas[df_ventas['sucursal'] == sucursal]
        # NOTA: Incluir todas las ventas (positivas y negativas/notas de crédito)
        # Las notas de crédito reducen el costo de repuestos vendidos
        
        # 1. COSTO DE REPUESTOS VENDIDOS EN MOSTRADOR (tipo RE)
        # IMPORTANTE: Usar la columna 'repuestos', no 'total' (como en el Excel)
        ventas_re = df_sucursal[df_sucursal['tipo_re_se'] == 'RE']
        total_repuestos_mostrador = ventas_re['repuestos'].sum() if len(ventas_re) > 0 and 'repuestos' in ventas_re.columns else 0
        # Si no hay columna repuestos o está vacía, usar el total como fallback
        if total_repuestos_mostrador == 0:
            total_repuestos_mostrador = ventas_re['total'].sum() if len(ventas_re) > 0 else 0
        costo_repuestos_mostrador = total_repuestos_mostrador * COSTO_PORCENTAJE
        
        if costo_repuestos_mostrador > 0:
            # Obtener fecha máxima
            fecha_max = df_sucursal['fecha'].max() if len(df_sucursal) > 0 else None
            mes_str = fecha_max.strftime("%B") if fecha_max is not None and pd.notna(fecha_max) else ''
            
            gastos_automaticos.append({
                'id': f'AUTO_REP_{sucursal}',
                'mes': mes_str,
                'fecha': fecha_max,
                'sucursal': sucursal,
                'area': 'REPUESTOS',
                'pct_postventa': 1.0,
                'pct_servicios': 0.0,
                'pct_repuestos': 1.0,
                'tipo': 'VARIABLE',
                'clasificacion': 'COSTO DE REPUESTOS VENDIDOS MOSTRADOR',
                'proveedor': 'JOHN DEERE',
                'total_usd': costo_repuestos_mostrador,
                'total_pct': costo_repuestos_mostrador,
                'total_pct_se': 0.0,
                'total_pct_re': costo_repuestos_mostrador,
                'automatico': True
            })
        
        # 2. COSTO DE REPUESTOS VENDIDOS EN SERVICIOS (repuestos dentro de tipo SE)
        ventas_se = df_sucursal[df_sucursal['tipo_re_se'] == 'SE']
        # El costo se calcula sobre el total de repuestos vendidos en servicios
        # Usamos la columna 'repuestos' que contiene el valor de repuestos en cada venta SE
        total_repuestos_servicios = ventas_se['repuestos'].sum() if 'repuestos' in ventas_se.columns else 0
        # Si no hay columna repuestos o está vacía, usar el total de la venta como aproximación
        if total_repuestos_servicios == 0:
            # Alternativa: calcular como porcentaje del total de ventas SE
            # Asumiendo que en servicios, los repuestos representan una parte del total
            total_repuestos_servicios = ventas_se['total'].sum() * 0.7  # Aproximación: 70% del total son repuestos
        costo_repuestos_servicios = total_repuestos_servicios * COSTO_PORCENTAJE
        
        if costo_repuestos_servicios > 0:
            # Obtener fecha máxima
            fecha_max = df_sucursal['fecha'].max() if len(df_sucursal) > 0 else None
            mes_str = fecha_max.strftime("%B") if fecha_max is not None and pd.notna(fecha_max) else ''
            
            gastos_automaticos.append({
                'id': f'AUTO_SERV_{sucursal}',
                'mes': mes_str,
                'fecha': fecha_max,
                'sucursal': sucursal,
                'area': 'SERVICIO',
                'pct_postventa': 1.0,
                'pct_servicios': 1.0,
                'pct_repuestos': 0.0,
                'tipo': 'VARIABLE',
                'clasificacion': 'COSTO DE REPUESTOS VENDIDOS EN SERVICIOS',
                'proveedor': 'JOHN DEERE',
                'total_usd': costo_repuestos_servicios,
                'total_pct': costo_repuestos_servicios,
                'total_pct_se': costo_repuestos_servicios,
                'total_pct_re': 0.0,
                'automatico': True
            })
    
    return pd.DataFrame(gastos_automaticos)

def obtener_gastos_totales_con_automaticos(fecha_inicio: str = None, fecha_fin: str = None) -> dict:
    """
    Obtiene todos los gastos (registrados + automáticos) y calcula totales
    
    IMPORTANTE: Excluye los gastos registrados que se calculan automáticamente
    (COSTO DE REPUESTOS VENDIDOS MOSTRADOR y COSTO DE REPUESTOS VENDIDOS EN SERVICIOS)
    para evitar duplicación, ya que estos se calculan dinámicamente.
    """
    df_gastos = get_gastos(fecha_inicio, fecha_fin)
    
    # Excluir gastos que se calculan automáticamente (para evitar duplicación)
    clasificaciones_automaticas = [
        'COSTO DE REPUESTOS VENDIDOS MOSTRADOR',
        'COSTO DE REPUESTOS VENDIDOS EN SERVICIOS'
    ]
    
    if len(df_gastos) > 0:
        # Filtrar gastos que NO son automáticos
        df_gastos = df_gastos[~df_gastos['clasificacion'].isin(clasificaciones_automaticas)]
    
    # Calcular gastos automáticos
    df_gastos_automaticos = obtener_gastos_automaticos(fecha_inicio, fecha_fin)
    
    # Combinar gastos registrados (sin los automáticos) y automáticos calculados
    if len(df_gastos_automaticos) > 0:
        # Asegurar que las columnas coincidan
        for col in df_gastos.columns:
            if col not in df_gastos_automaticos.columns:
                df_gastos_automaticos[col] = None
        
        # Combinar
        df_todos = pd.concat([df_gastos, df_gastos_automaticos], ignore_index=True)
    else:
        df_todos = df_gastos.copy()
    
    # Calcular totales
    gastos_postventa_total = df_todos['total_pct_se'].sum() + df_todos['total_pct_re'].sum() if len(df_todos) > 0 else 0
    gastos_se_total = df_todos['total_pct_se'].sum() if len(df_todos) > 0 else 0
    gastos_re_total = df_todos['total_pct_re'].sum() if len(df_todos) > 0 else 0
    
    return {
        'gastos_registrados': df_gastos,
        'gastos_automaticos': df_gastos_automaticos,
        'gastos_todos': df_todos,
        'gastos_postventa_total': gastos_postventa_total,
        'gastos_se_total': gastos_se_total,
        'gastos_re_total': gastos_re_total
    }

