"""
Módulo para cálculos de KPIs financieros
"""
import pandas as pd
from database import get_ventas
from gastos_automaticos import obtener_gastos_totales_con_automaticos

def calcular_factor_absorcion_servicios(
    fecha_inicio: str = None,
    fecha_fin: str = None,
    por_sucursal: bool = False
) -> dict:
    """
    Calcula el factor de absorción de servicios
    
    Factor de absorción = Ingresos Servicios / Gastos Fijos * 100
    Margen $ = Ingresos - Gastos Variables
    Resultado Operativo = Margen - Gastos Fijos
    """
    df_ventas = get_ventas(fecha_inicio, fecha_fin)
    gastos_totales = obtener_gastos_totales_con_automaticos(fecha_inicio, fecha_fin)
    
    df_gastos = gastos_totales['gastos_registrados']
    if len(gastos_totales['gastos_automaticos']) > 0:
        df_gastos = pd.concat([df_gastos, gastos_totales['gastos_automaticos']], ignore_index=True)
    
    gastos_fijos = df_gastos[df_gastos['tipo'] == 'FIJO']['total_pct_se'].sum() if len(df_gastos) > 0 else 0
    gastos_variables = df_gastos[df_gastos['tipo'] == 'VARIABLE']['total_pct_se'].sum() if len(df_gastos) > 0 else 0
    
    if por_sucursal:
        resultados = {}
        sucursales = df_ventas['sucursal'].dropna().unique()
        
        for sucursal in sucursales:
            df_ventas_suc = df_ventas[df_ventas['sucursal'] == sucursal]
            ingresos_servicios = df_ventas_suc[df_ventas_suc['tipo_re_se'] == 'SE']['total'].sum()
            
            df_gastos_suc = df_gastos[df_gastos['sucursal'] == sucursal]
            gastos_fijos_suc = df_gastos_suc[df_gastos_suc['tipo'] == 'FIJO']['total_pct_se'].sum()
            gastos_variables_suc = df_gastos_suc[df_gastos_suc['tipo'] == 'VARIABLE']['total_pct_se'].sum()
            
            factor_absorcion = (ingresos_servicios / gastos_fijos_suc * 100) if gastos_fijos_suc > 0 else 0
            margen = ingresos_servicios - gastos_variables_suc
            resultado_operativo = margen - gastos_fijos_suc
            
            resultados[sucursal] = {
                'ingresos_servicios': ingresos_servicios,
                'gastos_fijos': gastos_fijos_suc,
                'gastos_variables': gastos_variables_suc,
                'factor_absorcion': factor_absorcion,
                'margen': margen,
                'resultado_operativo': resultado_operativo
            }
        
        return resultados
    else:
        ingresos_servicios = df_ventas[df_ventas['tipo_re_se'] == 'SE']['total'].sum() if len(df_ventas) > 0 else 0
        
        factor_absorcion = (ingresos_servicios / gastos_fijos * 100) if gastos_fijos > 0 else 0
        margen = ingresos_servicios - gastos_variables
        resultado_operativo = margen - gastos_fijos
        
        return {
            'ingresos_servicios': ingresos_servicios,
            'gastos_fijos': gastos_fijos,
            'gastos_variables': gastos_variables,
            'factor_absorcion': factor_absorcion,
            'margen': margen,
            'resultado_operativo': resultado_operativo
        }

def calcular_factor_absorcion_repuestos(
    fecha_inicio: str = None,
    fecha_fin: str = None,
    por_sucursal: bool = False
) -> dict:
    """
    Calcula el factor de absorción de repuestos
    
    Factor de absorción = Ingresos Repuestos / Gastos Fijos * 100
    Margen $ = Ingresos - Gastos Variables
    Resultado Operativo = Margen - Gastos Fijos
    """
    df_ventas = get_ventas(fecha_inicio, fecha_fin)
    gastos_totales = obtener_gastos_totales_con_automaticos(fecha_inicio, fecha_fin)
    
    df_gastos = gastos_totales['gastos_registrados']
    if len(gastos_totales['gastos_automaticos']) > 0:
        df_gastos = pd.concat([df_gastos, gastos_totales['gastos_automaticos']], ignore_index=True)
    
    gastos_fijos = df_gastos[df_gastos['tipo'] == 'FIJO']['total_pct_re'].sum() if len(df_gastos) > 0 else 0
    gastos_variables = df_gastos[df_gastos['tipo'] == 'VARIABLE']['total_pct_re'].sum() if len(df_gastos) > 0 else 0
    
    if por_sucursal:
        resultados = {}
        sucursales = df_ventas['sucursal'].dropna().unique()
        
        for sucursal in sucursales:
            df_ventas_suc = df_ventas[df_ventas['sucursal'] == sucursal]
            ingresos_repuestos = df_ventas_suc[df_ventas_suc['tipo_re_se'] == 'RE']['total'].sum()
            
            df_gastos_suc = df_gastos[df_gastos['sucursal'] == sucursal]
            gastos_fijos_suc = df_gastos_suc[df_gastos_suc['tipo'] == 'FIJO']['total_pct_re'].sum()
            gastos_variables_suc = df_gastos_suc[df_gastos_suc['tipo'] == 'VARIABLE']['total_pct_re'].sum()
            
            factor_absorcion = (ingresos_repuestos / gastos_fijos_suc * 100) if gastos_fijos_suc > 0 else 0
            margen = ingresos_repuestos - gastos_variables_suc
            resultado_operativo = margen - gastos_fijos_suc
            
            resultados[sucursal] = {
                'ingresos_repuestos': ingresos_repuestos,
                'gastos_fijos': gastos_fijos_suc,
                'gastos_variables': gastos_variables_suc,
                'factor_absorcion': factor_absorcion,
                'margen': margen,
                'resultado_operativo': resultado_operativo
            }
        
        return resultados
    else:
        ingresos_repuestos = df_ventas[df_ventas['tipo_re_se'] == 'RE']['total'].sum() if len(df_ventas) > 0 else 0
        
        factor_absorcion = (ingresos_repuestos / gastos_fijos * 100) if gastos_fijos > 0 else 0
        margen = ingresos_repuestos - gastos_variables
        resultado_operativo = margen - gastos_fijos
        
        return {
            'ingresos_repuestos': ingresos_repuestos,
            'gastos_fijos': gastos_fijos,
            'gastos_variables': gastos_variables,
            'factor_absorcion': factor_absorcion,
            'margen': margen,
            'resultado_operativo': resultado_operativo
        }

def calcular_factor_absorcion_postventa(
    fecha_inicio: str = None,
    fecha_fin: str = None,
    por_sucursal: bool = False
) -> dict:
    """
    Calcula el factor de absorción de postventa (servicios + repuestos)
    
    Factor de absorción = Ingresos Totales / Gastos Fijos * 100
    Margen $ = Ingresos - Gastos Variables
    Resultado Operativo = Margen - Gastos Fijos
    """
    df_ventas = get_ventas(fecha_inicio, fecha_fin)
    gastos_totales = obtener_gastos_totales_con_automaticos(fecha_inicio, fecha_fin)
    
    df_gastos = gastos_totales['gastos_registrados']
    if len(gastos_totales['gastos_automaticos']) > 0:
        df_gastos = pd.concat([df_gastos, gastos_totales['gastos_automaticos']], ignore_index=True)
    
    gastos_fijos = (df_gastos[df_gastos['tipo'] == 'FIJO']['total_pct_se'].sum() + 
                   df_gastos[df_gastos['tipo'] == 'FIJO']['total_pct_re'].sum()) if len(df_gastos) > 0 else 0
    gastos_variables = (df_gastos[df_gastos['tipo'] == 'VARIABLE']['total_pct_se'].sum() + 
                       df_gastos[df_gastos['tipo'] == 'VARIABLE']['total_pct_re'].sum()) if len(df_gastos) > 0 else 0
    
    if por_sucursal:
        resultados = {}
        sucursales = df_ventas['sucursal'].dropna().unique()
        
        for sucursal in sucursales:
            df_ventas_suc = df_ventas[df_ventas['sucursal'] == sucursal]
            ingresos_totales = df_ventas_suc['total'].sum()
            
            df_gastos_suc = df_gastos[df_gastos['sucursal'] == sucursal]
            gastos_fijos_suc = (df_gastos_suc[df_gastos_suc['tipo'] == 'FIJO']['total_pct_se'].sum() + 
                               df_gastos_suc[df_gastos_suc['tipo'] == 'FIJO']['total_pct_re'].sum())
            gastos_variables_suc = (df_gastos_suc[df_gastos_suc['tipo'] == 'VARIABLE']['total_pct_se'].sum() + 
                                   df_gastos_suc[df_gastos_suc['tipo'] == 'VARIABLE']['total_pct_re'].sum())
            
            factor_absorcion = (ingresos_totales / gastos_fijos_suc * 100) if gastos_fijos_suc > 0 else 0
            margen = ingresos_totales - gastos_variables_suc
            resultado_operativo = margen - gastos_fijos_suc
            
            resultados[sucursal] = {
                'ingresos_totales': ingresos_totales,
                'gastos_fijos': gastos_fijos_suc,
                'gastos_variables': gastos_variables_suc,
                'factor_absorcion': factor_absorcion,
                'margen': margen,
                'resultado_operativo': resultado_operativo
            }
        
        return resultados
    else:
        ingresos_totales = df_ventas['total'].sum() if len(df_ventas) > 0 else 0
        
        factor_absorcion = (ingresos_totales / gastos_fijos * 100) if gastos_fijos > 0 else 0
        margen = ingresos_totales - gastos_variables
        resultado_operativo = margen - gastos_fijos
        
        return {
            'ingresos_totales': ingresos_totales,
            'gastos_fijos': gastos_fijos,
            'gastos_variables': gastos_variables,
            'factor_absorcion': factor_absorcion,
            'margen': margen,
            'resultado_operativo': resultado_operativo
        }

def calcular_punto_equilibrio(
    fecha_inicio: str = None,
    fecha_fin: str = None,
    por_sucursal: bool = False
) -> dict:
    """
    Calcula el punto de equilibrio
    
    Punto de Equilibrio = Gastos Totales
    Diferencia = Ingresos Actuales - Punto de Equilibrio
    """
    df_ventas = get_ventas(fecha_inicio, fecha_fin)
    gastos_totales = obtener_gastos_totales_con_automaticos(fecha_inicio, fecha_fin)
    
    gastos_total = gastos_totales['gastos_postventa_total']
    ingresos_actuales = df_ventas['total'].sum() if len(df_ventas) > 0 else 0
    
    if por_sucursal:
        resultados = {}
        sucursales = df_ventas['sucursal'].dropna().unique()
        
        for sucursal in sucursales:
            df_ventas_suc = df_ventas[df_ventas['sucursal'] == sucursal]
            ingresos_suc = df_ventas_suc['total'].sum()
            
            df_gastos_suc = gastos_totales['gastos_todos']
            df_gastos_suc = df_gastos_suc[df_gastos_suc['sucursal'] == sucursal] if len(df_gastos_suc) > 0 else pd.DataFrame()
            gastos_suc = (df_gastos_suc['total_pct_se'].sum() + df_gastos_suc['total_pct_re'].sum()) if len(df_gastos_suc) > 0 else 0
            
            diferencia = ingresos_suc - gastos_suc
            
            resultados[sucursal] = {
                'punto_equilibrio': gastos_suc,
                'ingresos_actuales': ingresos_suc,
                'gastos_totales': gastos_suc,
                'diferencia': diferencia
            }
        
        return resultados
    else:
        diferencia = ingresos_actuales - gastos_total
        
        return {
            'punto_equilibrio': gastos_total,
            'ingresos_actuales': ingresos_actuales,
            'gastos_totales': gastos_total,
            'diferencia': diferencia
        }

