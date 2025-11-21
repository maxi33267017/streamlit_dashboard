"""
Módulo de análisis con IA
"""
import pandas as pd
from datetime import timedelta
from gastos_automaticos import obtener_gastos_totales_con_automaticos

def get_ai_summary(df_ventas: pd.DataFrame, df_gastos: pd.DataFrame) -> dict:
    """
    Genera un resumen inteligente de los datos
    """
    insights = {
        'tendencias': [],
        'alertas': [],
        'recomendaciones': []
    }
    
    # Análisis de ventas
    if len(df_ventas) > 0:
        df_ventas['fecha'] = pd.to_datetime(df_ventas['fecha'])
        ventas_mensuales = df_ventas.groupby(df_ventas['fecha'].dt.to_period('M'))['total'].sum()
        
        if len(ventas_mensuales) > 1:
            # Tendencias
            if ventas_mensuales.iloc[-1] > ventas_mensuales.iloc[-2]:
                insights['tendencias'].append("📈 Las ventas muestran una tendencia creciente")
            elif ventas_mensuales.iloc[-1] < ventas_mensuales.iloc[-2]:
                insights['tendencias'].append("📉 Las ventas muestran una tendencia decreciente")
            
            # Análisis por tipo
            servicios = df_ventas[df_ventas['tipo_re_se'] == 'SE']['total'].sum()
            repuestos = df_ventas[df_ventas['tipo_re_se'] == 'RE']['total'].sum()
            
            if servicios > repuestos * 1.5:
                insights['tendencias'].append("🔧 Los servicios representan la mayor parte de los ingresos")
            elif repuestos > servicios * 1.5:
                insights['tendencias'].append("⚙️ Los repuestos representan la mayor parte de los ingresos")
    
    # Análisis de gastos vs ingresos
    if len(df_ventas) > 0 and len(df_gastos) > 0:
        total_ingresos = df_ventas['total'].sum()
        gastos_totales = obtener_gastos_totales_con_automaticos()
        gastos_postventa = gastos_totales['gastos_postventa_total']
        
        margen = total_ingresos - gastos_postventa
        margen_pct = (margen / total_ingresos * 100) if total_ingresos > 0 else 0
        
        if margen_pct < 10:
            insights['alertas'].append(f"⚠️ Margen muy bajo: {margen_pct:.1f}%. Revisar gastos o aumentar ingresos")
        elif margen_pct > 30:
            insights['tendencias'].append(f"✅ Excelente margen: {margen_pct:.1f}%")
        
        if gastos_postventa > total_ingresos:
            insights['alertas'].append("🚨 Los gastos superan los ingresos. Revisar urgentemente")
    
    # Recomendaciones
    if len(df_ventas) > 0:
        df_ventas['fecha'] = pd.to_datetime(df_ventas['fecha'])
        dias_sin_venta = (pd.Timestamp.now() - df_ventas['fecha'].max()).days
        if dias_sin_venta > 7:
            insights['recomendaciones'].append(f"📅 Hace {dias_sin_venta} días que no se registran ventas. Considerar seguimiento activo")
    
    # Predicción
    prediccion = predict_next_month(df_ventas)
    
    # Anomalías
    anomalias = detect_anomalies(df_ventas, df_gastos)
    
    # Recomendaciones adicionales
    recomendaciones = generate_recommendations(df_ventas, df_gastos)
    
    return {
        'insights': insights,
        'prediccion': prediccion,
        'anomalias': anomalias,
        'recomendaciones': recomendaciones
    }

def predict_next_month(df_ventas: pd.DataFrame) -> dict:
    """Predice ingresos para el próximo mes basado en tendencias"""
    if len(df_ventas) < 7:
        return {
            'prediccion': None,
            'confianza': 'Baja',
            'mensaje': 'Se necesitan más datos para hacer una predicción confiable'
        }
    
    df_ventas['fecha'] = pd.to_datetime(df_ventas['fecha'])
    df_ventas = df_ventas.sort_values('fecha')
    
    # Calcular promedio diario de las últimas 2 semanas
    fecha_limite = df_ventas['fecha'].max() - timedelta(days=14)
    ventas_recientes = df_ventas[df_ventas['fecha'] > fecha_limite]
    
    if len(ventas_recientes) == 0:
        return {
            'prediccion': None,
            'confianza': 'Baja',
            'mensaje': 'No hay datos recientes suficientes'
        }
    
    dias_activos = (ventas_recientes['fecha'].max() - ventas_recientes['fecha'].min()).days + 1
    promedio_diario = ventas_recientes['total'].sum() / max(dias_activos, 1)
    
    # Predicción para 30 días
    prediccion = promedio_diario * 30
    
    # Calcular confianza basada en variabilidad
    std_dev = ventas_recientes.groupby(ventas_recientes['fecha'].dt.date)['total'].sum().std()
    cv = (std_dev / promedio_diario) if promedio_diario > 0 else 1
    
    if cv < 0.3:
        confianza = 'Alta'
    elif cv < 0.6:
        confianza = 'Media'
    else:
        confianza = 'Baja'
    
    return {
        'prediccion': prediccion,
        'confianza': confianza,
        'promedio_diario': promedio_diario,
        'mensaje': f'Basado en el promedio diario de ${promedio_diario:,.2f} USD'
    }

def detect_anomalies(df_ventas: pd.DataFrame, df_gastos: pd.DataFrame) -> list:
    """Detecta anomalías en los datos"""
    anomalias = []
    
    if len(df_ventas) > 0:
        # Detectar ventas muy altas o muy bajas
        media = df_ventas['total'].mean()
        std = df_ventas['total'].std()
        
        ventas_anomalas = df_ventas[
            (df_ventas['total'] > media + 3 * std) | 
            (df_ventas['total'] < media - 3 * std)
        ]
        
        for _, venta in ventas_anomalas.iterrows():
            if venta['total'] > media + 3 * std:
                anomalias.append({
                    'tipo': 'Venta Excepcionalmente Alta',
                    'fecha': str(venta['fecha']),
                    'valor': venta['total'],
                    'descripcion': f"Venta de ${venta['total']:,.2f} USD al cliente {venta['cliente']} es significativamente mayor al promedio"
                })
    
    return anomalias

def generate_recommendations(df_ventas: pd.DataFrame, df_gastos: pd.DataFrame) -> list:
    """Genera recomendaciones basadas en los datos"""
    recomendaciones = []
    
    if len(df_ventas) > 0:
        # Análisis de concentración de clientes
        top_clientes = df_ventas.groupby('cliente')['total'].sum().sort_values(ascending=False).head(5)
        total_ventas = df_ventas['total'].sum()
        concentracion = (top_clientes.sum() / total_ventas * 100) if total_ventas > 0 else 0
        
        if concentracion > 50:
            recomendaciones.append(f"💼 Los 5 principales clientes representan el {concentracion:.1f}% de las ventas. Considerar diversificar la cartera")
        
        # Análisis por sucursal
        if 'sucursal' in df_ventas.columns:
            ventas_sucursal = df_ventas.groupby('sucursal')['total'].sum()
            if len(ventas_sucursal) > 1:
                desbalance = (ventas_sucursal.max() / ventas_sucursal.min()) if ventas_sucursal.min() > 0 else 0
                if desbalance > 3:
                    sucursal_max = ventas_sucursal.idxmax()
                    sucursal_min = ventas_sucursal.idxmin()
                    recomendaciones.append(f"🏢 Hay un desbalance significativo entre {sucursal_max} y {sucursal_min}. Revisar estrategias por sucursal")
    
    return recomendaciones

