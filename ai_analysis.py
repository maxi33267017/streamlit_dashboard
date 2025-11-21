"""
Módulo de análisis con IA mejorado
Incluye análisis estadístico local y opcionalmente Google Gemini API
"""
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from gastos_automaticos import obtener_gastos_totales_con_automaticos

# Intentar importar Google Gemini
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

# Intentar importar scikit-learn para ML
try:
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import PolynomialFeatures
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# Intentar importar statsmodels para ARIMA
try:
    from statsmodels.tsa.arima.model import ARIMA
    from statsmodels.tsa.stattools import adfuller
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False

# Intentar importar Prophet
try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

def detect_critical_alerts(df_ventas: pd.DataFrame, df_gastos: pd.DataFrame) -> list:
    """Detecta alertas críticas que requieren atención inmediata"""
    alertas_criticas = []
    
    if len(df_ventas) == 0:
        return alertas_criticas
    
    df_ventas['fecha'] = pd.to_datetime(df_ventas['fecha'])
    
    # 1. Caída drástica de ventas (>70% en últimos 7 días vs promedio anterior)
    if len(df_ventas) >= 14:
        ultimos_7_dias = df_ventas[df_ventas['fecha'] > (df_ventas['fecha'].max() - pd.Timedelta(days=7))]
        anteriores_7_dias = df_ventas[
            (df_ventas['fecha'] > (df_ventas['fecha'].max() - pd.Timedelta(days=14))) &
            (df_ventas['fecha'] <= (df_ventas['fecha'].max() - pd.Timedelta(days=7)))
        ]
        
        if len(anteriores_7_dias) > 0 and len(ultimos_7_dias) > 0:
            ventas_ultimas = ultimos_7_dias['total'].sum()
            ventas_anteriores = anteriores_7_dias['total'].sum()
            
            if ventas_anteriores > 0:
                caida_pct = ((ventas_anteriores - ventas_ultimas) / ventas_anteriores) * 100
                if caida_pct > 70:
                    alertas_criticas.append({
                        'tipo': 'CRITICA',
                        'titulo': '🚨 Caída Drástica de Ventas',
                        'descripcion': f'Las ventas cayeron {caida_pct:.1f}% en los últimos 7 días. Revisar urgentemente.',
                        'severidad': 'ALTA',
                        'fecha_deteccion': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
    
    # 2. Gastos superan ingresos
    if len(df_gastos) > 0:
        total_ingresos = df_ventas['total'].sum()
        gastos_totales = obtener_gastos_totales_con_automaticos()
        gastos_postventa = gastos_totales['gastos_postventa_total']
        
        if gastos_postventa > total_ingresos:
            diferencia = gastos_postventa - total_ingresos
            alertas_criticas.append({
                'tipo': 'CRITICA',
                'titulo': '🚨 Gastos Superan Ingresos',
                'descripcion': f'Los gastos (${gastos_postventa:,.2f}) superan los ingresos (${total_ingresos:,.2f}) en ${diferencia:,.2f}. Pérdida operativa detectada.',
                'severidad': 'ALTA',
                'fecha_deteccion': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
    
    # 3. Margen muy bajo (<5%)
    if len(df_gastos) > 0:
        total_ingresos = df_ventas['total'].sum()
        gastos_totales = obtener_gastos_totales_con_automaticos()
        gastos_postventa = gastos_totales['gastos_postventa_total']
        
        if total_ingresos > 0:
            margen_pct = ((total_ingresos - gastos_postventa) / total_ingresos) * 100
            if margen_pct < 5 and margen_pct > 0:
                alertas_criticas.append({
                    'tipo': 'CRITICA',
                    'titulo': '⚠️ Margen Crítico',
                    'descripcion': f'El margen es de solo {margen_pct:.1f}%. Revisar costos y precios urgentemente.',
                    'severidad': 'MEDIA',
                    'fecha_deteccion': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
    
    # 4. Sin ventas por más de 10 días
    if len(df_ventas) > 0:
        dias_sin_venta = (pd.Timestamp.now() - df_ventas['fecha'].max()).days
        if dias_sin_venta > 10:
            alertas_criticas.append({
                'tipo': 'CRITICA',
                'titulo': '🚨 Sin Ventas Recientes',
                'descripcion': f'No se han registrado ventas en {dias_sin_venta} días. Revisar operaciones.',
                'severidad': 'ALTA',
                'fecha_deteccion': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
    
    # 5. Concentración excesiva de un cliente (>80% de ventas)
    if len(df_ventas) > 0:
        top_cliente = df_ventas.groupby('cliente')['total'].sum().sort_values(ascending=False).iloc[0]
        total_ventas = df_ventas['total'].sum()
        concentracion = (top_cliente / total_ventas * 100) if total_ventas > 0 else 0
        
        if concentracion > 80:
            nombre_cliente = df_ventas.groupby('cliente')['total'].sum().sort_values(ascending=False).index[0]
            alertas_criticas.append({
                'tipo': 'CRITICA',
                'titulo': '⚠️ Alta Dependencia de Cliente',
                'descripcion': f'El cliente {nombre_cliente} representa el {concentracion:.1f}% de las ventas. Riesgo de concentración.',
                'severidad': 'MEDIA',
                'fecha_deteccion': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
    
    return alertas_criticas

def get_ai_summary(df_ventas: pd.DataFrame, df_gastos: pd.DataFrame, gemini_api_key: str = None) -> dict:
    """
    Genera un resumen inteligente de los datos
    Si se proporciona gemini_api_key, usa IA avanzada. Si no, usa análisis estadístico.
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
    
    # Predicción mejorada
    prediccion = predict_next_month_advanced(df_ventas)
    
    # Anomalías mejoradas
    anomalias = detect_anomalies_advanced(df_ventas, df_gastos)
    
    # Recomendaciones adicionales
    recomendaciones = generate_recommendations(df_ventas, df_gastos)
    
    # Detectar alertas críticas
    alertas_criticas = detect_critical_alerts(df_ventas, df_gastos)
    
    # Si hay API key de Gemini, mejorar con IA
    gemini_status = {
        'activo': False,
        'error': None,
        'insights_agregados': 0,
        'tendencias_agregadas': 0,
        'alertas_agregadas': 0,
        'recomendaciones_agregadas': 0,
        'debug_info': None
    }
    
    if gemini_api_key and GEMINI_AVAILABLE:
        try:
            # Logging más visible
            import sys
            sys.stderr.write(f"[GEMINI DEBUG] Llamando a get_gemini_insights con API key: {gemini_api_key[:10]}...\n")
            sys.stderr.flush()
            
            gemini_insights = get_gemini_insights(df_ventas, df_gastos, gemini_api_key)
            
            sys.stderr.write(f"[GEMINI DEBUG] Respuesta recibida. Tendencias: {len(gemini_insights.get('tendencias', []))}, Alertas: {len(gemini_insights.get('alertas', []))}, Recomendaciones: {len(gemini_insights.get('recomendaciones', []))}\n")
            sys.stderr.flush()
            
            # Contar cuántos insights se agregaron
            tendencias_gemini = gemini_insights.get('tendencias', [])
            alertas_gemini = gemini_insights.get('alertas', [])
            recomendaciones_gemini = gemini_insights.get('recomendaciones', [])
            
            # Ya se logueó arriba
            
            # Combinar insights de Gemini con los existentes
            if tendencias_gemini:
                insights['tendencias'].extend(tendencias_gemini)
            if alertas_gemini:
                insights['alertas'].extend(alertas_gemini)
            if recomendaciones_gemini:
                recomendaciones.extend(recomendaciones_gemini)
            
            gemini_status['activo'] = True
            gemini_status['tendencias_agregadas'] = len(tendencias_gemini)
            gemini_status['alertas_agregadas'] = len(alertas_gemini)
            gemini_status['recomendaciones_agregadas'] = len(recomendaciones_gemini)
            gemini_status['insights_agregados'] = len(tendencias_gemini) + len(alertas_gemini) + len(recomendaciones_gemini)
            gemini_status['debug_info'] = f"Gemini procesó {len(df_ventas)} ventas y {len(df_gastos)} gastos"
        except Exception as e:
            import traceback
            import sys
            gemini_status['error'] = str(e)
            gemini_status['debug_info'] = traceback.format_exc()
            sys.stderr.write(f"[GEMINI ERROR] Error al usar Gemini API: {e}\n")
            sys.stderr.write(f"[GEMINI ERROR] Traceback: {traceback.format_exc()}\n")
            sys.stderr.flush()
            # Continuar con análisis estadístico
    elif gemini_api_key and not GEMINI_AVAILABLE:
        gemini_status['error'] = 'Librería google-generativeai no está instalada'
        gemini_status['debug_info'] = 'Ejecuta: pip install google-generativeai'
    elif not gemini_api_key:
        gemini_status['debug_info'] = 'No se proporcionó API key de Gemini'
    
    return {
        'insights': insights,
        'prediccion': prediccion,
        'anomalias': anomalias,
        'recomendaciones': recomendaciones,
        'alertas_criticas': alertas_criticas,
        'usando_ia': gemini_api_key is not None and GEMINI_AVAILABLE,
        'gemini_status': gemini_status,
        'timestamp_analisis': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

def predict_next_month_advanced(df_ventas: pd.DataFrame) -> dict:
    """Predicción mejorada usando múltiples modelos ML (ARIMA, Prophet, Regresión Lineal)"""
    if len(df_ventas) < 7:
        return predict_next_month_simple(df_ventas)
    
    df_ventas['fecha'] = pd.to_datetime(df_ventas['fecha'])
    df_ventas = df_ventas.sort_values('fecha')
    
    # Agrupar por día
    ventas_diarias = df_ventas.groupby(df_ventas['fecha'].dt.date)['total'].sum().reset_index()
    ventas_diarias['fecha'] = pd.to_datetime(ventas_diarias['fecha'])
    
    if len(ventas_diarias) < 7:
        return predict_next_month_simple(df_ventas)
    
    predicciones = []
    metodos_usados = []
    
    # 1. Intentar Prophet (mejor para series temporales con estacionalidad)
    if len(ventas_diarias) >= 30 and PROPHET_AVAILABLE:
        try:
            df_prophet = pd.DataFrame({
                'ds': ventas_diarias['fecha'],
                'y': ventas_diarias['total']
            })
            
            model_prophet = Prophet(daily_seasonality=True, weekly_seasonality=True)
            model_prophet.fit(df_prophet)
            
            # Crear fechas futuras (30 días)
            future = model_prophet.make_future_dataframe(periods=30)
            forecast = model_prophet.predict(future)
            
            # Sumar los últimos 30 días de la predicción
            prediccion_prophet = forecast.tail(30)['yhat'].sum()
            
            predicciones.append(prediccion_prophet)
            metodos_usados.append('Prophet')
        except Exception as e:
            print(f"Error en Prophet: {e}")
    
    # 2. Intentar ARIMA (bueno para series estacionarias)
    if len(ventas_diarias) >= 14 and STATSMODELS_AVAILABLE:
        try:
            # Preparar serie temporal
            serie = ventas_diarias.set_index('fecha')['total']
            
            # Intentar hacer la serie estacionaria (diferencia si es necesario)
            try:
                # Probar ARIMA(1,1,1) primero
                model_arima = ARIMA(serie, order=(1, 1, 1))
                fitted_model = model_arima.fit()
                
                # Predecir 30 días
                forecast_arima = fitted_model.forecast(steps=30)
                prediccion_arima = float(forecast_arima.sum())
                
                predicciones.append(prediccion_arima)
                metodos_usados.append('ARIMA')
            except:
                # Si falla, intentar con orden más simple
                try:
                    model_arima = ARIMA(serie, order=(1, 0, 0))
                    fitted_model = model_arima.fit()
                    forecast_arima = fitted_model.forecast(steps=30)
                    prediccion_arima = float(forecast_arima.sum())
                    predicciones.append(prediccion_arima)
                    metodos_usados.append('ARIMA')
                except:
                    pass
        except Exception as e:
            print(f"Error en ARIMA: {e}")
    
    # 3. Regresión Lineal (fallback confiable)
    if len(ventas_diarias) >= 14 and SKLEARN_AVAILABLE:
        try:
            ventas_diarias['dias_desde_inicio'] = (ventas_diarias['fecha'] - ventas_diarias['fecha'].min()).dt.days
            
            X = ventas_diarias[['dias_desde_inicio']].values
            y = ventas_diarias['total'].values
            
            model = LinearRegression()
            model.fit(X, y)
            
            ultimo_dia = ventas_diarias['dias_desde_inicio'].max()
            dias_futuros = np.array([[ultimo_dia + i] for i in range(1, 31)])
            predicciones_lr = model.predict(dias_futuros)
            prediccion_lr = float(np.sum(predicciones_lr))
            
            predicciones.append(prediccion_lr)
            metodos_usados.append('Regresión Lineal')
        except Exception as e:
            print(f"Error en regresión: {e}")
    
    # Si hay predicciones de múltiples modelos, promediar
    if predicciones:
        prediccion_final = np.mean(predicciones)
        
        # Calcular confianza basada en la consistencia entre modelos
        if len(predicciones) >= 2:
            std_predicciones = np.std(predicciones)
            cv = (std_predicciones / prediccion_final) if prediccion_final > 0 else 1
            
            if cv < 0.15:  # Muy consistentes entre modelos
                confianza = 'Alta'
            elif cv < 0.30:
                confianza = 'Media'
            else:
                confianza = 'Baja'
        else:
            confianza = 'Media'
        
        metodo_str = ' + '.join(metodos_usados) if len(metodos_usados) > 1 else metodos_usados[0]
        if len(metodos_usados) > 1:
            metodo_str += f' (Promedio de {len(metodos_usados)} modelos)'
        
        return {
            'prediccion': float(prediccion_final),
            'confianza': confianza,
            'promedio_diario': float(prediccion_final / 30),
            'mensaje': f'Basado en {metodo_str}',
            'metodo': metodo_str,
            'predicciones_individuales': {metodo: float(pred) for metodo, pred in zip(metodos_usados, predicciones)}
        }
    else:
        # Fallback a método simple
        return predict_next_month_simple(df_ventas)

def predict_next_month_simple(df_ventas: pd.DataFrame) -> dict:
    """Predicción simple basada en promedio diario (método original)"""
    if len(df_ventas) < 7:
        return {
            'prediccion': None,
            'confianza': 'Baja',
            'mensaje': 'Se necesitan más datos para hacer una predicción confiable',
            'metodo': 'Promedio Simple'
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
            'mensaje': 'No hay datos recientes suficientes',
            'metodo': 'Promedio Simple'
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
        'mensaje': f'Basado en el promedio diario de ${promedio_diario:,.2f} USD',
        'metodo': 'Promedio Simple'
    }

def detect_anomalies_advanced(df_ventas: pd.DataFrame, df_gastos: pd.DataFrame) -> list:
    """Detección mejorada de anomalías con patrones temporales"""
    anomalias = []
    
    if len(df_ventas) == 0:
        return anomalias
    
    df_ventas['fecha'] = pd.to_datetime(df_ventas['fecha'])
    
    # 1. Anomalías por desviación estándar (método original)
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
                'fecha': str(venta['fecha'].date()),
                'valor': float(venta['total']),
                'descripcion': f"Venta de ${venta['total']:,.2f} USD al cliente {venta['cliente']} es significativamente mayor al promedio (más de 3 desviaciones estándar)",
                'categoria': 'Valor Atípico'
            })
    
    # 2. Detectar patrones temporales anómalos
    if len(df_ventas) >= 7:
        # Agrupar por día de la semana
        df_ventas['dia_semana'] = df_ventas['fecha'].dt.day_name()
        ventas_por_dia = df_ventas.groupby('dia_semana')['total'].mean()
        
        # Detectar días con ventas inusualmente bajas o altas
        media_diaria = ventas_por_dia.mean()
        std_diaria = ventas_por_dia.std()
        
        for dia, promedio in ventas_por_dia.items():
            if promedio < media_diaria - 2 * std_diaria:
                anomalias.append({
                    'tipo': 'Patrón Temporal Anómalo',
                    'fecha': f'Día: {dia}',
                    'valor': float(promedio),
                    'descripcion': f"Las ventas promedio del {dia} son significativamente menores al promedio general",
                    'categoria': 'Patrón Temporal'
                })
    
    # 3. Detectar estacionalidad o cambios de tendencia abruptos
    if len(df_ventas) >= 14:
        # Agrupar por semana
        df_ventas['semana'] = df_ventas['fecha'].dt.to_period('W')
        ventas_semanales = df_ventas.groupby('semana')['total'].sum()
        
        if len(ventas_semanales) >= 3:
            # Detectar caídas o subidas abruptas (>50% cambio)
            for i in range(1, len(ventas_semanales)):
                cambio = ((ventas_semanales.iloc[i] - ventas_semanales.iloc[i-1]) / ventas_semanales.iloc[i-1] * 100) if ventas_semanales.iloc[i-1] > 0 else 0
                
                if abs(cambio) > 50:
                    tipo_cambio = "aumento" if cambio > 0 else "caída"
                    anomalias.append({
                        'tipo': f'Cambio Abrupto de Tendencia ({tipo_cambio})',
                        'fecha': str(ventas_semanales.index[i]),
                        'valor': float(ventas_semanales.iloc[i]),
                        'descripcion': f"Cambio del {abs(cambio):.1f}% en ventas semanales. Semana anterior: ${ventas_semanales.iloc[i-1]:,.2f}, Semana actual: ${ventas_semanales.iloc[i]:,.2f}",
                        'categoria': 'Tendencia'
                    })
    
    # 4. Detectar períodos sin ventas
    if len(df_ventas) > 0:
        df_ventas_sorted = df_ventas.sort_values('fecha')
        for i in range(1, len(df_ventas_sorted)):
            dias_diferencia = (df_ventas_sorted.iloc[i]['fecha'] - df_ventas_sorted.iloc[i-1]['fecha']).days
            if dias_diferencia > 7:
                anomalias.append({
                    'tipo': 'Período Sin Ventas',
                    'fecha': f"{df_ventas_sorted.iloc[i-1]['fecha'].date()} a {df_ventas_sorted.iloc[i]['fecha'].date()}",
                    'valor': 0,
                    'descripcion': f"Período de {dias_diferencia} días sin ventas registradas",
                    'categoria': 'Temporal'
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
        
        # Análisis de estacionalidad
        if len(df_ventas) >= 30:
            df_ventas['fecha'] = pd.to_datetime(df_ventas['fecha'])
            df_ventas['mes'] = df_ventas['fecha'].dt.month
            ventas_mensuales = df_ventas.groupby('mes')['total'].sum()
            
            if len(ventas_mensuales) > 1:
                variacion = ventas_mensuales.std() / ventas_mensuales.mean() if ventas_mensuales.mean() > 0 else 0
                if variacion > 0.3:
                    recomendaciones.append(f"📅 Se detecta variabilidad estacional significativa ({variacion*100:.1f}%). Considerar planificación estacional")
    
    return recomendaciones

def test_gemini_connection(api_key: str) -> dict:
    """Prueba la conexión con Gemini API"""
    if not GEMINI_AVAILABLE:
        return {
            'success': False,
            'error': 'Librería google-generativeai no está instalada. Ejecuta: pip install google-generativeai'
        }
    
    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        # Hacer una prueba simple
        response = model.generate_content("Responde solo con 'OK' si puedes leer esto.")
        
        return {
            'success': True,
            'message': 'Conexión exitosa con Gemini API',
            'model': 'gemini-1.5-flash',
            'response_preview': response.text[:100] if response.text else 'Sin respuesta'
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': 'Error al conectar con Gemini API'
        }

def get_gemini_insights(df_ventas: pd.DataFrame, df_gastos: pd.DataFrame, api_key: str) -> dict:
    """Obtiene insights avanzados usando Google Gemini API"""
    if not GEMINI_AVAILABLE:
        return {'tendencias': [], 'alertas': [], 'recomendaciones': []}
    
    try:
        # Configurar Gemini
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        # Preparar resumen de datos para Gemini
        resumen_datos = {
            'total_ventas': len(df_ventas),
            'total_ingresos': float(df_ventas['total'].sum()) if len(df_ventas) > 0 else 0,
            'ventas_re': len(df_ventas[df_ventas['tipo_re_se'] == 'RE']) if len(df_ventas) > 0 else 0,
            'ventas_se': len(df_ventas[df_ventas['tipo_re_se'] == 'SE']) if len(df_ventas) > 0 else 0,
            'ingresos_re': float(df_ventas[df_ventas['tipo_re_se'] == 'RE']['total'].sum()) if len(df_ventas) > 0 else 0,
            'ingresos_se': float(df_ventas[df_ventas['tipo_re_se'] == 'SE']['total'].sum()) if len(df_ventas) > 0 else 0,
            'top_clientes': df_ventas.groupby('cliente')['total'].sum().sort_values(ascending=False).head(5).to_dict() if len(df_ventas) > 0 else {},
            'ventas_por_sucursal': df_ventas.groupby('sucursal')['total'].sum().to_dict() if len(df_ventas) > 0 and 'sucursal' in df_ventas.columns else {},
        }
        
        if len(df_gastos) > 0:
            gastos_totales = obtener_gastos_totales_con_automaticos()
            resumen_datos['gastos_totales'] = float(gastos_totales['gastos_postventa_total'])
            resumen_datos['margen'] = float(resumen_datos['total_ingresos'] - resumen_datos['gastos_totales'])
        
        # Crear prompt para Gemini
        prompt = f"""
Analiza los siguientes datos financieros de un negocio de postventa (servicios y repuestos) y proporciona:
1. Tendencias identificadas (máximo 3)
2. Alertas importantes (máximo 3)
3. Recomendaciones estratégicas específicas y accionables (máximo 5)

Datos:
- Total de ventas: {resumen_datos['total_ventas']}
- Ingresos totales: ${resumen_datos['total_ingresos']:,.2f} USD
- Ventas RE (Repuestos): {resumen_datos['ventas_re']} registros, ${resumen_datos['ingresos_re']:,.2f} USD
- Ventas SE (Servicios): {resumen_datos['ventas_se']} registros, ${resumen_datos['ingresos_se']:,.2f} USD
- Top 5 clientes: {resumen_datos['top_clientes']}
- Ventas por sucursal: {resumen_datos['ventas_por_sucursal']}
- Gastos totales: ${resumen_datos.get('gastos_totales', 0):,.2f} USD
- Margen: ${resumen_datos.get('margen', 0):,.2f} USD

Responde en formato JSON con esta estructura:
{{
    "tendencias": ["tendencia 1", "tendencia 2"],
    "alertas": ["alerta 1", "alerta 2"],
    "recomendaciones": ["recomendación 1", "recomendación 2"]
}}

Sé específico, conciso y enfocado en acciones prácticas para mejorar el negocio.
"""
        
        # Llamar a Gemini
        import sys
        sys.stderr.write(f"[GEMINI] Enviando prompt a Gemini (longitud: {len(prompt)} caracteres)\n")
        sys.stderr.flush()
        
        response = model.generate_content(prompt)
        
        # Parsear respuesta (puede venir como texto o JSON)
        respuesta_texto = response.text
        sys.stderr.write(f"[GEMINI] Respuesta recibida (longitud: {len(respuesta_texto)} caracteres)\n")
        sys.stderr.write(f"[GEMINI] Primeros 300 caracteres: {respuesta_texto[:300]}\n")
        sys.stderr.flush()
        
        # Intentar extraer JSON de la respuesta
        import json
        import re
        
        # Buscar JSON en la respuesta
        json_match = re.search(r'\{.*\}', respuesta_texto, re.DOTALL)
        if json_match:
            try:
                json_str = json_match.group()
                import sys
                sys.stderr.write(f"[GEMINI] JSON extraído exitosamente\n")
                resultado = json.loads(json_str)
                sys.stderr.write(f"[GEMINI] JSON parseado correctamente\n")
                resultado_final = {
                    'tendencias': resultado.get('tendencias', []),
                    'alertas': resultado.get('alertas', []),
                    'recomendaciones': resultado.get('recomendaciones', [])
                }
                sys.stderr.write(f"[GEMINI] Retornando {len(resultado_final['tendencias'])} tendencias, {len(resultado_final['alertas'])} alertas, {len(resultado_final['recomendaciones'])} recomendaciones\n")
                sys.stderr.flush()
                return resultado_final
            except json.JSONDecodeError as e:
                import sys
                sys.stderr.write(f"[GEMINI ERROR] Error parseando JSON: {e}\n")
                sys.stderr.write(f"[GEMINI ERROR] JSON problemático: {json_str[:300]}\n")
                sys.stderr.flush()
            except Exception as e:
                import sys
                sys.stderr.write(f"[GEMINI ERROR] Error inesperado: {e}\n")
                sys.stderr.flush()
        else:
            import sys
            sys.stderr.write(f"[GEMINI WARNING] No se encontró JSON en la respuesta\n")
            sys.stderr.write(f"[GEMINI WARNING] Respuesta: {respuesta_texto[:500]}\n")
            sys.stderr.flush()
        
        # Si no se puede parsear JSON, devolver vacío
        import sys
        sys.stderr.write(f"[GEMINI WARNING] Retornando diccionario vacío\n")
        sys.stderr.flush()
        return {'tendencias': [], 'alertas': [], 'recomendaciones': []}
        
    except Exception as e:
        print(f"Error al usar Gemini: {e}")
        return {'tendencias': [], 'alertas': [], 'recomendaciones': []}
