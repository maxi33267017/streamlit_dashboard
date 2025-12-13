"""
Módulo de análisis con IA mejorado
Incluye análisis estadístico local y opcionalmente Google Gemini API
"""
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from gastos_automaticos import obtener_gastos_totales_con_automaticos

# Intentar importar funciones de historial (puede no estar disponible en todas las versiones)
try:
    from database import guardar_analisis_ia
    HISTORIAL_DISPONIBLE = True
except ImportError:
    HISTORIAL_DISPONIBLE = False

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


def _safe_sum_column(df: pd.DataFrame | None, column: str) -> float:
    """Suma segura para columnas que pueden no existir."""
    if df is None or column not in df.columns:
        return 0.0
    return float(df[column].fillna(0).sum())


def _resolve_gastos_context(
    df_gastos: pd.DataFrame | None,
    gastos_context: dict | None = None,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
) -> dict:
    """
    Devuelve un diccionario con totales de gastos usando, en orden:
    - El contexto recibido
    - Los datos del DataFrame proporcionado
    - Una consulta a la base (fallback)
    """
    if gastos_context:
        return gastos_context

    if df_gastos is not None and len(df_gastos) > 0:
        total_se = _safe_sum_column(df_gastos, "total_pct_se")
        total_re = _safe_sum_column(df_gastos, "total_pct_re")
        total = total_se + total_re
        if total == 0.0:
            total = _safe_sum_column(df_gastos, "monto")
        if total == 0.0:
            total = _safe_sum_column(df_gastos, "total_pct")
        return {
            "gastos_registrados": df_gastos,
            "gastos_automaticos": None,
            "gastos_todos": df_gastos,
            "gastos_postventa_total": float(total),
            "gastos_se_total": float(total_se),
            "gastos_re_total": float(total_re),
        }

    return obtener_gastos_totales_con_automaticos(fecha_inicio, fecha_fin)


def _format_usd(value: float) -> str:
    try:
        return f"${value:,.2f}"
    except Exception:
        return f"${value}"


def _build_branch_results(df_ventas: pd.DataFrame, df_gastos: pd.DataFrame) -> list[dict]:
    def _filter_compartidos(df: pd.DataFrame) -> pd.DataFrame:
        if "sucursal" not in df.columns:
            return df
        return df[~df["sucursal"].fillna("").str.upper().eq("COMPARTIDOS")]

    if len(df_ventas) > 0:
        ingresos_df = _filter_compartidos(df_ventas)
        ingresos_sucursal = (
            ingresos_df.groupby("sucursal")["total"].sum().reset_index().rename(columns={"total": "ingresos"})
        )
    else:
        ingresos_sucursal = pd.DataFrame(columns=["sucursal", "ingresos"])

    if len(df_gastos) > 0:
        df_costos = _filter_compartidos(df_gastos.copy())
        if "total_pct" not in df_costos.columns:
            df_costos["total_pct"] = df_costos["total_pct_se"].fillna(0) + df_costos["total_pct_re"].fillna(0)
        costos_sucursal = (
            df_costos.groupby("sucursal")["total_pct"].sum().reset_index().rename(columns={"total_pct": "gastos"})
        )
    else:
        costos_sucursal = pd.DataFrame(columns=["sucursal", "gastos"])

    if not len(ingresos_sucursal) and not len(costos_sucursal):
        return []

    resultados = (
        ingresos_sucursal.merge(costos_sucursal, on="sucursal", how="outer")
        .fillna(0)
        .assign(resultado=lambda df: df["ingresos"] - df["gastos"])
    )
    return (
        resultados.round({"ingresos": 2, "gastos": 2, "resultado": 2})
        .to_dict("records")
        if len(resultados)
        else []
    )


def _build_fallback_sections(
    resultados_sucursal: list[dict],
    share_servicios: float,
    share_repuestos: float,
) -> dict:
    rec_suc = []
    oportunidades = []
    riesgos = []

    if resultados_sucursal:
        ordenados = sorted(resultados_sucursal, key=lambda row: row.get("resultado", 0))
        worst = ordenados[0]
        best = ordenados[-1]
        rec_suc.append(
            f"{worst.get('sucursal', 'Sucursal')} "
            f"aporta { _format_usd(worst.get('ingresos', 0)) } frente a gastos de "
            f"{ _format_usd(worst.get('gastos', 0)) }. Activar acciones comerciales y revisar gastos fijos."
        )
        rec_suc.append(
            f"Replicar las palancas de {best.get('sucursal', 'Sucursal')} "
            f"(resultado { _format_usd(best.get('resultado', 0)) }) en sucursales más chicas."
        )

        for row in ordenados:
            if row.get("resultado", 0) < 0:
                riesgos.append(
                    f"{row.get('sucursal', 'Sucursal')} está en déficit de { _format_usd(abs(row.get('resultado', 0))) }."
                )
        if not riesgos:
            riesgos.append(
                f"{worst.get('sucursal', 'Sucursal')} es la más ajustada (resultado { _format_usd(worst.get('resultado', 0)) })."
            )

        low_volume = sorted(resultados_sucursal, key=lambda row: row.get("ingresos", 0))
        for row in low_volume:
            if row.get("resultado", 0) > 0:
                oportunidades.append(
                    f"{row.get('sucursal', 'Sucursal')} tiene margen positivo con bajo volumen "
                    f"({ _format_usd(row.get('ingresos', 0)) }). Escalar servicios allí generará impacto rápido."
                )
                break
        if not oportunidades:
            oportunidades.append(
                f"{best.get('sucursal', 'Sucursal')} lidera el resultado. Profundizar cross-selling para sostener el liderazgo."
            )

    rec_mix = []
    if share_servicios >= 0.6:
        rec_mix.append(
            "Los servicios explican más del 60% del mix. Impulsar repuestos de mostrador y paquetes de mantenimiento "
            "para equilibrar el margen."
        )
    elif share_repuestos >= 0.6:
        rec_mix.append(
            "Los repuestos dominan el mix. Ofrecer bundles con mano de obra para aumentar horas facturables."
        )
    else:
        rec_mix.append("El mix RE/SE está equilibrado. Mantener campañas combinadas para sostener el aporte.")

    return {
        "recomendaciones_sucursales": rec_suc,
        "recomendaciones_mix": rec_mix,
        "oportunidades": oportunidades,
        "riesgos": riesgos,
    }


def _future_working_mask(
    last_date: pd.Timestamp | None,
    horizon: int = 30,
) -> tuple[np.ndarray, int, list[pd.Timestamp]]:
    base_date = last_date if last_date is not None and not pd.isna(last_date) else pd.Timestamp.now()
    future_dates = [base_date + timedelta(days=i) for i in range(1, horizon + 1)]
    mask = np.array([1.0 if date.weekday() < 6 else 0.0 for date in future_dates], dtype=float)
    working_days = int(mask.sum())
    if working_days == 0:
        mask = np.ones(horizon, dtype=float)
        working_days = horizon
    return mask, working_days, future_dates

def detect_critical_alerts(
    df_ventas: pd.DataFrame,
    df_gastos: pd.DataFrame,
    gastos_context: dict | None = None,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    referencia_corte: pd.Timestamp | None = None,
) -> list:
    """Detecta alertas críticas que requieren atención inmediata"""
    alertas_criticas = []
    gastos_totales_ctx = _resolve_gastos_context(df_gastos, gastos_context, fecha_inicio, fecha_fin) or {}
    gastos_postventa_total = gastos_totales_ctx.get("gastos_postventa_total", 0.0)
    
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
        gastos_postventa = gastos_postventa_total
        
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
        gastos_postventa = gastos_postventa_total
        
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
        referencia = referencia_corte or (pd.to_datetime(fecha_fin) if fecha_fin else pd.Timestamp.now())
        dias_sin_venta = (referencia - df_ventas['fecha'].max()).days
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

def get_ai_summary(
    df_ventas: pd.DataFrame,
    df_gastos: pd.DataFrame,
    gemini_api_key: str = None,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    gastos_context: dict | None = None,
    productividad_context: dict | None = None,
) -> dict:
    """
    Genera un resumen inteligente de los datos
    Si se proporciona gemini_api_key, usa IA avanzada. Si no, usa análisis estadístico.
    fecha_inicio/fin permiten restringir el cálculo a un período específico.
    """
    insights = {
        'tendencias': [],
        'alertas': [],
        'recomendaciones': []
    }
    gastos_totales_ctx = _resolve_gastos_context(df_gastos, gastos_context, fecha_inicio, fecha_fin) or {}
    gastos_postventa_total = gastos_totales_ctx.get('gastos_postventa_total', 0.0)
    referencia_corte = pd.to_datetime(fecha_fin) if fecha_fin else pd.Timestamp.now()
    
    resultados_sucursal = _build_branch_results(df_ventas, df_gastos)
    share_servicios = 0.0
    share_repuestos = 0.0
    total_mix = 0.0

    # Análisis de ventas
    if len(df_ventas) > 0:
        df_ventas['fecha'] = pd.to_datetime(df_ventas['fecha'])
        ventas_mensuales = df_ventas.groupby(df_ventas['fecha'].dt.to_period('M'))['total'].sum()
        
        if len(ventas_mensuales) > 1:
            if ventas_mensuales.iloc[-1] > ventas_mensuales.iloc[-2]:
                insights['tendencias'].append("📈 Las ventas muestran una tendencia creciente dentro del período seleccionado.")
            elif ventas_mensuales.iloc[-1] < ventas_mensuales.iloc[-2]:
                insights['tendencias'].append("📉 Las ventas muestran una tendencia decreciente dentro del período seleccionado.")
        
        servicios = df_ventas[df_ventas['tipo_re_se'] == 'SE']['total'].sum()
        repuestos = df_ventas[df_ventas['tipo_re_se'] == 'RE']['total'].sum()
        total_mix = servicios + repuestos
        if total_mix > 0:
            share_serv = servicios / total_mix
            share_rep = repuestos / total_mix
            share_servicios = share_serv
            share_repuestos = share_rep
            insights['tendencias'].append(
                f"⚖️ Mix del período: Servicios {share_serv:.1%} vs Repuestos {share_rep:.1%}."
            )
            if share_serv >= 0.6:
                insights['tendencias'].append("🔧 Los servicios explican más del 60% del mix y sostienen la contribución.")
            elif share_rep >= 0.6:
                insights['tendencias'].append("⚙️ Los repuestos concentran más del 60% del mix y apalancan la rentabilidad.")
    
    # Análisis de gastos vs ingresos
    if len(df_ventas) > 0 and len(df_gastos) > 0:
        total_ingresos = df_ventas['total'].sum()
        gastos_postventa = gastos_postventa_total
        
        margen = total_ingresos - gastos_postventa
        margen_pct = (margen / total_ingresos * 100) if total_ingresos > 0 else 0
        
        if margen_pct <= 0:
            insights['alertas'].append("🚨 El período cerró con pérdida operativa luego de gastos postventa.")
        elif margen_pct < 15:
            insights['alertas'].append(f"⚠️ Margen ajustado: {margen_pct:.1f}%. Hay poco colchón para absorber desvíos.")
        else:
            insights['tendencias'].append(f"✅ Margen saludable del {margen_pct:.1f}% que cubre cómodamente los gastos fijos.")
        
        if gastos_postventa > total_ingresos:
            insights['alertas'].append("🚨 Los gastos superan los ingresos. Revisar urgentemente")
    
    # Recomendaciones
    if len(df_ventas) > 0:
        df_ventas['fecha'] = pd.to_datetime(df_ventas['fecha'])
        dias_sin_venta = (referencia_corte - df_ventas['fecha'].max()).days
        if dias_sin_venta > 7:
            insights['recomendaciones'].append(f"📅 Hace {dias_sin_venta} días que no se registran ventas. Considerar seguimiento activo.")
        else:
            ticket_promedio = df_ventas['total'].mean()
            insights['recomendaciones'].append(
                f"🎯 Ticket promedio del período: ${ticket_promedio:,.2f}. Reforzar upselling para sostenerlo."
            )
    
    fallback_sections = _build_fallback_sections(resultados_sucursal, share_servicios, share_repuestos)
    productividad_data = None
    if productividad_context:
        productividad_data = {
            'ingresos_mo_asistencia': float(productividad_context.get('ingresos_mo_asistencia', 0.0) or 0.0),
            'horas_vendidas': float(productividad_context.get('horas_vendidas', 0.0) or 0.0),
            'horas_disponibles': float(productividad_context.get('horas_disponibles', 0.0) or 0.0),
            'dias_habiles': int(productividad_context.get('dias_habiles', 0) or 0),
            'tarifa': float(productividad_context.get('tarifa', 0.0) or 0.0),
            'tecnicos': int(productividad_context.get('tecnicos', 0) or 0),
            'productividad_pct': float(productividad_context.get('productividad_pct', 0.0) or 0.0),
        }
    if productividad_data is None and len(df_ventas) and 'fecha' in df_ventas.columns:
        try:
            periodo_inicio = pd.to_datetime(df_ventas['fecha']).min().date()
            periodo_fin = pd.to_datetime(df_ventas['fecha']).max().date()
            horas_habiles, dias_habiles = compute_working_hours(periodo_inicio, periodo_fin)
        except Exception:
            horas_habiles, dias_habiles = 0.0, 0
        ventas_se_local = df_ventas[df_ventas['tipo_re_se'] == 'SE'] if len(df_ventas) else pd.DataFrame()
        mano_obra_total = ventas_se_local['mano_obra'].fillna(0).sum() if len(ventas_se_local) else 0.0
        asistencia_total = ventas_se_local['asistencia'].fillna(0).sum() if len(ventas_se_local) else 0.0
        ingresos_mo_asistencia = mano_obra_total + asistencia_total
        tarifa_hora = 60.0
        tecnicos_default = 7
        horas_disponibles = horas_habiles * tecnicos_default if horas_habiles and tecnicos_default else 0.0
        horas_vendidas = ingresos_mo_asistencia / tarifa_hora if tarifa_hora > 0 else 0.0
        productividad_pct = (horas_vendidas / horas_disponibles) if horas_disponibles > 0 else 0.0
        productividad_data = {
            'ingresos_mo_asistencia': ingresos_mo_asistencia,
            'horas_vendidas': horas_vendidas,
            'horas_disponibles': horas_disponibles,
            'dias_habiles': dias_habiles,
            'tarifa': tarifa_hora,
            'tecnicos': tecnicos_default,
            'productividad_pct': productividad_pct,
        }

    if productividad_data:
        horas_disponibles = productividad_data.get('horas_disponibles', 0.0) or 0.0
        horas_vendidas = productividad_data.get('horas_vendidas', 0.0) or 0.0
        productividad_pct = (horas_vendidas / horas_disponibles) if horas_disponibles > 0 else productividad_data.get('productividad_pct', 0.0)
        ocupacion_pct = productividad_pct * 100
        insights['tendencias'].append(
            f"🛠️ Productividad del taller: {horas_vendidas:,.1f} h vendidas vs {horas_disponibles:,.1f} h disponibles "
            f"({ocupacion_pct:.1f}% de ocupación)."
        )
        if ocupacion_pct < 50:
            insights['alertas'].append(
                f"⚠️ Baja ocupación operativa ({ocupacion_pct:.1f}%). Incrementar horas facturables o ajustar estructura."
            )
        if ocupacion_pct < 65:
            insights['recomendaciones'].append(
                "📉 Reforzar planificación diaria (turnos, viáticos y tiempos muertos) para elevar la ocupación hacia el 70%."
            )

    top_clientes_local = []
    if len(df_ventas) > 0 and {'cliente', 'sucursal'}.issubset(df_ventas.columns):
        top_clientes_local = (
            df_ventas.groupby(['cliente', 'sucursal'])['total']
            .sum()
            .reset_index()
            .sort_values('total', ascending=False)
            .head(10)
        )
        top_clientes_local['total'] = top_clientes_local['total'].round(2)
        top_clientes_local = top_clientes_local.to_dict('records')

    # Predicción mejorada
    prediccion = predict_next_month_advanced(df_ventas)
    
    # Anomalías mejoradas
    anomalias = detect_anomalies_advanced(df_ventas, df_gastos)
    
    # Recomendaciones adicionales
    recomendaciones = generate_recommendations(df_ventas, df_gastos)
    if not insights['recomendaciones'] and recomendaciones:
        insights['recomendaciones'].extend(recomendaciones[:2])
        recomendaciones = recomendaciones[2:]
    
    # Detectar alertas críticas
    alertas_criticas = detect_critical_alerts(
        df_ventas,
        df_gastos,
        gastos_context=gastos_totales_ctx,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        referencia_corte=referencia_corte,
    )
    
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
    
    extra_sections = {}
    if gemini_api_key and GEMINI_AVAILABLE:
        try:
            # Logging más visible
            import sys
            sys.stderr.write(f"[GEMINI DEBUG] Llamando a get_gemini_insights con API key: {gemini_api_key[:10]}...\n")
            sys.stderr.flush()
            
            gemini_insights = get_gemini_insights(
                df_ventas, df_gastos, gemini_api_key, productividad_data
            )
            
            sys.stderr.write(f"[GEMINI DEBUG] Respuesta recibida. Tendencias: {len(gemini_insights.get('tendencias', []))}, Alertas: {len(gemini_insights.get('alertas', []))}, Recomendaciones: {len(gemini_insights.get('recomendaciones', []))}\n")
            sys.stderr.flush()
            
            # Contar cuántos insights se agregaron
            tendencias_gemini = gemini_insights.get('tendencias', [])
            alertas_gemini = gemini_insights.get('alertas', [])
            recomendaciones_gemini = gemini_insights.get('recomendaciones', [])
            
            # Ya se logueó arriba
            
            extra_sections = {
                'recomendaciones_sucursales': gemini_insights.get('recomendaciones_sucursales', []),
                'recomendaciones_mix': gemini_insights.get('recomendaciones_mix', []),
                'oportunidades': gemini_insights.get('oportunidades', []),
                'riesgos': gemini_insights.get('riesgos', []),
            }
            
            # Combinar insights de Gemini con los existentes
            # IMPORTANTE: Agregar al final para poder identificarlos después
            if tendencias_gemini:
                insights['tendencias'].extend(tendencias_gemini)
            if alertas_gemini:
                insights['alertas'].extend(alertas_gemini)
            if recomendaciones_gemini:
                # Agregar al final para poder identificarlas
                recomendaciones.extend(recomendaciones_gemini)
            
            gemini_status['activo'] = True
            gemini_status['tendencias_agregadas'] = len(tendencias_gemini)
            gemini_status['alertas_agregadas'] = len(alertas_gemini)
            gemini_status['recomendaciones_agregadas'] = len(recomendaciones_gemini)
            gemini_status['insights_agregados'] = len(tendencias_gemini) + len(alertas_gemini) + len(recomendaciones_gemini)
            gemini_status['debug_info'] = f"Gemini procesó {len(df_ventas)} ventas y {len(df_gastos)} gastos"
            gemini_status['extra_sections'] = extra_sections
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
    
    timestamp_analisis = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Guardar en historial si está disponible
    if HISTORIAL_DISPONIBLE:
        try:
            fuente = 'gemini' if (gemini_api_key and GEMINI_AVAILABLE and gemini_status.get('activo', False)) else 'local'
            
            # Guardar tendencias
            for tendencia in insights.get('tendencias', []):
                guardar_analisis_ia('tendencia', fuente, tendencia, {'timestamp': timestamp_analisis})
            
            # Guardar alertas
            for alerta in insights.get('alertas', []):
                guardar_analisis_ia('alerta', fuente, alerta, {'timestamp': timestamp_analisis})
            
            # Guardar recomendaciones
            todas_recomendaciones = insights.get('recomendaciones', []) + recomendaciones
            for recomendacion in todas_recomendaciones:
                guardar_analisis_ia('recomendacion', fuente, recomendacion, {'timestamp': timestamp_analisis})
            
            # Guardar anomalías
            for anomalia in anomalias:
                guardar_analisis_ia('anomalia', fuente, anomalia.get('descripcion', str(anomalia)), {
                    'timestamp': timestamp_analisis,
                    'tipo': anomalia.get('tipo', 'desconocido'),
                    'severidad': anomalia.get('severidad', 'media')
                })
            
            # Guardar predicción
            if prediccion and prediccion.get('prediccion'):
                pred_texto = f"Predicción: {prediccion.get('prediccion', 0):,.2f} USD - Confianza: {prediccion.get('confianza', 'N/A')} - {prediccion.get('mensaje', '')}"
                guardar_analisis_ia('prediccion', fuente, pred_texto, {
                    'timestamp': timestamp_analisis,
                    'valor': prediccion.get('prediccion'),
                    'confianza': prediccion.get('confianza'),
                    'metodo': prediccion.get('metodo', 'desconocido')
                })
            
            # Guardar alertas críticas
            for alerta in alertas_criticas:
                guardar_analisis_ia('alerta', fuente, f"{alerta.get('titulo', '')} - {alerta.get('descripcion', '')}", {
                    'timestamp': timestamp_analisis,
                    'severidad': alerta.get('severidad', 'media'),
                    'fecha_deteccion': alerta.get('fecha_deteccion', '')
                })
        except Exception as e:
            # Si falla el guardado, continuar sin error (no crítico)
            import sys
            sys.stderr.write(f"[HISTORIAL] Error al guardar historial: {e}\n")
    
    gemini_extra = gemini_status.get('extra_sections', {}) if isinstance(gemini_status, dict) else {}
    extra_sections_result = {}
    for key in ['recomendaciones_sucursales', 'recomendaciones_mix', 'oportunidades', 'riesgos']:
        extra_sections_result[key] = gemini_extra.get(key) or fallback_sections.get(key, [])

    return {
        'insights': insights,
        'prediccion': prediccion,
        'anomalias': anomalias,
        'recomendaciones': recomendaciones,
        'alertas_criticas': alertas_criticas,
        'usando_ia': gemini_api_key is not None and GEMINI_AVAILABLE,
        'gemini_status': gemini_status,
        'timestamp_analisis': timestamp_analisis,
        'recomendaciones_sucursales': extra_sections_result.get('recomendaciones_sucursales', []),
        'recomendaciones_mix': extra_sections_result.get('recomendaciones_mix', []),
        'oportunidades': extra_sections_result.get('oportunidades', []),
        'riesgos': extra_sections_result.get('riesgos', []),
        'top_clientes_detalle': top_clientes_local,
        'productividad': productividad_data,
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
    projection_days = 30
    last_date = ventas_diarias['fecha'].max()
    working_mask, working_days, future_dates = _future_working_mask(last_date, projection_days)
    
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
            future = model_prophet.make_future_dataframe(periods=projection_days)
            forecast = model_prophet.predict(future)
            
            forecast_tail = forecast.tail(projection_days)['yhat'].to_numpy()
            prediccion_prophet = float(np.sum(forecast_tail * working_mask))
            
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
                
                forecast_arima = fitted_model.forecast(steps=projection_days)
                prediccion_arima = float(np.sum(forecast_arima * working_mask))
                
                predicciones.append(prediccion_arima)
                metodos_usados.append('ARIMA')
            except:
                # Si falla, intentar con orden más simple
                try:
                    model_arima = ARIMA(serie, order=(1, 0, 0))
                    fitted_model = model_arima.fit()
                    forecast_arima = fitted_model.forecast(steps=projection_days)
                    prediccion_arima = float(np.sum(forecast_arima * working_mask))
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
            dias_futuros = np.array([[ultimo_dia + i] for i in range(1, projection_days + 1)])
            predicciones_lr = model.predict(dias_futuros)
            prediccion_lr = float(np.sum(predicciones_lr * working_mask))
            
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
        
        promedio_diario = float(prediccion_final / projection_days)
        promedio_diario_habil = float(prediccion_final / working_days) if working_days else promedio_diario
        return {
            'prediccion': float(prediccion_final),
            'confianza': confianza,
            'promedio_diario': promedio_diario,
            'promedio_diario_habil': promedio_diario_habil,
            'mensaje': f'Basado en {metodo_str}',
            'metodo': metodo_str,
            'dias_habiles': working_days,
            'horizonte_dias': projection_days,
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
    
    projection_days = 30
    last_date = ventas_recientes['fecha'].max()
    working_mask, working_days, _ = _future_working_mask(last_date, projection_days)
    prediccion = promedio_diario * working_days
    
    # Calcular confianza basada en variabilidad
    std_dev = ventas_recientes.groupby(ventas_recientes['fecha'].dt.date)['total'].sum().std()
    cv = (std_dev / promedio_diario) if promedio_diario > 0 else 1
    
    if cv < 0.3:
        confianza = 'Alta'
    elif cv < 0.6:
        confianza = 'Media'
    else:
        confianza = 'Baja'
    
    promedio_diario_habil = promedio_diario if working_days == projection_days else prediccion / working_days
    
    return {
        'prediccion': prediccion,
        'confianza': confianza,
        'promedio_diario': promedio_diario,
        'promedio_diario_habil': promedio_diario_habil,
        'mensaje': f'Basado en el promedio diario de ${promedio_diario:,.2f} USD',
        'metodo': 'Promedio Simple',
        'dias_habiles': working_days,
        'horizonte_dias': projection_days,
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
        
        # Primero, listar modelos disponibles
        try:
            available_models = genai.list_models()
            modelos_disponibles = [m.name.split('/')[-1] for m in available_models if 'generateContent' in m.supported_generation_methods]
        except:
            modelos_disponibles = []
        
        # Intentar con diferentes nombres de modelo (ordenados por preferencia)
        # Los modelos disponibles son Gemini 2.0/2.5 (no 1.5)
        modelos_a_probar = ['gemini-2.0-flash', 'gemini-2.5-flash', 'gemini-2.0-flash-exp', 'gemini-2.5-pro']
        
        for modelo_nombre in modelos_a_probar:
            try:
                model = genai.GenerativeModel(modelo_nombre)
                # Hacer una prueba simple
                response = model.generate_content("Responde solo con 'OK' si puedes leer esto.")
                
                return {
                    'success': True,
                    'message': f'Conexión exitosa con Gemini API usando modelo: {modelo_nombre}',
                    'model': modelo_nombre,
                    'response_preview': response.text[:100] if response.text else 'Sin respuesta',
                    'modelos_disponibles': modelos_disponibles[:5] if modelos_disponibles else []
                }
            except Exception as e:
                error_str = str(e)
                if '404' in error_str or 'not found' in error_str.lower():
                    continue  # Probar siguiente modelo
                elif '429' in error_str or 'quota' in error_str.lower() or 'rate limit' in error_str.lower():
                    return {
                        'success': False,
                        'error': f'Cuota agotada para {modelo_nombre}. Espera unos minutos o verifica tu plan de Google AI Studio.',
                        'modelos_disponibles': modelos_disponibles[:5] if modelos_disponibles else [],
                        'tipo_error': 'quota_exceeded'
                    }
                else:
                    # Otro error, devolver información útil
                    return {
                        'success': False,
                        'error': f'Error con modelo {modelo_nombre}: {error_str}',
                        'modelos_disponibles': modelos_disponibles[:5] if modelos_disponibles else []
                    }
        
        # Si llegamos aquí, ningún modelo funcionó
        return {
            'success': False,
            'error': 'Ninguno de los modelos probados está disponible. Verifica tu API key y permisos.',
            'modelos_probados': modelos_a_probar,
            'modelos_disponibles': modelos_disponibles[:10] if modelos_disponibles else []
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': 'Error al conectar con Gemini API'
        }

def get_gemini_insights(
    df_ventas: pd.DataFrame,
    df_gastos: pd.DataFrame,
    api_key: str,
    productividad: dict | None = None,
) -> dict:
    """Obtiene insights avanzados usando Google Gemini API"""
    if not GEMINI_AVAILABLE:
        return {
            'tendencias': [],
            'alertas': [],
            'recomendaciones': [],
            'recomendaciones_sucursales': [],
            'recomendaciones_mix': [],
            'oportunidades': [],
            'riesgos': [],
        }
    
    try:
        # Configurar Gemini
        genai.configure(api_key=api_key)
        
        # Listar modelos disponibles primero
        try:
            available_models = genai.list_models()
            modelos_disponibles = [m.name.split('/')[-1] for m in available_models if 'generateContent' in m.supported_generation_methods]
            import sys
            sys.stderr.write(f"[GEMINI] Modelos disponibles: {', '.join(modelos_disponibles[:5])}\n")
            sys.stderr.flush()
        except Exception as e:
            import sys
            sys.stderr.write(f"[GEMINI WARNING] No se pudieron listar modelos: {e}\n")
            sys.stderr.flush()
            modelos_disponibles = []
        
        # Intentar con diferentes modelos (ordenados por preferencia)
        # Los modelos disponibles son Gemini 2.0/2.5, no 1.5
        modelos_a_probar = ['gemini-2.0-flash', 'gemini-2.5-flash', 'gemini-2.0-flash-exp', 'gemini-2.5-pro']
        # Si hay modelos disponibles, priorizar los que están en la lista
        if modelos_disponibles:
            modelos_a_probar = [m for m in modelos_a_probar if m in modelos_disponibles] + [m for m in modelos_disponibles[:5] if m not in modelos_a_probar]
        
        model = None
        modelo_usado = None
        
        for modelo_nombre in modelos_a_probar[:5]:  # Limitar a 5 intentos
            try:
                model = genai.GenerativeModel(modelo_nombre)
                # Probar que funciona con un prompt simple
                test_response = model.generate_content("test")
                modelo_usado = modelo_nombre
                break
            except Exception as e:
                error_str = str(e)
                if '404' in error_str or 'not found' in error_str.lower():
                    import sys
                    sys.stderr.write(f"[GEMINI] Modelo {modelo_nombre} no disponible (404), probando siguiente...\n")
                    sys.stderr.flush()
                    continue
                elif '429' in error_str or 'quota' in error_str.lower() or 'rate limit' in error_str.lower():
                    import sys
                    sys.stderr.write(f"[GEMINI ERROR] Cuota agotada para {modelo_nombre}. Espera unos minutos o verifica tu plan.\n")
                    sys.stderr.flush()
                    # No lanzar error, devolver vacío para que continúe con análisis estadístico
                    return {'tendencias': [], 'alertas': [], 'recomendaciones': []}
                else:
                    import sys
                    sys.stderr.write(f"[GEMINI ERROR] Error con modelo {modelo_nombre}: {error_str}\n")
                    sys.stderr.flush()
                    raise
        
        if model is None:
            import sys
            sys.stderr.write(f"[GEMINI ERROR] Ningún modelo disponible. Modelos probados: {modelos_a_probar[:5]}\n")
            if modelos_disponibles:
                sys.stderr.write(f"[GEMINI] Modelos disponibles según API: {', '.join(modelos_disponibles[:10])}\n")
            sys.stderr.flush()
            return {'tendencias': [], 'alertas': [], 'recomendaciones': []}
        
        import sys
        sys.stderr.write(f"[GEMINI] ✅ Usando modelo: {modelo_usado}\n")
        sys.stderr.flush()
        
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
        
        if len(df_gastos) > 0 and gastos_postventa_total:
            resumen_datos['gastos_totales'] = float(gastos_postventa_total)
            resumen_datos['margen'] = float(resumen_datos['total_ingresos'] - resumen_datos['gastos_totales'])
        # Top clientes por sucursal (cliente + sucursal)
        top_clientes_detalle_records = []
        if len(df_ventas) > 0 and {'cliente', 'sucursal'}.issubset(df_ventas.columns):
            top_clientes_detalle = (
                df_ventas.groupby(['cliente', 'sucursal'])['total']
                .sum()
                .reset_index()
                .sort_values('total', ascending=False)
                .head(10)
            )
            top_clientes_detalle['total'] = top_clientes_detalle['total'].round(2)
            top_clientes_detalle_records = top_clientes_detalle.to_dict('records')
        resumen_datos['top_clientes_detalle'] = top_clientes_detalle_records

        # Ventas SE/RE por sucursal (monto y cantidad)
        ventas_tipo_detalle = []
        if len(df_ventas) > 0 and 'sucursal' in df_ventas.columns:
            ventas_tipo_detalle = (
                df_ventas.groupby(['sucursal', 'tipo_re_se'])['total']
                .agg(['sum', 'count'])
                .reset_index()
                .rename(columns={'sum': 'monto', 'count': 'cantidad'})
            )
            ventas_tipo_detalle['monto'] = ventas_tipo_detalle['monto'].round(2)
            ventas_tipo_detalle = ventas_tipo_detalle.to_dict('records')
        resumen_datos['ventas_tipo_detalle'] = ventas_tipo_detalle

        # Resultados por sucursal (ingresos vs gastos)
        resultados_sucursal = []
        if len(df_ventas) > 0:
            ingresos_sucursal = (
                df_ventas.groupby('sucursal')['total'].sum().reset_index().rename(columns={'total': 'ingresos'})
            )
        else:
            ingresos_sucursal = pd.DataFrame(columns=['sucursal', 'ingresos'])
        if len(df_gastos) > 0:
            df_costos = df_gastos.copy()
            if 'total_pct' not in df_costos.columns:
                df_costos['total_pct'] = df_costos['total_pct_se'].fillna(0) + df_costos['total_pct_re'].fillna(0)
            costos_sucursal = (
                df_costos.groupby('sucursal')['total_pct'].sum().reset_index().rename(columns={'total_pct': 'gastos'})
            )
        else:
            costos_sucursal = pd.DataFrame(columns=['sucursal', 'gastos'])
        if len(ingresos_sucursal) or len(costos_sucursal):
            resultados_sucursal = (
                ingresos_sucursal.merge(costos_sucursal, on='sucursal', how='outer')
                .fillna(0)
                .assign(resultado=lambda df: df['ingresos'] - df['gastos'])
                .round({'ingresos': 2, 'gastos': 2, 'resultado': 2})
                .to_dict('records')
            )
        resumen_datos['resultado_sucursal'] = resultados_sucursal
        if productividad:
            resumen_datos['productividad'] = productividad
        
        # Crear prompt para Gemini - más específico y estructurado
        margen_pct = (resumen_datos.get('margen', 0) / resumen_datos['total_ingresos'] * 100) if resumen_datos['total_ingresos'] > 0 else 0
        
        productividad_block = ""
        if productividad:
            productividad_block = (
                "\nPRODUCTIVIDAD DEL TALLER:\n"
                f"- Ingresos MO + asistencia: {_format_usd(productividad.get('ingresos_mo_asistencia', 0.0))}\n"
                f"- Horas vendidas estimadas: {productividad.get('horas_vendidas', 0.0):,.1f} h\n"
                f"- Horas disponibles: {productividad.get('horas_disponibles', 0.0):,.1f} h "
                f"(ocupación {productividad.get('productividad_pct', 0.0)*100:.1f}%)\n"
                f"- Técnicos activos: {productividad.get('tecnicos', 0)} | Tarifa promedio: "
                f"{_format_usd(productividad.get('tarifa', 0.0))}/h\n"
            )

        prompt = f"""Eres un analista financiero experto en postventa (servicios + repuestos). Analiza los datos y responde con hallazgos profundos y accionables.

DATOS FINANCIEROS:
- Total de ventas registradas: {resumen_datos['total_ventas']}
- Ingresos totales: ${resumen_datos['total_ingresos']:,.2f} USD
- Ventas de Repuestos (RE): {resumen_datos['ventas_re']} registros, ${resumen_datos['ingresos_re']:,.2f} USD
- Ventas de Servicios (SE): {resumen_datos['ventas_se']} registros, ${resumen_datos['ingresos_se']:,.2f} USD
- Gastos totales: ${resumen_datos.get('gastos_totales', 0):,.2f} USD
- Margen: ${resumen_datos.get('margen', 0):,.2f} USD ({margen_pct:.1f}%)
- Ventas por sucursal: {resumen_datos['ventas_por_sucursal']}
- Top 10 clientes por sucursal (cliente, sucursal, ventas): {resumen_datos.get('top_clientes_detalle', [])}
- Ventas RE/SE por sucursal (monto y cantidad): {resumen_datos.get('ventas_tipo_detalle', [])}
- Resultado operativo por sucursal (ingresos, gastos, resultado): {resumen_datos.get('resultado_sucursal', [])}
{productividad_block}

INSTRUCCIONES:
Analiza patrones sofisticados (mix de negocios, productividad, sucursales fuertes/débiles, concentración de clientes) y responde con acciones concretas. 
Evalúa la productividad del taller contra una referencia de 65%-70% de ocupación; si está por debajo, explica causas y acciones específicas.
NO repitas datos triviales, aporta interpretación. Vincula cada insight a sucursales, técnicos o métricas cuando sea posible.

Responde SOLO en JSON válido con esta estructura exacta:
{{
  "tendencias": ["", ""],
  "alertas": ["", ""],
  "recomendaciones": ["", ""],
  "recomendaciones_sucursales": ["", ""],
  "recomendaciones_mix": ["", ""],
  "oportunidades": ["", ""],
  "riesgos": ["", ""]
}}

Donde:
- recomendaciones_sucursales: planes concretos para mejorar resultados por sucursal (costos altos, pocas ventas, productividad, etc.).
- recomendaciones_mix: acciones para equilibrar o potenciar las ventas RE vs SE (ej. impulsar mostrador en sucursal X).
- oportunidades: iniciativas con upside (cross selling, clientes con potencial, sucursales con capacidad ociosa).
- riesgos: amenazas detectadas al comparar ingresos vs gastos por sucursal, dependencia de clientes, caídas de margen.
- Usa máximo 3 ítems por categoría; prioriza lo crítico.
- Referencia nombres de sucursal y clientes cuando aplique.
- Responde SOLO con el JSON (sin texto adicional).
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
                    'recomendaciones': resultado.get('recomendaciones', []),
                    'recomendaciones_sucursales': resultado.get('recomendaciones_sucursales', []),
                    'recomendaciones_mix': resultado.get('recomendaciones_mix', []),
                    'oportunidades': resultado.get('oportunidades', []),
                    'riesgos': resultado.get('riesgos', []),
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
        return {
            'tendencias': [],
            'alertas': [],
            'recomendaciones': [],
            'recomendaciones_sucursales': [],
            'recomendaciones_mix': [],
            'oportunidades': [],
            'riesgos': [],
        }
        
    except Exception as e:
        print(f"Error al usar Gemini: {e}")
        return {
            'tendencias': [],
            'alertas': [],
            'recomendaciones': [],
            'recomendaciones_sucursales': [],
            'recomendaciones_mix': [],
            'oportunidades': [],
            'riesgos': [],
        }
