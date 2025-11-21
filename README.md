# 📊 Aplicación de Gestión de Postventa

Aplicación web sencilla para registrar ventas y gastos, con reportes y KPIs en tiempo real.

## 🚀 Características

- ✅ **Registro de Ventas**: Formulario completo para registrar nuevas ventas
- ✅ **Registro de Gastos**: Gestión de gastos con asignación porcentual
- ✅ **Importación desde Excel**: Importa datos desde tu archivo Excel existente
- ✅ **Dashboard con KPIs**: Métricas en tiempo real y visualizaciones
- ✅ **Reportes Detallados**: Análisis por sucursal, cliente, tipo, etc.
- ✅ **Análisis con IA**: Insights, predicciones, detección de anomalías y recomendaciones
- ✅ **Base de Datos SQLite**: Almacenamiento local y eficiente
- ✅ **Plantillas de Gastos**: Crea plantillas reutilizables para gastos recurrentes
- ✅ **Adjuntar Comprobantes**: Adjunta PDFs o imágenes a las ventas
- ✅ **Previsualización de PDFs**: Visualiza comprobantes sin descargarlos
- ✅ **Extracción de PDFs con IA**: Prueba diferentes métodos para extraer datos de comprobantes

## 📋 Requisitos

- Python 3.8 o superior
- pip (gestor de paquetes de Python)

## 🔧 Instalación

1. Instala las dependencias:
```bash
pip install -r requirements.txt
```

2. Ejecuta la aplicación:
```bash
streamlit run app.py
```

3. Abre tu navegador en la URL que aparece (generalmente `http://localhost:8501`)

## 📥 Importar Datos Existentes

1. Ve a la sección "📥 Importar Excel"
2. Sube tu archivo Excel (debe tener las hojas "REGISTRO VENTAS" y "REGISTRO GASTOS")
3. Haz clic en "Importar Todo" para importar todos los datos

## 📊 Estructura de la Aplicación

### Páginas Principales:

1. **🏠 Dashboard**: KPIs principales, gráficos y métricas
2. **💰 Registrar Venta**: Formulario para nuevas ventas
3. **💸 Registrar Gasto**: Formulario para nuevos gastos
4. **⚙️ Plantillas Gastos**: Gestión de plantillas de gastos reutilizables
5. **📥 Importar Excel**: Importación masiva desde Excel
6. **📋 Ver Registros**: Visualización y gestión de registros
7. **📈 Reportes**: Reportes detallados y análisis
8. **🤖 Análisis IA**: Análisis inteligente con predicciones y recomendaciones
9. **🔍 Probar Extracción PDF**: Prueba diferentes métodos para extraer datos de PDFs

## 💾 Base de Datos

La aplicación usa SQLite (`postventa.db`) que se crea automáticamente al iniciar.

### Tablas:
- `ventas`: Registro de todas las ventas
- `gastos`: Registro de todos los gastos
- `plantillas_gastos`: Plantillas reutilizables para gastos

## 📝 Notas

- Todos los valores están en **USD**
- Los valores en pesos son solo de referencia
- Los gastos automáticos se calculan basándose en las ventas de repuestos
- El factor de absorción se calcula como: Ingresos / Gastos Fijos × 100

## 🔐 Seguridad

⚠️ **IMPORTANTE**: Este proyecto excluye archivos sensibles del control de versiones:
- Base de datos (`postventa.db`)
- Archivos de credenciales (`.json` de Service Accounts)
- Archivos Excel con datos reales
- Comprobantes PDF/imágenes

Si necesitas configurar credenciales, crea un archivo `.env` basándote en `.env.example`.

## 🚀 Despliegue

### Acceso desde red local:
1. Edita `.streamlit/config.toml` y asegúrate de que `address = "0.0.0.0"`
2. Ejecuta `streamlit run app.py`
3. Accede desde otros dispositivos usando `http://TU_IP:8501`

### Despliegue en producción:
- **Streamlit Cloud**: Conecta tu repositorio de GitHub
- **Heroku**: Usa el buildpack de Streamlit
- **AWS/DigitalOcean**: Ejecuta en un servidor con Python instalado

