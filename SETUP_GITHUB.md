# 🚀 Guía para Publicar en GitHub

## Paso 1: Crear el repositorio en GitHub

1. Ve a [GitHub](https://github.com) e inicia sesión
2. Haz clic en el botón **"+"** (arriba a la derecha) → **"New repository"**
3. Completa:
   - **Repository name**: `postventa-app` (o el nombre que prefieras)
   - **Description**: "Aplicación web para gestión de postventa con Streamlit"
   - **Visibility**: Elige **Private** (recomendado) o **Public**
   - **NO marques** "Initialize with README" (ya tienes uno)
4. Haz clic en **"Create repository"**

## Paso 2: Conectar tu repositorio local con GitHub

GitHub te mostrará comandos. Usa estos (reemplaza `TU_USUARIO` y `TU_REPO`):

```bash
# Si ya hiciste el commit inicial, solo necesitas:
git remote add origin https://github.com/TU_USUARIO/TU_REPO.git
git branch -M main
git push -u origin main
```

## Paso 3: Hacer el commit inicial

Si aún no has hecho commits:

```bash
# Agregar todos los archivos (excepto los excluidos en .gitignore)
git add .

# Hacer el commit
git commit -m "Initial commit: Aplicación de gestión de postventa"

# Conectar con GitHub (reemplaza con tu URL)
git remote add origin https://github.com/TU_USUARIO/TU_REPO.git

# Subir al repositorio
git push -u origin main
```

## Paso 4: Verificar que todo esté bien

1. Ve a tu repositorio en GitHub
2. Verifica que NO aparezcan:
   - ❌ `postventa.db` (base de datos)
   - ❌ `postventa-invoice-parser-*.json` (credenciales)
   - ❌ Archivos `.xlsx` con datos reales
   - ❌ Carpeta `comprobantes/`

## 🔒 Archivos que NO se suben (por seguridad)

Gracias al `.gitignore`, estos archivos NO se subirán:
- ✅ Base de datos SQLite
- ✅ Credenciales JSON
- ✅ Archivos Excel con datos
- ✅ Comprobantes PDF/imágenes
- ✅ Logs y archivos temporales

## 📝 Próximos pasos

1. **Agregar descripción al repositorio**: Edita el README si quieres
2. **Configurar GitHub Actions** (opcional): Para CI/CD
3. **Desplegar en Streamlit Cloud**: Conecta tu repo para hosting gratuito

## 🆘 Solución de problemas

### Error: "remote origin already exists"
```bash
git remote remove origin
git remote add origin https://github.com/TU_USUARIO/TU_REPO.git
```

### Error: "failed to push some refs"
```bash
git pull origin main --allow-unrelated-histories
git push -u origin main
```

### Ver qué archivos se van a subir
```bash
git ls-files
```

