# Analizador de Colas Call Center

Aplicación web para analizar datos de call center exportados desde Genesys.

## 🎯 Funcionalidades

- **Upload de archivos CSV** desde Genesys
- **Análisis automático** de 6 colas diferentes
- **Descarga de resultados** en formato ZIP
- **Interfaz web intuitiva** con Streamlit

## 🚀 Uso Local

1. Instalar dependencias:
```bash
pip install -r requirements.txt
```

2. Ejecutar la aplicación:
```bash
streamlit run app.py
```

3. Abrir en navegador: http://localhost:8501

## 📊 Análisis Generados

La aplicación genera 6 archivos CSV con análisis detallados:

1. **Mesa de Ayuda** - Análisis por intervalos de 30min
2. **Central Telefónica** - Métricas de atención telefónica  
3. **Fraude** - Análisis de llamadas entrantes
4. **Fraude Salida** - Análisis de llamadas salientes
5. **Servicios Administrativos** - Rendimiento operacional
6. **Redes Sociales** - Métricas de interacciones digitales

## 📁 Archivos de Entrada Requeridos

- `Detalle del rendimiento de colas.csv`
- `Resumen de línea de tiempo de estado de agente.csv`

Ambos archivos deben ser exportados desde Genesys para el mismo período.

## 🌐 Deploy en Streamlit Cloud

1. Subir código a GitHub
2. Conectar repositorio en [share.streamlit.io](https://share.streamlit.io)  
3. Deploy automático 24/7

---

**Desarrollado para análisis automatizado de call center**