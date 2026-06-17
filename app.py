# -*- coding: utf-8 -*-
"""
ANALIZADOR DE COLAS - INTERFAZ WEB
==================================
Aplicación web Streamlit para analizar datos de call center.

Permite subir los archivos CSV de Genesys y generar automáticamente
los 6 análisis de colas en formato CSV.

Autor: Sistema de Análisis de Call Center
"""

import streamlit as st
import pandas as pd
import os
import subprocess
import sys
import zipfile
import io
from datetime import datetime
import tempfile
import shutil

def main():
    st.set_page_config(
        page_title="Analizador de Colas Call Center",
        page_icon="📊",
        layout="wide"
    )
    
    # Título principal
    st.title("📊 Analizador de Colas Call Center")
    st.markdown("---")
    
    # Crear pestañas
    tab1, tab2 = st.tabs(["📁 Procesamiento de Archivos", "📊 Gráfico de Colas por Hora"])
    
    with tab1:
        # Descripción
        st.markdown("""
        ### 🎯 ¿Qué hace esta aplicación?
        
        Esta herramienta procesa los datos exportados de **Genesys** y genera automáticamente 
        **6 análisis completos** de las diferentes colas del call center:
        
        - 📞 **Mesa de Ayuda** - Análisis detallado por intervalos (28 colas)
        - ☎️ **Central Telefónica** - Métricas de atención
        - 🛡️ **Fraude** - Análisis de llamadas entrantes
        - 📤 **Fraude Salida** - Análisis de llamadas salientes  
        - 🏢 **Servicios Administrativos** - Rendimiento operacional
        - 📱 **Redes Sociales** - Métricas de interacciones digitales
        """)
        
        st.markdown("---")
        
        # Sección de carga de archivos
        st.markdown("### 📁 Subir Archivos de Genesys")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🔢 Detalle del rendimiento de colas.csv**")
            archivo_detalle = st.file_uploader(
                "Selecciona el archivo de detalle de rendimiento",
                type=['csv'],
                key="detalle",
                help="Archivo exportado desde Genesys con datos de rendimiento por colas"
            )
        
        with col2:
            st.markdown("**⏱️ Resumen de línea de tiempo de estado de agente.csv**")
            archivo_timeline = st.file_uploader(
                "Selecciona el archivo de timeline de agentes",
                type=['csv'],
                key="timeline", 
                help="Archivo exportado desde Genesys con estados de agentes por tiempo"
            )
        
        # Cargar archivo detalle en session_state si se sube
        if archivo_detalle is not None:
            if 'df_detalle' not in st.session_state or st.session_state.get('detalle_filename') != archivo_detalle.name:
                try:
                    # Intentar leer con ; y si no ,
                    try:
                        df = pd.read_csv(archivo_detalle, delimiter=';', encoding='utf-8')
                    except Exception:
                        archivo_detalle.seek(0)
                        df = pd.read_csv(archivo_detalle, delimiter=',', encoding='utf-8')
                    st.session_state['df_detalle'] = df
                    st.session_state['detalle_filename'] = archivo_detalle.name
                    # Restaurar puntero
                    archivo_detalle.seek(0)
                except Exception as e:
                    st.error(f"Error al cargar datos del archivo para gráfico: {e}")
        else:
            if 'df_detalle' in st.session_state:
                del st.session_state['df_detalle']
            if 'detalle_filename' in st.session_state:
                del st.session_state['detalle_filename']
        
        # Verificar si ambos archivos están cargados
        if archivo_detalle is not None and archivo_timeline is not None:
            st.success("✅ Ambos archivos cargados correctamente")
            
            # Mostrar información de los archivos
            col1, col2 = st.columns(2)
            with col1:
                st.info(f"📄 **{archivo_detalle.name}**\n\nTamaño: {archivo_detalle.size:,} bytes")
            with col2:
                st.info(f"📄 **{archivo_timeline.name}**\n\nTamaño: {archivo_timeline.size:,} bytes")
            
            st.markdown("---")
            
            # Botón de procesamiento
            if st.button("🚀 **GENERAR ANÁLISIS COMPLETO**", type="primary", use_container_width=True):
                procesar_archivos(archivo_detalle, archivo_timeline)
        
        else:
            st.warning("⚠️ Por favor sube ambos archivos CSV para continuar")
            
            # Información adicional
            with st.expander("ℹ️ ¿Cómo obtener estos archivos desde Genesys?"):
                st.markdown("""
                ### 📋 Pasos para exportar desde Genesys:
                
                **Para el archivo de Detalle del rendimiento:**
                1. Ir a **Informes** → **Colas**
                2. Seleccionar el rango de fechas deseado
                3. Exportar como CSV con el nombre: `Detalle del rendimiento de colas.csv`
                
                **Para el archivo de Timeline de agentes:**
                1. Ir a **Informes** → **Agentes** → **Línea de tiempo**
                2. Seleccionar el mismo rango de fechas
                3. Exportar como CSV con el nombre: `Resumen de línea de tiempo de estado de agente.csv`
                
                ⚠️ **Importante:** Ambos archivos deben corresponder al mismo período de tiempo.
                """)
                
    with tab2:
        mostrar_grafico_colas()

def procesar_archivos(archivo_detalle, archivo_timeline):
    """Procesa los archivos subidos y genera los análisis"""
    
    # Crear directorio temporal
    with tempfile.TemporaryDirectory() as temp_dir:
        
        # Guardar archivos en directorio temporal
        detalle_path = os.path.join(temp_dir, "ExportadosGenesysprueba", "Detalle del rendimiento de colas.csv")
        timeline_path = os.path.join(temp_dir, "ExportadosGenesysprueba", "Resumen de línea de tiempo de estado de agente.csv")
        
        # Crear directorios necesarios
        os.makedirs(os.path.dirname(detalle_path), exist_ok=True)
        os.makedirs(os.path.join(temp_dir, "ExportadosGenerados"), exist_ok=True)
        
        # Escribir archivos
        with open(detalle_path, "wb") as f:
            f.write(archivo_detalle.getvalue())
        with open(timeline_path, "wb") as f:
            f.write(archivo_timeline.getvalue())
        
        # Verificar que los archivos se crearon correctamente
        if os.path.exists(detalle_path) and os.path.exists(timeline_path):
            detalle_size = os.path.getsize(detalle_path)
            timeline_size = os.path.getsize(timeline_path)
            st.info(f"📁 Archivos guardados:\n- Detalle: {detalle_size:,} bytes\n- Timeline: {timeline_size:,} bytes")
        else:
            st.error("❌ Error: No se pudieron guardar los archivos correctamente")
            return
        
        # Copiar scripts de análisis al directorio temporal
        scripts_originales = [
            "AnalisisMDA.py",
            "AnalisisCentral.py", 
            "AnalisisFraude.py",
            "AnalisisFraudeSalida.py",
            "AnalisisServicios.py",
            "AnalisisRedes.py"
        ]
        
        # Scripts auxiliares que también se necesitan
        scripts_auxiliares = [
            "Analisis_timeline_mda.py",
            "Analisis_timeline_fraude.py"
        ]
        
        # Mostrar progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            total_scripts = len(scripts_originales)
            
            # Primero copiar todos los scripts auxiliares
            for script_aux in scripts_auxiliares:
                if os.path.exists(script_aux):
                    script_aux_temp = os.path.join(temp_dir, script_aux)
                    shutil.copy2(script_aux, script_aux_temp)
            
            for i, script in enumerate(scripts_originales):
                status_text.text(f"🔄 Ejecutando {script.replace('.py', '').replace('Analisis', 'Análisis ')}...")
                
                # Copiar script al directorio temporal
                script_temp = os.path.join(temp_dir, script)
                shutil.copy2(script, script_temp)
                
                # Ejecutar script en el directorio temporal
                resultado = subprocess.run(
                    [sys.executable, script_temp],
                    cwd=temp_dir,
                    capture_output=True,
                    text=True
                )
                
                if resultado.returncode == 0:
                    st.success(f"✅ {script} completado")
                else:
                    st.error(f"❌ Error en {script}")
                    with st.expander(f"Ver detalles del error - {script}"):
                        if resultado.stderr:
                            st.code(resultado.stderr)
                        if resultado.stdout:
                            st.code(resultado.stdout)
                
                # Actualizar progress bar
                progress_bar.progress((i + 1) / total_scripts)
            
            # Verificar archivos generados
            archivos_generados = []
            archivos_esperados = [
                "Analisis_Mesa_Ayuda_Por_Intervalos.csv",
                "Analisis_Central_Por_intervalos.csv",
                "Analisis_Fraude_Por_intervalos.csv", 
                "Analisis_FraudeOut_Por_intervalos.csv",
                "Analisis_Servicios_Por_intervalos.csv",
                "Analisis_Redes_Por_intervalos.csv"
            ]
            
            exportados_dir = os.path.join(temp_dir, "ExportadosGenerados")
            
            for archivo in archivos_esperados:
                archivo_path = os.path.join(exportados_dir, archivo)
                if os.path.exists(archivo_path):
                    archivos_generados.append((archivo, archivo_path))
            
            status_text.text("✅ ¡Procesamiento completado!")
            
            if archivos_generados:
                st.success(f"🎉 **¡Análisis completado!** Se generaron {len(archivos_generados)} archivos")
                
                # Crear ZIP con todos los archivos
                zip_buffer = io.BytesIO()
                
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    for archivo_nombre, archivo_path in archivos_generados:
                        zip_file.write(archivo_path, archivo_nombre)
                
                zip_buffer.seek(0)
                
                # Botón de descarga
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                st.download_button(
                    label="📥 **DESCARGAR TODOS LOS ANÁLISIS (.ZIP)**",
                    data=zip_buffer.getvalue(),
                    file_name=f"Analisis_Colas_{timestamp}.zip",
                    mime="application/zip",
                    use_container_width=True,
                    type="primary"
                )
                
                # Mostrar detalles de archivos generados
                with st.expander("📋 Archivos generados"):
                    for archivo_nombre, archivo_path in archivos_generados:
                        tamaño = os.path.getsize(archivo_path)
                        st.write(f"📄 **{archivo_nombre}** - {tamaño:,} bytes")
                        
                        # Permitir descarga individual
                        with open(archivo_path, "rb") as f:
                            st.download_button(
                                f"Descargar {archivo_nombre}",
                                data=f.read(),
                                file_name=archivo_nombre,
                                mime="text/csv",
                                key=f"download_{archivo_nombre}"
                            )
                
            else:
                st.error("❌ No se pudo generar ningún archivo de análisis")
                
        except Exception as e:
            st.error(f"❌ Error durante el procesamiento: {str(e)}")

def mostrar_grafico_colas():
    st.markdown("### 📊 Top 6 Colas MA con más Llamadas por Hora")
    
    if 'df_detalle' not in st.session_state or st.session_state['df_detalle'] is None:
        st.info("ℹ️ Sube el archivo **Detalle del rendimiento de colas.csv** en la pestaña de procesamiento para ver el gráfico.")
        return
        
    df = st.session_state['df_detalle'].copy()
    
    # Verificar columnas necesarias
    if 'Nombre de cola' not in df.columns or 'Inicio del intervalo' not in df.columns:
        st.error("❌ El archivo subido no contiene las columnas necesarias ('Nombre de cola', 'Inicio del intervalo')")
        return
        
    # Filtrar solo colas que comiencen con MA_
    df_ma = df[df['Nombre de cola'].str.startswith('MA_', na=False)].copy()
    
    if len(df_ma) == 0:
        st.warning("⚠️ No se encontraron colas que comiencen con 'MA_' en los datos.")
        return
        
    # Convertir 'Inicio del intervalo' a datetime
    try:
        df_ma['datetime_inicio'] = pd.to_datetime(df_ma['Inicio del intervalo'], format='%d/%m/%y %H:%M')
    except Exception:
        try:
            df_ma['datetime_inicio'] = pd.to_datetime(df_ma['Inicio del intervalo'], dayfirst=True)
        except Exception as e:
            st.error(f"❌ Error al procesar las fechas: {e}")
            return
            
    # Extraer la hora completa (en formato 24h, ej. 19:00, 20:00)
    df_ma['hora_completa'] = df_ma['datetime_inicio'].dt.floor('h')
    
    # Generar etiquetas amigables en formato 12h y 24h (ej: 07:00 PM (19:00))
    def formatear_hora(dt):
        if pd.isna(dt):
            return "N/A"
        h24 = dt.strftime('%H:00')
        h12 = dt.strftime('%I:00 %p')
        return f"{h12} ({h24})"
        
    df_ma['hora_label'] = df_ma['hora_completa'].apply(formatear_hora)
    
    # Obtener horas únicas ordenadas
    horas_disponibles = sorted(df_ma['hora_label'].unique())
    
    if not horas_disponibles:
        st.warning("⚠️ No se encontraron horas disponibles en los datos.")
        return
        
    # Selector de hora
    hora_seleccionada = st.selectbox(
        "⏰ Selecciona una hora para el análisis:",
        options=horas_disponibles,
        help="Muestra las colas activas durante esa hora completa (agrupando intervalos de 30 minutos)."
    )
    
    # Convertir columna 'Oferta' a numérico
    if 'Oferta' in df_ma.columns:
        df_ma['Oferta'] = pd.to_numeric(df_ma['Oferta'], errors='coerce').fillna(0)
    else:
        st.error("❌ No se encontró la columna 'Oferta' en los datos.")
        return
        
    # Filtrar por hora seleccionada
    df_filtrado = df_ma[df_ma['hora_label'] == hora_seleccionada]
    
    # Agrupar por cola y sumar Oferta
    resumen_colas = df_filtrado.groupby('Nombre de cola')['Oferta'].sum().reset_index()
    
    # Filtrar colas con llamadas > 0
    resumen_colas = resumen_colas[resumen_colas['Oferta'] > 0]
    
    # Ordenar y tomar top 6
    resumen_colas = resumen_colas.sort_values(by='Oferta', ascending=False).head(6)
    
    if len(resumen_colas) == 0:
        st.info("ℹ️ No hay llamadas registradas para las colas de Mesa de Ayuda (MA_) en la hora seleccionada.")
        return
        
    # Renombrar columnas para mejor visualización
    resumen_colas = resumen_colas.rename(columns={'Nombre de cola': 'Cola', 'Oferta': 'Llamadas Recibidas (Oferta)'})
    
    st.markdown(f"#### 📈 Top colas en el horario {hora_seleccionada}")
    
    # Mostrar como gráfico de barras interactivo
    st.bar_chart(
        data=resumen_colas,
        x='Cola',
        y='Llamadas Recibidas (Oferta)',
        use_container_width=True
    )
    
    # Mostrar tabla resumen para mayor precisión
    st.dataframe(
        resumen_colas.reset_index(drop=True),
        use_container_width=True
    )

if __name__ == "__main__":
    main()