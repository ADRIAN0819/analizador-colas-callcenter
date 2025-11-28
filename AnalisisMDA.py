import pandas as pd
import numpy as np
import subprocess
import sys
import os

def convertir_tiempo_a_segundos(tiempo_str):
    """Convierte formato 'Xm Ys' a segundos"""
    if pd.isna(tiempo_str) or tiempo_str == '':
        return 0
    
    tiempo_str = str(tiempo_str).strip()
    if tiempo_str == '0' or tiempo_str == '0.0':
        return 0
    
    segundos_totales = 0
    
    if 'm' in tiempo_str:
        partes = tiempo_str.split('m')
        minutos = int(partes[0].strip())
        segundos_totales += minutos * 60
        
        if len(partes) > 1 and 's' in partes[1]:
            segundos_str = partes[1].replace('s', '').strip()
            if segundos_str:
                segundos = int(segundos_str)
                segundos_totales += segundos
    elif 's' in tiempo_str:
        segundos = int(tiempo_str.replace('s', '').strip())
        segundos_totales = segundos
    else:
        try:
            segundos_totales = float(tiempo_str)
        except:
            segundos_totales = 0
    
    return segundos_totales

def obtener_datos_agentes():
    """Obtiene los datos de agentes conectados importando del script de timeline"""
    try:
        print("   ✅ Importando datos desde Analisis_timeline_mda.py")
        
        # Importar la función del script de timeline
        from Analisis_timeline_mda import analizar_linea_tiempo_MA_corregido
        
        # Ejecutar el análisis y obtener los datos
        agentes_por_intervalo = analizar_linea_tiempo_MA_corregido()
        
        print(f"   � Intervalos con agentes detectados: {len(agentes_por_intervalo)}")
        
        # Mostrar algunos ejemplos relevantes
        print(f"   🔍 Ejemplos de agentes por intervalo (desde timeline):")
        ejemplos_importantes = ['00:00-00:30', '00:30-01:00', '18:00-18:30', '19:00-19:30', '14:30-15:00']
        for intervalo in ejemplos_importantes:
            if intervalo in agentes_por_intervalo:
                print(f"      {intervalo}: {agentes_por_intervalo[intervalo]} agentes")
        
        # El timeline corregido ya devuelve números, no conjuntos
        return agentes_por_intervalo
        
    except Exception as e:
        print(f"   ❌ Error en análisis: {e}")
        return {}

def main():
    print("📊 ANÁLISIS DE MESA DE AYUDA")
    print("=" * 40)
    
    # Archivo de datos - usar archivos de la carpeta principal VisualStudio
    archivo_rendimiento = "ExportadosGenesysprueba/Detalle del rendimiento de colas.csv"
    
    # print("📖 Leyendo archivo de rendimiento...")
    try:
        df = pd.read_csv(archivo_rendimiento, delimiter=';', encoding='utf-8')
        print(f"✅ Registros cargados: {len(df)}")
    except:
        try:
            df = pd.read_csv(archivo_rendimiento, delimiter=',', encoding='utf-8')
            print(f"✅ Registros cargados: {len(df)}")
        except Exception as e:
            print(f"❌ Error al cargar: {e}")
            return
    
    # Filtrar datos de Mesa de Ayuda - Lista específica de 26 colas
    colas_mesa_ayuda = [
        'A_MA Total',
        'CI_Banca_Movil',
        'CI_Multired_Victual',
        'CI_Otras_Consultas',
        'CI_Pagalo',
        'Cl_ATM',
        'MA_ActualizarDatos',
        'MA_Agente_Corresponsal',
        'MA_Banca Internet',
        'MA_Banca Platino',
        'MA_BloqueoCredito',
        'MA_BloqueoDebito',
        'MA_Credito',
        'MA_Cronograma',
        'MA_CuentaAhorros',
        'MA_CuentaCorriente',
        'MA_Cuenta_DNI',
        'MA_Debito',
        'MA_Depositos',
        'MA_Giros',
        'MA_Onp',
        'MA_Otros_Tramites',
        'MA_PagaloPe',
        'MA_Prestamos',
        'MA_Reclamos',
        'MA_Tasas'
    ]
    
    df_mesa_ayuda = df[
        (df['Nombre de cola'].isin(colas_mesa_ayuda)) &
        (~df['Nombre de cola'].str.contains(';', na=False))  # Excluir registros consolidados con ';'
    ].copy()
    
    print(f"🎯 Registros de Mesa de Ayuda: {len(df_mesa_ayuda)}")
    
    # Validar colas encontradas vs esperadas
    colas_encontradas = sorted(df_mesa_ayuda['Nombre de cola'].unique())
    print(f"🎯 Registros de Mesa de Ayuda: {len(df_mesa_ayuda)}")
    
    # print(f"\n📊 COLAS PROCESADAS:")
    # for i, cola in enumerate(colas_encontradas, 1):
    #     registros = len(df_mesa_ayuda[df_mesa_ayuda['Nombre de cola'] == cola])
    #     print(f"   {i:2d}. {cola:<25} : {registros:4d} registros")
    
    # Verificar colas faltantes
    # colas_faltantes = set(colas_mesa_ayuda) - set(colas_encontradas)
    # if colas_faltantes:
    #     print(f"\n⚠️ COLAS NO ENCONTRADAS EN LOS DATOS:")
    #     for cola in sorted(colas_faltantes):
    #         print(f"   - {cola}")
    # else:
    #     print(f"\n✅ Todas las colas especificadas están presentes en los datos")
    
    # Convertir fechas (lógica exacta)
    try:
        df_mesa_ayuda['fecha'] = pd.to_datetime(df_mesa_ayuda['Inicio del intervalo'], format='%d/%m/%y %H:%M').dt.date
        df_mesa_ayuda['hora_inicio'] = pd.to_datetime(df_mesa_ayuda['Inicio del intervalo'], format='%d/%m/%y %H:%M').dt.strftime('%H:%M')
        df_mesa_ayuda['hora_fin'] = pd.to_datetime(df_mesa_ayuda['Fin del intervalo'], format='%d/%m/%y %H:%M').dt.strftime('%H:%M')
    except:
        df_mesa_ayuda['fecha'] = pd.to_datetime(df_mesa_ayuda['Inicio del intervalo'], dayfirst=True).dt.date
        df_mesa_ayuda['hora_inicio'] = pd.to_datetime(df_mesa_ayuda['Inicio del intervalo'], dayfirst=True).dt.strftime('%H:%M')
        df_mesa_ayuda['hora_fin'] = pd.to_datetime(df_mesa_ayuda['Fin del intervalo'], dayfirst=True).dt.strftime('%H:%M')
    
    # DEBUG: Mostrar fechas disponibles en los datos DESPUÉS de la conversión
    # if len(df_mesa_ayuda) > 0:
    #     fechas_disponibles = sorted(df_mesa_ayuda['fecha'].unique())
    #     print(f"\n📅 FECHAS DISPONIBLES EN LOS DATOS:")
    #     for fecha in fechas_disponibles:
    #         cantidad = len(df_mesa_ayuda[df_mesa_ayuda['fecha'] == fecha])
    #         print(f"   📊 {fecha}: {cantidad} registros")
    
    # Detectar automáticamente la fecha de análisis (tomar la fecha más común)
    fecha_mas_comun = df_mesa_ayuda['fecha'].mode().iloc[0] if len(df_mesa_ayuda) > 0 else None
    if fecha_mas_comun:
        fecha_analisis = fecha_mas_comun
        # print(f"📅 Fecha detectada automáticamente: {fecha_analisis}")
    else:
        # Fallback: usar la fecha más reciente en los datos
        if len(df_mesa_ayuda) > 0:
            fecha_analisis = df_mesa_ayuda['fecha'].max()
            print(f"📅 Usando fecha más reciente en datos: {fecha_analisis}")
        else:
            print("❌ ERROR: No hay datos para procesar")
            return
    
    df_mesa_ayuda = df_mesa_ayuda[df_mesa_ayuda['fecha'] == fecha_analisis]
    print(f"📅 Datos filtrados para {fecha_analisis}: {len(df_mesa_ayuda)}")
    
    # Convertir columnas (lógica exacta)
    columnas_numericas = ['Oferta', 'Contestadas', 'Abandonadas', 'Cumplen el SLA']
    for col in columnas_numericas:
        if col in df_mesa_ayuda.columns:
            df_mesa_ayuda[col] = pd.to_numeric(df_mesa_ayuda[col], errors='coerce').fillna(0)
    
    df_mesa_ayuda['Manejo medio'] = df_mesa_ayuda['Manejo medio'].apply(convertir_tiempo_a_segundos)
    
    # Crear intervalo key
    df_mesa_ayuda['intervalo_key'] = df_mesa_ayuda['hora_inicio'] + '-' + df_mesa_ayuda['hora_fin']
    
    # Obtener datos de agentes
    print(f"\n🔗 Obteniendo datos de agentes conectados...")
    agentes_por_intervalo = obtener_datos_agentes()
    print(f"✅ Datos de agentes obtenidos: {len(agentes_por_intervalo)} intervalos")
    
    # Procesar intervalos sumando TODAS las colas individuales
    print(f"\n🔄 Procesando intervalos sumando todas las colas...")
    
    resultados = []
    
    for intervalo in df_mesa_ayuda['intervalo_key'].unique():
        grupo = df_mesa_ayuda[df_mesa_ayuda['intervalo_key'] == intervalo]
        
        # Convertir columnas a numérico y sumar TODAS las colas
        grupo_copy = grupo.copy()
        columnas_numericas = ['Oferta', 'Contestadas', 'Abandonadas', 'Cumplen el SLA']
        for col in columnas_numericas:
            if col in grupo_copy.columns:
                grupo_copy[col] = pd.to_numeric(grupo_copy[col], errors='coerce').fillna(0)
        
        # Sumar todos los valores
        llamadas_recibidas = int(grupo_copy['Oferta'].sum())
        llamadas_atendidas = int(grupo_copy['Contestadas'].sum())
        llamadas_abandonadas = int(grupo_copy['Abandonadas'].sum())
        llamadas_atendidas_20 = int(grupo_copy['Cumplen el SLA'].sum())
        
        # Para TMO, usar la metodología EXACTA encontrada: suma de manejo total / suma de llamadas manejadas
        tmo_total_segundos = 0
        llamadas_manejadas_total = 0
        
        for _, row in grupo_copy.iterrows():
            if pd.notna(row['Manejo total']) and pd.notna(row['Manejo medio']) and row['Manejo total'] > 0 and row['Manejo medio'] > 0:
                manejo_total = convertir_tiempo_a_segundos(row['Manejo total']) if isinstance(row['Manejo total'], str) else float(row['Manejo total'])
                manejo_medio = convertir_tiempo_a_segundos(row['Manejo medio']) if isinstance(row['Manejo medio'], str) else float(row['Manejo medio'])
                
                if manejo_total > 0 and manejo_medio > 0:
                    llamadas_manejadas = manejo_total / manejo_medio
                    tmo_total_segundos += manejo_total
                    llamadas_manejadas_total += llamadas_manejadas
        
        if llamadas_manejadas_total > 0:
            tmo_promedio_segundos = tmo_total_segundos / llamadas_manejadas_total
            tmo_minutos = tmo_promedio_segundos / 60
        else:
            tmo_minutos = 0
        
        # Obtener intervalo display
        if len(grupo) > 0:
            primer_registro = grupo.iloc[0]
            intervalo_display = f"{primer_registro['hora_inicio']}-{primer_registro['hora_fin']}"
        else:
            continue
        
        # Niveles
        nivel_atencion = (llamadas_atendidas / llamadas_recibidas * 100) if llamadas_recibidas > 0 else 0
        nivel_servicio = (llamadas_atendidas_20 / llamadas_recibidas * 100) if llamadas_recibidas > 0 else 0
        
        # Validación específica para 19:00-19:30 (donde encontramos la fórmula exacta)
        if intervalo_display == "19:00-19:30":
            print(f"\n🎯 VALIDACIÓN 19:00-19:30 (FÓRMULA EXACTA):")
            print(f"   Colas procesadas: {len(grupo)}")
            print(f"   Oferta: {llamadas_recibidas}")
            print(f"   Contestadas: {llamadas_atendidas}")
            print(f"   Abandonadas: {llamadas_abandonadas}")
            print(f"   TMO: {tmo_minutos:.2f} minutos ({tmo_promedio_segundos:.1f} segundos)")
            print(f"   Nivel atención: {nivel_atencion:.1f}%")
            print(f"   Nivel servicio: {nivel_servicio:.1f}%")
            print(f"   👥 Agentes en cola: {agentes_conectados}")
            
            # Verificar si coincide con Genesys (416 segundos = 6m 56s)
            if abs(tmo_promedio_segundos - 416) <= 2:
                print(f"   ✅ TMO EXACTO GENESYS! Diferencia: {abs(tmo_promedio_segundos - 416):.1f}s")
            else:
                print(f"   ⚠️ Diferencia con Genesys (416s): {abs(tmo_promedio_segundos - 416):.1f}s")
        
        # Validación para 18:00-18:30 
        elif intervalo_display == "18:00-18:30":
            print(f"\n🎯 VALIDACIÓN 18:00-18:30:")
            print(f"   Colas procesadas: {len(grupo)}")
            print(f"   Oferta: {llamadas_recibidas}")
            print(f"   Contestadas: {llamadas_atendidas}")
            print(f"   Abandonadas: {llamadas_abandonadas}")
            print(f"   TMO: {tmo_minutos:.2f} minutos ({tmo_promedio_segundos:.0f} segundos)")
            print(f"   Nivel atención: {nivel_atencion:.1f}%")
            print(f"   Nivel servicio: {nivel_servicio:.1f}%")
            print(f"   👥 Agentes en cola: {agentes_conectados} (análisis timeline corregido)")
            
            # Verificar si coincide con Genesys (95 llamadas, 5m 45s = 345s)
            if llamadas_recibidas == 95 and abs(tmo_promedio_segundos - 345) <= 5:
                print(f"   ✅ VALORES EXACTOS GENESYS!")
            else:
                print(f"   ⚠️ Diferencia con Genesys esperado")
            
            # Mostrar desglose por cola
            print(f"   DESGLOSE POR COLA:")
            for _, row in grupo_copy.iterrows():
                if row['Oferta'] > 0:
                    print(f"     {row['Nombre de cola'][:30]:30s}: {row['Oferta']:3.0f} llamadas")
        
        # Obtener agentes conectados del timeline corregido
        agentes_conectados = agentes_por_intervalo.get(intervalo_display, 0)
        
        # Mostrar información del intervalo
        if agentes_conectados > 0:
            print(f"   ✅ Intervalo {intervalo_display}: {agentes_conectados} agentes (timeline corregido)")
        else:
            print(f"   ⚠️ Intervalo {intervalo_display}: 0 agentes detectados")
        
        # Si nivel de servicio muy bajo, usar nivel de atención
        if nivel_servicio < 5 and nivel_atencion > 20:
            print(f"   ⚠️ Nivel servicio muy bajo ({nivel_servicio:.2f}%) en {intervalo_display}, usando nivel de atención ({nivel_atencion:.2f}%)")
            nivel_servicio = nivel_atencion
        
        # Calcular tasa de abandono
        tasa_abandono = (llamadas_abandonadas / llamadas_recibidas * 100) if llamadas_recibidas > 0 else 0
        
        resultado = {
            'Intervalo': intervalo_display,
            'Llamadas_Recibidas': llamadas_recibidas,
            'Llamadas_Atendidas': llamadas_atendidas,
            'Llamadas_Abandonadas': llamadas_abandonadas,
            'Llamadas_Atendidas_<20': llamadas_atendidas_20,
            'Nivel_de_Atencion_%': round(nivel_atencion, 2),
            'Nivel_de_Servicio_%': round(nivel_servicio, 2),
            'Tasa_Abandono_%': round(tasa_abandono, 2),
            'TMO': f"00:{int(tmo_promedio_segundos // 60):02d}:{int(tmo_promedio_segundos % 60):02d}",
            'TMO_Segundos_Calculo': round(tmo_promedio_segundos, 0),  # Para cálculos internos
            'Asesores_Conectados': agentes_conectados,
            'Asesores_Requeridos': '',
            'Productividad_Promedio': '',
            'Productividad_Meta': '',
            'Proyectado': '',
            'Desviacion': ''
        }
        
        resultados.append(resultado)
    
    print(f"📋 Intervalos procesados: {len(resultados)}")
    
    # Guardar CSV
    df_resultados = pd.DataFrame(resultados)
    archivo_salida = "ExportadosGenerados/Analisis_Mesa_Ayuda_Por_Intervalos.csv"
    
    print(f"\n✅ ARCHIVO GENERADO: {archivo_salida}")
    print(f"📊 Total intervalos: {len(resultados)}")
    
    # Mostrar resumen
    if len(resultados) > 0:
        total_llamadas = df_resultados['Llamadas_Recibidas'].sum()
        total_atendidas = df_resultados['Llamadas_Atendidas'].sum()
        total_abandonadas = df_resultados['Llamadas_Abandonadas'].sum()
        tmo_promedio_segundos = df_resultados['TMO_Segundos_Calculo'].mean()
        tmo_promedio_minutos = tmo_promedio_segundos / 60
        
        print(f"\n📈 RESUMEN:")
        print(f"📞 Total llamadas recibidas: {total_llamadas:,}")
        print(f"✅ Total llamadas atendidas: {total_atendidas:,}")
        print(f"❌ Total llamadas abandonadas: {total_abandonadas:,}")
        print(f"⏱️ TMO promedio: {tmo_promedio_minutos:.2f} minutos (00:{int(tmo_promedio_segundos // 60):02d}:{int(tmo_promedio_segundos % 60):02d})")
        print(f"📊 Nivel de atención promedio: {(total_atendidas/total_llamadas*100):.2f}%")
        
        # Eliminar columna auxiliar antes de guardar
        df_final = df_resultados.drop('TMO_Segundos_Calculo', axis=1)
        df_final.to_csv(archivo_salida, index=False)
        
        # Análisis adicional con información cruzada
        print(f"\n🔍 ANÁLISIS CRUZADO CON TIMELINE:")
        agentes_promedio = df_resultados['Asesores_Conectados'].mean()
        tasa_abandono_promedio = df_resultados['Tasa_Abandono_%'].mean()
        
        print(f"👥 Agentes promedio en cola: {agentes_promedio:.1f}")
        print(f"❌ Tasa de abandono promedio: {tasa_abandono_promedio:.2f}%")
        
        # Encontrar intervalos pico
        intervalo_mas_llamadas = df_resultados.loc[df_resultados['Llamadas_Recibidas'].idxmax()]
        intervalo_mas_agentes = df_resultados.loc[df_resultados['Asesores_Conectados'].idxmax()]
        
        print(f"\n📈 PICOS IDENTIFICADOS:")
        print(f"📞 Más llamadas: {intervalo_mas_llamadas['Intervalo']} ({intervalo_mas_llamadas['Llamadas_Recibidas']} llamadas)")
        print(f"👥 Más agentes: {intervalo_mas_agentes['Intervalo']} ({intervalo_mas_agentes['Asesores_Conectados']} agentes)")
        
        # Ejecutar validación detallada de agentes conectados desde el script especializado
        print(f"\n" + "="*100)
        print(f"🔍 EJECUTANDO VALIDACIÓN DETALLADA DESDE Analisis_timeline_mda.py")
        print("=" * 90)
        try:
            from Analisis_timeline_mda import analizar_linea_tiempo_MA_corregido
            analizar_linea_tiempo_MA_corregido()
        except Exception as e:
            print(f"⚠️ No se pudo ejecutar validación detallada: {e}")
    else:
        print("\n⚠️ No se generaron resultados - revisar filtros de fecha")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Error en el análisis: {e}")
        import traceback
        traceback.print_exc()