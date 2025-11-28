#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ANÁLISIS DE SERVICIOS ADMINISTRATIVOS
=====================================
Script para analizar el rendimiento de Servicios Administrativos con datos de agentes conectados.

Funcionalidades:
- Procesa datos de la cola 'Srv_administrativos'
- Calcula métricas por intervalos de 30 minutos
- Obtiene cantidad de agentes conectados desde timeline (2 agentes específicos)
- Genera archivo CSV con análisis completo

Agentes específicos de Servicios Administrativos:
- Servicios Generales - Ayllin Mori
- AG0179 Luis Acosta

Archivos de entrada:
- ExportadosGenesysprueba/Detalle del rendimiento de colas.csv
- ExportadosGenesysprueba/Resumen de línea de tiempo de estado de agente.csv (vía timeline)

Archivo de salida:
- ExportadosGenerados/Analisis_Servicios_Por_intervalos.csv
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
import os

def obtener_agentes_servicios_conectados():
    """
    Obtiene el número de agentes de Servicios conectados por intervalo desde el análisis de timeline.
    Solo considera los 2 agentes específicos de Servicios Administrativos.
    """
    
    try:
        # print("🔗 Obteniendo datos de agentes de Servicios conectados...")
        
        # Agentes específicos de Servicios Administrativos
        agentes_servicios = [
            "Servicios Generales - Ayllin Mori",
            #"AG0179 Luis Acosta"
            "AG0181 - Soledad Garcia"
        ]
        
        # Leer datos de timeline
        df = pd.read_csv('ExportadosGenesysprueba/Resumen de línea de tiempo de estado de agente.csv', delimiter=';')
        # print(f"📊 Total registros timeline: {len(df)}")
        
        # Filtrar solo agentes de Servicios
        servicios_data = df[
            df['Nombre del agente'].apply(
                lambda x: any(agente in str(x) for agente in agentes_servicios)
            )
        ]
        # print(f"📋 Registros agentes Servicios: {len(servicios_data)}")
        
        if len(servicios_data) == 0:
            # print("⚠️ No se encontraron agentes de Servicios en timeline")
            return {}
        
        # Filtrar solo registros donde están "En la cola"
        en_cola_data = servicios_data[
            servicios_data['Estado principal'].str.contains('cola', case=False, na=False)
        ]
        # print(f"🎯 Registros 'En la cola' Servicios: {len(en_cola_data)}")
        
        if len(en_cola_data) == 0:
            # print("⚠️ No se encontraron agentes 'En la cola' para Servicios")
            return {}
        
        # Crear diccionario para contar agentes por intervalo
        intervalos_agentes = defaultdict(set)
        
        # Procesar cada registro
        for _, row in en_cola_data.iterrows():
            try:
                inicio_str = str(row['Hora de inicio'])
                fin_str = str(row['Hora de finalización'])
                agente = str(row['Nombre del agente'])
                
                # Verificar si tiene fin o no
                sin_fin = pd.isna(row['Hora de finalización']) or fin_str == 'nan' or fin_str.strip() == ''
                
                # Parsear fechas
                inicio_dt = pd.to_datetime(inicio_str, format='%d/%m/%y %H:%M:%S', dayfirst=True)
                
                if sin_fin:
                    # Para agentes sin fin, asumir que siguen hasta el final del día
                    fin_dt = inicio_dt.replace(hour=23, minute=59, second=59)
                else:
                    fin_dt = pd.to_datetime(fin_str, format='%d/%m/%y %H:%M:%S', dayfirst=True)
                
                # Generar intervalos de 30 minutos que cubra este período
                current_time = inicio_dt
                while current_time < fin_dt:
                    # Calcular inicio y fin del intervalo de 30 minutos
                    minuto = current_time.minute
                    if minuto < 30:
                        intervalo_inicio = current_time.replace(minute=0, second=0, microsecond=0)
                        intervalo_fin = current_time.replace(minute=30, second=0, microsecond=0)
                    else:
                        intervalo_inicio = current_time.replace(minute=30, second=0, microsecond=0)
                        intervalo_fin = (current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
                    
                    # Verificar si hay superposición con el período del agente
                    if inicio_dt < intervalo_fin and fin_dt > intervalo_inicio:
                        # Calcular tiempo efectivo en este intervalo
                        tiempo_inicio = max(inicio_dt, intervalo_inicio)
                        tiempo_fin_calc = min(fin_dt, intervalo_fin)
                        tiempo_en_intervalo = (tiempo_fin_calc - tiempo_inicio).total_seconds() / 60  # minutos
                        
                        # Solo contar si estuvo al menos 5 minutos en el intervalo
                        if tiempo_en_intervalo >= 5:
                            # Formato del intervalo
                            if intervalo_inicio.hour == 23 and intervalo_inicio.minute == 30:
                                intervalo_key = "23:30-00:00"
                            else:
                                hora_inicio = f"{intervalo_inicio.hour:02d}:{intervalo_inicio.minute:02d}"
                                hora_fin = f"{intervalo_fin.hour:02d}:{intervalo_fin.minute:02d}"
                                intervalo_key = f"{hora_inicio}-{hora_fin}"
                            
                            intervalos_agentes[intervalo_key].add(agente)
                    
                    # Avanzar al siguiente intervalo
                    current_time = intervalo_fin
                    
            except Exception as e:
                continue
        
        # Convertir a diccionario con conteos
        resultado = {}
        for intervalo in [f"{h:02d}:00-{h:02d}:30" for h in range(24)] + [f"{h:02d}:30-{h+1:02d}:00" for h in range(23)] + ["23:30-00:00"]:
            resultado[intervalo] = len(intervalos_agentes.get(intervalo, set()))
        
        # print(f"✅ Intervalos procesados para Servicios: {len([v for v in resultado.values() if v > 0])}")
        return resultado
        
    except Exception as e:
        # print(f"❌ Error obteniendo agentes Servicios: {e}")
        return {}

def procesar_archivo_servicios():
    """Función principal para procesar el archivo de Servicios Administrativos"""
    
    print("🎬 ANÁLISIS DE SERVICIOS ADMINISTRATIVOS")
    print("=" * 40)
    
    try:
        # Leer archivo
        archivo_servicios = 'ExportadosGenesysprueba/Detalle del rendimiento de colas.csv'
        # print(f"📁 Leyendo archivo: {archivo_servicios}")
        
        if not os.path.exists(archivo_servicios):
            print(f"❌ Error: No se encuentra el archivo {archivo_servicios}")
            return
        
        df = pd.read_csv(archivo_servicios, delimiter=';')
        print(f"✅ Registros cargados: {len(df)}")
        
        # Filtrar solo registros de Servicios Administrativos
        servicios_data = df[df['Nombre de cola'] == 'Srv_administrativos']
        print(f"🎯 Registros de Servicios: {len(servicios_data)}")
        
        if len(servicios_data) == 0:
            print("❌ Error: No se encontraron registros de Servicios Administrativos")
            return
        
        # Obtener datos de agentes conectados
        agentes_por_intervalo = obtener_agentes_servicios_conectados()
        
        # Procesar datos por intervalos
        resultados = []
        intervalos_procesados = 0
        
        for _, row in servicios_data.iterrows():
            try:
                # Extraer datos básicos - conversión segura
                inicio_intervalo = str(row['Inicio del intervalo'])
                
                # Convertir valores de manera segura
                oferta_raw = pd.to_numeric(row['Oferta'], errors='coerce')
                oferta = int(oferta_raw) if pd.notna(oferta_raw) else 0
                
                contestadas_raw = pd.to_numeric(row['Contestadas'], errors='coerce')
                contestadas = int(contestadas_raw) if pd.notna(contestadas_raw) else 0
                
                abandonadas_raw = pd.to_numeric(row['Abandonadas'], errors='coerce')
                abandonadas = int(abandonadas_raw) if pd.notna(abandonadas_raw) else 0
                
                cumplen_sla_raw = pd.to_numeric(row['Cumplen el SLA'], errors='coerce')
                cumplen_sla = int(cumplen_sla_raw) if pd.notna(cumplen_sla_raw) else 0
                
                manejo_total_raw = pd.to_numeric(row['Manejo total'], errors='coerce')
                manejo_total = float(manejo_total_raw) if pd.notna(manejo_total_raw) else 0.0
                
                # Crear intervalo en formato HH:MM-HH:MM
                try:
                    inicio_dt = pd.to_datetime(inicio_intervalo, format='%d/%m/%y %H:%M', dayfirst=True)
                    fin_dt = inicio_dt + timedelta(minutes=30)
                    
                    if inicio_dt.hour == 23 and inicio_dt.minute == 30:
                        intervalo_str = "23:30-00:00"
                    else:
                        intervalo_str = f"{inicio_dt.hour:02d}:{inicio_dt.minute:02d}-{fin_dt.hour:02d}:{fin_dt.minute:02d}"
                    
                    fecha_str = inicio_dt.strftime('%Y-%m-%d')
                except:
                    continue
                
                # Calcular métricas
                nivel_atencion = (contestadas / oferta * 100) if oferta > 0 else 0
                nivel_servicio = (cumplen_sla / oferta * 100) if oferta > 0 else 0
                tmo_segundos = (manejo_total / contestadas) if contestadas > 0 else 0
                tmo_minutos = tmo_segundos / 60
                
                # Formatear TMO como HH:MM:SS
                if tmo_segundos > 0:
                    horas = int(tmo_segundos // 3600)
                    minutos = int((tmo_segundos % 3600) // 60)
                    segundos = int(tmo_segundos % 60)
                    tmo_formato = f"{horas:02d}:{minutos:02d}:{segundos:02d}"
                else:
                    tmo_formato = "00:00:00"
                
                # Obtener agentes conectados
                agentes_conectados = agentes_por_intervalo.get(intervalo_str, 0)
                
                # Debug para algunos intervalos
                # print(f"  📞 Datos básicos: Oferta={oferta}, Contestadas={contestadas}, Abandonadas={abandonadas}, <=20s={cumplen_sla}")
                # print(f"  ⏱️ TMO: {tmo_minutos:.2f} min ({tmo_segundos:.1f}s) | Manejo medio: {tmo_segundos:.1f}s")
                
                # Crear registro resultado (sin conversiones adicionales, ya están convertidos)
                resultado = {
                    'Intervalo': intervalo_str,
                    'Fecha': fecha_str,
                    'Llamadas_Recibidas': oferta,
                    'Llamadas_Atendidas': contestadas,
                    'Llamadas_Abandonadas': abandonadas,
                    'Llamadas_Atendidas_20s': cumplen_sla,
                    'Nivel_Atencion': round(nivel_atencion, 2),
                    'Nivel_Servicio': round(nivel_servicio, 2),
                    'TMO': tmo_formato,
                    'Asesores_Conectados': agentes_conectados,
                    'Asesores_Requeridos': '',
                    'Llamadas_Atendidas_Por_Agente': '',
                    'Proyectado': '',
                    'Desviacion': ''
                }
                
                resultados.append(resultado)
                intervalos_procesados += 1
                
            except Exception as e:
                print(f"❌ Error procesando intervalo: {e}")
                continue
        
        # Crear DataFrame y guardar
        if resultados:
            # Crear carpeta si no existe
            if not os.path.exists('ExportadosGenerados'):
                os.makedirs('ExportadosGenerados')
            
            df_resultado = pd.DataFrame(resultados)
            archivo_salida = 'ExportadosGenerados/Analisis_Servicios_Por_intervalos.csv'
            df_resultado.to_csv(archivo_salida, index=False)
            
            print(f"📋 ARCHIVO GENERADO: {archivo_salida}")
            print(f"📊 Total intervalos: {len(resultados)}")
            
            # Calcular resumen simplificado
            total_llamadas = sum(r['Llamadas_Recibidas'] for r in resultados)
            print(f"✅ Total llamadas recibidas: {total_llamadas:,}")
            
        else:
            print("❌ No se generaron resultados")
            
    except Exception as e:
        print(f"❌ Error general: {e}")

if __name__ == "__main__":
    procesar_archivo_servicios()