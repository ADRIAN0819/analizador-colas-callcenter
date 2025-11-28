# -*- coding: utf-8 -*-
"""
Análisis de Redes Sociales por Intervalos
==========================================
Script para analizar el rendimiento de Redes Sociales con datos de agentes conectados.

Funcionalidades:
- Procesa datos del archivo 'Detalle del rendimiento de colas.csv'
- Calcula métricas de interacciones por intervalos de 30 minutos
- Obtiene cantidad de agentes conectados desde timeline (4 agentes específicos de Redes)
- Genera CSV con análisis completo: Analisis_Redes_Por_intervalos.csv

Colas de Redes Sociales:
- rs_facebook
- rs_facebook_muro
- rs_instagram
- rs_instagram_muro
- rs_youtube

Agentes específicos de Redes Sociales:
- A365 0303 Sandro Zapata
- AG0006 XIOMARA ACUÑA
- A365_0301_Carlos_Tume
- A365_0304_Yasmin_Sanchez

Autor: Sistema de Análisis de Call Center
Fecha: Octubre 2025
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from collections import defaultdict

def obtener_agentes_redes_conectados():
    """
    Obtiene el número de agentes de Redes Sociales conectados por intervalo desde el análisis de timeline.
    Solo considera los 4 agentes específicos de Redes Sociales.
    """
    try:
        # print("Obteniendo datos de agentes de Redes Sociales conectados...")
        
        # Agentes específicos de Redes Sociales
        agentes_redes = [
            'A365 0303 Sandro Zapata',
            'AG0006 XIOMARA ACUÑA',
            'A365_0301_Carlos_Tume',
            'A365_0304_Yasmin_Sanchez',
            'AG0240 MARIA ALCANTARA',
            'A365_0307_Patricia_Chaupis',
            'A365_0308_Adriana_Donayre',
            'A365_0306_Alfredo_Quispe',
            'A365_0309_Allisson_Peña',
            'A365_0310_Jadira_Shareba'
        ]
        
        # Leer archivo de timeline
        df = pd.read_csv('ExportadosGenesysprueba/Resumen de línea de tiempo de estado de agente.csv', delimiter=';')
        # print(f"Total registros timeline: {len(df)}")
        
        # Filtrar solo agentes de Redes Sociales
        redes_data = df[
            df['Nombre del agente'].apply(
                lambda x: any(agente in str(x) for agente in agentes_redes)
            )
        ]
        # print(f"Registros agentes Redes: {len(redes_data)}")
        
        if len(redes_data) == 0:
            # print("No se encontraron agentes de Redes Sociales en timeline")
            return {}
        
        # Filtrar solo registros donde están "En la cola"
        redes_cola = redes_data[
            redes_data['Estado principal'].str.contains('cola', case=False, na=False)
        ]
        # print(f"Registros 'En la cola' Redes: {len(redes_cola)}")
        
        if len(redes_cola) == 0:
            # print("No se encontraron agentes 'En la cola' para Redes Sociales")
            return {}
        
        # Procesar intervalos usando la misma lógica que analisis_timeline_mda
        intervalos_agentes = defaultdict(set)
        
        # Detectar fecha automáticamente
        fecha_objetivo = None
        for _, row in redes_cola.iterrows():
            inicio_str = str(row['Hora de inicio'])
            if inicio_str and inicio_str != 'nan' and ' ' in inicio_str:
                try:
                    fecha_objetivo = datetime.strptime(inicio_str.split(' ')[0], '%d/%m/%y')
                    break
                except:
                    continue
        
        if fecha_objetivo is None:
            # print("No se pudo detectar fecha automática")
            return {}
        
        # Procesar registros por agente con lógica mejorada (igual que MDA)
        for agente in redes_cola['Nombre del agente'].unique():
            registros_agente = redes_cola[redes_cola['Nombre del agente'] == agente].copy()
            registros_procesados = procesar_registros_agente_mejorado(registros_agente.to_dict('records'))
            
            for registro in registros_procesados:
                try:
                    inicio_dt = datetime.strptime(registro['inicio'], '%d/%m/%y %H:%M:%S')
                    
                    if registro['fin']:
                        fin_dt = datetime.strptime(registro['fin'], '%d/%m/%y %H:%M:%S')
                    else:
                        # Sin fin, hasta final del día
                        fin_dt = fecha_objetivo.replace(hour=23, minute=59, second=59)
                    
                    # Solo procesar registros del día objetivo
                    if inicio_dt.date() != fecha_objetivo.date():
                        continue
                    
                    # Generar intervalos de 30 minutos para este registro
                    intervalos = []
                    base_time = fecha_objetivo.replace(hour=0, minute=0, second=0, microsecond=0)
                    
                    for i in range(48):  # 48 intervalos de 30 min
                        intervalo_inicio = base_time + timedelta(minutes=i*30)
                        intervalo_fin = intervalo_inicio + timedelta(minutes=30)
                        
                        # Crear clave del intervalo compatible
                        if intervalo_inicio.hour == 0 and intervalo_inicio.minute == 0:
                            intervalo_key = "00:00-00:30"
                        elif intervalo_inicio.hour == 23 and intervalo_inicio.minute == 30:
                            intervalo_key = "23:30-00:00"
                        else:
                            hora_inicio = intervalo_inicio.hour
                            if intervalo_inicio.minute == 0:
                                intervalo_key = f"{hora_inicio:02d}:00-{hora_inicio:02d}:30"
                            else:
                                siguiente_hora = hora_inicio + 1 if hora_inicio < 23 else 0
                                if siguiente_hora == 0:
                                    intervalo_key = "23:30-00:00"
                                else:
                                    intervalo_key = f"{hora_inicio:02d}:30-{siguiente_hora:02d}:00"
                        
                        intervalos.append({
                            'inicio': intervalo_inicio,
                            'fin': intervalo_fin,
                            'key': intervalo_key
                        })
                    
                    # Calcular overlap para cada intervalo
                    for intervalo in intervalos:
                        i_start = intervalo['inicio']
                        i_end = intervalo['fin']
                        key = intervalo['key']
                        
                        # Calcular superposición
                        if inicio_dt < i_end and fin_dt > i_start:
                            overlap_start = max(i_start, inicio_dt)
                            overlap_end = min(i_end, fin_dt)
                            overlap_minutes = (overlap_end - overlap_start).total_seconds() / 60
                            
                            # Solo contar si el overlap es >= 5 minutos
                            if overlap_minutes >= 5:
                                intervalos_agentes[key].add(agente)
                
                except Exception as e:
                    # print(f"Error procesando registro de {agente}: {e}")
                    continue
        
        # Convertir a diccionario con conteos
        resultado = {}
        for intervalo, agentes in intervalos_agentes.items():
            resultado[intervalo] = len(agentes)
        
        # print(f"Intervalos procesados para Redes Sociales: {len(resultado)}")
        return resultado
        
    except Exception as e:
        # print(f"Error obteniendo agentes Redes Sociales: {e}")
        return {}

def procesar_registros_agente_mejorado(registros_agente):
    """
    Procesa los registros de un agente específico, resolviendo casos de registros "sin fin".
    Adaptado de la lógica de analisis_timeline_mda.py
    """
    registros_procesados = []
    
    for registro in registros_agente:
        inicio = registro.get('Hora de inicio')
        fin = registro.get('Hora de finalización')
        
        # Verificar si el registro tiene fin válido
        sin_fin = pd.isna(fin) or str(fin).strip().lower() == 'nan' or str(fin).strip() == ''
        
        if sin_fin:
            # Buscar un registro posterior que pueda indicar el fin
            fin_encontrado = None
            
            # Buscar en otros registros del mismo agente
            for otro_registro in registros_agente:
                if otro_registro == registro:
                    continue
                
                otro_inicio = str(otro_registro.get('Hora de inicio', ''))
                otro_fin = str(otro_registro.get('Hora de finalización', ''))
                
                # Caso 1: Mismo inicio pero con fin válido
                if otro_inicio == str(inicio) and otro_fin and otro_fin.strip().lower() != 'nan':
                    fin_encontrado = otro_fin
                    break
                
                # Caso 2: Inicio posterior que podría indicar el fin del registro actual
                if otro_inicio and otro_inicio.strip().lower() != 'nan':
                    try:
                        inicio_actual = datetime.strptime(str(inicio), '%d/%m/%y %H:%M:%S')
                        inicio_posterior = datetime.strptime(otro_inicio, '%d/%m/%y %H:%M:%S')
                        
                        # Si el inicio posterior está entre 1 y 180 minutos después
                        diferencia_minutos = (inicio_posterior - inicio_actual).total_seconds() / 60
                        if 1 <= diferencia_minutos <= 180:
                            fin_encontrado = otro_inicio
                            break
                    except:
                        continue
            
            fin = fin_encontrado
        
        # Agregar registro procesado
        registros_procesados.append({
            'inicio': str(inicio),
            'fin': str(fin) if fin and str(fin).strip().lower() != 'nan' else None,
            'agente': registro.get('Nombre del agente', '')
        })
    
    return registros_procesados

def procesar_archivo_redes():
    """
    Procesa el archivo de rendimiento de Redes Sociales y genera el análisis por intervalos.
    """
    print("📱 ANÁLISIS DE REDES SOCIALES")
    print("=" * 40)
    
    # Verificar existencia del archivo
    archivo_redes = 'ExportadosGenesysprueba/Detalle del rendimiento de colas.csv'
    
    try:
        # Leer archivo
        df = pd.read_csv(archivo_redes, delimiter=';', encoding='utf-8')
        print(f"✅ Registros cargados: {len(df)}")
        
        # Colas de Redes Sociales a procesar
        colas_redes = ['rs_facebook', 'rs_facebook_muro', 'rs_instagram', 'rs_instagram_muro', 'rs_youtube']
        
        # Filtrar solo datos de Redes Sociales
        redes_data = df[df['Nombre de cola'].isin(colas_redes)].copy()
        print(f"🎯 Registros de Redes Sociales: {len(redes_data)}")
        
        if len(redes_data) == 0:
            print("❌ No se encontraron datos de Redes Sociales")
            return
        
        # Debug: mostrar distribución por cola
        print("Distribución por cola:")
        for cola in colas_redes:
            count = len(redes_data[redes_data['Nombre de cola'] == cola])
            print(f"  {cola}: {count} registros")
        
        # Obtener datos de agentes conectados
        agentes_por_intervalo = obtener_agentes_redes_conectados()
        
        # Agrupar datos por intervalo (sumar métricas de todas las colas)
        intervalos_agrupados = defaultdict(lambda: {
            'oferta': 0, 'contestadas': 0, 'abandonadas': 0, 
            'cumplen_sla': 0, 'manejo_medio': 0, 'manejo_total': 0,
            'inicio_intervalo': None, 'fin_intervalo': None
        })
        
        for _, row in redes_data.iterrows():
            try:
                # Extraer información del intervalo
                inicio_intervalo = str(row['Inicio del intervalo'])
                fin_intervalo = str(row['Fin del intervalo'])
                
                # Parsear fechas
                inicio = pd.to_datetime(inicio_intervalo, format='%d/%m/%y %H:%M', dayfirst=True)
                fin = pd.to_datetime(fin_intervalo, format='%d/%m/%y %H:%M', dayfirst=True)
                
                # Crear clave del intervalo
                if inicio.hour == 23 and inicio.minute == 30:
                    intervalo_key = "23:30-00:00"
                else:
                    intervalo_key = f"{inicio.hour:02d}:{inicio.minute:02d}-{fin.hour:02d}:{fin.minute:02d}"
                
                # Acumular métricas - corregir manejo de NaN
                oferta = pd.to_numeric(row['Oferta'], errors='coerce')
                oferta = oferta if pd.notna(oferta) else 0
                
                contestadas = pd.to_numeric(row['Contestadas'], errors='coerce')
                contestadas = contestadas if pd.notna(contestadas) else 0
                
                abandonadas = pd.to_numeric(row['Abandonadas'], errors='coerce')
                abandonadas = abandonadas if pd.notna(abandonadas) else 0
                
                cumplen_sla = pd.to_numeric(row['Cumplen el SLA'], errors='coerce')
                cumplen_sla = cumplen_sla if pd.notna(cumplen_sla) else 0
                
                manejo_medio = pd.to_numeric(row['Manejo medio'], errors='coerce')
                manejo_medio = manejo_medio if pd.notna(manejo_medio) else 0
                
                manejo_total = pd.to_numeric(row['Manejo total'], errors='coerce')
                manejo_total = manejo_total if pd.notna(manejo_total) else 0
                
                intervalos_agrupados[intervalo_key]['oferta'] += oferta
                intervalos_agrupados[intervalo_key]['contestadas'] += contestadas
                intervalos_agrupados[intervalo_key]['abandonadas'] += abandonadas
                intervalos_agrupados[intervalo_key]['cumplen_sla'] += cumplen_sla
                intervalos_agrupados[intervalo_key]['manejo_total'] += manejo_total
                
                # Para manejo_medio, usar promedio ponderado
                if contestadas > 0 and manejo_medio > 0:
                    intervalos_agrupados[intervalo_key]['manejo_medio'] += manejo_medio * contestadas
                
                # Debug removido - acumulación funcionando correctamente
                
                # Guardar tiempos del intervalo
                if intervalos_agrupados[intervalo_key]['inicio_intervalo'] is None:
                    intervalos_agrupados[intervalo_key]['inicio_intervalo'] = inicio
                    intervalos_agrupados[intervalo_key]['fin_intervalo'] = fin
                    
            except Exception as e:
                print(f"Error procesando registro: {e}")
                continue
        
        # Procesar resultados finales
        resultados = []
        
        print(f"Intervalos agrupados encontrados: {len(intervalos_agrupados)}")
        
        for intervalo_key, datos in intervalos_agrupados.items():
            try:
                inicio = datos['inicio_intervalo']
                fin = datos['fin_intervalo']
                
                oferta = datos['oferta']
                contestadas = datos['contestadas']
                abandonadas = datos['abandonadas']
                cumplen_sla = datos['cumplen_sla']
                manejo_total = datos['manejo_total']
                
                # Calcular TMO promedio ponderado
                if contestadas > 0:
                    tmo_segundos = datos['manejo_medio'] / contestadas
                else:
                    tmo_segundos = 0
                
                # Calcular métricas
                nivel_atencion = (contestadas / oferta * 100) if oferta > 0 else 0
                
                # Agentes conectados (desde timeline)
                agentes_conectados = agentes_por_intervalo.get(intervalo_key, 0)
                
                # Interacciones atendidas por agente (se deja vacío como solicitado)
                interacciones_por_agente = ""
                
                # Crear registro del resultado
                resultado = {
                    'Intervalo': f"{inicio.strftime('%H:%M')}-{fin.strftime('%H:%M')}",
                    'Fecha': inicio.strftime('%Y-%m-%d'),
                    'Interacciones_Recibidas': int(oferta) if pd.notna(oferta) else 0,
                    'Interacciones_Atendidas': int(contestadas) if pd.notna(contestadas) else 0,
                    'Interacciones_Abandonadas': int(abandonadas) if pd.notna(abandonadas) else 0,
                    'Nivel_Atencion': round(nivel_atencion, 2) if pd.notna(nivel_atencion) else 0,
                    'TMO': f"00:{int(tmo_segundos // 60):02d}:{int(tmo_segundos % 60):02d}" if tmo_segundos > 0 else "00:00:00",
                    'Asesores_Conectados': int(agentes_conectados) if pd.notna(agentes_conectados) else 0,
                    'Interacciones_Atendidas_Por_Agente': interacciones_por_agente
                }
                
                resultados.append(resultado)
                
            except Exception as e:
                print(f"Error procesando intervalo {intervalo_key}: {e}")
                continue
        
        # Crear DataFrame con resultados
        df_resultado = pd.DataFrame(resultados)
        
        if len(df_resultado) == 0:
            print("❌ No se generaron resultados válidos")
            return
        
        # Ordenar por intervalo
        df_resultado = df_resultado.sort_values(['Fecha', 'Intervalo'])
        
        # Generar archivo CSV
        archivo_salida = 'ExportadosGenerados/Analisis_Redes_Por_intervalos.csv'
        df_resultado.to_csv(archivo_salida, index=False, encoding='utf-8')
        
        print(f"📋 ARCHIVO GENERADO: {archivo_salida}")
        print(f"📊 Total intervalos: {len(df_resultado)}")
        
        # Mostrar resumen
        total_recibidas = df_resultado['Interacciones_Recibidas'].sum()
        total_atendidas = df_resultado['Interacciones_Atendidas'].sum()
        total_abandonadas = df_resultado['Interacciones_Abandonadas'].sum()
        
        print(f"Total interacciones recibidas: {total_recibidas:,}")
        # print(f"Total interacciones atendidas: {total_atendidas:,}")
        # print(f"Total interacciones abandonadas: {total_abandonadas:,}")
        
        # Mostrar información de agentes
        agentes_promedio = df_resultado[df_resultado['Asesores_Conectados'] > 0]['Asesores_Conectados'].mean()
        # print(f"Agentes promedio conectados: {agentes_promedio:.1f}" if not np.isnan(agentes_promedio) else "👥 Agentes: No detectados en timeline")
        
    except Exception as e:
        print(f"❌ Error procesando archivo: {e}")
        return

if __name__ == "__main__":
    procesar_archivo_redes()