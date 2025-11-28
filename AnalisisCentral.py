#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Análisis de Central Telefónica por Intervalos
==============================================
Script para analizar el rendimiento de Central Telefónica con datos de agentes conectados.

Funcionalidades:
- Procesa datos del archivo 'Detalle del rendimiento de cola_Central.csv'
- Calcula métricas de llamadas por intervalos de 30 minutos
- Obtiene cantidad de agentes conectados desde timeline (4 agentes específicos)
- Genera CSV con análisis completo: Analisis_Central_Por_intervalos.csv

Agentes específicos de Central:
- AG0188 ELIZABETH RADA
- AG0185 GEOVANNA CHU  
- AG0184 KARINA SOLSOL
- AG0181 - Soledad Garcia

Autor: Sistema de Análisis de Call Center
Fecha: Octubre 2025
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from collections import defaultdict

def obtener_agentes_central_conectados():
    """
    Obtiene el número de agentes de Central conectados por intervalo desde el análisis de timeline.
    Solo considera los 4 agentes específicos de Central Telefónica.
    """
    try:
        # print("Obteniendo datos de agentes de Central conectados...")
        
        # Agentes específicos de Central Telefónica
        agentes_central = [
            'AG0188 ELIZABETH RADA',
            'AG0185 GEOVANNA CHU',
            'AG0184 KARINA SOLSOL',
            'AG0179 Luis Acosta',
            'AG0186 CLAUDIA ALCARAZO',
            #'AG0181 - Soledad Garcia'
        ]
        
        # Leer archivo de timeline
        df = pd.read_csv('ExportadosGenesysprueba/Resumen de línea de tiempo de estado de agente.csv', delimiter=';')
        # print(f"Total registros timeline: {len(df)}")
        
        # Filtrar solo agentes de Central
        central_data = df[
            df['Nombre del agente'].apply(
                lambda x: any(agente in str(x) for agente in agentes_central)
            )
        ]
        # print(f"Registros agentes Central: {len(central_data)}")
        
        if len(central_data) == 0:
            # print("No se encontraron agentes de Central en timeline")
            return {}
        
        # Filtrar solo registros donde están "En la cola"
        central_cola = central_data[
            central_data['Estado principal'].str.contains('cola', case=False, na=False)
        ]
        # print(f"Registros 'En la cola' Central: {len(central_cola)}")
        
        if len(central_cola) == 0:
            # print("No se encontraron agentes 'En la cola' para Central")
            return {}
        
        # Procesar intervalos
        intervalos_agentes = defaultdict(set)
        
        for _, row in central_cola.iterrows():
            try:
                # Procesar fechas y horas - usar las columnas correctas
                inicio_str = str(row['Hora de inicio'])
                fin_str = str(row['Hora de finalización'])
                agente = str(row['Nombre del agente'])
                
                # Verificar si tiene fin o no
                sin_fin = pd.isna(row['Hora de finalización']) or fin_str == 'nan' or fin_str.strip() == ''
                
                # Parsing de fechas
                inicio = pd.to_datetime(inicio_str, format='%d/%m/%y %H:%M:%S', dayfirst=True)
                
                if sin_fin:
                    # Para agentes sin fin, asumir que siguen hasta el final del día
                    fin = inicio.replace(hour=23, minute=59, second=59)
                else:
                    fin = pd.to_datetime(fin_str, format='%d/%m/%y %H:%M:%S', dayfirst=True)
                
                # print(f"Procesando {agente}: {inicio} - {'SIN FIN' if sin_fin else fin}")
                
                # Generar intervalos de 30 minutos que cubra este período
                current_time = inicio
                while current_time < fin:
                    # Calcular inicio y fin del intervalo de 30 minutos
                    minuto = current_time.minute
                    if minuto < 30:
                        intervalo_inicio = current_time.replace(minute=0, second=0, microsecond=0)
                        intervalo_fin = current_time.replace(minute=30, second=0, microsecond=0)
                    else:
                        intervalo_inicio = current_time.replace(minute=30, second=0, microsecond=0)
                        intervalo_fin = (current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
                    
                    # Verificar si hay superposición con el período del agente
                    if inicio < intervalo_fin and fin > intervalo_inicio:
                        # Calcular tiempo efectivo en este intervalo
                        tiempo_inicio = max(inicio, intervalo_inicio)
                        tiempo_fin_calc = min(fin, intervalo_fin)
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
                            # print(f"  ✅ Agregado a {intervalo_key}: {tiempo_en_intervalo:.1f} min")
                    
                    # Avanzar al siguiente intervalo
                    current_time = intervalo_fin
                    
            except Exception as e:
                # print(f"Error procesando registro: {e}")
                continue
        
        # Convertir a diccionario con conteos
        resultado = {}
        for intervalo, agentes in intervalos_agentes.items():
            resultado[intervalo] = len(agentes)
        
        # print(f"Intervalos procesados para Central: {len(resultado)}")
        return resultado
        
    except Exception as e:
        # print(f"Error obteniendo agentes Central: {e}")
        return {}

def procesar_archivo_central():
    """
    Procesa el archivo de rendimiento de Central Telefónica y genera el análisis por intervalos.
    """
    print("📞 ANÁLISIS DE CENTRAL TELEFÓNICA")
    print("=" * 40)
    
    # Verificar existencia del archivo
    archivo_central = 'ExportadosGenesysprueba/Detalle del rendimiento de colas.csv'
    
    try:
        # Leer archivo
        df = pd.read_csv(archivo_central, delimiter=';', encoding='utf-8')
        print(f"✅ Registros cargados: {len(df)}")
        
        # Filtrar solo datos de Central Telefónica
        central_data = df[df['Nombre de cola'] == 'Central Telefonica'].copy()
        print(f"🎯 Registros de Central: {len(central_data)}")
        
        if len(central_data) == 0:
            print("❌ No se encontraron datos de Central Telefónica")
            return
        
        # Obtener datos de agentes conectados
        agentes_por_intervalo = obtener_agentes_central_conectados()
        
        # Procesar datos por intervalos
        resultados = []
        
        for _, row in central_data.iterrows():
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
                
                # Obtener métricas básicas
                oferta = pd.to_numeric(row['Oferta'], errors='coerce')
                contestadas = pd.to_numeric(row['Contestadas'], errors='coerce')
                abandonadas = pd.to_numeric(row['Abandonadas'], errors='coerce')
                
                # Seguridad contra valores NaN
                oferta = oferta if pd.notna(oferta) else 0
                contestadas = contestadas if pd.notna(contestadas) else 0
                abandonadas = abandonadas if pd.notna(abandonadas) else 0
                
                # Calcular llamadas atendidas <20 segundos usando "Cumplen el SLA"
                cumplen_sla = pd.to_numeric(row['Cumplen el SLA'], errors='coerce')
                llamadas_20s = cumplen_sla if pd.notna(cumplen_sla) else 0
                
                # print(f"  Datos básicos: Oferta={oferta}, Contestadas={contestadas}, Abandonadas={abandonadas}, <=20s={llamadas_20s}")
                
                # Calcular métricas
                nivel_atencion = (contestadas / oferta * 100) if oferta > 0 else 0
                nivel_servicio = (llamadas_20s / oferta * 100) if oferta > 0 else 0
                
                # TMO usando la misma metodología que Mesa de Ayuda: Manejo total / llamadas manejadas = Manejo medio
                manejo_medio = pd.to_numeric(row['Manejo medio'], errors='coerce')
                manejo_total = pd.to_numeric(row['Manejo total'], errors='coerce')
                
                # Seguridad contra valores NaN
                manejo_medio = manejo_medio if pd.notna(manejo_medio) else 0
                manejo_total = manejo_total if pd.notna(manejo_total) else 0
                
                # TMO = Manejo medio (igual fórmula que Mesa de Ayuda: manejo_total / llamadas_manejadas)
                tmo_segundos = manejo_medio
                tmo_minutos = tmo_segundos / 60 if tmo_segundos > 0 else 0
                
                # print(f"  ⏱TMO: {tmo_minutos:.2f} min ({tmo_segundos:.1f}s) | Manejo medio: {manejo_medio:.1f}s")
                
                # Agentes conectados (desde timeline)
                agentes_conectados = agentes_por_intervalo.get(intervalo_key, 0)
                
                # Llamadas atendidas por agente
                llamadas_por_agente = contestadas / agentes_conectados if agentes_conectados > 0 else 0
                
                # Crear registro del resultado
                resultado = {
                    'Intervalo': f"{inicio.strftime('%H:%M')}-{fin.strftime('%H:%M')}",
                    'Fecha': inicio.strftime('%Y-%m-%d'),
                    'Llamadas_Recibidas': int(oferta) if pd.notna(oferta) else 0,
                    'Llamadas_Atendidas': int(contestadas) if pd.notna(contestadas) else 0,
                    'Llamadas_Abandonadas': int(abandonadas) if pd.notna(abandonadas) else 0,
                    'Llamadas_Atendidas_20s': int(llamadas_20s) if pd.notna(llamadas_20s) else 0,
                    'Nivel_Atencion': round(nivel_atencion, 2) if pd.notna(nivel_atencion) else 0,
                    'Nivel_Servicio': round(nivel_servicio, 2) if pd.notna(nivel_servicio) else 0,
                    'TMO': f"00:{int(tmo_segundos // 60):02d}:{int(tmo_segundos % 60):02d}" if tmo_segundos > 0 else "00:00:00",
                    'Asesores_Conectados': int(agentes_conectados) if pd.notna(agentes_conectados) else 0,
                    'Asesores_Requeridos': '',  # Sin información como solicitado
                    'Llamadas_Atendidas_Por_Agente': round(llamadas_por_agente, 2) if pd.notna(llamadas_por_agente) else 0,
                    'Proyectado': '',  # Sin información como solicitado
                    'Desviacion': ''   # Sin información como solicitado
                }
                
                resultados.append(resultado)
                
            except Exception as e:
                # print(f"Error procesando registro: {e}")
                continue
        
        # Crear DataFrame con resultados
        df_resultado = pd.DataFrame(resultados)
        
        if len(df_resultado) == 0:
            print("❌ No se generaron resultados válidos")
            return
        
        # Ordenar por intervalo
        df_resultado = df_resultado.sort_values(['Fecha', 'Intervalo'])
        
        # Generar archivo CSV
        archivo_salida = 'ExportadosGenerados/Analisis_Central_Por_intervalos.csv'
        df_resultado.to_csv(archivo_salida, index=False, encoding='utf-8')
        
        print(f"📋 ARCHIVO GENERADO: {archivo_salida}")
        print(f"📊 Total intervalos: {len(df_resultado)}")
        
        # Mostrar resumen
        # print("\nRESUMEN:")
        total_recibidas = df_resultado['Llamadas_Recibidas'].sum()
        total_atendidas = df_resultado['Llamadas_Atendidas'].sum()
        total_abandonadas = df_resultado['Llamadas_Abandonadas'].sum()
        tmo_promedio = df_resultado[df_resultado['TMO'] != '00:00:00']['TMO'].apply(lambda x: sum(int(t) * 60**i for i, t in enumerate(reversed(x.split(':')))) / 60).mean()
        nivel_atencion_promedio = df_resultado[df_resultado['Nivel_Atencion'] > 0]['Nivel_Atencion'].mean()
        
        print(f"Total llamadas recibidas: {total_recibidas:,}")
        # print(f"Total llamadas atendidas: {total_atendidas:,}")
        # print(f"Total llamadas abandonadas: {total_abandonadas:,}")
        # print(f"TMO promedio: {tmo_promedio:.2f} minutos" if not np.isnan(tmo_promedio) else "⏱️ TMO promedio: No disponible")
        # print(f"Nivel de atención promedio: {nivel_atencion_promedio:.2f}%" if not np.isnan(nivel_atencion_promedio) else "📊 Nivel de atención: No disponible")
        
        # Mostrar información de agentes
        agentes_promedio = df_resultado[df_resultado['Asesores_Conectados'] > 0]['Asesores_Conectados'].mean()
        # print(f"Agentes promedio conectados: {agentes_promedio:.1f}" if not np.isnan(agentes_promedio) else "👥 Agentes: No detectados en timeline")
        
        # Mostrar picos
        # if len(df_resultado) > 0:
        #     max_llamadas_idx = df_resultado['Llamadas_Recibidas'].idxmax()
        #     max_agentes_idx = df_resultado['Asesores_Conectados'].idxmax()
        #     
        #     if pd.notna(max_llamadas_idx):
        #         max_llamadas_intervalo = df_resultado.loc[max_llamadas_idx, 'Intervalo']
        #         max_llamadas_cantidad = df_resultado.loc[max_llamadas_idx, 'Llamadas_Recibidas']
        #         print(f"Más llamadas: {max_llamadas_intervalo} ({max_llamadas_cantidad} llamadas)")
        #     
        #     if pd.notna(max_agentes_idx) and df_resultado.loc[max_agentes_idx, 'Asesores_Conectados'] > 0:
        #         max_agentes_intervalo = df_resultado.loc[max_agentes_idx, 'Intervalo']
        #         max_agentes_cantidad = df_resultado.loc[max_agentes_idx, 'Asesores_Conectados']
        #         print(f" Más agentes: {max_agentes_intervalo} ({max_agentes_cantidad} agentes)")
        
    except Exception as e:
        print(f"❌ Error procesando archivo: {e}")
        return

if __name__ == "__main__":
    procesar_archivo_central()