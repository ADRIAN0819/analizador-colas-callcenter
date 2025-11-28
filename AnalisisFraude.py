#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ANÁLISIS DE FRAUDE - PROCESADOR DE INTERVALOS
===============================================
Procesa datos de la cola de Fraude y genera análisis por intervalos de 30 minutos.
Integra con timeline de agentes para contar asesores conectados.

Colas procesadas: "Fraude", "Fraude_MA"
Archivo de entrada: ExportadosGenesysprueba/Detalle del rendimiento de colas.csv
Archivo de salida: Analisis_Fraude_Por_intervalos.csv
Timeline: Resumen de línea de tiempo de estado de agente.csv
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
import sys

def obtener_agentes_fraude_conectados():
    """
    Obtiene el número de agentes de Fraude conectados por intervalo de 30 minutos
    usando la lógica corregida de Analisis_timeline_fraude.py
    """
    try:
        # Importar la función del script de timeline de Fraude
        from Analisis_timeline_fraude import analizar_linea_tiempo_fraude_corregido
        
        # Ejecutar el análisis y obtener los datos
        agentes_por_intervalo = analizar_linea_tiempo_fraude_corregido()
        
        return agentes_por_intervalo
        
    except Exception as e:
        print(f"Error obteniendo agentes Fraude: {e}")
        return {}

def obtener_agentes_fraude(inicio, fin):
    """
    Obtiene el número de agentes de Fraude conectados en un intervalo específico.
    """
    agentes_por_intervalo = obtener_agentes_fraude_conectados()
    
    # Crear clave del intervalo
    if inicio.hour == 23 and inicio.minute == 30:
        intervalo_key = "23:30-00:00"
    else:
        intervalo_key = f"{inicio.hour:02d}:{inicio.minute:02d}-{fin.hour:02d}:{fin.minute:02d}"
    
    return agentes_por_intervalo.get(intervalo_key, 0)

def main():
    try:
        print("🔍 ANÁLISIS DE FRAUDE - INICIANDO")
        print("=" * 50)
        
        # Leer archivo de datos de Fraude
        fraude_data = pd.read_csv('ExportadosGenesysprueba/Detalle del rendimiento de colas.csv', delimiter=';')
        print(f"📊 Total registros cargados: {len(fraude_data)}")
        
        # Filtrar registros SOLO de Fraude y Fraude_MA (no incluir combinaciones)
        fraude_filtrado = fraude_data[
            fraude_data['Nombre de cola'].isin(['Fraude', 'Fraude_MA'])
        ]
        print(f"🎯 Registros filtrados para Fraude: {len(fraude_filtrado)}")
        
        if len(fraude_filtrado) == 0:
            print("❌ No se encontraron registros de Fraude")
            return
        
        # Obtener datos de agentes conectados
        agentes_por_intervalo = obtener_agentes_fraude_conectados()
        
        # Procesar datos por intervalos - AGRUPAR por intervalo para evitar duplicados
        resultados = []
        intervalos_procesados = {}
        
        for _, row in fraude_filtrado.iterrows():
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
                
                # Si ya procesamos este intervalo, sumar los datos
                if intervalo_key not in intervalos_procesados:
                    intervalos_procesados[intervalo_key] = {
                        'inicio': inicio,
                        'fin': fin,
                        'oferta_total': 0,
                        'contestadas_total': 0,
                        'abandonadas_total': 0,
                        'retener_total': 0,
                        'llamadas_30s_total': 0,
                        'manejo_total_seg': 0,
                        'llamadas_manejadas_total': 0,  # Para TMO exacto
                        'registros_con_manejo': 0
                    }
                
                # Obtener métricas básicas y sumarlas
                oferta = pd.to_numeric(row['Oferta'], errors='coerce')
                contestadas = pd.to_numeric(row['Contestadas'], errors='coerce')
                abandonadas = pd.to_numeric(row['Abandonadas'], errors='coerce')
                retener = pd.to_numeric(row['Retener'], errors='coerce')
                cumplen_sla = pd.to_numeric(row['Cumplen el SLA'], errors='coerce')
                manejo_medio = pd.to_numeric(row['Manejo medio'], errors='coerce')
                manejo_total = pd.to_numeric(row['Manejo total'], errors='coerce')
                
                # Seguridad contra valores NaN y sumar
                intervalos_procesados[intervalo_key]['oferta_total'] += oferta if pd.notna(oferta) else 0
                intervalos_procesados[intervalo_key]['contestadas_total'] += contestadas if pd.notna(contestadas) else 0
                intervalos_procesados[intervalo_key]['abandonadas_total'] += abandonadas if pd.notna(abandonadas) else 0
                intervalos_procesados[intervalo_key]['retener_total'] += retener if pd.notna(retener) else 0
                intervalos_procesados[intervalo_key]['llamadas_30s_total'] += cumplen_sla if pd.notna(cumplen_sla) else 0
                
                # Para TMO, usar la metodología EXACTA de Mesa de Ayuda: suma de manejo total / suma de llamadas manejadas
                if pd.notna(manejo_medio) and manejo_medio > 0 and pd.notna(manejo_total) and manejo_total > 0:
                    llamadas_manejadas = manejo_total / manejo_medio
                    intervalos_procesados[intervalo_key]['manejo_total_seg'] += manejo_total
                    intervalos_procesados[intervalo_key]['llamadas_manejadas_total'] += llamadas_manejadas
                    intervalos_procesados[intervalo_key]['registros_con_manejo'] += 1
                    
            except Exception as e:
                continue
        
        # Generar resultados finales agrupados
        for intervalo_key, datos in intervalos_procesados.items():
            inicio = datos['inicio']
            fin = datos['fin']
            
            oferta = datos['oferta_total']
            contestadas = datos['contestadas_total']
            abandonadas = datos['abandonadas_total']
            retener = datos['retener_total']
            llamadas_30s = datos['llamadas_30s_total']
            
            
            # Calcular métricas
            nivel_atencion = (contestadas / oferta * 100) if oferta > 0 else 0
            nivel_servicio = (llamadas_30s / oferta * 100) if oferta > 0 else 0
            nivel_retencion = (retener / contestadas * 100) if contestadas > 0 else 0
            
            # TMO usando metodología EXACTA de Mesa de Ayuda: suma total / suma llamadas manejadas
            if datos['llamadas_manejadas_total'] > 0:
                tmo_segundos = datos['manejo_total_seg'] / datos['llamadas_manejadas_total']
            else:
                tmo_segundos = 0
            
            # Obtener agentes conectados desde timeline
            agentes_conectados = obtener_agentes_fraude(inicio, fin)
            
            # Crear registro del resultado
            resultado = {
                'Intervalo': f"{inicio.strftime('%H:%M')}-{fin.strftime('%H:%M')}",
                'Fecha': inicio.strftime('%Y-%m-%d'),
                'Llamadas_Recibidas': int(oferta),
                'Llamadas_Atendidas': int(contestadas),
                'Llamadas_Retenidas': int(retener),
                'Llamadas_Abandonadas': int(abandonadas),
                'Llamadas_Atendidas_30s': int(llamadas_30s),
                'Nivel_Atencion': round(nivel_atencion, 2),
                'Nivel_Servicio': round(nivel_servicio, 2),
                'Nivel_Retencion': round(nivel_retencion, 2),
                'TMO': f"00:{int(tmo_segundos // 60):02d}:{int(tmo_segundos % 60):02d}" if tmo_segundos > 0 else "00:00:00",
                'Asesores_Conectados': int(agentes_conectados)
            }
            
            resultados.append(resultado)
        
        if not resultados:
            print("❌ No se generaron resultados válidos")
            return
        
        # Crear DataFrame y guardarlo
        df_resultado = pd.DataFrame(resultados)
        df_resultado.to_csv('ExportadosGenerados/Analisis_Fraude_Por_intervalos.csv', index=False)
        print(f"✅ ARCHIVO GENERADO: ExportadosGenerados/Analisis_Fraude_Por_intervalos.csv")
        
    except Exception as e:
        print(f"❌ Error general: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()