# -*- coding: utf-8 -*-
"""
ANÁLISIS DE FRAUDE SALIDA - PROCESADOR DE LLAMADAS SALIENTES
============================================================
Procesa datos de la cola de Fraude Salida y genera análisis por intervalos de 30 minutos.
Enfocado en llamadas salientes y TMO.

Cola procesada: "Fraude Salida"
Archivo de entrada: ExportadosGenesysprueba/Detalle del rendimiento de colas.csv
Archivo de salida: Analisis_FraudeOut_Por_intervalos.csv
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
import sys

def main():
    try:
        print("📞 ANÁLISIS DE FRAUDE SALIDA - INICIANDO")
        print("=" * 50)
        
        # Leer archivo de datos
        fraude_salida_data = pd.read_csv('ExportadosGenesysprueba/Detalle del rendimiento de colas.csv', delimiter=';')
        print(f"📊 Total registros cargados: {len(fraude_salida_data)}")
        
        # Filtrar registros SOLO de Fraude Salida
        fraude_salida_filtrado = fraude_salida_data[
            fraude_salida_data['Nombre de cola'] == 'Fraude Salida'
        ]
        print(f"🎯 Registros filtrados para Fraude Salida: {len(fraude_salida_filtrado)}")
        
        if len(fraude_salida_filtrado) == 0:
            print("❌ No se encontraron registros de Fraude Salida")
            return
        
        # Procesar datos por intervalos - AGRUPAR por intervalo para evitar duplicados
        resultados = []
        intervalos_procesados = {}
        
        for _, row in fraude_salida_filtrado.iterrows():
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
                        'contactando_total': 0,  # Llamadas salientes
                        'manejo_total_seg': 0,
                        'llamadas_manejadas_total': 0,  # Para TMO exacto
                        'registros_con_manejo': 0
                    }
                
                # Obtener métricas específicas para llamadas salientes
                contactando = pd.to_numeric(row['Contactando'], errors='coerce')
                manejo_medio = pd.to_numeric(row['Manejo medio'], errors='coerce')
                manejo_total = pd.to_numeric(row['Manejo total'], errors='coerce')
                
                # Seguridad contra valores NaN y sumar
                intervalos_procesados[intervalo_key]['contactando_total'] += contactando if pd.notna(contactando) else 0
                
                # Para TMO, usar la metodología EXACTA: suma de manejo total / suma de llamadas manejadas
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
            
            llamadas_salientes = datos['contactando_total']
            
            # TMO usando metodología EXACTA: suma total / suma llamadas manejadas
            if datos['llamadas_manejadas_total'] > 0:
                tmo_segundos = datos['manejo_total_seg'] / datos['llamadas_manejadas_total']
            else:
                tmo_segundos = 0
            
            # Crear registro del resultado
            resultado = {
                'Intervalo': f"{inicio.strftime('%H:%M')}-{fin.strftime('%H:%M')}",
                'Fecha': inicio.strftime('%Y-%m-%d'),
                'Llamadas_Salientes': int(llamadas_salientes),
                'TMO': f"00:{int(tmo_segundos // 60):02d}:{int(tmo_segundos % 60):02d}" if tmo_segundos > 0 else "00:00:00"
            }
            
            resultados.append(resultado)
        
        if not resultados:
            print("❌ No se generaron resultados válidos")
            return
        
        # Crear DataFrame y guardarlo
        df_resultado = pd.DataFrame(resultados)
        
        # Ordenar por intervalo para mantener orden cronológico
        df_resultado['Hora_Inicio'] = pd.to_datetime(df_resultado['Intervalo'].str.split('-').str[0], format='%H:%M').dt.time
        df_resultado = df_resultado.sort_values('Hora_Inicio')
        df_resultado = df_resultado.drop('Hora_Inicio', axis=1)
        
        df_resultado.to_csv('ExportadosGenerados/Analisis_FraudeOut_Por_intervalos.csv', index=False)
        print(f"📋 ARCHIVO GENERADO: ExportadosGenerados/Analisis_FraudeOut_Por_intervalos.csv")
        print(f"📊 Total intervalos: {len(df_resultado)}")
        
        # Mostrar resumen
        total_llamadas_salientes = df_resultado['Llamadas_Salientes'].sum()
        print(f"📞 Total llamadas salientes: {total_llamadas_salientes}")
        
        print("✅ Análisis Fraude Salida - COMPLETADO EXITOSAMENTE")
        
    except Exception as e:
        print(f"❌ Error general: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()