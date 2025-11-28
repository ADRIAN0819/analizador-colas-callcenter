import pandas as pd
from datetime import datetime, timedelta
import os
from collections import defaultdict

def procesar_registros_agente_mejorado(registros_agente):
    """
    Procesa todos los registros de un agente y resuelve conflictos sin fin
    Busca registros posteriores con fin para completar los registros sin fin
    LÓGICA EXACTA DE FRAUDE
    """
    registros_procesados = []
    
    for _, row in registros_agente.iterrows():
        hora_inicio_completa = str(row['Hora de inicio'])
        hora_fin_completa = str(row['Hora de finalización'])
        sin_fin = pd.isna(hora_fin_completa) or hora_fin_completa == 'nan' or str(hora_fin_completa).strip() == ''
        
        if sin_fin:
            # NUEVA LÓGICA: Buscar si hay un registro posterior con la misma hora de inicio
            # o un registro que pueda indicar el verdadero fin
            
            if ' ' in hora_inicio_completa:
                try:
                    hora_inicio_dt = datetime.strptime(hora_inicio_completa, '%d/%m/%y %H:%M:%S')
                    
                    # Buscar registros posteriores del mismo agente
                    registros_posteriores = registros_agente[registros_agente.index != row.name]
                    
                    fin_encontrado = None
                    for _, reg_post in registros_posteriores.iterrows():
                        hora_inicio_post = str(reg_post['Hora de inicio'])
                        hora_fin_post = str(reg_post['Hora de finalización'])
                        
                        # Verificar si es el mismo momento de inicio
                        if hora_inicio_post == hora_inicio_completa:
                            sin_fin_post = pd.isna(hora_fin_post) or hora_fin_post == 'nan' or str(hora_fin_post).strip() == ''
                            if not sin_fin_post:
                                fin_encontrado = hora_fin_post
                                break
                        
                        # O si es un registro que inicia justo después
                        elif hora_inicio_post != hora_inicio_completa and not pd.isna(hora_inicio_post):
                            try:
                                hora_inicio_post_dt = datetime.strptime(hora_inicio_post, '%d/%m/%y %H:%M:%S')
                                # Si el siguiente registro inicia poco después, podría ser el fin del anterior
                                diff_minutos = (hora_inicio_post_dt - hora_inicio_dt).total_seconds() / 60
                                if 1 <= diff_minutos <= 180:  # Entre 1 minuto y 3 horas
                                    fin_encontrado = hora_inicio_post
                                    break
                            except:
                                continue
                    
                    # Usar el fin encontrado o mantener sin fin
                    registro_procesado = {
                        'inicio': hora_inicio_completa,
                        'fin': fin_encontrado if fin_encontrado else None,
                        'sin_fin_resuelto': fin_encontrado is not None
                    }
                    
                except:
                    registro_procesado = {
                        'inicio': hora_inicio_completa,
                        'fin': None,
                        'sin_fin_resuelto': False
                    }
        else:
            # Registro con fin normal
            registro_procesado = {
                'inicio': hora_inicio_completa,
                'fin': hora_fin_completa,
                'sin_fin_resuelto': True
            }
        
        registros_procesados.append(registro_procesado)
    
    return registros_procesados

def detectar_fecha_automatica_mda():
    """
    Detecta automáticamente la fecha más relevante en los datos de MDA
    """
    try:
        # Leer archivo de timeline para detectar fechas
        ruta_timeline = 'ExportadosGenesysprueba/Resumen de línea de tiempo de estado de agente.csv'
        
        if not os.path.exists(ruta_timeline):
            return None
        
        # Leer datos y buscar fechas
        df = pd.read_csv(ruta_timeline, encoding='utf-8', delimiter=';')
        
        # Buscar columnas con fechas/intervalos
        fecha_encontrada = None
        
        if 'Inicio del intervalo' in df.columns:
            # Extraer fecha del primer registro no nulo
            for _, row in df.iterrows():
                inicio_interval = str(row['Inicio del intervalo'])
                if inicio_interval and inicio_interval != 'nan':
                    try:
                        # Formato típico: "19/10/25 00:00"
                        fecha_parte = inicio_interval.split(' ')[0]
                        fecha_dt = datetime.strptime(fecha_parte, '%d/%m/%y')
                        fecha_encontrada = fecha_dt.strftime('%d/%m/%Y')
                        break
                    except:
                        continue
        
        if not fecha_encontrada and 'Hora de inicio' in df.columns:
            # Buscar en timeline de agentes
            for _, row in df.iterrows():
                hora_inicio = str(row['Hora de inicio'])
                if hora_inicio and hora_inicio != 'nan':
                    try:
                        # Formato típico: "19/10/25 07:54:42"
                        fecha_parte = hora_inicio.split(' ')[0]
                        fecha_dt = datetime.strptime(fecha_parte, '%d/%m/%y')
                        fecha_encontrada = fecha_dt.strftime('%d/%m/%Y')
                        break
                    except:
                        continue
        
        return fecha_encontrada
        
    except Exception as e:
        return None

def analizar_linea_tiempo_MA_corregido(fecha_objetivo=None):
    """
    Análisis de timeline para agentes de Mesa de Ayuda con LÓGICA EXACTA DE FRAUDE
    Auto-detecta la fecha si no se especifica.
    
    Args:
        fecha_objetivo: Fecha en formato 'dd/mm/yyyy' para generar la tabla.
                       Si es None, se detecta automáticamente.
    
    Returns:
        dict: Diccionario con intervalos como keys y cantidad de agentes como values
    """
    
    # Agentes a excluir específicos de MDA (redes, servicios, central)
    agentes_excluidos = [
        "A365_0301_Carlos_Tume", 
        "A365_0302_Victoria_Vargas",
        "A365_0304_Yasmin_Sanchez",
        "A365_0314_Jorge_Dominguez",
        "A365_0316_Anthony_Zurita",
        "A365_0317_Milagros_Reyes",
        "A365_0318_Bryan_Ramos",
        "A365_0311_Adrian_Calderon",
        "A365_0315_Roberto_Ojeda",
        "A365_0319_Enzo_Chavez",
        'AG0188 ELIZABETH RADA',
        'AG0185 GEOVANNA CHU',
        'AG0184 KARINA SOLSOL',
        'AG0181 - Soledad Garcia',
        "A365 0303 Sandro Zapata", 
        "Servicios Generales - Ayllin Mori",
        "AG0179 Luis Acosta"
    ]
    
    try:
        # Auto-detectar fecha si no se especifica
        if fecha_objetivo is None:
            fecha_objetivo = detectar_fecha_automatica_mda()
            
            if fecha_objetivo is None:
                # Fallback usando lógica original
                df_temp = pd.read_csv('ExportadosGenesysprueba/Resumen de línea de tiempo de estado de agente.csv', delimiter=';')
                fecha_mas_comun = df_temp['Inicio del intervalo'].mode().iloc[0] if len(df_temp) > 0 else "13/10/25 00:00"
                fecha_parte = fecha_mas_comun.split(' ')[0]
                fecha_dt = datetime.strptime(fecha_parte, '%d/%m/%y')
                fecha_objetivo = fecha_dt.strftime('%d/%m/%Y')
        
        # Convertir fecha objetivo
        fecha_obj = datetime.strptime(fecha_objetivo, '%d/%m/%Y')
        
        # Leer datos
        df = pd.read_csv('ExportadosGenesysprueba/Resumen de línea de tiempo de estado de agente.csv', delimiter=';')
        
        # Filtrar por Supervisor_MA y estado "En la cola" (case insensitive)
        if 'Nombre de la división' in df.columns:
            df_filtrado = df[
                (df['Nombre de la división'].str.contains('supervisor_ma', case=False, na=False)) & 
                (df['Estado principal'] == 'En la cola')
            ].copy()
        else:
            return {}
        
        if len(df_filtrado) == 0:
            return {}
        
        # EXCLUSIÓN ESPECÍFICA DE MDA: Aplicar filtros de agentes excluidos
        df_filtrado = df_filtrado[
            ~df_filtrado['Nombre del agente'].apply(
                lambda x: any(excluido in str(x) for excluido in agentes_excluidos)
            )
        ]
        
        if len(df_filtrado) == 0:
            return {}
        
        # Crear intervalos de 30 minutos (formato compatible con código existente)
        intervalos_30min = []
        for h in range(24):
            intervalos_30min.append(f"{h:02d}:00-{h:02d}:30")
            if h == 23:
                intervalos_30min.append("23:30-00:00")
            else:
                intervalos_30min.append(f"{h:02d}:30-{h+1:02d}:00")
        
        # Inicializar contadores
        agentes_en_cola_por_intervalo = {intervalo: set() for intervalo in intervalos_30min}
        
        # Generar intervalos dinámicos para cálculos (lógica mejorada)
        intervalos_calculo = []
        hora_actual = fecha_obj.replace(hour=0, minute=0, second=0, microsecond=0)
        
        while hora_actual.date() == fecha_obj.date():
            hora_fin = hora_actual + timedelta(minutes=30)
            intervalo_key = f"{hora_actual.strftime('%H:%M')}-{hora_fin.strftime('%H:%M')}"
            # Convertir al formato sin espacios esperado
            if intervalo_key == "00:00-00:30":
                intervalo_compatibilidad = "00:00-00:30"
            elif intervalo_key == "23:30-00:00":
                intervalo_compatibilidad = "23:30-00:00"
            else:
                h = hora_actual.hour
                if hora_actual.minute == 0:
                    intervalo_compatibilidad = f"{h:02d}:00-{h:02d}:30"
                else:
                    next_h = h + 1 if h < 23 else 0
                    if next_h == 0:
                        intervalo_compatibilidad = "23:30-00:00"
                    else:
                        intervalo_compatibilidad = f"{h:02d}:30-{next_h:02d}:00"
            
            intervalos_calculo.append({
                'intervalo_key': intervalo_compatibilidad,
                'inicio': hora_actual,
                'fin': hora_fin
            })
            hora_actual = hora_fin
        
        # USAR LÓGICA MEJORADA para resolver duplicados de agentes (EXACTA DE FRAUDE)
        df_procesado_lista = []
        agentes_unicos = df_filtrado['Nombre del agente'].unique()
        
        for agente in agentes_unicos:
            registros_agente = df_filtrado[df_filtrado['Nombre del agente'] == agente]
            registros_mejorados = procesar_registros_agente_mejorado(registros_agente)
            
            # Convertir de vuelta a DataFrame
            for reg in registros_mejorados:
                fila = registros_agente.iloc[0].copy()  # Usar primera fila como plantilla
                fila['Hora de inicio'] = reg['inicio']
                fila['Hora de finalización'] = reg['fin']
                df_procesado_lista.append(fila)
        
        # Crear DataFrame procesado
        if df_procesado_lista:
            df_procesado = pd.DataFrame(df_procesado_lista)
        else:
            df_procesado = pd.DataFrame()
        
        # Procesar cada registro mejorado (LÓGICA EXACTA DE FRAUDE)
        for _, registro in df_procesado.iterrows():
            try:
                # Verificar si el registro tiene valores válidos
                hora_inicio_completa = registro['Hora de inicio']
                hora_fin_completa = registro['Hora de finalización']
                agente = registro['Nombre del agente']
                
                # Manejo mejorado de registros nulos
                if pd.isna(hora_inicio_completa):
                    continue
                
                # Manejo de registros sin fin (característica del script original)
                if pd.isna(hora_fin_completa):
                    # Estimar hora de fin basada en el final del día
                    hora_fin_completa = f"{fecha_obj.strftime('%d/%m/%y')} 23:59:59"
                
                # Extraer fecha y hora del formato completo (dd/mm/yy HH:MM:SS)
                inicio_turno = datetime.strptime(str(hora_inicio_completa), '%d/%m/%y %H:%M:%S')
                fin_turno = datetime.strptime(str(hora_fin_completa), '%d/%m/%y %H:%M:%S')
                
                # LÓGICA MEJORADA: Solo procesar turnos que realmente afecten la fecha objetivo
                procesar_turno = False
                
                # Caso 1: Turno completamente en la fecha objetivo
                if inicio_turno.date() == fecha_obj.date() and fin_turno.date() == fecha_obj.date():
                    procesar_turno = True
                    
                # Caso 2: Turno que empieza antes de la fecha objetivo pero termina en ella
                elif inicio_turno.date() < fecha_obj.date() and fin_turno.date() == fecha_obj.date():
                    procesar_turno = True
                    
                # Caso 3: Turno que empieza en la fecha objetivo pero termina después
                elif inicio_turno.date() == fecha_obj.date() and fin_turno.date() > fecha_obj.date():
                    procesar_turno = True
                
                if not procesar_turno:
                    continue
                
                # Evaluar cada intervalo de 30 minutos
                for intervalo_info in intervalos_calculo:
                    intervalo_inicio = intervalo_info['inicio']
                    intervalo_fin = intervalo_info['fin']
                    intervalo_key = intervalo_info['intervalo_key']
                    
                    # Calcular la superposición exacta en minutos
                    minutos_superposicion = 0
                    
                    # Para turnos del mismo día
                    if inicio_turno.date() == fin_turno.date() == fecha_obj.date():
                        if intervalo_inicio < fin_turno and intervalo_fin > inicio_turno:
                            # Calcular superposición exacta
                            inicio_superposicion = max(intervalo_inicio, inicio_turno)
                            fin_superposicion = min(intervalo_fin, fin_turno)
                            minutos_superposicion = (fin_superposicion - inicio_superposicion).total_seconds() / 60
                    
                    # Para turnos que cruzan medianoche hacia la fecha objetivo
                    elif inicio_turno.date() < fecha_obj.date() and fin_turno.date() == fecha_obj.date():
                        # Solo los intervalos desde medianoche hasta el fin del turno
                        if intervalo_fin <= fin_turno:
                            inicio_superposicion = max(intervalo_inicio, fecha_obj.replace(hour=0, minute=0))
                            fin_superposicion = min(intervalo_fin, fin_turno)
                            minutos_superposicion = (fin_superposicion - inicio_superposicion).total_seconds() / 60
                    
                    # Para turnos que cruzan desde la fecha objetivo hacia el siguiente día
                    elif inicio_turno.date() == fecha_obj.date() and fin_turno.date() > fecha_obj.date():
                        # Solo los intervalos desde el inicio hasta medianoche
                        if intervalo_inicio >= inicio_turno:
                            inicio_superposicion = max(intervalo_inicio, inicio_turno)
                            fin_superposicion = min(intervalo_fin, fecha_obj.replace(hour=23, minute=59, second=59))
                            minutos_superposicion = (fin_superposicion - inicio_superposicion).total_seconds() / 60
                    
                    # REGLA: Solo agregar si estuvo conectado al menos 5 minutos
                    if minutos_superposicion >= 5:
                        agentes_en_cola_por_intervalo[intervalo_key].add(agente)
                        
            except Exception as e:
                continue
        
        # Convertir sets a conteos (formato compatible)
        resultado = {intervalo: len(agentes) for intervalo, agentes in agentes_en_cola_por_intervalo.items()}
        
        # Mostrar resultados ordenados como en Fraude
        print(f"\n🕐 AGENTES EN COLA POR INTERVALOS DE 30 MINUTOS (MDA - {fecha_objetivo}):")
        print("=" * 80)
        
        # Mostrar solo intervalos con agentes
        intervalos_con_agentes = {k: v for k, v in resultado.items() if v > 0}
        if intervalos_con_agentes:
            for intervalo, count in sorted(intervalos_con_agentes.items()):
                print(f"🕐 {intervalo}: {count:2d} agentes en cola")
        else:
            print("No se encontraron agentes en ningún intervalo")
        
        # Análisis específico de madrugada
        agentes_madrugada = {k: v for k, v in intervalos_con_agentes.items() if k.startswith(('00:', '01:', '02:', '03:', '04:', '05:'))}
        if agentes_madrugada:
            print(f"\n🌙 ANÁLISIS ESPECÍFICO DE MADRUGADA:")
            print("=" * 50)
            for intervalo, count in sorted(agentes_madrugada.items()):
                print(f"🌙 {intervalo}: {count} agentes")
        
        # Resumen
        print(f"\n📈 RESUMEN:")
        print("=" * 50)
        print(f"📊 Intervalos con actividad: {len(intervalos_con_agentes)}")
        if intervalos_con_agentes:
            max_agentes = max(intervalos_con_agentes.values())
            max_intervalo = next(k for k, v in intervalos_con_agentes.items() if v == max_agentes)
            print(f"🔥 Pico máximo: {max_agentes} agentes en {max_intervalo}")
        
        return resultado
        
    except Exception as e:
        print(f"❌ ERROR en análisis MDA: {e}")
        return {}

def ejecutar_testing():
    """
    Función para ejecutar el testing manual del análisis MDA
    """
    resultado = analizar_linea_tiempo_MA_corregido()
    
    if not resultado:
        print("❌ No se obtuvieron resultados del análisis")
    else:
        print(f"✅ Análisis completado. Diccionario devuelto con {len(resultado)} intervalos.")

if __name__ == "__main__":
    # Solo ejecutar testing si se corre directamente
    ejecutar_testing()