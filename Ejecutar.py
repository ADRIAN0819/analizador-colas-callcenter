# -*- coding: utf-8 -*-

"""
EJECUTOR PRINCIPAL - ANÁLISIS COMPLETO DE COLAS
=============================================
Este script ejecuta los análisis principales de colas:
- Mesa de Ayuda (MDA)
- Central Telefónica  
- Fraude
- Fraude Salida
- Servicios Administrativos
- Redes Sociales

Genera automáticamente los 6 archivos CSV de exportado en la carpeta ExportadosGenerados.

Archivos de entrada (ExportadosGenesysprueba):
- Detalle del rendimiento de colas.csv
- Resumen de línea de tiempo de estado de agente.csv

Archivos de salida (ExportadosGenerados):
- Analisis_Mesa_Ayuda_Por_Intervalos.csv
- Analisis_Central_Por_intervalos.csv  
- Analisis_Fraude_Por_intervalos.csv
- Analisis_FraudeOut_Por_intervalos.csv
- Analisis_Servicios_Por_intervalos.csv
- Analisis_Redes_Por_intervalos.csv
"""

import subprocess
import sys
import os
from datetime import datetime

def verificar_archivos_entrada():
    """Verifica que existan los archivos de entrada necesarios"""
    print("🔍 VERIFICANDO ARCHIVOS DE ENTRADA")
    print("=" * 50)
    
    archivos_necesarios = [
        "ExportadosGenesysprueba/Detalle del rendimiento de colas.csv",
        "ExportadosGenesysprueba/Resumen de línea de tiempo de estado de agente.csv"
    ]
    
    todos_existen = True
    for archivo in archivos_necesarios:
        if os.path.exists(archivo):
            print(f"✅ {archivo}")
        else:
            print(f"❌ {archivo} - NO ENCONTRADO")
            todos_existen = False
    
    if not todos_existen:
        print("\n⚠️ ERROR: Faltan archivos de entrada necesarios")
        return False
    
    print("✅ Todos los archivos de entrada están disponibles\n")
    return True

def crear_carpeta_salida():
    """Crea la carpeta de salida si no existe"""
    if not os.path.exists("ExportadosGenerados"):
        os.makedirs("ExportadosGenerados")
        print("📁 Carpeta ExportadosGenerados creada")

def ejecutar_script(nombre_script, descripcion):
    """Ejecuta un script de Python y maneja errores"""
    print(f"🚀 EJECUTANDO: {descripcion}")
    print("=" * 60)
    
    try:
        # Obtener la ruta del ejecutable de Python
        python_executable = sys.executable
        
        # Ejecutar el script sin capturar la salida para evitar problemas de codificación
        resultado = subprocess.run(
            [python_executable, nombre_script],
            check=False
        )
        
        if resultado.returncode == 0:
            print(f"✅ {descripcion} - COMPLETADO EXITOSAMENTE")
            print()
            return True
        else:
            print(f"❌ {descripcion} - ERROR (código: {resultado.returncode})")
            print()
            return False
            
    except Exception as e:
        print(f"❌ Error ejecutando {nombre_script}: {e}")
        print()
        return False

def verificar_archivos_salida():
    """Verifica que se hayan generado todos los archivos de salida"""
    print("🔍 VERIFICANDO ARCHIVOS GENERADOS")
    print("=" * 50)
    archivos_esperados = [
        ("ExportadosGenerados/Analisis_Mesa_Ayuda_Por_Intervalos.csv", "Mesa de Ayuda"),
        ("ExportadosGenerados/Analisis_Central_Por_intervalos.csv", "Central Telefónica"),
        ("ExportadosGenerados/Analisis_Fraude_Por_intervalos.csv", "Fraude"),
        ("ExportadosGenerados/Analisis_FraudeOut_Por_intervalos.csv", "Fraude Salida"),
        ("ExportadosGenerados/Analisis_Servicios_Por_intervalos.csv", "Servicios Administrativos"),
        ("ExportadosGenerados/Analisis_Redes_Por_intervalos.csv", "Redes Sociales")
    ]
        
    todos_generados = True
    for archivo, descripcion in archivos_esperados:
        if os.path.exists(archivo):            # Obtener información del archivo
            tamano = os.path.getsize(archivo)
            modificacion = datetime.fromtimestamp(os.path.getmtime(archivo))
            print(f"✅ {descripcion}: {os.path.basename(archivo)}")
            print(f"   📊 Tamaño: {tamano:,} bytes")
            print(f"   🕒 Modificado: {modificacion.strftime('%H:%M:%S')}")
        else:
            print(f"❌ {descripcion}: {os.path.basename(archivo)} - NO GENERADO")
            todos_generados = False
    
    return todos_generados

def main():
    """Función principal que ejecuta todos los análisis"""
    print("🚀 EJECUTOR PRINCIPAL DE ANÁLISIS DE COLAS")
    print("=" * 60)
    print(f"⏰ Inicio: {datetime.now().strftime('%H:%M:%S')}")
    print()
    
    # Verificar archivos de entrada
    if not verificar_archivos_entrada():
        print("🛑 Ejecución cancelada debido a archivos faltantes")
        sys.exit(1)
    
    # Crear carpeta de salida
    crear_carpeta_salida()
    
    # Lista de scripts a ejecutar
    scripts = [
        ("AnalisisMDA.py", "Análisis Mesa de Ayuda"),
        ("AnalisisCentral.py", "Análisis Central Telefónica"),
        ("AnalisisFraude.py", "Análisis Fraude"),
        ("AnalisisFraudeSalida.py", "Análisis Fraude Salida"),
        ("AnalisisServicios.py", "Análisis Servicios Administrativos"),
        ("AnalisisRedes.py", "Análisis Redes Sociales")
    ]
    
    # Ejecutar cada script
    exitosos = 0
    for script, descripcion in scripts:
        if os.path.exists(script):
            if ejecutar_script(script, descripcion):
                exitosos += 1
            else:
                print(f"⚠️ Error en {script}, continuando con el siguiente...")
        else:
            print(f"❌ Script no encontrado: {script}")
    
    print("📊 RESUMEN DE EJECUCIÓN")
    print("=" * 60)
    print(f"✅ Scripts ejecutados exitosamente: {exitosos}/{len(scripts)}")
    print(f"⏰ Finalización: {datetime.now().strftime('%H:%M:%S')}")
    print()
    
    # Verificar archivos generados
    if verificar_archivos_salida():
        print("🎉 TODOS LOS ANÁLISIS COMPLETADOS EXITOSAMENTE")
        print("📁 Archivos generados en: ExportadosGenerados/")
    else:
        print("⚠️ Algunos archivos no se generaron correctamente")
    
    print("=" * 60)

if __name__ == "__main__":
    main()