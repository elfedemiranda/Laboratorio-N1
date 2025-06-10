from os import path
from domain.dataset_csv import DatasetCSV
from domain.dataset_excel import DatasetExcel
from data.data_saver import DataSaver

def main():
    """Función principal del programa de análisis de datos"""
    print("="*60)
    print("🚀 INICIANDO SISTEMA DE ANÁLISIS DE DATOS")
    print("="*60)
    
    # Configuración de rutas de archivos
    ruta_archivo_csv = path.join(path.dirname(__file__), "files/obj-del-gto-acum-1-al-12.csv")
    ruta_archivo_excel = path.join(path.dirname(__file__), "files/ejec-funcion-1.xlsx")
        
    print(f"📂 Archivos configurados:")
    print(f"  • CSV: {path.basename(ruta_archivo_csv)}")
    print(f"  • Excel: {path.basename(ruta_archivo_excel)}")
    print("-"*60)
    
    # ========================================
    # PROCESAMIENTO DE ARCHIVO CSV
    # ========================================
    print("\n📊 PROCESANDO DATASET CSV")
    print("-"*40)
    
    procesador_csv = DatasetCSV(ruta_archivo_csv)
    
    if procesador_csv.cargar_datos():
        print("🔍 Mostrando información del archivo CSV:")
        procesador_csv.mostrar_informacion_archivo()
        
        print("\n📈 Generando resumen estadístico del CSV:")
        procesador_csv.mostrar_resumen_estadistico()
        
        # Ejemplo de verificación de columnas específicas
        columnas_esperadas = ['fecha', 'monto', 'descripcion']  # Ajustar según tu CSV
        procesador_csv.verificar_columnas_requeridas(columnas_esperadas)
        
    else:
        print("❌ Error al procesar el archivo CSV")
    
    # ========================================
    # PROCESAMIENTO DE ARCHIVO EXCEL
    # ========================================
    print("\n📊 PROCESANDO DATASET EXCEL")
    print("-"*40)
    
    procesador_excel = DatasetExcel(ruta_archivo_excel)
    
    if procesador_excel.cargar_datos():
        print("🔍 Mostrando información del archivo Excel:")
        procesador_excel.mostrar_informacion_hojas()
        procesador_excel.obtener_informacion_basica()
        
        print("\n📈 Generando resumen estadístico del Excel:")
        procesador_excel.mostrar_resumen_estadistico()
        
    else:
        print("❌ Error al procesar el archivo Excel")
    
    # ========================================
    # GUARDADO DE DATOS PROCESADOS
    # ========================================
    print("\n💾 GUARDANDO DATOS PROCESADOS")
    print("-"*40)
    
    gestor_guardado = DataSaver()
    
    # Guardar datasets procesados si están disponibles
    datasets_a_guardar = [
        (procesador_csv, "datos-csv-procesados"),
        (procesador_excel, "datos-excel-procesados"),
    ]
    
    for dataset, nombre_archivo in datasets_a_guardar:
        if dataset.esta_cargado:
            print(f"💾 Guardando: {nombre_archivo}")
            
            # Guardar en múltiples formatos
            gestor_guardado.guardar_dataframe(dataset.dataframe, f"{nombre_archivo}.csv")
            gestor_guardado.guardar_dataframe(dataset.dataframe, f"{nombre_archivo}.xlsx")
            
            # Crear reporte de resumen
            crear_reporte_dataset(dataset, nombre_archivo)
        else:
            print(f"⚠️  Saltando {nombre_archivo}: datos no disponibles")
    
    # ========================================
    # RESUMEN FINAL
    # ========================================
    mostrar_resumen_final(procesador_csv, procesador_excel)

def crear_reporte_dataset(dataset, nombre_dataset):
    """Crea un reporte detallado de un dataset procesado"""
    try:
        nombre_reporte = f"reporte_{nombre_dataset}.txt"
        
        with open(nombre_reporte, 'w', encoding='utf-8') as archivo:
            archivo.write("="*60 + "\n")
            archivo.write(f"REPORTE DE ANÁLISIS: {nombre_dataset.upper()}\n")
            archivo.write("="*60 + "\n\n")
            
            archivo.write(f"Archivo fuente: {dataset.ruta_archivo}\n")
            archivo.write(f"Fecha de procesamiento: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            if dataset.esta_cargado:
                df = dataset.dataframe
                archivo.write(f"Dimensiones: {df.shape[0]} filas x {df.shape[1]} columnas\n")
                archivo.write(f"Memoria utilizada: {df.memory_usage(deep=True).sum() / 1024:.2f} KB\n\n")
                
                archivo.write("COLUMNAS DEL DATASET:\n")
                archivo.write("-" * 30 + "\n")
                for i, columna in enumerate(df.columns, 1):
                    tipo_dato = str(df[columna].dtype)
                    valores_nulos = df[columna].isnull().sum()
                    archivo.write(f"{i:2d}. {columna} ({tipo_dato}) - Nulos: {valores_nulos}\n")
                
                archivo.write(f"\nTOTAL DE VALORES FALTANTES: {df.isnull().sum().sum()}\n")
                archivo.write(f"FILAS ÚNICAS: {len(df.drop_duplicates())}\n")
                
        print(f"📄 Reporte creado: {nombre_reporte}")
        
    except Exception as e:
        print(f"❌ Error al crear reporte: {str(e)}")

def mostrar_resumen_final(csv_dataset, excel_dataset, api_dataset):
    """Muestra un resumen final de todo el procesamiento"""
    print("\n" + "="*60)
    print("📋 RESUMEN FINAL DEL PROCESAMIENTO")
    print("="*60)
    
    datasets = [
        ("CSV", csv_dataset),
        ("Excel", excel_dataset), 
    ]
    
    total_registros = 0
    datasets_exitosos = 0
    
    for nombre, dataset in datasets:
        if dataset.esta_cargado:
            filas = len(dataset.dataframe)
            columnas = len(dataset.dataframe.columns)
            total_registros += filas
            datasets_exitosos += 1
            estado = "✅ EXITOSO"
            info = f"{filas:,} registros, {columnas} columnas"
        else:
            estado = "❌ FALLIDO"
            info = "Sin datos"
        
        print(f"📊 {nombre:<6} | {estado} | {info}")
    
    print("-"*60)
    print(f"🎯 Datasets procesados exitosamente: {datasets_exitosos}/3")
    print(f"📈 Total de registros procesados: {total_registros:,}")
    print(f"💾 Archivos de salida generados en directorio actual")
    print("="*60)
    
    if datasets_exitosos > 0:
        print("🎉 ¡Procesamiento completado con éxito!")
    else:
        print("⚠️  No se pudieron procesar los datos correctamente")

def verificar_archivos_entrada():
    """Verifica que los archivos de entrada existan"""
    archivos = [
        "files/obj-del-gto-acum-1-a1-12.csv",
        "files/ejec-funcion-1.xlsx"
    ]
    
    archivos_faltantes = []
    for archivo in archivos:
        ruta_completa = path.join(path.dirname(__file__), archivo)
        if not path.exists(ruta_completa):
            archivos_faltantes.append(archivo)
    
    if archivos_faltantes:
        print("⚠️  ADVERTENCIA: Archivos faltantes:")
        for archivo in archivos_faltantes:
            print(f"   • {archivo}")
        print("   Algunos procesos podrían fallar.\n")
        return False
    
    print("✅ Todos los archivos de entrada están disponibles\n")
    return True

if __name__ == "__main__":
    import pandas as pd
    
    # Verificar archivos antes de empezar
    verificar_archivos_entrada()
    
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Procesamiento interrumpido por el usuario")
    except Exception as e:
        print(f"\n❌ Error crítico en el programa principal: {str(e)}")
        print("💡 Revisa los archivos de entrada y configuración")
    finally:
        print("\n👋 Fin del programa")