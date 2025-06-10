from abc import ABC, abstractmethod
import pandas as pd

class Dataset(ABC):
    def __init__(self, ruta_archivo):
        self.__ruta_archivo = ruta_archivo
        self.__dataframe = None
        self.__esta_cargado = False

    # Propiedades para lectura
    @property
    def dataframe(self):
        """Retorna el DataFrame cargado"""
        return self.__dataframe

    @property
    def ruta_archivo(self):
        """Retorna la ruta del archivo fuente"""
        return self.__ruta_archivo
    
    @property
    def esta_cargado(self):
        """Indica si los datos han sido cargados exitosamente"""
        return self.__esta_cargado

    # Setter para el dataframe con validaciones básicas
    @dataframe.setter
    def dataframe(self, nuevo_dataframe):
        if nuevo_dataframe is not None and not isinstance(nuevo_dataframe, pd.DataFrame):
            raise TypeError("Los datos deben ser un DataFrame de pandas")
        self.__dataframe = nuevo_dataframe
        self.__esta_cargado = nuevo_dataframe is not None

    @abstractmethod
    def cargar_datos(self):
        """Método abstracto para cargar datos desde la fuente"""
        pass

    def validar_integridad_datos(self):
        """Valida la integridad y calidad de los datos cargados"""
        if not self.esta_cargado:
            raise ValueError("Error: No se han cargado datos para validar")
        
        problemas_encontrados = []
        
        # Verificar datos faltantes
        valores_nulos = self.dataframe.isnull().sum().sum()
        if valores_nulos > 0:
            mensaje = f"⚠️  Se detectaron {valores_nulos} valores faltantes en el dataset"
            print(mensaje)
            problemas_encontrados.append("valores_faltantes")

        # Verificar filas duplicadas
        filas_duplicadas = self.dataframe.duplicated().sum()
        if filas_duplicadas > 0:
            mensaje = f"⚠️  Se encontraron {filas_duplicadas} filas duplicadas"
            print(mensaje)
            problemas_encontrados.append("filas_duplicadas")
        
        # Verificar si el dataset está vacío
        if len(self.dataframe) == 0:
            mensaje = "❌ El dataset está vacío"
            print(mensaje)
            problemas_encontrados.append("dataset_vacio")
        
        if not problemas_encontrados:
            print("✅ Validación completada: Los datos se encuentran en buen estado")
        
        return len(problemas_encontrados) == 0

    def limpiar_y_transformar_datos(self):
        """Aplica transformaciones de limpieza a los datos"""
        if not self.esta_cargado:
            print("❌ No hay datos cargados para procesar")
            return False
        
        print("🔄 Iniciando proceso de limpieza y transformación...")
        
        # Normalizar nombres de columnas
        columnas_originales = len(self.dataframe.columns)
        self.__dataframe.columns = (self.dataframe.columns
                                   .str.lower()
                                   .str.replace(" ", "_")
                                   .str.replace("-", "_"))
        
        # Eliminar filas duplicadas
        filas_antes = len(self.dataframe)
        self.__dataframe = self.dataframe.drop_duplicates()
        filas_eliminadas = filas_antes - len(self.dataframe)
        
        # Limpiar espacios en columnas de texto
        columnas_texto = self.dataframe.select_dtypes(include="object").columns
        for columna in columnas_texto:
            self.__dataframe.loc[:, columna] = (self.__dataframe[columna]
                                               .astype(str)
                                               .str.strip())
        
        print(f"✅ Transformaciones aplicadas exitosamente:")
        print(f"   - Nombres de columnas normalizados: {columnas_originales} columnas")
        print(f"   - Filas duplicadas eliminadas: {filas_eliminadas}")
        print(f"   - Columnas de texto limpiadas: {len(columnas_texto)}")
        
        return True

    def obtener_informacion_basica(self):
        """Muestra información básica del dataset"""
        if not self.esta_cargado:
            print("❌ No hay datos cargados para mostrar información")
            return None
        
        print("\n" + "="*50)
        print("📊 INFORMACIÓN DEL DATASET")
        print("="*50)
        print(f"📂 Archivo fuente: {self.ruta_archivo}")
        print(f"📏 Dimensiones: {self.dataframe.shape[0]} filas x {self.dataframe.shape[1]} columnas")
        print(f"💾 Memoria utilizada: {self.dataframe.memory_usage(deep=True).sum() / 1024:.2f} KB")
        print("\n📋 Tipos de datos por columna:")
        print(self.dataframe.dtypes.to_string())
        print("="*50)

    def mostrar_resumen_estadistico(self):
        """Genera y muestra un resumen estadístico completo"""
        if not self.esta_cargado:
            print("❌ No hay datos disponibles para generar resumen")
            return None
        
        print("\n" + "="*60)
        print("📈 RESUMEN ESTADÍSTICO DEL DATASET")
        print("="*60)
        
        # Mostrar primeras filas
        print("\n🔍 Primeras 3 filas del dataset:")
        print(self.dataframe.head(3).to_string())
        
        # Resumen estadístico
        print("\n📊 Estadísticas descriptivas:")
        print(self.dataframe.describe(include='all').to_string())
        
        print("="*60)
        return self.dataframe.describe(include='all')

    def verificar_columnas_requeridas(self, columnas_requeridas):
        """Verifica si el dataset contiene las columnas especificadas"""
        if not self.esta_cargado:
            return False
        
        columnas_faltantes = []
        for columna in columnas_requeridas:
            if columna not in self.dataframe.columns:
                columnas_faltantes.append(columna)
        
        if columnas_faltantes:
            print(f"⚠️  Columnas faltantes: {', '.join(columnas_faltantes)}")
            return False
        
        print("✅ Todas las columnas requeridas están presentes")
        return True




