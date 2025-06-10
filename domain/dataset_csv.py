import pandas as pd
from domain.dataset import Dataset
import chardet

class DatasetCSV(Dataset):
    def __init__(self, ruta_archivo, separador=None, codificacion=None):
        super().__init__(ruta_archivo)
        self.__separador = separador
        self.__codificacion = codificacion
        self.__separador_detectado = None
        self.__codificacion_detectada = None

    @property
    def separador(self):
        """Retorna el separador utilizado en el CSV"""
        return self.__separador or self.__separador_detectado

    @property
    def codificacion(self):
        """Retorna la codificación utilizada en el CSV"""
        return self.__codificacion or self.__codificacion_detectada

    def cargar_datos(self):
        """Carga datos desde un archivo CSV"""
        try:
            print(f"📂 Iniciando carga del archivo CSV: {self.ruta_archivo}")
            
            # Verificar extensión del archivo
            if not self._validar_extension_csv():
                return False
            
            # Detectar codificación si no está especificada
            if not self.__codificacion:
                self._detectar_codificacion()
            
            # Detectar separador si no está especificado
            if not self.__separador:
                self._detectar_separador()
            
            # Cargar datos del CSV
            df = self._leer_archivo_csv()
            
            if df is not None:
                self.dataframe = df
                print(f"✅ Archivo CSV cargado exitosamente")
                print(f"   - Separador utilizado: '{self.separador}'")
                print(f"   - Codificación utilizada: {self.codificacion}")
                print(f"   - Registros cargados: {len(df)}")
                print(f"   - Columnas encontradas: {len(df.columns)}")
                
                # Validar automáticamente después de cargar
                if self.validar_integridad_datos():
                    self.limpiar_y_transformar_datos()
                
                return True
            else:
                print("❌ No se pudieron cargar los datos del archivo CSV")
                return False
                
        except FileNotFoundError:
            print(f"❌ Error: No se encontró el archivo '{self.ruta_archivo}'")
            return False
        except PermissionError:
            print(f"❌ Error: Sin permisos para acceder al archivo '{self.ruta_archivo}'")
            return False
        except UnicodeDecodeError as e:
            print(f"❌ Error de codificación: {str(e)}")
            print("💡 Sugerencia: Intenta especificar una codificación diferente (utf-8, latin-1, cp1252)")
            return False
        except Exception as e:
            print(f"❌ Error inesperado al cargar archivo CSV: {str(e)}")
            return False

    def _validar_extension_csv(self):
        """Valida que el archivo tenga una extensión CSV válida"""
        extensiones_validas = ['.csv', '.txt', '.tsv']
        extension = self.ruta_archivo.lower()
        
        for ext in extensiones_validas:
            if extension.endswith(ext):
                return True
        
        print(f"⚠️  Advertencia: '{self.ruta_archivo}' no tiene una extensión CSV reconocida")
        print(f"   Extensiones válidas: {', '.join(extensiones_validas)}")
        return True  # Permitir intentar cargar de todos modos

    def _detectar_codificacion(self):
        """Detecta automáticamente la codificación del archivo"""
        try:
            print("🔍 Detectando codificación del archivo...")
            
            with open(self.ruta_archivo, 'rb') as archivo:
                muestra = archivo.read(10000)  # Leer primeros 10KB
                resultado = chardet.detect(muestra)
                
            self.__codificacion_detectada = resultado['encoding']
            confianza = resultado['confidence']
            
            print(f"   Codificación detectada: {self.__codificacion_detectada} (confianza: {confianza:.2%})")
            
        except Exception as e:
            print(f"⚠️  No se pudo detectar la codificación: {str(e)}")
            self.__codificacion_detectada = 'utf-8'  # Fallback por defecto
            print("   Usando codificación por defecto: utf-8")

    def _detectar_separador(self):
        """Detecta automáticamente el separador del CSV"""
        separadores_comunes = [',', ';', '\t', '|']
        
        try:
            print("🔍 Detectando separador del archivo...")
            
            # Leer las primeras líneas para analizar
            with open(self.ruta_archivo, 'r', encoding=self.codificacion) as archivo:
                lineas_muestra = [archivo.readline() for _ in range(5)]
            
            mejor_separador = ','
            max_columnas = 0
            
            for sep in separadores_comunes:
                try:
                    # Intentar leer con cada separador
                    df_prueba = pd.read_csv(self.ruta_archivo, 
                                          sep=sep, 
                                          encoding=self.codificacion,
                                          nrows=5)
                    
                    num_columnas = len(df_prueba.columns)
                    
                    if num_columnas > max_columnas:
                        max_columnas = num_columnas
                        mejor_separador = sep
                        
                except:
                    continue
            
            self.__separador_detectado = mejor_separador
            sep_nombre = {',' : 'coma', ';': 'punto y coma', '\t': 'tabulación', '|': 'pipe'}
            nombre_sep = sep_nombre.get(mejor_separador, mejor_separador)
            
            print(f"   Separador detectado: '{mejor_separador}' ({nombre_sep})")
            
        except Exception as e:
            print(f"⚠️  No se pudo detectar el separador: {str(e)}")
            self.__separador_detectado = ','  # Fallback por defecto
            print("   Usando separador por defecto: coma (,)")

    def _leer_archivo_csv(self):
        """Lee los datos del archivo CSV"""
        try:
            # Configurar parámetros de lectura
            parametros_lectura = {
                'sep': self.separador,
                'encoding': self.codificacion,
                'na_values': ['', 'N/A', 'NULL', 'null', 'NaN', 'n/a'],
                'skipinitialspace': True,  # Eliminar espacios al inicio
                'low_memory': False  # Leer todo el archivo para inferir tipos
            }
            
            # Leer el archivo
            df = pd.read_csv(self.ruta_archivo, **parametros_lectura)
            
            return df
            
        except Exception as e:
            print(f"❌ Error al leer el archivo CSV: {str(e)}")
            return None

    def cambiar_parametros_carga(self, nuevo_separador=None, nueva_codificacion=None):
        """Permite cambiar los parámetros de carga y recargar el archivo"""
        cambios_realizados = []
        
        if nuevo_separador and nuevo_separador != self.__separador:
            self.__separador = nuevo_separador
            cambios_realizados.append(f"separador → '{nuevo_separador}'")
        
        if nueva_codificacion and nueva_codificacion != self.__codificacion:
            self.__codificacion = nueva_codificacion
            cambios_realizados.append(f"codificación → {nueva_codificacion}")
        
        if cambios_realizados:
            print(f"🔄 Aplicando cambios: {', '.join(cambios_realizados)}")
            return self.cargar_datos()
        else:
            print("⚠️  No se especificaron cambios en los parámetros")
            return False

    def mostrar_informacion_archivo(self):
        """Muestra información detallada del archivo CSV"""
        print("\n" + "="*50)
        print("📄 INFORMACIÓN DEL ARCHIVO CSV")
        print("="*50)
        print(f"📂 Ruta: {self.ruta_archivo}")
        print(f"🔤 Codificación: {self.codificacion}")
        print(f"📊 Separador: '{self.separador}'")
        
        if self.esta_cargado:
            print(f"📏 Dimensiones: {self.dataframe.shape[0]} filas x {self.dataframe.shape[1]} columnas")
            
            # Mostrar primeras columnas
            columnas_muestra = list(self.dataframe.columns[:5])
            if len(self.dataframe.columns) > 5:
                columnas_muestra.append("...")
                columnas_muestra.append(f"({len(self.dataframe.columns)} columnas total)")
            
            print(f"📋 Columnas: {', '.join(map(str, columnas_muestra))}")
        
        print("="*50)

    def exportar_con_nuevos_parametros(self, ruta_destino, separador_salida=',', codificacion_salida='utf-8'):
        """Exporta el dataset con nuevos parámetros de formato"""
        if not self.esta_cargado:
            print("❌ No hay datos cargados para exportar")
            return False
        
        try:
            self.dataframe.to_csv(ruta_destino, 
                                sep=separador_salida, 
                                encoding=codificacion_salida, 
                                index=False)
            
            print(f"✅ Archivo exportado exitosamente:")
            print(f"   - Destino: {ruta_destino}")
            print(f"   - Separador: '{separador_salida}'")
            print(f"   - Codificación: {codificacion_salida}")
            return True
            
        except Exception as e:
            print(f"❌ Error al exportar: {str(e)}")
            return False

    def previsualizar_estructura(self, num_lineas=5):
        """Muestra una previsualización de la estructura del archivo"""
        if not self.esta_cargado:
            print("❌ No hay datos cargados para previsualizar")
            return
        
        print(f"\n🔍 PREVISUALIZACIÓN - Primeras {num_lineas} filas:")
        print("-" * 60)
        print(self.dataframe.head(num_lineas).to_string())
        print("-" * 60)