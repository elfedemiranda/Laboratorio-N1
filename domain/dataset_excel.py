import pandas as pd
from domain.dataset import Dataset

class DatasetExcel(Dataset):
    def __init__(self, ruta_archivo, nombre_hoja=None):
        super().__init__(ruta_archivo)
        self.__nombre_hoja = nombre_hoja
        self.__hojas_disponibles = []

    @property
    def nombre_hoja(self):
        """Retorna el nombre de la hoja actualmente seleccionada"""
        return self.__nombre_hoja
    
    @property
    def hojas_disponibles(self):
        """Retorna la lista de hojas disponibles en el archivo Excel"""
        return self.__hojas_disponibles

    def cargar_datos(self):
        """Carga datos desde un archivo Excel"""
        try:
            print(f"📂 Iniciando carga del archivo Excel: {self.ruta_archivo}")
            
            # Verificar extensión del archivo
            if not self._validar_extension_excel():
                return False
            
            # Obtener información de las hojas disponibles
            self._obtener_hojas_disponibles()
            
            # Cargar datos de la hoja especificada
            df = self._leer_archivo_excel()
            
            if df is not None:
                self.dataframe = df
                print(f"✅ Archivo Excel cargado exitosamente")
                print(f"   - Hoja utilizada: {self.nombre_hoja or 'Primera hoja'}")
                print(f"   - Registros cargados: {len(df)}")
                print(f"   - Columnas encontradas: {len(df.columns)}")
                
                # Validar automáticamente después de cargar
                if self.validar_integridad_datos():
                    self.limpiar_y_transformar_datos()
                
                return True
            else:
                print("❌ No se pudieron cargar los datos del archivo Excel")
                return False
                
        except FileNotFoundError:
            print(f"❌ Error: No se encontró el archivo '{self.ruta_archivo}'")
            return False
        except PermissionError:
            print(f"❌ Error: Sin permisos para acceder al archivo '{self.ruta_archivo}'")
            return False
        except Exception as e:
            print(f"❌ Error inesperado al cargar archivo Excel: {str(e)}")
            return False

    def _validar_extension_excel(self):
        """Valida que el archivo tenga una extensión Excel válida"""
        extensiones_validas = ['.xlsx', '.xls', '.xlsm']
        extension = self.ruta_archivo.lower()
        
        for ext in extensiones_validas:
            if extension.endswith(ext):
                return True
        
        print(f"⚠️  Advertencia: '{self.ruta_archivo}' no tiene una extensión Excel reconocida")
        print(f"   Extensiones válidas: {', '.join(extensiones_validas)}")
        return True  # Permitir intentar cargar de todos modos

    def _obtener_hojas_disponibles(self):
        """Obtiene la lista de hojas disponibles en el archivo Excel"""
        try:
            archivo_excel = pd.ExcelFile(self.ruta_archivo)
            self.__hojas_disponibles = archivo_excel.sheet_names
            print(f"📋 Hojas disponibles en el archivo: {', '.join(self.hojas_disponibles)}")
        except Exception as e:
            print(f"⚠️  No se pudieron obtener las hojas del archivo: {str(e)}")
            self.__hojas_disponibles = []

    def _leer_archivo_excel(self):
        """Lee los datos del archivo Excel"""
        try:
            # Configurar parámetros de lectura
            parametros_lectura = {
                'engine': 'openpyxl',
                'na_values': ['', 'N/A', 'NULL', 'null', 'NaN']
            }
            
            # Agregar nombre de hoja si está especificado
            if self.nombre_hoja:
                if self.nombre_hoja in self.hojas_disponibles:
                    parametros_lectura['sheet_name'] = self.nombre_hoja
                else:
                    print(f"⚠️  La hoja '{self.nombre_hoja}' no existe. Usando la primera hoja disponible")
                    parametros_lectura['sheet_name'] = 0
            else:
                parametros_lectura['sheet_name'] = 0
            
            # Leer el archivo
            df = pd.read_excel(self.ruta_archivo, **parametros_lectura)
            
            return df
            
        except Exception as e:
            print(f"❌ Error al leer el archivo Excel: {str(e)}")
            return None

    def cambiar_hoja(self, nuevo_nombre_hoja):
        """Cambia a una hoja diferente del archivo Excel"""
        if not self.hojas_disponibles:
            print("❌ No hay información de hojas disponibles")
            return False
        
        if nuevo_nombre_hoja not in self.hojas_disponibles:
            print(f"❌ La hoja '{nuevo_nombre_hoja}' no existe")
            print(f"   Hojas disponibles: {', '.join(self.hojas_disponibles)}")
            return False
        
        print(f"🔄 Cambiando de hoja: '{self.nombre_hoja}' → '{nuevo_nombre_hoja}'")
        self.__nombre_hoja = nuevo_nombre_hoja
        
        # Recargar datos con la nueva hoja
        return self.cargar_datos()

    def mostrar_informacion_hojas(self):
        """Muestra información detallada de todas las hojas del archivo"""
        if not self.hojas_disponibles:
            print("❌ No hay información de hojas disponibles")
            return
        
        print("\n" + "="*50)
        print("📊 INFORMACIÓN DE HOJAS DEL ARCHIVO EXCEL")
        print("="*50)
        
        for i, hoja in enumerate(self.hojas_disponibles, 1):
            estado = "← ACTIVA" if hoja == self.nombre_hoja else ""
            print(f"{i}. {hoja} {estado}")
        
        print("="*50)

    def exportar_hoja_actual(self, ruta_destino):
        """Exporta la hoja actual a un nuevo archivo Excel"""
        if not self.esta_cargado:
            print("❌ No hay datos cargados para exportar")
            return False
        
        try:
            self.dataframe.to_excel(ruta_destino, index=False, engine='openpyxl')
            print(f"✅ Hoja exportada exitosamente a: {ruta_destino}")
            return True
        except Exception as e:
            print(f"❌ Error al exportar: {str(e)}")
            return False