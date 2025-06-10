import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table

class DataSaver:
    def __init__(self, host='localhost', user='root', password='', database='analisis_datos', port=3306):
        """
        Inicializa la conexión a MySQL y guarda los parámetros de conexión.
        Si no se provee conexión correcta, se notifica el error.
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.engine = None

        self._crear_conexion()

    def _crear_conexion(self):
        """
        Crea el engine de SQLAlchemy para la conexión a MySQL.
        Se construye la cadena de conexión con los parámetros establecidos.
        """
        try:
            connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            self.engine = create_engine(connection_string, pool_recycle=3600)
            print("🔗 Conexión a MySQL establecida exitosamente")
        except Exception as e:
            print(f"❌ Error al conectar a MySQL: {str(e)}")
            self.engine = None

    def guardar_dataframe(self, dataframe: pd.DataFrame, destino: str, if_exists='replace'):
        """
        Guarda el DataFrame en el destino especificado.
        
        - Si 'destino' es un archivo (extensión .csv, .xlsx o .xls), se exporta al disco.
        - Si no tiene extensión reconocida, se asume que es el nombre de una tabla en MySQL.
        
        Parámetros:
          • dataframe: DataFrame a guardar.
          • destino: Ruta de archivo de salida o nombre de la tabla.
          • if_exists: Comportamiento en caso de que la tabla exista (opciones: 'fail', 'replace', 'append').
        """
        if dataframe is None or dataframe.empty:
            print("⚠️  El DataFrame está vacío. No se realizará el guardado.")
            return False

        _, extension = os.path.splitext(destino)
        extension = extension.lower()

        try:
            if extension == '.csv':
                # Guardar como archivo CSV
                dataframe.to_csv(destino, index=False)
                print(f"✅ DataFrame guardado correctamente como archivo CSV: {destino}")
            elif extension in ['.xlsx', '.xls']:
                # Guardar como archivo Excel
                dataframe.to_excel(destino, index=False, engine='openpyxl')
                print(f"✅ DataFrame guardado correctamente como archivo Excel: {destino}")
            else:
                # Se asume que 'destino' es el nombre de la tabla en MySQL.
                if self.engine:
                    dataframe.to_sql(name=destino, con=self.engine, index=False, if_exists=if_exists)
                    print(f"✅ DataFrame almacenado en la tabla MySQL '{destino}' correctamente")
                else:
                    print("❌ No se pudo conectar a MySQL. Verifica la configuración de conexión.")
                    return False
            return True
        except Exception as e:
            print(f"❌ Error al guardar DataFrame en '{destino}': {str(e)}")
            return False

    def actualizar_registros(self, tabla: str, dataframe: pd.DataFrame, clave: str):
        """
        Actualiza registros en la tabla MySQL basándose en el valor único de la columna 'clave'.
        Se espera que la tabla ya exista y que 'clave' actúe como la columna identificadora (primary key).
        """
        if dataframe is None or dataframe.empty:
            print("⚠️  El DataFrame está vacío. No se realizará la actualización.")
            return False
        if not self.engine:
            print("❌ Conexión a MySQL no establecida. No se puede actualizar.")
            return False

        try:
            metadata = MetaData(bind=self.engine)
            tabla_obj = Table(tabla, metadata, autoload_with=self.engine)
            conn = self.engine.connect()

            for _, fila in dataframe.iterrows():
                datos = fila.to_dict()
                valor_clave = datos.get(clave)
                if valor_clave is None:
                    print(f"⚠️  Fila sin valor en la columna clave '{clave}'. Se omitirá.")
                    continue
                stmt = tabla_obj.update().where(tabla_obj.c[clave] == valor_clave).values(**datos)
                conn.execute(stmt)

            conn.close()
            print(f"✅ Registros actualizados en la tabla MySQL '{tabla}' basándose en la clave '{clave}'")
            return True
        except Exception as e:
            print(f"❌ Error al actualizar registros en la tabla '{tabla}': {str(e)}")
            return False

    def eliminar_registro(self, tabla: str, llave: str, valor_llave):
        """
        Elimina un registro de la tabla MySQL basándose en el valor único asignado a la columna 'llave'.
        """
        if not self.engine:
            print("❌ Conexión a MySQL no establecida. No se puede eliminar registros.")
            return False

        try:
            metadata = MetaData(bind=self.engine)
            tabla_obj = Table(tabla, metadata, autoload_with=self.engine)
            conn = self.engine.connect()

            stmt = tabla_obj.delete().where(tabla_obj.c[llave] == valor_llave)
            conn.execute(stmt)
            conn.close()

            print(f"✅ Registro eliminado de la tabla '{tabla}' donde {llave} = {valor_llave}")
            return True
        except Exception as e:
            print(f"❌ Error al eliminar registro de la tabla '{tabla}': {str(e)}")
            return False