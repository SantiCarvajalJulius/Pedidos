import os
import logging

# Limitar threads
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"]      = "1"
os.environ["MKL_NUM_THREADS"]      = "1"
os.environ["NUMEXPR_NUM_THREADS"]  = "1"

import pandas as pd
import mysql.connector
from dotenv import load_dotenv

# ——— Configuración de logging ———
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()
MYSQL_HOST     = os.getenv("MYSQL_HOST")
MYSQL_USER     = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

# Conexión a la base de datos MySQL
conexion = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE,
    allow_local_infile=True
)
cursor = conexion.cursor()

# Rutas y diccionario de carpetas/tablas
carpeta_consultas = 'consultas'
carpetas_tablas = {
    'jcom2': ('t_temp_seller_pedidos_jcom2', 't_seller_pedidos_jcom2'),
    'jcom1': ('t_temp_seller_pedidos_jcom',  't_seller_pedidos_jcom')
}
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Crear subcarpetas si no existen
for subcarpeta in carpetas_tablas:
    path = os.path.join(BASE_DIR, subcarpeta)
    os.makedirs(path, exist_ok=True)
    logger.info(f"Existe o fue creada la carpeta: {path}")

def crear_tabla_si_no_existe(nombre_tabla, tipo):
    cursor.execute(f"SHOW TABLES LIKE '{nombre_tabla}'")
    if not cursor.fetchone():
        logger.info(f"La tabla '{nombre_tabla}' no existe, creando...")
        sql_file = 'create_temp_table.sql' if tipo=='temp' else 'create_final_table.sql'
        ruta_sql = os.path.join(carpeta_consultas, sql_file)
        with open(ruta_sql, 'r') as f:
            sql = f.read().format(tabla=nombre_tabla)
        cursor.execute(sql)
        conexion.commit()
        logger.info(f"Tabla '{nombre_tabla}' creada.")
    else:
        logger.info(f"Tabla '{nombre_tabla}' ya existe.")

# Procesamiento de archivos
for subcarpeta, (tabla_temp, tabla_final) in carpetas_tablas.items():

    # Asegurar tablas
    crear_tabla_si_no_existe(tabla_temp, 'temp')
    crear_tabla_si_no_existe(tabla_final, 'final')

    carpeta = os.path.join(BASE_DIR, subcarpeta)
    archivos_txt = [f for f in os.listdir(carpeta) if f.endswith('.txt')]

    for archivo_txt in archivos_txt:
        ruta_archivo = os.path.join(carpeta, archivo_txt)
        try:
            # 1) Leer TXT
            df = pd.read_csv(ruta_archivo, sep='\t', engine='python')
            logger.info(f"Leído '{archivo_txt}' en '{subcarpeta}' ({len(df)} filas).")

            # 2) Eliminar datos antiguos de tabla temporal
            with open(os.path.join(carpeta_consultas, 'delete_query.sql')) as f:
                query = f.read().format(tabla_temp=tabla_temp)
            cursor.execute(query); conexion.commit()
            logger.info(f"Datos anteriores borrados en '{tabla_temp}'.")

            # 3) Insertar en tabla temporal
            ruta_mysql = os.path.abspath(ruta_archivo).replace("\\", "/")
            with open(os.path.join(carpeta_consultas, 'insercion_query.sql')) as f:
                query = f.read().format(ruta_completa_mysql=ruta_mysql, tabla_temp=tabla_temp)
            cursor.execute(query); conexion.commit()
            logger.info(f"Datos de '{archivo_txt}' cargados en '{tabla_temp}'.")

            # 4) Actualizar 'indice'
            with open(os.path.join(carpeta_consultas, 'update_indice_query.sql')) as f:
                cursor.execute(f.read().format(tabla_temp=tabla_temp)); conexion.commit()
            logger.info(f"Columna 'indice' actualizada en '{tabla_temp}'.")

            # 5) Actualizar 'indicemd5'
            with open(os.path.join(carpeta_consultas, 'update_md5_query.sql')) as f:
                cursor.execute(f.read().format(tabla_temp=tabla_temp)); conexion.commit()
            logger.info(f"Columna 'indicemd5' actualizada en '{tabla_temp}'.")

            # 6) Insertar en tabla final
            with open(os.path.join(carpeta_consultas, 'insercion_final_query.sql')) as f:
                cursor.execute(f.read().format(tabla_temp=tabla_temp, tabla_final=tabla_final)); conexion.commit()
            logger.info(f"Nuevos datos insertados en '{tabla_final}'.")

            # 7) Actualizar datos en tabla final
            with open(os.path.join(carpeta_consultas, 'update_final_query.sql')) as f:
                cursor.execute(f.read().format(tabla_temp=tabla_temp, tabla_final=tabla_final)); conexion.commit()
            logger.info(f"Datos actualizados en '{tabla_final}' desde '{tabla_temp}'.")

        except pd.errors.ParserError as e:
            logger.error(f"ParserError en '{archivo_txt}': {e}")
        except Exception as e:
            logger.error(f"Error procesando '{archivo_txt}': {e}")
        else:
            # Elimina el archivo solo si no hubo excepción
            try:
                os.remove(ruta_archivo)
                logger.info(f"Archivo eliminado: '{ruta_archivo}'.")
            except Exception as e:
                logger.error(f"No se pudo borrar '{ruta_archivo}': {e}")

# Cerrar conexión
cursor.close()
conexion.close()
logger.info("Conexión MySQL cerrada y proceso finalizado.")
