import os

# Limitar threads
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"]      = "1"
os.environ["MKL_NUM_THREADS"]      = "1"
os.environ["NUMEXPR_NUM_THREADS"]  = "1"

import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import logging

# Directorio base del script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ——— Configuración de logging a archivo TXT ———
log_path = os.path.join(BASE_DIR, 'log.txt')
logging.basicConfig(
    filename=log_path,
    filemode='a',  # 'w' para sobrescribir cada vez; 'a' para anexar
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)
logger.info("===== Inicio de proceso =====")

# Cargar variables de entorno
load_dotenv(os.path.join(BASE_DIR, '.env'))
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
logger.info("Conexión a MySQL establecida")

# Rutas y diccionario de carpetas/tablas
carpeta_consultas = os.path.join(BASE_DIR, 'consultas')
carpetas_tablas = {
    'jcom2': ('t_temp_seller_pedidos_jcom2', 't_seller_pedidos_jcom2'),
    'jcom1': ('t_temp_seller_pedidos_jcom',  't_seller_pedidos_jcom')
}

# Crear subcarpetas si no existen
for subcarpeta in carpetas_tablas:
    path = os.path.join(BASE_DIR, subcarpeta)
    os.makedirs(path, exist_ok=True)
    logger.info(f"Carpeta lista: {path}")

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
    crear_tabla_si_no_existe(tabla_temp, 'temp')
    crear_tabla_si_no_existe(tabla_final, 'final')

    carpeta = os.path.join(BASE_DIR, subcarpeta)
    archivos_txt = [f for f in os.listdir(carpeta) if f.lower().endswith('.txt')]

    for archivo_txt in archivos_txt:
        ruta_archivo = os.path.join(carpeta, archivo_txt)
        try:
            df = pd.read_csv(ruta_archivo, sep='\t', engine='python')
            logger.info(f"Leído '{archivo_txt}' ({len(df)} filas)")

            # 1) Borrar antiguos en temp
            with open(os.path.join(carpeta_consultas, 'delete_query.sql')) as f:
                q = f.read().format(tabla_temp=tabla_temp)
            cursor.execute(q); conexion.commit()
            logger.info(f"Limpiada tabla temporal '{tabla_temp}'")

            # 2) Cargar en temp
            mysql_path = os.path.abspath(ruta_archivo).replace('\\', '/')
            with open(os.path.join(carpeta_consultas, 'insercion_query.sql')) as f:
                q = f.read().format(ruta_completa_mysql=mysql_path, tabla_temp=tabla_temp)
            cursor.execute(q); conexion.commit()
            logger.info(f"Cargados datos de '{archivo_txt}' en '{tabla_temp}'")

            # 3) Update índice
            with open(os.path.join(carpeta_consultas, 'update_indice_query.sql')) as f:
                cursor.execute(f.read().format(tabla_temp=tabla_temp)); conexion.commit()
            logger.info(f"Índice actualizado en '{tabla_temp}'")

            # 4) Update MD5
            with open(os.path.join(carpeta_consultas, 'update_md5_query.sql')) as f:
                cursor.execute(f.read().format(tabla_temp=tabla_temp)); conexion.commit()
            logger.info(f"MD5 actualizado en '{tabla_temp}'")

            # 5) Insertar en final
            with open(os.path.join(carpeta_consultas, 'insercion_final_query.sql')) as f:
                cursor.execute(f.read().format(tabla_temp=tabla_temp, tabla_final=tabla_final)); conexion.commit()
            logger.info(f"Insertados nuevos en '{tabla_final}'")

            # 6) Update final
            with open(os.path.join(carpeta_consultas, 'update_final_query.sql')) as f:
                cursor.execute(f.read().format(tabla_temp=tabla_temp, tabla_final=tabla_final)); conexion.commit()
            logger.info(f"Actualizados registros en '{tabla_final}' desde '{tabla_temp}'")

        except pd.errors.ParserError as e:
            logger.error(f"ParserError en '{archivo_txt}': {e}")
        except Exception as e:
            logger.error(f"Error procesando '{archivo_txt}': {e}")
        else:
            # Eliminar TXT tras éxito
            try:
                os.remove(ruta_archivo)
                logger.info(f"Archivo eliminado: '{ruta_archivo}'")
            except Exception as e:
                logger.error(f"No se pudo eliminar '{ruta_archivo}': {e}")

# Cerrar conexión
cursor.close()
conexion.close()
logger.info("Conexión MySQL cerrada")
logger.info("===== Fin de proceso =====")
