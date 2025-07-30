import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Obtener los datos de conexión desde las variables de entorno
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
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

# Ruta a la carpeta con las consultas
carpeta_consultas = 'consultas'

# Diccionario de carpetas y tablas
carpetas_tablas = {
    'jcom2': ('t_temp_seller_pedidos_jcom2', 't_seller_pedidos_jcom2'),
    'jcom1': ('t_temp_seller_pedidos_jcom', 't_seller_pedidos_jcom')
}

# Función para verificar y crear tabla si no existe
def crear_tabla_si_no_existe(nombre_tabla, tipo):
    cursor.execute(f"SHOW TABLES LIKE '{nombre_tabla}'")
    resultado = cursor.fetchone()

    if not resultado:
        print(f"La tabla '{nombre_tabla}' no existe, creando...")

        if tipo == 'temp':
            ruta_sql = os.path.join(carpeta_consultas, 'create_temp_table.sql')
        else:
            ruta_sql = os.path.join(carpeta_consultas, 'create_final_table.sql')

        with open(ruta_sql, 'r') as file:
            sql_creacion = file.read().format(tabla=nombre_tabla)

        cursor.execute(sql_creacion)
        conexion.commit()
        print(f"Tabla '{nombre_tabla}' creada.")
    else:
        print(f"La tabla '{nombre_tabla}' ya existe.")

# Recorrer cada carpeta en el diccionario
for subcarpeta, (tabla_temp, tabla_final) in carpetas_tablas.items():

    # Crear tablas si no existen
    crear_tabla_si_no_existe(tabla_temp, tipo='temp')
    crear_tabla_si_no_existe(tabla_final, tipo='final')

    # Verificar si la carpeta existe y tiene archivos TXT
    if os.path.exists(subcarpeta) and os.listdir(subcarpeta):
        archivos_txt = [archivo for archivo in os.listdir(subcarpeta) if archivo.endswith('.txt')]

        for archivo_txt in archivos_txt:
            try:
                # Ruta completa del archivo
                ruta_archivo = os.path.join(subcarpeta, archivo_txt)
                
                # Leer el archivo TXT delimitado por tabulaciones
                df = pd.read_csv(ruta_archivo, sep='\t', engine='python')
                
                print(f"Archivo '{archivo_txt}' procesado en '{subcarpeta}'.")

                # Asegurar que la ruta sea absoluta para MySQL
                ruta_completa_mysql = os.path.abspath(ruta_archivo).replace("\\", "/")

                # Leer y ejecutar la consulta de eliminar datos antiguos en la tabla temporal
                with open(os.path.join(carpeta_consultas, 'delete_query.sql'), 'r') as file:
                    delete_query = file.read().format(tabla_temp=tabla_temp)
                cursor.execute(delete_query)
                conexion.commit()
                print(f"Datos anteriores eliminados de la tabla temporal '{tabla_temp}'.")

                # Leer y ejecutar la consulta de inserción en la tabla temporal
                with open(os.path.join(carpeta_consultas, 'insercion_query.sql'), 'r') as file:
                    insercion = file.read().format(ruta_completa_mysql=ruta_completa_mysql, tabla_temp=tabla_temp)
                cursor.execute(insercion)
                conexion.commit()
                print(f"Datos del archivo '{archivo_txt}' insertados en la tabla temporal '{tabla_temp}'.")

                # Leer y ejecutar la consulta de actualización de la columna 'indice'
                with open(os.path.join(carpeta_consultas, 'update_indice_query.sql'), 'r') as file:
                    update_indice = file.read().format(tabla_temp=tabla_temp)
                cursor.execute(update_indice)
                conexion.commit()
                print(f"Columna 'indice' actualizada en '{tabla_temp}'.")

                # Leer y ejecutar la consulta de actualización de la columna 'indicemd5'
                with open(os.path.join(carpeta_consultas, 'update_md5_query.sql'), 'r') as file:
                    update_md5 = file.read().format(tabla_temp=tabla_temp)
                cursor.execute(update_md5)
                conexion.commit()
                print(f"Columna 'indicemd5' actualizada en '{tabla_temp}'.")

                # Leer y ejecutar la consulta de inserción de datos nuevos en la tabla final
                with open(os.path.join(carpeta_consultas, 'insercion_final_query.sql'), 'r') as file:
                    insercion_final = file.read().format(tabla_temp=tabla_temp, tabla_final=tabla_final)
                cursor.execute(insercion_final)
                conexion.commit()
                print(f"Datos nuevos insertados en '{tabla_final}' desde '{tabla_temp}'.")

                # Leer y ejecutar la consulta de actualización de los datos en la tabla final
                with open(os.path.join(carpeta_consultas, 'update_final_query.sql'), 'r') as file:
                    update_final = file.read().format(tabla_temp=tabla_temp, tabla_final=tabla_final)
                cursor.execute(update_final)
                conexion.commit()
                print(f"Datos actualizados en la tabla final '{tabla_final}'.")

            except pd.errors.ParserError as e:
                print(f"Error de formato en el archivo '{archivo_txt}' en '{subcarpeta}': {str(e)}")
            except Exception as e:
                print(f"Error al procesar el archivo '{archivo_txt}' en '{subcarpeta}': {str(e)}")
                continue

# Cerrar conexión
cursor.close()
conexion.close()
