# Pedidos — Actualización de pedidos de Amazon a MySQL

Este proyecto carga y actualiza en MySQL los pedidos de Amazon descargados manualmente (últimos 30 días), utilizando archivos .txt delimitados por tabulaciones.
El script principal es pedidos.py. Trabaja con dos “orígenes” (subcarpetas) por defecto: jcom1 y jcom2.

## ¿Qué hace?

* Crea las tablas (temporal y final) si no existen, usando las plantillas SQL en consultas/.

* Procesa cada archivo .txt que encuentre en jcom1/ y jcom2/ (tab-delimited).

* Carga a tabla temporal mediante LOAD DATA LOCAL INFILE.

* Calcula/actualiza columnas técnicas (indice, indicemd5).

* Inserta y actualiza en la tabla final solo los nuevos/cambiados.

* Imprime logs legibles del avance y errores por archivo.

* El script usa rutas absolutas desde la ubicación del archivo (BASE_DIR) para no depender de desde dónde lo ejecutas.

# Flujo rápido de uso

* Descarga el informe de pedidos de Amazon (período últimos 30 días) como .txt tabulado.

* Pega el archivo en la subcarpeta que corresponda a la cuenta:

jcom1/ → tabla final t_seller_pedidos_jcom

jcom2/ → tabla final t_seller_pedidos_jcom2

* Ejecuta el script: ** python3 pedidos.py **

* Verifica en MySQL que se hayan cargado/actualizado los pedidos.

Requisitos
Python 3.9+ (probado en 3.10–3.13)

MySQL 5.7/8.0 con:

local_infile = ON (para LOAD DATA LOCAL INFILE)

Usuario con permisos para INSERT/UPDATE/CREATE en el esquema destino

Paquetes Python:

python-dotenv

pandas

numpy

mysql-connector-python

OpenBLAS / hilos: el script ya fija OPENBLAS_NUM_THREADS=1 antes de importar pandas/numpy para evitar límites de procesos en servidores compartidos.
