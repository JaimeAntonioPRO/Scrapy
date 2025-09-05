# 1. Importar las librerías necesarias
from flask import Flask, jsonify, request
from flask_cors import CORS
import pyodbc
import subprocess # Para poder ejecutar comandos del sistema (como 'scrapy crawl')
import os # Para construir rutas de archivos de forma segura

# 2. Configuración inicial de Flask
app = Flask(__name__)
# CORS permite que tu frontend (React) se comunique con este backend sin problemas de seguridad del navegador
CORS(app) 

# 3. Función para conectar a la Base de Datos
# Centralizamos la conexión para no repetir el código.
def get_db_connection():
    # --- ¡IMPORTANTE! MODIFICA ESTOS DATOS CON LOS TUYOS ---
    connection = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'                  # O el nombre/IP de tu servidor SQL
        'DATABASE=BASE_SCRAPY;'          # El nombre de tu base de datos
        'UID=sa;'                    # Tu usuario de SQL Server
        'PWD=prointernet;'                 # Tu contraseña
    )
    return connection

# 4. Definición de las rutas (Endpoints) del API

# --- ENDPOINT PARA OBTENER LOS PRODUCTOS DE LA BASE DE DATOS ---
@app.route('/api/productos', methods=['GET'])
def get_productos():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Query SQL usando los nombres de tu tabla y columnas
        cursor.execute("""
            SELECT 
                ID_PROD, 
                TITULOS_PROD, 
                PRECIO_PROD, 
                URL_IMG_PROD, 
                TIENDA_PROD, 
                FECHA_SCRAPING_PROD 
            FROM PRODUCTOS_T 
            ORDER BY FECHA_SCRAPING_PROD DESC
        """)
        
        # Convertimos los resultados a una lista de diccionarios para enviarlos como JSON
        productos = []
        for row in cursor.fetchall():
            productos.append({
                "Id": row.ID_PROD,
                "Titulo": row.TITULOS_PROD,
                "Precio": row.PRECIO_PROD,
                "UrlImagen": row.URL_IMG_PROD,
                "Tienda": row.TIENDA_PROD,
                "FechaScraping": row.FECHA_SCRAPING_PROD.isoformat() # Convertir fecha a texto
            })
            
        conn.close()
        return jsonify(productos)
    except Exception as e:
        # Si algo sale mal, devolvemos un error claro
        return jsonify({"error": f"Error al conectar o consultar la base de datos: {str(e)}"}), 500


# api.py

# api.py

# --- ENDPOINT CORRECTO Y SIMPLIFICADO ---
@app.route('/api/iniciar-spider', methods=['POST'])
def iniciar_spider():
    data = request.json
    nombre_spider = data.get('spider')
    query = data.get('query')
    max_products = data.get('max_products', 100)

    if not nombre_spider or not query:
        return jsonify({"error": "El nombre del spider y el término de búsqueda son requeridos"}), 400

    try:
        # Construimos el comando directamente con los datos recibidos
        comando = (
            f"scrapy crawl {nombre_spider} "
            f"-a query=\"{query}\" "
            f"-a max_products={max_products}"
        )
        
        # Ejecutamos el comando en segundo plano
        # Asegúrate de que la terminal donde corres 'python api.py'
        # esté en la carpeta que contiene 'scrapy.cfg'
        subprocess.Popen(comando, shell=True)

        mensaje = f"Spider '{nombre_spider}' iniciado para buscar '{query}' (máx {max_products} productos)."
        return jsonify({"mensaje": mensaje}), 200
        
    except Exception as e:
        return jsonify({"error": f"Error al intentar ejecutar el spider: {str(e)}"}), 500

# 5. Punto de entrada para ejecutar el servidor Flask
if __name__ == '__main__':
    # debug=True hace que el servidor se reinicie automáticamente cuando guardas cambios.
    # Quítalo cuando pases a producción.
    app.run(debug=True, port=5000)