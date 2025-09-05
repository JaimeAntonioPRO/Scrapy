import pyodbc

class SqlServerPipeline:
    def __init__(self):
        # --- CONFIGURA TU CONEXIÓN AQUÍ ---
        self.connection = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=host.docker.internal;'
            'DATABASE=BASE_SCRAPY;' # El nombre de tu DB
            'UID=sa;'
            'PWD=prointernet;'
        )
        self.cursor = self.connection.cursor()

    def process_item(self, item, spider):
        try:
            # === QUERÍA ACTUALIZADO ===
            # Usamos los nombres de columna de PRODUCTOS_T
            query = """
                INSERT INTO PRODUCTOS_T (TITULOS_PROD, PRECIO_PROD, URL_IMG_PROD, TIENDA_PROD)
                VALUES (?, ?, ?, ?)
            """
            self.cursor.execute(
                query,
                item['titulo'],
                item['precio'],
                item['url_imagen'],
                spider.name
            )
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            spider.logger.error(f"Error al guardar en la base de datos: {e}")
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()