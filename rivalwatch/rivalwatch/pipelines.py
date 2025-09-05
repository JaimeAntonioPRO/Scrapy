import pyodbc
from scrapy.exceptions import DropItem # Importamos la excepción especial para descartar items

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
        # --- PASO DE VALIDACIÓN ---
        # Verificamos que los campos esenciales no estén vacíos o nulos.
        if not item.get('titulo') or not item.get('precio') or not item.get('url_imagen'):
            # Si falta algún dato, lanzamos DropItem.
            # Scrapy lo captura, detiene el procesamiento de este item y lo registra.
            raise DropItem(f"Item descartado por datos incompletos: {item['titulo']}")

        # Si la validación pasa, continuamos con el proceso de inserción.
        try:
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
        
        return item # Es importante retornar el item si se procesó correctamente

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()