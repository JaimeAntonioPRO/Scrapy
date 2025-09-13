# heb.py (Versión Definitiva Final - Corregida para aceptar respuesta 206)
import scrapy
import json
import unicodedata
import logging
from urllib.parse import quote_plus

class HebSpider(scrapy.Spider):
    name = "heb"
    allowed_domains = ["heb.com.mx"]
    
    # El token de segmento para "HEB Vic. Campestre"
    segment_token = "eyJjYW1wYWlnbnMiOm51bGwsImNoYW5uZWwiOiIxIiwicHJpY2VUYWJsZXMiOm51bGwsInJlZ2lvbklkIjpudWxsLCJ1dG1fY2FtcGFpZ24iOm51bGwsInV0bV9zb3VyY2UiOm51bGwsInV0bWlfcGNhcnRhbyI6bnVsbCwiY3VycmVuY3kiOnsiY29kZSI6Ik1YTiIsInN5bWJvbCI6IiQifSwic2VsbGVycyI6W3siaWQiOiIyIiwibmFtZSI6IkhFQiBWaWMuIENhbXBlc3RyZSJ9XSwiY2hhbm5lbFByaXZhY3kiOiJwdWJsaWMifQ=="
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
    }

    def __init__(self, *args, **kwargs):
        super(HebSpider, self).__init__(*args, **kwargs)
        self.scraped_count = 0
        self.page = 0
        self.max_products = int(getattr(self, "max_products", 50))
        self.query = getattr(self, "query", None)
        
    def start_requests(self):
        if not self.query:
            self.logger.error("No se proporcionó un término de búsqueda ('query'). Usa -a query='tu_busqueda'")
            return

        yield self._make_api_request()

    def _make_api_request(self):
        _from = self.page * 24
        _to = (self.page + 1) * 24 - 1
        
        slug = unicodedata.normalize('NFKD', self.query).encode('ascii', 'ignore').decode('utf-8').lower().replace(' ', '-')
        
        api_url = (
            f"https://www.heb.com.mx/api/catalog_system/pub/products/search/{slug}"
            f"?_q={quote_plus(self.query)}&map=ft&_from={_from}&_to={_to}"
        )
        
        self.logger.info(f"Realizando petición a la API, página {self.page + 1}")
        
        return scrapy.Request(
            url=api_url,
            callback=self.parse_api,
            errback=self.errback_api,
            cookies={'vtex_segment': self.segment_token}
        )

    def parse_api(self, response):
        # --- CORRECCIÓN CLAVE ---
        # Aceptamos tanto 200 (OK) como 206 (Contenido Parcial) como respuestas válidas.
        if response.status not in [200, 206]:
            self.logger.error(f"La API devolvió un error inesperado {response.status}. Contenido: {response.text}")
            return
            
        try:
            products = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"No se pudo decodificar el JSON de la API. Status: {response.status}, Contenido: {response.text}")
            return

        if not products:
            self.logger.info("No se encontraron más productos. Finalizando.")
            return

        for product in products:
            if self.scraped_count >= self.max_products:
                self.logger.info(f"Límite de {self.max_products} productos alcanzado. Finalizando.")
                return

            items = product.get('items', [])
            if not items: continue
            
            sellers = items[0].get('sellers', [])
            if not sellers: continue
            
            price_info = sellers[0].get('commertialOffer', {})
            images = items[0].get('images', [])
            image_url = images[0].get('imageUrl') if images else None
            
            yield {
                'titulo': product.get('productName'),
                'precio': price_info.get('Price'),
                'url_imagen': image_url,
            }
            self.scraped_count += 1
        
        if self.scraped_count < self.max_products:
            self.page += 1
            yield self._make_api_request()

    def errback_api(self, failure):
        self.logger.error(f"Error al contactar la API: {failure.value}")