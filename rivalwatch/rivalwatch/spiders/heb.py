# heb.py (Versión final con Playwright)
import json
import re
from decimal import Decimal, InvalidOperation
from urllib.parse import quote_plus
import scrapy
from scrapy_playwright.page import PageMethod # Importamos PageMethod
import unicodedata

def _to_float(x: str) -> float | None:
    if not x:
        return None
    s = str(x).strip().replace("$", "").replace(",", "")
    if not s:
        return None
    try:
        return float(Decimal(s))
    except (InvalidOperation, TypeError):
        return None

class HebSpider(scrapy.Spider):
    name = "heb"
    allowed_domains = ["heb.com.mx"]

    def start_requests(self):
        self.scraped_count = 0
        self.max_products = int(getattr(self, "max_products", 50))
        
        query = getattr(self, "query", None)
        if not query:
            self.logger.error("No se proporcionó un término de búsqueda ('query').")
            return

        slug = unicodedata.normalize('NFKD', query).encode('ascii', 'ignore').decode('utf-8').lower().replace(' ', '-')
        url = f"https://www.heb.com.mx/{slug}?_q={quote_plus(query)}&map=ft"
        
        # Usamos Playwright para la página de listado para asegurar que cargue
        yield scrapy.Request(
            url, 
            callback=self.parse_listing,
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "section a[href*='/p']", timeout=30000),
                ],
            }
        )

    def parse_listing(self, response):
        product_links = response.css('section a[href*="/p"]::attr(href)').getall()
        
        for link in product_links:
            if self.scraped_count >= self.max_products:
                self.logger.info(f"Límite de {self.max_products} productos alcanzado.")
                return 

            full_url = response.urljoin(link)
            
            # Usamos Playwright para la página de producto, esperando a que el precio aparezca
            yield scrapy.Request(
                full_url, 
                callback=self.parse_product,
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        # Esta es la clave: espera a que CUALQUIERA de estos selectores de precio esté visible
                        PageMethod("wait_for_selector", "div.price, span[class*='currencyInteger'], span[class*='sellingPrice']", timeout=20000),
                    ],
                }
            )
            self.scraped_count += 1
            
        # Lógica de paginación
        if self.scraped_count < self.max_products:
            next_page = response.css('a[rel="next"]::attr(href)').get()
            if next_page:
                yield response.follow(next_page, callback=self.parse_listing, meta={"playwright": True})

    def parse_product(self, response):
        title = (response.css("h1 span[class*='productName']::text").get() or 
                 response.css('meta[property="og:title"]::attr(content)').get() or "").strip()

        image_url = (response.css('img[class*="productImageTag"]::attr(src)').get() or 
                     response.css('meta[property="og:image"]::attr(content)').get())
        
        price = None
        # Como ya esperamos a que el precio cargue, ahora la extracción es mucho más fiable
        
        # Intento 1: Precio dividido (entero + fracción)
        integer_part = response.css("span[class*='currencyInteger']::text").get()
        if integer_part:
            fraction_part = response.css("span[class*='currencyFraction']::text").get() or "00"
            price = _to_float(f"{integer_part}.{fraction_part}")

        # Intento 2: Precio en un solo contenedor
        if not price:
            price_text = response.css("div.price ::text").getall()
            if price_text:
                price = _to_float("".join(price_text))

        # Intento 3: Otro contenedor común
        if not price:
             price_text = response.css("span[class*='sellingPrice']::text").get()
             price = _to_float(price_text)

        yield {
            'titulo': title,
            'precio': price,
            'url_imagen': image_url,
        }