import json
import time
from urllib.parse import urlencode

import scrapy


def _now_ms():
    return int(time.time() * 1000)


class WalmartGraphQLSpider(scrapy.Spider):
    name = "walmart_mx_graphql"
    custom_settings = {
        # No necesitamos Playwright
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 0.3,
        "DEFAULT_REQUEST_HEADERS": {
            # Headers realistas para parecer navegador
            "Accept": "*/*",
            "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
            "Content-Type": "application/json",
            "Origin": "https://super.walmart.com.mx",
            "Referer": "https://super.walmart.com.mx/",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
            ),
        },
        "FEED_EXPORT_ENCODING": "utf-8",
        "RETRY_TIMES": 3,
        "AUTOTHROTTLE_ENABLED": True,
    }

    # parámetros CLI
    # scrapy crawl walmart_mx_graphql -O out.jsonl -a query="Harina" -a max_products=50
    def __init__(self, query: str, max_products: int = 100, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_query = query
        self.max_products = int(max_products)

        # ====== GraphQL strings ======
        # 1) Query de búsqueda: trae resultados con campos básicos (id, nombre, precios, url)
        #    Esta query es compacta para minimizar fricción; si quieres más campos, agrégalos.
        self.SEARCH_QUERY = """
        query SearchProducts($query: String!, $from: Int!, $size: Int!) {
          search(query: $query, from: $from, size: $size) {
            total
            products {
              usItemId
              name
              canonicalUrl
              imageInfo { thumbnailUrl }
              priceInfo { currentPrice { price priceString } }
              availabilityStatus
              sellerName
            }
          }
        }
        """.strip()

        # 2) Query de detalle por ItemId (basada en tu “ItemById”, recortada a lo esencial)
        #    Si necesitas más campos, añádelos en los fragmentos de abajo.
        self.ITEM_BY_ID_QUERY = """
        query ItemById(
          $iId: String!,
          $pageType: String!,
          $tenant: String!,
          $version: String = "v1",
          $postProcessingVersion: Int = 1
        ) {
          product(itemId: $iId, selected: true) {
            id
            usItemId
            name
            canonicalUrl
            imageInfo { thumbnailUrl allImages { id url } }
            availabilityStatus
            priceInfo {
              currentPrice { price priceString }
              listPrice    { price priceString }
              wasPrice     { price priceString }
            }
            brand
            sellerName
          }
          seoItemMetaData(id: $iId) {
            metaTitle
            metaDescription
            canonicalURL
          }
        }
        """.strip()

    # endpoints
    SEARCH_ENDPOINT = "https://super.walmart.com.mx/api/graphql"
    ITEM_ENDPOINT_TMPL = "https://super.walmart.com.mx/orchestra/graphql/ip/{item_id}"

    def start_requests(self):
        # Paginamos en chunks de 24 (o el tamaño que prefieras)
        page_size = 24
        fetched = 0
        from_ = 0

        while fetched < self.max_products:
            left = self.max_products - fetched
            size = page_size if left > page_size else left

            variables = {"query": self.search_query, "from": from_, "size": size}
            payload = {
                "operationName": "SearchProducts",
                "query": self.SEARCH_QUERY,
                "variables": variables,
            }

            yield scrapy.Request(
                url=self.SEARCH_ENDPOINT,
                method="POST",
                body=json.dumps(payload),
                callback=self.parse_search,
                headers={"Content-Type": "application/json"},
                cb_kwargs={"from_": from_, "size": size},
            )

            fetched += size
            from_ += size

            # Evita enviar demasiadas páginas si el total es menor (lo ajustamos en parse_search)
            # Cortaremos el loop si ya no hay más productos.

    def parse_search(self, response, from_, size):
        try:
            data = response.json()
        except Exception:
            self.logger.warning("Respuesta de búsqueda no-JSON (%s)", response.text[:200])
            return

        # Navega al payload
        search = (data.get("data") or {}).get("search") or {}
        products = search.get("products") or []
        total = search.get("total") or 0

        if not products:
            self.logger.warning("Sin resultados en búsqueda para '%s' (from=%s, size=%s)",
                                self.search_query, from_, size)
            return

        # Para cada producto, emitimos request de detalle (ItemById)
        for p in products:
            us_item_id = p.get("usItemId")
            if not us_item_id:
                continue

            # Variables mínimas para ItemById (tenant MX y page type global)
            variables = {
                "iId": str(us_item_id),
                "pageType": "ItemPageGlobal",
                "tenant": "WALMART-MX",
                # Dejar defaults para 'version' y 'postProcessingVersion'
            }

            payload = {
                "operationName": "ItemById",
                "query": self.ITEM_BY_ID_QUERY,
                "variables": variables,
            }

            url = self.ITEM_ENDPOINT_TMPL.format(item_id=us_item_id)
            yield scrapy.Request(
                url=url,
                method="POST",
                body=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                callback=self.parse_item,
                cb_kwargs={"search_hit": p},
            )

        # Si el total real es menor a lo solicitado, no seguimos paginando
        if from_ + size >= total:
            return

    def parse_item(self, response, search_hit):
        try:
            data = response.json()
        except Exception:
            self.logger.warning("Detalle no-JSON para %s: %s", search_hit.get("usItemId"), response.text[:200])
            return

        prod = ((data.get("data") or {}).get("product")) or {}
        seo = ((data.get("data") or {}).get("seoItemMetaData")) or {}

        # Campos de salida unificados (mezcla de hit de búsqueda + detalle)
        yield {
            "source": "walmart_mx",
            "query": self.search_query,
            "us_item_id": prod.get("usItemId") or search_hit.get("usItemId"),
            "name": prod.get("name") or search_hit.get("name"),
            "brand": prod.get("brand"),
            "price": (prod.get("priceInfo") or {}).get("currentPrice", {}).get("price"),
            "price_string": (prod.get("priceInfo") or {}).get("currentPrice", {}).get("priceString"),
            "list_price": (prod.get("priceInfo") or {}).get("listPrice", {}).get("price"),
            "was_price": (prod.get("priceInfo") or {}).get("wasPrice", {}).get("price"),
            "availability": prod.get("availabilityStatus") or search_hit.get("availabilityStatus"),
            "seller": prod.get("sellerName") or search_hit.get("sellerName"),
            "canonical_url": (
                (prod.get("canonicalUrl"))
                or (seo.get("canonicalURL"))
                or search_hit.get("canonicalUrl")
            ),
            "thumbnail": (
                ((prod.get("imageInfo") or {}).get("thumbnailUrl"))
                or ((search_hit.get("imageInfo") or {}).get("thumbnailUrl"))
            ),
            "images": ((prod.get("imageInfo") or {}).get("allImages")) or [],
            "seo_title": seo.get("metaTitle"),
            "seo_description": seo.get("metaDescription"),
            "ts": _now_ms(),
        }
