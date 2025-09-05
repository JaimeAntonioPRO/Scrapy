# soriana.py
import json
import re
from decimal import Decimal, InvalidOperation
from urllib.parse import urlparse, quote_plus
import scrapy
from scrapy_playwright.page import PageMethod


def _to_float(x: str) -> float | None:
    if not x:
        return None
    s = x.strip().replace("\xa0", " ").replace(",", "")
    try:
        return float(Decimal(s))
    except InvalidOperation:
        m = re.search(r"(\d+(?:[.,]\d{1,2})?)", x)
        if m:
            try:
                return float(Decimal(m.group(1).replace(",", ".")))
            except InvalidOperation:
                return None
    return None


def _extract_currency(response) -> str | None:
    cur = response.css('input#clevertap-currency::attr(value)').get()
    if cur:
        return cur.strip()
    cur = response.css('meta[property="product:price:currency"]::attr(content)').get()
    if cur:
        return cur.strip()
    for txt in response.css('script[type="application/ld+json"]::text').getall():
        try:
            data = json.loads(txt)
            nodes = data if isinstance(data, list) else [data]
            for n in nodes:
                if isinstance(n, dict):
                    offers = n.get("offers")
                    if isinstance(offers, dict):
                        cc = offers.get("priceCurrency")
                        if cc:
                            return str(cc).strip()
        except Exception:
            pass
    return None


def _extract_sku(response, url: str) -> str | None:
    # 1) atributo data-sku o nodos visibles
    sku = response.css("[data-sku]::attr(data-sku)").get()
    if sku:
        return sku.strip()
    sku = (response.css(".sku::text").get() or "").strip() or (response.css('[itemprop="sku"]::attr(content)').get() or "").strip()
    if sku:
        return sku

    # 2) JSON-LD
    for txt in response.css('script[type="application/ld+json"]::text').getall():
        try:
            data = json.loads(txt)
            nodes = data if isinstance(data, list) else [data]
            for n in nodes:
                if isinstance(n, dict):
                    pid = n.get("sku") or n.get("productID") or n.get("mpn")
                    if pid:
                        return str(pid).strip()
        except Exception:
            pass

    # 3) fallback: dígitos del final de la URL (VTEX suele tener .../1234567.html)
    path = urlparse(url).path or ""
    m = re.search(r"/(\d{5,})\.html", path)
    if m:
        return m.group(1)
    return None


def _title_contains_all_terms(title: str, terms: list[str]) -> bool:
    t = title.lower()
    return all(term.lower() in t for term in terms if term)


class SorianaSpider(scrapy.Spider):
    name = "soriana"
    allowed_domains = ["soriana.com"]

    # punto de entrada por defecto: departamento
    start_urls = [
        "https://www.soriana.com/despensa/",
    ]

    custom_settings = {
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 45_000,
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 0.5,
        "RETRY_TIMES": 3,
        "ROBOTSTXT_OBEY": True,
    }

    # -------- ARRANQUE --------
    def start_requests(self):
        self.max_products = int(getattr(self, "max_products", 0)) or None
        self.max_pages = int(getattr(self, "max_pages", 0)) or None
        self.require_terms = str(getattr(self, "require_terms", "false")).lower() in ("1", "true", "yes")

        product_urls = getattr(self, "product_urls", None)
        if product_urls:
            for url in [u.strip() for u in product_urls.split(",") if u.strip()]:
                yield scrapy.Request(url, callback=self.parse_product, meta={"playwright": False})
            return

        query = getattr(self, "query", None)
        if query:
            for q in [t.strip() for t in query.split(",") if t.strip()]:
                search_url = f"https://www.soriana.com/buscar?q={quote_plus(q)}"
                yield scrapy.Request(
                    search_url,
                    callback=self.parse_search,
                    cb_kwargs={"query_terms": [w for w in q.split() if w]},
                    meta={
                        "playwright": True,
                        "playwright_page_methods": [
                            PageMethod("route", "**/*", self._route_block_noise),
                            PageMethod("wait_for_load_state", "domcontentloaded"),
                            PageMethod("wait_for_load_state", "networkidle"),
                            PageMethod(
                                "wait_for_selector",
                                "a.vtex-product-summary-2-x-clearLink, a[data-testid='productSummaryLink'], a[href$='.html']",
                                timeout=20_000,
                            ),
                        ],
                    },
                    dont_filter=True,
                )
            return

        # Categorías por defecto (departamentos)
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse_category,
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        PageMethod("route", "**/*", self._route_block_noise),
                        PageMethod("wait_for_load_state", "domcontentloaded"),
                        PageMethod("wait_for_load_state", "networkidle"),
                        PageMethod("wait_for_timeout", 1200),
                    ],
                },
            )

    # -------- LISTADO: BÚSQUEDA --------
    def parse_search(self, response, query_terms: list[str]):
        yielded = 0
        for url in self._iter_product_links(response):
            if self.require_terms:
                card_title = (
                    response.xpath(f'//a[@href="{urlparse(url).path}"]//text()').get()
                    or ""
                ).strip()
                if card_title and not _title_contains_all_terms(card_title, query_terms):
                    continue
            yield scrapy.Request(url, callback=self.parse_product, meta={"playwright": False})
            yielded += 1
            if self.max_products and yielded >= self.max_products:
                break

        # Paginación
        if (not self.max_products) or (yielded < self.max_products):
            next_url = self._find_next_page(response)
            if next_url and (not self.max_pages or response.meta.get("page_no", 1) < self.max_pages):
                yield scrapy.Request(
                    response.urljoin(next_url),
                    callback=self.parse_search,
                    cb_kwargs={"query_terms": query_terms},
                    meta={
                        "playwright": True,
                        "page_no": response.meta.get("page_no", 1) + 1,
                        "playwright_page_methods": [
                            PageMethod("route", "**/*", self._route_block_noise),
                            PageMethod("wait_for_load_state", "domcontentloaded"),
                            PageMethod("wait_for_load_state", "networkidle"),
                            PageMethod("wait_for_timeout", 1000),
                        ],
                    },
                )

    # -------- LISTADO: CATEGORÍA --------
    def parse_category(self, response):
        yielded = 0
        for url in self._iter_product_links(response):
            yield scrapy.Request(url, callback=self.parse_product, meta={"playwright": False})
            yielded += 1
            if self.max_products and yielded >= self.max_products:
                break

        if (not self.max_products) or (yielded < self.max_products):
            next_url = self._find_next_page(response)
            if next_url and (not self.max_pages or response.meta.get("page_no", 1) < self.max_pages):
                yield scrapy.Request(
                    response.urljoin(next_url),
                    callback=self.parse_category,
                    meta={
                        "playwright": True,
                        "page_no": response.meta.get("page_no", 1) + 1,
                        "playwright_page_methods": [
                            PageMethod("route", "**/*", self._route_block_noise),
                            PageMethod("wait_for_load_state", "domcontentloaded"),
                            PageMethod("wait_for_load_state", "networkidle"),
                            PageMethod("wait_for_timeout", 1000),
                        ],
                    },
                )

    # -------- FICHA DE PRODUCTO --------
    def parse_product(self, response):
        # Precio desde input oculto
        price_raw = response.css('input#clevertap-price::attr(value)').get()
        price = _to_float(price_raw)

        # Fallback JSON-LD si no hay input
        if price is None:
            for txt in response.css('script[type="application/ld+json"]::text').getall():
                try:
                    data = json.loads(txt)
                    nodes = data if isinstance(data, list) else [data]
                    for n in nodes:
                        if isinstance(n, dict):
                            offers = n.get("offers")
                            if isinstance(offers, dict):
                                p = offers.get("price")
                                if p:
                                    price = _to_float(str(p))
                                    if price is not None:
                                        break
                    if price is not None:
                        break
                except Exception:
                    pass

        # Reintento único con JS si sigue sin precio
        tried_js = response.meta.get("tried_js")
        if price is None and not tried_js:
            yield response.request.replace(
                meta={
                    "playwright": True,
                    "tried_js": True,
                    "playwright_page_methods": [
                        PageMethod("route", "**/*", self._route_block_noise),
                        PageMethod("wait_for_load_state", "domcontentloaded"),
                        PageMethod("wait_for_load_state", "networkidle"),
                        PageMethod("wait_for_selector", "#clevertap-price", timeout=20_000),
                    ],
                },
                dont_filter=True,
            )
            return
        
        # === AÑADIDO: EXTRAER IMAGEN ===
        image_url = response.css('meta[property="og:image"]::attr(content)').get()

        currency = _extract_currency(response) or "MXN"
        title = (response.css("h1::text").get() or "").strip()
        sku = _extract_sku(response, response.url)
        in_stock = bool(response.css(".in-stock, .available, [data-availability='inStock']"))

        # === CAMBIO CLAVE: Usa los nombres de campo que espera la pipeline ===
        yield {
            'titulo': title,
            'precio': price,
            'url_imagen': image_url,
            # Los siguientes campos son extra, la pipeline no los usará
            "store": "soriana",
            "url": response.url,
            "sku": sku,
            "currency": currency,
        }

    # -------- Utilidades de listado --------
    def _iter_product_links(self, response):
        sels = [
            'a.vtex-product-summary-2-x-clearLink::attr(href)',
            'a.product-card__link::attr(href)',
            'a.product-item__link::attr(href)',
            'section [data-sku] a::attr(href)',
            'a[data-testid="productSummaryLink"]::attr(href)',
            'a[href$=".html"]::attr(href)',
        ]
        seen = set()
        for sel in sels:
            for href in response.css(sel).getall():
                href = href.strip()
                if not href:
                    continue
                abs_url = response.urljoin(href)
                if any(x in abs_url for x in ("/static-pages/", "/blog/", "/tiendas/", "/servicios/")):
                    continue
                if not abs_url.endswith(".html"):
                    continue
                if abs_url in seen:
                    continue
                seen.add(abs_url)
                yield abs_url

    def _find_next_page(self, response) -> str | None:
        for nsel in [
            'a[rel="next"]::attr(href)',
            'link[rel="next"]::attr(href)',
            'a.pagination-next::attr(href)',
            'a.pagination__next::attr(href)',
            'a[aria-label="Siguiente"]::attr(href)',
        ]:
            url = response.css(nsel).get()
            if url:
                return url
        m = re.search(r'(?:[?&])page=(\d+)', response.url)
        if m:
            cur = int(m.group(1))
            return re.sub(r'(?:[?&])page=\d+', f'?page={cur+1}', response.url)
        return None

    # -------- Playwright: bloquear ruido --------
    async def _route_block_noise(self, route, request):
        rtype = request.resource_type
        if rtype in ("image", "font", "media"):
            return await route.abort()
        url = request.url
        if any(d in url for d in ("google-analytics.com", "googletagmanager.com", "facebook.net", "doubleclick.net")):
            return await route.abort()
        return await route.continue_()