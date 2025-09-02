import json
import re
from decimal import Decimal, InvalidOperation
import scrapy
from scrapy_playwright.page import PageMethod

# --- al tope del archivo, después de imports ---
PRODUCT_URL_RE = re.compile(r"/\d+\.html(?:\?.*)?$", re.IGNORECASE)
NON_PRODUCT_BLOCKLIST = (
    "/static-pages/",
    "/recarga-",
    "/responsabilidad-social",
    "/terminos-",
    "/aviso-de-privacidad",
    "/facturacion",
)

def _looks_like_product_url(url: str) -> bool:
    if any(bad in url for bad in NON_PRODUCT_BLOCKLIST):
        return False
    return bool(PRODUCT_URL_RE.search(url))

def _sku_from_url(url: str) -> str | None:
    m = re.search(r"/(\d+)\.html", url)
    return m.group(1) if m else None

# ---------- helpers ----------
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

def _regex_product_links(html: str) -> list[str]:
    """
    Fallback súper robusto: pesca URLs que terminen en .html y parezcan ficha.
    Ej: /aceite-.../1919311.html
    """
    links = set()
    for m in re.finditer(r'href="([^"]+?/\d+\.html)"', html):
        links.add(m.group(1))
    # A veces vienen sin el /digits.html, intenta .html genérico:
    for m in re.finditer(r'href="([^"]+?\.html)"', html):
        links.add(m.group(1))
    return list(links)

# ---------- spider ----------
class SorianaSpider(scrapy.Spider):
    name = "soriana"
    allowed_domains = ["soriana.com"]

    # Ajustes específicos del spider para Playwright y timeouts
    custom_settings = {
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True, "args": ["--no-sandbox", "--disable-gpu"]},
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 90000,  # 90s
        "PLAYWRIGHT_PAGE_GOTO_WAIT_UNTIL": "domcontentloaded",
        # Mantén tu throttling global en settings.py
    }

    start_urls = [
        "https://www.soriana.com/despensa/",
    ]

    # --------- ARRANQUE ---------
    def start_requests(self):
        # A) URLs específicas de producto (más rápido, sin JS)
        product_urls = getattr(self, "product_urls", None)
        if product_urls:
            for url in [u.strip() for u in product_urls.split(",") if u.strip()]:
                yield scrapy.Request(url, callback=self.parse_product, meta={"playwright": False})
            return

        # B) Búsquedas por query (usa JS para asegurar listado)
        query = getattr(self, "query", None)
        if query:
            for q in [t.strip() for t in query.split(",") if t.strip()]:
                url = f"https://www.soriana.com/search/?text={q.replace(' ', '+')}"
                yield scrapy.Request(
                    url,
                    callback=self.parse_category,
                    meta={
                        "playwright": True,
                        "playwright_page_goto_kwargs": {"wait_until": "domcontentloaded", "timeout": 60000},
                        "playwright_page_methods": [
                            PageMethod("wait_for_load_state", "domcontentloaded"),
                            PageMethod("wait_for_load_state", "networkidle"),
                            PageMethod("wait_for_timeout", 1200),
                        ],
                        "page_no": 1,
                    },
                )
            return

        # C) Categorías (como /despensa/)
        max_pages = int(getattr(self, "max_pages", 1))
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse_category,
                meta={
                    "playwright": True,
                    "playwright_page_goto_kwargs": {"wait_until": "domcontentloaded", "timeout": 60000},
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "domcontentloaded"),
                        PageMethod("wait_for_load_state", "networkidle"),
                        PageMethod("wait_for_timeout", 1200),
                    ],
                    "page_no": 1,
                    "max_pages": max_pages,
                },
            )

    # --------- LISTA / CATEGORÍA ---------
    def parse_category(self, response):
        page_no = response.meta.get("page_no", 1)
        max_pages = int(response.meta.get("max_pages", 1))

        # 1) Intenta selectores típicos VTEX
        product_link_selectors = [
            'a.vtex-product-summary-2-x-clearLink::attr(href)',
            'a.product-card__link::attr(href)',
            'a.product-item__link::attr(href)',
            'section [data-sku] a::attr(href)',
            'a[data-testid="productSummaryLink"]::attr(href)',
            'a[href*="/p/"]::attr(href)',
            'a[href$=".html"]::attr(href)',
        ]

        seen = set()
        for sel in product_link_selectors:
            for href in response.css(sel).getall():
                href = href.strip()
                if not href:
                    continue
                url = response.urljoin(href)
                if url in seen:
                    continue
                # nuevo: filtra solo fichas válidas
                if not _looks_like_product_url(url):
                    continue
                seen.add(url)
                yield scrapy.Request(url, callback=self.parse_product, meta={"playwright": False})

        # 2) Si no encontró nada, reintenta UNA vez con JS largo esperando algún patrón
        if not seen and not response.meta.get("js_retried"):
            yield response.request.replace(
                meta={
                    **response.meta,
                    "playwright": True,
                    "js_retried": True,
                    "playwright_page_goto_kwargs": {"wait_until": "domcontentloaded", "timeout": 90000},
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "domcontentloaded"),
                        PageMethod("wait_for_load_state", "networkidle"),
                        # prueba varios selectores comunes; si alguno aparece, seguimos
                        PageMethod("wait_for_selector", 'a[href$=".html"]', timeout=25000),
                    ],
                },
                dont_filter=True,
            )
            return

        # 3) Fallback: regex en el HTML crudo (agarra /xxxx.html)
        if not seen:
              for href in _regex_product_links(response.text):
                url = response.urljoin(href)
                if url in seen:
                    continue
                if not _looks_like_product_url(url):
                    continue
                seen.add(url)
                yield scrapy.Request(url, callback=self.parse_product, meta={"playwright": False})

        # 4) Paginación (si el spider recibió max_pages>1)
        if page_no < max_pages:
            # Busca rel=next u otros:
            next_selectors = [
                'a[rel="next"]::attr(href)',
                'link[rel="next"]::attr(href)',
                'a.pagination-next::attr(href)',
                'a.pagination__next::attr(href)',
                'a[aria-label="Siguiente"]::attr(href)',
            ]
            next_url = None
            for nsel in next_selectors:
                next_url = response.css(nsel).get()
                if next_url:
                    break

            # Si no hay rel=next visible, intenta patrón de ?page=
            if not next_url:
                m = re.search(r'[?&]page=(\d+)', response.url)
                next_page = (int(m.group(1)) + 1) if m else (page_no + 1)
                base = re.sub(r'([?&])page=\d+', r'\1', response.url)  # limpia page existente
                sep = '&' if '?' in base else '?'
                next_url = f"{base}{sep}page={next_page}"

            yield scrapy.Request(
                response.urljoin(next_url),
                callback=self.parse_category,
                meta={
                    "playwright": True,
                    "playwright_page_goto_kwargs": {"wait_until": "domcontentloaded", "timeout": 60000},
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "domcontentloaded"),
                        PageMethod("wait_for_load_state", "networkidle"),
                        PageMethod("wait_for_timeout", 800),
                    ],
                    "page_no": page_no + 1,
                    "max_pages": max_pages,
                },
            )

    # --------- FICHA ---------
    def parse_product(self, response):
        # precio desde input oculto
        price_raw = response.css('input#clevertap-price::attr(value)').get()
        price = _to_float(price_raw)

        # JSON-LD fallback
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

        # Si aún nada y veníamos sin JS, reintenta 1 vez con JS (por si el input lo inyecta el frontend)
        tried_js = response.meta.get("tried_js")

# si la URL no es de ficha, no reintentes con JS (evita timeouts en páginas informativas)
        if price is None and not tried_js and _looks_like_product_url(response.url):
            yield response.request.replace(
                meta={
                    "playwright": True,
                    "tried_js": True,
                    "playwright_page_goto_kwargs": {"wait_until": "domcontentloaded", "timeout": 60000},
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "domcontentloaded"),
                        PageMethod("wait_for_load_state", "networkidle"),
                        PageMethod("wait_for_selector", "#clevertap-price", timeout=20000),
                    ],
                },
                dont_filter=True,
            )
            return

        currency = _extract_currency(response) or "MXN"
        title = (response.css("h1::text").get() or "").strip()

        # SKU robusto: data-sku, meta og, JSON-LD u otro texto
        sku = (
            response.css("[data-sku]::attr(data-sku)").get()
            or response.css('meta[property="product:retailer_item_id"]::attr(content)').get()
            or response.css(".sku::text").get()
            or ""
        )
        sku = sku.strip() if sku else sku

        # Último intento de SKU vía JSON-LD
        if not sku:
            for txt in response.css('script[type="application/ld+json"]::text').getall():
                try:
                    data = json.loads(txt)
                    nodes = data if isinstance(data, list) else [data]
                    for n in nodes:
                        if isinstance(n, dict):
                            s = n.get("sku")
                            if s:
                                sku = str(s).strip()
                                break
                    if sku:
                        break
                except Exception:
                    pass

        in_stock = bool(response.css(".in-stock, .available, [data-availability='inStock']"))

        yield {
            "store": "soriana",
            "url": response.url,
            "title": title,
            "sku": sku,
            "price": price,
            "price_raw": price_raw,
            "currency": currency,
            "in_stock": in_stock,
        }