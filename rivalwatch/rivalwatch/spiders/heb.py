# heb.py (Scrapy puro, sin Playwright)
import json
import re
from decimal import Decimal, InvalidOperation
from urllib.parse import urlparse, quote_plus
import scrapy
# añade arriba:
import unicodedata
from urllib.parse import quote_plus

def _slug_for_path(q: str) -> str:
    # quita acentos -> ascii
    s = unicodedata.normalize("NFKD", q)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    # reemplaza no alfanum por guiones
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or "buscar"


def _to_float(x: str) -> float | None:
    if not x:
        return None
    s = x.strip().replace("\xa0", " ").replace("\u202f", " ").replace(",", "")
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
    cur = response.css('meta[property="product:price:currency"]::attr(content)').get()
    if cur:
        return cur.strip()
    cur = response.css('meta[itemprop="priceCurrency"]::attr(content)').get()
    if cur:
        return cur.strip()
    for txt in response.css('script[type="application/ld+json"]::text').getall():
        try:
            data = json.loads(txt)
            nodes = data if isinstance(data, list) else [data]
            for n in nodes:
                if isinstance(n, dict):
                    offers = n.get("offers")
                    if isinstance(offers, dict) and offers.get("priceCurrency"):
                        return str(offers["priceCurrency"]).strip()
        except Exception:
            pass
    return None


def _extract_sku_from_url(url: str) -> str | None:
    path = urlparse(url).path or ""
    for pat in (r"/(\d+)/p(?:[/?]|$)", r"-([0-9]{5,})/p(?:[/?]|$)"):
        m = re.search(pat, path)
        if m:
            return m.group(1)
    return None


def _extract_sku_any(response, url: str) -> str | None:
    sku = _extract_sku_from_url(url)
    if sku:
        return sku
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
    return None


class HebSpider(scrapy.Spider):
    name = "heb"
    allowed_domains = ["heb.com.mx"]

    _SUBDEPT_SLUGS = [
        "aceites-y-mantecas",
        "alimentos-enlatados-y-conservas",
        "sopas-y-pastas",
        "aderezos-y-salsas",
        "horneado-y-reposteria",
        "galletas",
        "cafe",
        "desechables",
        "untables-y-miel",
        "especias-y-condimentos",
        "arroz-frijol-y-semillas",
        "caldos-y-concentrados",
    ]
    start_urls = [f"https://www.heb.com.mx/{slug}" for slug in _SUBDEPT_SLUGS]

    custom_settings = {
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 0.5,
        "RETRY_TIMES": 3,
        "ROBOTSTXT_OBEY": True,
        "FEED_EXPORT_ENCODING": "utf-8",
        # Cabeceras normales para evitar compresiones raras del servidor
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    }

    def start_requests(self):
        self.max_products = int(getattr(self, "max_products", 0)) or None
        self.max_pages = int(getattr(self, "max_pages", 0)) or None

        product_urls = getattr(self, "product_urls", None)
        if product_urls:
            for url in [u.strip() for u in product_urls.split(",") if u.strip()]:
                yield scrapy.Request(url, callback=self.parse_product)
            return

        query = getattr(self, "query", None)
        if query:
            for q in [t.strip() for t in query.split(",") if t.strip()]:
                slug = _slug_for_path(q)
                url = f"https://www.heb.com.mx/{slug}?_q={quote_plus(q)}&map=ft"
                yield scrapy.Request(url, callback=self.parse_listing, dont_filter=True)
            return
        
        
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse_listing)

    # ---------- LISTADO ----------
    def parse_listing(self, response):
        yielded = 0
        link_sels = [
            'a[href$="/p"]::attr(href)',
            'a.product-card__link::attr(href)',
            'a[data-testid="productSummaryLink"]::attr(href)',
            'a[href*="/p?"]::attr(href)',
        ]
        seen = set()
        for sel in link_sels:
            for href in response.css(sel).getall():
                url = response.urljoin(href.strip())
                if not (url.endswith("/p") or "/p?" in url):
                    continue
                if url in seen:
                    continue
                seen.add(url)
                yield scrapy.Request(url, callback=self.parse_product)
                yielded += 1
                if self.max_products and yielded >= self.max_products:
                    break
            if self.max_products and yielded >= self.max_products:
                break

        if (not self.max_products) or (yielded < self.max_products):
            next_url = self._find_next_page(response)
            if next_url and (not self.max_pages or response.meta.get("page_no", 1) < self.max_pages):
                yield scrapy.Request(
                    response.urljoin(next_url),
                    callback=self.parse_listing,
                    meta={"page_no": response.meta.get("page_no", 1) + 1},
                )

    def _find_next_page(self, response) -> str | None:
        for sel in (
            'a[rel="next"]::attr(href)',
            'link[rel="next"]::attr(href)',
            'a.pagination-next::attr(href)',
            'a.pagination__next::attr(href)',
            'a[aria-label="Siguiente"]::attr(href)',
        ):
            url = response.css(sel).get()
            if url:
                return url
        m = re.search(r'(?:[?&])page=(\d+)', response.url)
        if m:
            cur = int(m.group(1))
            return re.sub(r'(?:[?&])page=\d+', f'?page={cur+1}', response.url)
        return None

    # ---------- FICHA ----------
    def parse_product(self, response):
        # PRECIO (DOM)
        price_raw = None
        for sel in ("div.price ::text", "span.price ::text", '[itemprop="price"]::attr(content)'):
            texts = [t.strip() for t in response.css(sel).getall() if t.strip()]
            if texts:
                price_raw = " ".join(texts)
                break
        price = _to_float(price_raw)

        # PRECIO (JSON-LD) si hace falta
        if price is None:
            for txt in response.css('script[type="application/ld+json"]::text').getall():
                try:
                    data = json.loads(txt)
                    nodes = data if isinstance(data, list) else [data]
                    for n in nodes:
                        if isinstance(n, dict):
                            offers = n.get("offers")
                            if isinstance(offers, dict) and offers.get("price"):
                                price = _to_float(str(offers["price"]))
                                if price is not None:
                                    break
                    if price is not None:
                        break
                except Exception:
                    pass

        # Último recurso: buscar un blob JSON típico de VTEX con "price" numérico
        if price is None:
            m = re.search(r'"price"\s*:\s*("?[\d\.,]+"?)', response.text)
            if m:
                price = _to_float(m.group(1).strip('"'))

        # TÍTULO
        title = (response.css("h1::text").get() or "").strip()
        if not title:
            title = (response.css('meta[property="og:title"]::attr(content)').get() or "").strip()
        if not title:
            for txt in response.css('script[type="application/ld+json"]::text').getall():
                try:
                    data = json.loads(txt)
                    nodes = data if isinstance(data, list) else [data]
                    for n in nodes:
                        if isinstance(n, dict) and n.get("@type") == "Product":
                            nm = n.get("name")
                            if nm:
                                title = str(nm).strip()
                                break
                    if title:
                        break
                except Exception:
                    pass

        # SKU
        sku = _extract_sku_any(response, response.url)

        currency = _extract_currency(response) or "MXN"
        in_stock = bool(response.css(".in-stock, .available, [data-availability='inStock']"))

        yield {
            "store": "heb",
            "url": response.url,
            "title": title,
            "sku": sku,
            "price": price,
            "price_raw": price_raw,
            "currency": currency,
            "in_stock": in_stock,
        }