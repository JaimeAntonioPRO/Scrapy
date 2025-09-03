# rivalwatch/spiders/walmart.py
import json
import re
from urllib.parse import urlencode, urljoin, urlparse, quote_plus

import scrapy
from scrapy import Request
from scrapy_playwright.page import PageMethod


class WalmartMxSpider(scrapy.Spider):
    name = "walmart_mx"
    allowed_domains = ["super.walmart.com.mx", "walmart.com.mx"]

    custom_settings = {
        # Playwright
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 60_000,
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": True,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        },
        "PLAYWRIGHT_CONTEXT_ARGS": {
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
            "java_script_enabled": True,
            "ignore_https_errors": True,
            "locale": "es-MX",
            "timezone_id": "America/Mexico_City",
            "extra_http_headers": {
                "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Upgrade-Insecure-Requests": "1",
            },
        },

        # Scrapy
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 0.5,
        "RETRY_TIMES": 3,

        # Export
        "FEED_EXPORT_ENCODING": "utf-8",
    }

    def add_options(self, parser):
        parser.add_argument("-a", "--query", help="Texto a buscar (p.ej. Harina)")
        parser.add_argument(
            "-a", "--max_products", type=int, default=100, help="Límite de productos"
        )

    def __init__(self, query=None, max_products=100, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = (query or "").strip()
        self.max_products = int(max_products or 100)
        self._seen = set()
        self._emitted = 0

    # ---- Playwright helpers -------------------------------------------------

    async def _route_block_noise(self, route):
        req = route.request
        rtype = req.resource_type
        url = req.url
        if rtype in ("image", "media", "font"):
            return await route.abort()
        if any(s in url for s in (
            "doubleclick.net",
            "googletagmanager.com",
            "google-analytics.com",
            "facebook.net",
            "hotjar.com",
        )):
            return await route.abort()
        return await route.continue_()

    def _playwright_methods(self):
        """Esperas genéricas + auto-scroll para disparar contenido perezoso."""
        return [
            PageMethod(
                "add_init_script",
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});",
            ),
            PageMethod("route", "**/*", self._route_block_noise),
            PageMethod("wait_for_load_state", "domcontentloaded"),
            PageMethod("wait_for_load_state", "networkidle"),
            # Auto-scroll simple
            PageMethod(
                "evaluate",
                "() => new Promise(res => {"
                "  let y=0; const step=900;"
                "  const id=setInterval(()=>{"
                "    window.scrollBy(0, step); y+=step;"
                "    if (y >= document.body.scrollHeight) { clearInterval(id); setTimeout(res, 600); }"
                "  }, 180);"
                "})",
            ),
            PageMethod("wait_for_timeout", 800),
        ]

    # ---- Crawl flow ---------------------------------------------------------

    def start_requests(self):
        if not self.query:
            self.logger.warning("No se proporcionó -a query='...'  (p.ej. Harina)")
            return

        base = "https://super.walmart.com.mx/search"
        url = f"{base}?{urlencode({'q': self.query})}"

        yield Request(
            url,
            callback=self.parse_search,
            meta={
                "playwright": True,
                "playwright_page_methods": self._playwright_methods(),
                "search_url": url,
                "page_no": 1,
            },
            headers={"Accept": "text/html,application/xhtml+xml"},
            dont_filter=True,
        )

    def parse_search(self, response):
        """Extrae URLs de producto desde la página de resultados."""
        page_no = response.meta.get("page_no", 1)

        # Posible muro anti-bots: HTML 200 pero sin grid
        if any(s in response.text.lower() for s in ("captcha", "verify you are human", "acceso denegado")):
            self.logger.warning(f"[P{page_no}] Posible bot wall en {response.url}. Considera usar proxy residencial.")
            return

        product_urls = set()

        # 1) Anchors que terminan en /p
        for href in response.css('a[href$="/p"]::attr(href)').getall():
            product_urls.add(urljoin(response.url, href))

        # 2) __NEXT_DATA__ (Next.js)
        next_data = response.css('script#__NEXT_DATA__::text').get()
        if next_data:
            try:
                data = json.loads(next_data)

                def walk(obj):
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            if k in ("link", "url") and isinstance(v, str) and v.endswith("/p"):
                                product_urls.add(urljoin(response.url, v))
                            else:
                                walk(v)
                    elif isinstance(obj, list):
                        for it in obj:
                            walk(it)

                walk(data)
            except Exception:
                pass

        # 3) Fallback: regex "/algo/p"
        if not product_urls:
            for m in re.finditer(r'"/([^"]+?/p)"', response.text):
                product_urls.add(urljoin(response.url, m.group(1)))

        if not product_urls:
            self.logger.warning(f"[P{page_no}] No se encontraron productos en {response.url}")

        # Encola detalle de producto
        for url in product_urls:
            if self._emitted >= self.max_products:
                break
            if url in self._seen:
                continue
            self._seen.add(url)
            yield Request(
                url,
                callback=self.parse_product,
                meta={
                    "playwright": True,
                    "playwright_page_methods": self._playwright_methods(),
                },
                headers={"Accept": "text/html,application/xhtml+xml"},
                dont_filter=True,
            )

        # Paginación robusta
        if self._emitted < self.max_products:
            next_url = None

            # a) <link rel="next">
            next_href = response.css('link[rel="next"]::attr(href)').get()
            if next_href:
                next_url = urljoin(response.url, next_href)

            # b) <a aria-label="Siguiente"> (CSS sin el flag i)
            if not next_url:
                a_next = response.css('a[aria-label*="Siguiente"]::attr(href)').get()
                if a_next:
                    next_url = urljoin(response.url, a_next)

            # c) XPath case-insensitive con translate()
            if not next_url:
                a_next = response.xpath(
                    '//a[contains(translate(@aria-label,"SIENTEAG","sienteag"),"siguiente")]/@href'
                ).get()
                if a_next:
                    next_url = urljoin(response.url, a_next)

            # d) Construcción manual: ?q=...&page=N
            if not next_url:
                base_search = response.meta.get("search_url") or response.url
                q = self.query or ""
                next_url = f"https://super.walmart.com.mx/search?q={quote_plus(q)}&page={page_no+1}"

            if next_url:
                yield Request(
                    next_url,
                    callback=self.parse_search,
                    meta={
                        "playwright": True,
                        "playwright_page_methods": self._playwright_methods(),
                        "search_url": response.meta.get("search_url") or response.url,
                        "page_no": page_no + 1,
                    },
                    headers={"Accept": "text/html,application/xhtml+xml"},
                    dont_filter=True,
                )

    def parse_product(self, response):
        """Extrae datos del detalle de producto."""
        url = response.url

        # Preferimos JSON-LD si existe
        data = self._collect_jsonld(response)

        title = self._first_nonempty(
            data.get("name"),
            response.css("h1::text").get(),
            response.css('meta[property="og:title"]::attr(content)').get(),
            response.css('meta[name="twitter:title"]::attr(content)').get(),
        )

        price_raw = None
        currency = "MXN"
        in_stock = None

        offers = data.get("offers") or {}
        if isinstance(offers, dict):
            price_raw = offers.get("price") or (offers.get("priceSpecification") or {}).get("price")
            currency = offers.get("priceCurrency", currency)
            availability = (offers.get("availability") or "").lower()
            if "instock" in availability:
                in_stock = True
            elif "outofstock" in availability:
                in_stock = False

        if price_raw is None:
            price_raw = self._find_price_in_html(response.text)

        price = None
        if price_raw is not None:
            try:
                price = float(str(price_raw).replace(",", "").strip())
            except Exception:
                price = None

        sku = data.get("sku") or self._sku_from_url(url)

        if in_stock is None:
            txt = response.text.lower()
            if "agotado" in txt or "sin existencias" in txt:
                in_stock = False
            elif "agregar al carrito" in txt or "añadir al carrito" in txt:
                in_stock = True

        item = {
            "store": "walmart",
            "url": url,
            "title": (title or "").strip(),
            "sku": sku,
            "price": price,
            "price_raw": price_raw,
            "currency": currency or "MXN",
            "in_stock": bool(in_stock) if in_stock is not None else None,
        }

        self._emitted += 1
        yield item

    # ---- Utilities ----------------------------------------------------------

    def _collect_jsonld(self, response):
        scripts = response.css('script[type="application/ld+json"]::text').getall()
        for s in scripts:
            s = s.strip()
            if not s:
                continue
            try:
                data = json.loads(s)
            except Exception:
                try:
                    s_fixed = "[" + ",".join(part for part in re.split(r"}\s*{", s.replace("\n", " ")) if part) + "]"
                    data = json.loads(s_fixed)
                except Exception:
                    continue

            candidates = data if isinstance(data, list) else [data]
            for obj in candidates:
                if not isinstance(obj, dict):
                    continue
                typ = (obj.get("@type") or obj.get("type") or "")
                if isinstance(typ, list):
                    typ = " ".join(typ)
                if "Product" in str(typ):
                    return obj
        return {}

    def _sku_from_url(self, url: str):
        try:
            path = urlparse(url).path.rstrip("/")
            if not path.endswith("/p"):
                return None
            slug = path.split("/")[-2]
            m = re.search(r"(\d+)$", slug)
            return m.group(1) if m else slug
        except Exception:
            return None

    def _first_nonempty(self, *vals):
        for v in vals:
            if v:
                v = str(v).strip()
                if v:
                    return v
        return None

    def _find_price_in_html(self, html: str):
        snippets = []
        for kw in ("price", "Precio", "precio", "sales", "offer", "Oferta", "oferta"):
            for m in re.finditer(r".{0,80}" + re.escape(kw) + r".{0,80}", html, flags=re.I | re.S):
                snippets.append(m.group(0))
        if not snippets:
            snippets = [html[:50000]]

        price_re = re.compile(
            r"\$?\s*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})|[0-9]+(?:[.,][0-9]{2})?)",
            flags=re.M,
        )
        for chunk in snippets:
            for m in price_re.finditer(chunk):
                raw = m.group(1)
                if raw.count(",") > 1 and "." not in raw:
                    raw = raw.replace(".", "").replace(",", ".")
                return raw
        return None
