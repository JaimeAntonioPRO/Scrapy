BOT_NAME = "rivalwatch"
SPIDER_MODULES = ["rivalwatch.spiders"]
NEWSPIDER_MODULE = "rivalwatch.spiders"

# --- Playwright via Download Handler ---
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

# Reactor asyncio estándar
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

# Playwright
PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHT_LAUNCH_OPTIONS = {"headless": True}
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 45_000  # ms

# Ética / estabilidad
ROBOTSTXT_OBEY = True
AUTOTHROTTLE_ENABLED = True
CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 4
DOWNLOAD_DELAY = 0.5
RETRY_ENABLED = True
RETRY_TIMES = 3

# Evita el gzip raro en robots.txt
DEFAULT_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "RivalWatchBot/1.0 (Scrapy; monitoring; contact: [email protected])"
    ),
    "Accept-Encoding": "identity",
}

FEED_EXPORT_ENCODING = "utf-8"


# === AÑADE ESTO AL FINAL DEL ARCHIVO ===
# Activa el pipeline para que los datos se guarden en SQL Server
ITEM_PIPELINES = {
   'rivalwatch.pipelines.SqlServerPipeline': 300,
}