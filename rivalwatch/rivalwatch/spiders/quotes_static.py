import datetime as dt
import scrapy

class QuotesStaticSpider(scrapy.Spider):
    name = "quotes_static"
    allowed_domains = ["quotes.toscrape.com"]
    start_urls = ["https://quotes.toscrape.com/"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.25,      # sed amable con el sitio
        "AUTOTHROTTLE_ENABLED": True # regula la velocidad
    }

    def parse(self, response):
        for q in response.css(".quote"):
            title = q.css(".text::text").get()
            author = q.css(".author::text").get()
            yield {
                "store": "quotes-static",
                "sku": f"Q-{author}-{hash(title)%100000}",
                "title": title,
                "price": 10.0,              # demo
                "currency": "USD",          # demo
                "in_stock": True,           # demo
                "url": response.url,
                "ts": dt.datetime.utcnow().isoformat(),
            }

        next_page = response.css("li.next a::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)