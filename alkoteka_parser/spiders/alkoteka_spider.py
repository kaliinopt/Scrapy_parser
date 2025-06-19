import scrapy
import time
from urllib.parse import urlparse
from alkoteka_parser.items import AlkotekaItem

CITY_UUID = "985b3eea-46b4-11e7-83ff-00155d026416"
PER_PAGE = 20


class AlkotekaSpider(scrapy.Spider):
    name = "alkoteka"
    allowed_domains = ["alkoteka.com"]
    start_urls = [
        "https://alkoteka.com/catalog/slaboalkogolnye-napitki-2",
        "https://alkoteka.com/catalog/vino",
        "https://alkoteka.com/catalog/krepkiy-alkogol",
    ]

    custom_settings = {
        "COOKIES_ENABLED": True,
        "FEEDS": {
            "items.json": {
                "format": "json",
                "encoding": "utf8",
                "indent": 4,
                "overwrite": True,
            },
        },
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "Referer": "https://alkoteka.com/",
        },
    }

    def start_requests(self):
        # Первый запрос для получения csrf и cookies
        yield scrapy.Request(
            url=f"https://alkoteka.com/web-api/v1/csrf-cookie?city_uuid={CITY_UUID}",
            callback=self.after_csrf,
            cookies={
                "city": "Krasnodar",
                "age_confirmed": "true",
            },
        )

    def after_csrf(self, response):
        # Обработка куки из заголовков Set-Cookie
        cookies = response.request.cookies.copy()
        set_cookie_header = response.headers.get("Set-Cookie", b"").decode("utf-8")

        for cookie_str in set_cookie_header.split(","):
            cookie_parts = cookie_str.split(";")[0].strip().split("=")
            if len(cookie_parts) == 2:
                key, value = cookie_parts
                if key in ["XSRF-TOKEN", "sid"]:
                    cookies[key] = value

        # По каждой категории запускаем API-запрос
        for url in self.start_urls:
            category = urlparse(url).path.strip("/").split("/")[-1]
            api_url = self._build_api_url(category, page=1)

            yield scrapy.Request(
                url=api_url,
                callback=self.parse_api,
                headers={
                    "Accept": "application/json",
                    "X-Requested-With": "XMLHttpRequest",
                },
                cookies=cookies,
                meta={"cookies": cookies, "page": 1, "category": category},
            )

    def _build_api_url(self, category_slug, page):
        return (
            f"https://alkoteka.com/web-api/v1/product?"
            f"city_uuid={CITY_UUID}&page={page}&per_page={PER_PAGE}&root_category_slug={category_slug}"
        )

    def parse_api(self, response):
        cookies = response.meta["cookies"]
        page = response.meta["page"]
        category = response.meta["category"]

        try:
            data = response.json()
        except Exception:
            self.logger.warning(f"Invalid JSON at page {page}, category {category}")
            return

        products = data.get("results") or []
        if not isinstance(products, list):
            self.logger.warning(f"Unexpected format for products at page {page}")
            return

        self.logger.info(
            f"Parsed {len(products)} products for category '{category}', page {page}"
        )

        for product in products:
            current = float(product.get("price") or 0)
            original = float(product.get("prev_price") or product.get("price") or 0)
            sale_tag = ""
            if original and current and current < original:
                discount = round((1 - current / original) * 100)
                sale_tag = f"Скидка {discount}%"

            filtes_data = product.get("filter_labels")

            metadata = {
                "__description": "",
                "vendor_code": product.get("vendor_code", ""),
                "filters": {item["filter"]: item["title"] for item in filtes_data},
            }

            item = AlkotekaItem(
                timestamp=int(time.time()),
                RPC=product.get("vendor_code", "unknown"),
                url=product.get("product_url", ""),
                title=product.get("name", ""),
                marketing_tags=product.get("action_labels", []),
                brand=product.get("subname", ""),
                section=[product.get("category", {}).get("name", "")],
                price_data={
                    "current": current,
                    "original": original,
                    "sale_tag": sale_tag,
                },
                stock={
                    "in_stock": bool(product.get("available")),
                    "count": int(product.get("quantity_total") or 0),
                },
                assets={
                    "main_image": product.get("image_url", ""),
                    "set_images": (
                        [product.get("image_url")] if product.get("image_url") else []
                    ),
                    "view360": [],
                    "video": [],
                },
                variants=1,
                metadata=metadata,
            )
            yield item

        # Пагинация
        meta = data.get("meta") or {}
        total_items = meta.get("total", 0)
        per_page = meta.get("per_page", PER_PAGE)
        total_pages = (total_items + per_page - 1) // per_page

        if page < total_pages:
            next_page = page + 1
            next_url = self._build_api_url(category, page=next_page)
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_api,
                headers={
                    "Accept": "application/json",
                    "X-Requested-With": "XMLHttpRequest",
                },
                cookies=cookies,
                meta={"cookies": cookies, "page": next_page, "category": category},
            )
