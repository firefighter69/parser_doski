"""
Website parser for www.doski.ru with data extraction and logging.
"""

import asyncio
import logging
import requests
import time
import urllib.robotparser
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

try:
    import trafilatura
    HAS_TRAFILATURA = True
except ImportError:
    HAS_TRAFILATURA = False
    print("Warning: trafilatura not available, using basic text extraction")

from storage import DataStorage
from utils import rate_limiter, validate_url

logger = logging.getLogger(__name__)

# --- Selenium utility for dynamic content ---
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def get_rendered_html(url, timeout=20):
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    # Можно добавить user-agent для "обмана" сайта
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(timeout)
    driver.get(url)
    time.sleep(3)  # Даем странице прогрузиться (если нужно)
    html = driver.page_source
    driver.quit()
    return html


def format_listing_for_telegram(self, listing):
    """Форматирует объявление для отправки в Telegram."""
    msg = (
        f"<b>{listing['title']}</b>\n"
        f"{listing.get('price', '')}\n"
        f"{listing.get('description', '')}\n"
        f"<a href=\"{listing['url']}\">Подробнее</a>"
    )
    return msg


def _extract_listings(self, html_content):
    """
    Извлекает объявления с doski.ru из таблицы <table class="ml">.
    Каждое объявление — это <tr>, где есть <a class="sbj">.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    listings = []

    rows = soup.select('table.ml tr')
    for row in rows:
        title_elem = row.select_one('a.sbj')
        if not title_elem:
            continue

        title = title_elem.get_text(strip=True)
        url = urljoin(self.base_url, title_elem['href'])

        price_elem = row.select_one('td[align="right"] b')
        price = price_elem.get_text(strip=True) if price_elem else ""

        desc = ""
        try:
            br = title_elem.find_next('br')
            if br and isinstance(br.next_sibling, str):
                desc = br.next_sibling.strip()
        except Exception:
            pass

        listings.append({
            "id": url.split('/')[-1].split('.')[0],
            "title": title,
            "url": url,
            "price": price,
            "description": desc,
            "parsed_at": datetime.now().isoformat()
        })

    return listings


async def parse_category(self, category_url):
    """Парсит одну категорию, отправляет объявления в Telegram."""
    logger.info(f"Parsing category: {category_url}")
    await self.telegram_bot.send_message(f"🔍 Starting to parse: {category_url}")

    # Здесь у вас получение html через Selenium/requests
    html = get_rendered_html(category_url)

    # Сохраняем для отладки (по желанию)
    with open("debug_category.html", "w", encoding="utf-8") as f:
        f.write(html)

    listings = self._extract_listings(html)

    for listing in listings:
        self.storage.save_listing(listing)

    # Отправляем объявления в Telegram
    for listing in listings:
        msg = self.format_listing_for_telegram(listing)
        await self.telegram_bot.send_message(msg, parse_mode='HTML')

    logger.info(f"Found {len(listings)} listings in {category_url}")
    await self.telegram_bot.send_message(
        f"✅ Parsed {len(listings)} listings from category\n"
        f"📊 Total stored: {self.storage.get_total_count()}"
    )

    return listings


# -------------------------------------------

class DoskiParser:
    """Main parser class for www.doski.ru website."""

    def __init__(self, config, telegram_bot):
        self.config = config
        self.telegram_bot = telegram_bot
        self.storage = DataStorage()
        self.base_url = "https://www.doski.ru"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        # Отключаем проверку SSL для проблемных сайтов
        self.session.verify = False
        # Подавляем предупреждения SSL
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Настройка прокси
        self.proxy_list = []
        self.current_proxy_index = 0
        self._setup_proxy()

        self.robots_parser = self._load_robots_txt()

    def _setup_proxy(self):
        """Setup proxy configuration for the session."""
        if not self.config.get('proxy_enabled', False):
            logger.info("Proxy disabled")
            return

        if self.config.get('proxy_list'):
            self.proxy_list = self.config['proxy_list']
        else:
            if self.config.get('proxy_http'):
                self.proxy_list.append(self.config['proxy_http'])
            if self.config.get('proxy_https'):
                self.proxy_list.append(self.config['proxy_https'])
            if self.config.get('proxy_socks'):
                self.proxy_list.append(self.config['proxy_socks'])

        if self.proxy_list:
            self._set_proxy(self.proxy_list[0])
            logger.info(f"Proxy configured: {len(self.proxy_list)} proxy(ies) available")
        else:
            logger.warning("Proxy enabled but no proxy URLs provided")

    def _set_proxy(self, proxy_url):
        """Set proxy for the current session."""
        try:
            if proxy_url.startswith('socks'):
                try:
                    import socks
                    self.session.proxies = {
                        'http': proxy_url,
                        'https': proxy_url
                    }
                    logger.info(f"SOCKS proxy set: {proxy_url}")
                except ImportError:
                    logger.error(
                        "SOCKS proxy requires 'requests[socks]' package. Install with: pip install requests[socks]")
                    return False
            else:
                self.session.proxies = {
                    'http': proxy_url,
                    'https': proxy_url
                }
                logger.info(f"HTTP proxy set: {proxy_url}")
            return True
        except Exception as e:
            logger.error(f"Error setting proxy {proxy_url}: {e}")
            return False

    def _rotate_proxy(self):
        """Rotate to the next proxy in the list."""
        if not self.proxy_list or len(self.proxy_list) <= 1:
            return False

        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
        new_proxy = self.proxy_list[self.current_proxy_index]

        if self._set_proxy(new_proxy):
            logger.info(f"Rotated to proxy {self.current_proxy_index + 1}/{len(self.proxy_list)}")
            return True
        return False

    def _load_robots_txt(self):
        """Load and parse robots.txt file."""
        try:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(f"{self.base_url}/robots.txt")
            rp.read()
            return rp
        except Exception as e:
            logger.warning(f"Could not load robots.txt: {e}")
            return None

    def _can_fetch(self, url):
        """Check if URL can be fetched according to robots.txt."""
        if not self.robots_parser:
            return True
        return self.robots_parser.can_fetch('*', url)

    @rate_limiter(delay=2.0)
    async def _fetch_page(self, url):
        """Fetch a single page with rate limiting and error handling."""
        if not self._can_fetch(url):
            logger.warning(f"Robots.txt disallows fetching: {url}")
            return None

        max_proxy_retries = len(self.proxy_list) if self.proxy_list else 1

        for proxy_attempt in range(max_proxy_retries):
            try:
                response = self.session.get(url, timeout=self.config.get('timeout', 10))
                response.raise_for_status()
                return response
            except (requests.exceptions.ProxyError, requests.exceptions.ConnectionError) as e:
                logger.warning(f"Proxy error for {url}: {e}")

                if self.config.get('proxy_rotate', False) and self._rotate_proxy():
                    logger.info(f"Retrying with different proxy (attempt {proxy_attempt + 1}/{max_proxy_retries})")
                    continue
                else:
                    break
            except requests.RequestException as e:
                logger.error(f"Error fetching {url}: {e}")
                break

        if self.session.proxies and self.config.get('proxy_enabled', False):
            logger.warning(f"All proxies failed for {url}, trying without proxy")
            old_proxies = self.session.proxies.copy()
            self.session.proxies = {}

            try:
                response = self.session.get(url, timeout=self.config.get('timeout', 10))
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logger.error(f"Error fetching {url} without proxy: {e}")
                self.session.proxies = old_proxies

        await self.telegram_bot.send_message(f"❌ Error fetching {url}: Connection failed")
        return None

    def _extract_listings(self, html_content):
        """
        Извлекает объявления с doski.ru из таблицы <table class="ml">.
        Каждое объявление — это <tr>, где есть <a class="sbj">.
        """
        from bs4 import BeautifulSoup
        from urllib.parse import urljoin

        soup = BeautifulSoup(html_content, 'html.parser')
        listings = []

        # Ищем строки-объявления
        rows = soup.select('table.ml tr')
        for row in rows:
            title_elem = row.select_one('a.sbj')
            if not title_elem:
                continue  # Это не объявление

            title = title_elem.get_text(strip=True)
            url = urljoin(self.base_url, title_elem['href'])

            # Цена
            price_elem = row.select_one('td[align="right"] b')
            price = price_elem.get_text(strip=True) if price_elem else ""

            # Описание (текст после <br>)
            desc = ""
            try:
                # <a class="sbj">...<br>Описание...
                br = title_elem.find_next('br')
                if br and isinstance(br.next_sibling, str):
                    desc = br.next_sibling.strip()
            except Exception:
                pass

            listings.append({
                "id": url.split('/')[-1].split('.')[0],
                "title": title,
                "url": url,
                "price": price,
                "description": desc,
                "parsed_at": datetime.now().isoformat()
            })

        return listings

    def _parse_listing_item(self, item):
        """Parse individual listing item."""
        listing = {
            'id': None,
            'title': '',
            'description': '',
            'price': '',
            'location': '',
            'date': '',
            'url': '',
            'images': [],
            'parsed_at': datetime.now().isoformat()
        }

        title_selectors = ['h2', 'h3', '.title', '[class*="title"]', 'a']
        for selector in title_selectors:
            title_elem = item.select_one(selector)
            if title_elem:
                listing['title'] = title_elem.get_text(strip=True)
                break

        link = item.find('a', href=True)
        if link:
            listing['url'] = urljoin(self.base_url, link['href'])

        price_selectors = ['.price', '[class*="price"]', '[class*="cost"]']
        for selector in price_selectors:
            price_elem = item.select_one(selector)
            if price_elem:
                listing['price'] = price_elem.get_text(strip=True)
                break

        location_selectors = ['.location', '[class*="location"]', '[class*="city"]']
        for selector in location_selectors:
            location_elem = item.select_one(selector)
            if location_elem:
                listing['location'] = location_elem.get_text(strip=True)
                break

        desc_selectors = ['.description', '.desc', '[class*="description"]']
        for selector in desc_selectors:
            desc_elem = item.select_one(selector)
            if desc_elem:
                listing['description'] = desc_elem.get_text(strip=True)[:200]
                break

        images = item.find_all('img', src=True)
        for img in images:
            img_url = urljoin(self.base_url, img['src'])
            if validate_url(img_url):
                listing['images'].append(img_url)

        if listing['url']:
            parts = [part for part in listing['url'].split('/') if part]
            listing['id'] = parts[-1] if parts else None
        elif listing['title']:
            listing['id'] = str(hash(listing['title']))[:10]

        return listing if listing['title'] else None

    async def parse_category(self, category_url):
        """Parse a specific category page using Selenium for dynamic content."""
        logger.info(f"Parsing category: {category_url}")
        await self.telegram_bot.send_message(f"🔍 Starting to parse: {category_url}")

        # Используем Selenium для получения динамического контента
        try:
            html = get_rendered_html(category_url)
        except Exception as e:
            logger.error(f"Selenium error: {e}")
            await self.telegram_bot.send_message(f"❌ Selenium error: {e}")
            return []

        # --- Сохраняем HTML для отладки ---
        import os
        try:
            with open("debug_category.html", "w", encoding="utf-8") as f:
                f.write(html)
            logger.info(f"HTML saved to {os.path.abspath('debug_category.html')} (size={len(html)})")
            print(f"HTML saved to {os.path.abspath('debug_category.html')} (size={len(html)})")
        except Exception as e:
            logger.error(f"Error saving HTML: {e}")
            print(f"Error saving HTML: {e}")

        # --- /Сохраняем HTML для отладки ---

        if HAS_TRAFILATURA:
            text_content = trafilatura.extract(html)
        else:
            soup_text = BeautifulSoup(html, 'html.parser')
            text_content = soup_text.get_text(strip=True, separator=' ')[:500]

        listings = self._extract_listings(html)

        for listing in listings:
            self.storage.save_listing(listing)

        logger.info(f"Found {len(listings)} listings in {category_url}")
        await self.telegram_bot.send_message(
            f"✅ Parsed {len(listings)} listings from category\n"
            f"📊 Total stored: {self.storage.get_total_count()}"
        )

        return listings

    async def parse_main_page(self):
        """Parse the main page to discover categories."""
        logger.info("Parsing main page for categories")
        await self.telegram_bot.send_message("🏠 Parsing main page for categories...")

        response = await self._fetch_page(self.base_url)
        if not response:
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        categories = []

        logger.info(f"Main page HTML length: {len(response.text)} characters")
        logger.info(f"Main page status code: {response.status_code}")

        if len(response.text) > 0:
            sample_html = response.text[:1000] if len(response.text) > 1000 else response.text
            logger.info(f"HTML sample: {sample_html[:200]}...")

        category_selectors = [
            'a[href*="/cat-"]',
            'a[href*="/category/"]',
            'a[href*="/cat/"]',
            'a[href*="/section/"]',
            'a[href*="/region/"]',
            'a[href*="/city/"]',
            '.category-link',
            '[class*="category"] a',
            'nav a',
            '.menu a',
            'a'
        ]

        all_links = []
        for selector in category_selectors:
            links = soup.select(selector)
            logger.info(f"Selector '{selector}': found {len(links)} links")
            for link in links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                if href and text and len(text) > 2 and len(text) < 50:
                    if not any(skip in href.lower() for skip in
                               ['mailto:', 'tel:', 'javascript:', '#', 'login', 'register', 'search']):
                        all_links.append({
                            'href': href,
                            'text': text,
                            'selector': selector
                        })

        for link_data in all_links:
            category_url = urljoin(self.base_url, link_data['href'])
            if validate_url(category_url):
                categories.append({
                    'name': link_data['text'],
                    'url': category_url,
                    'found_by': link_data['selector']
                })

        seen = set()
        unique_categories = []
        for cat in categories:
            if cat['url'] not in seen:
                seen.add(cat['url'])
                unique_categories.append(cat)

        logger.info(f"Found {len(unique_categories)} unique categories")
        for i, cat in enumerate(unique_categories[:10]):
            logger.info(f"Category {i + 1}: '{cat['name']}' -> {cat['url']}")

        if len(unique_categories) < 3:
            logger.warning(f"Found only {len(unique_categories)} categories, using fallback categories")

            fallback_categories = [
                {'name': 'Недвижимость - Квартиры продажа',
                 'url': f"{self.base_url}/cat-nedvizhimost/zhilaya/kvartiry/prodam/"},
                {'name': 'Недвижимость - Квартиры аренда',
                 'url': f"{self.base_url}/cat-nedvizhimost/zhilaya/kvartiry/sdau/"},
                {'name': 'Транспорт - Легковые авто',
                 'url': f"{self.base_url}/cat-transport/legkovye-avtomobili/prodam/"},
                {'name': 'Недвижимость - Дома продажа',
                 'url': f"{self.base_url}/cat-nedvizhimost/zhilaya/doma-dachi/prodam/"},
                {'name': 'Детские товары',
                 'url': f"{self.base_url}/cat-detskiy-mir/detskaya-odezhda-obuv/detskaya-odezhda/prodam/"},
                {'name': 'Животные - Собаки', 'url': f"{self.base_url}/cat-zhivotnye-i-rasteniya/sobaki/podaru/"},
                {'name': 'Одежда женская', 'url': f"{self.base_url}/cat-lichnye-veschi/odezhda/platya-bluzki-ubki/"},
                {'name': 'Работа - Вакансии', 'url': f"{self.base_url}/cat-rabota/vakansii/"},
            ]

            unique_categories = fallback_categories[:5]
            logger.info(f"Using {len(unique_categories)} fallback categories")
        else:
            unique_categories = unique_categories[:5]
            logger.info(f"Using {len(unique_categories)} found categories")

        await self.telegram_bot.send_message(f"📂 Found {len(unique_categories)} categories")

        return unique_categories

    async def full_parse(self):
        """Perform a full parsing session."""
        start_time = datetime.now()
        await self.telegram_bot.send_message(
            f"🚀 Starting full parse session at {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        try:
            categories = await self.parse_main_page()

            total_listings = 0
            parsed_categories = 0

            max_categories = self.config.max_categories_per_session
            for i, category in enumerate(categories[:max_categories]):
                try:
                    listings = await self.parse_category(category['url'])
                    total_listings += len(listings)
                    parsed_categories += 1

                    if i < len(categories) - 1:
                        await asyncio.sleep(self.config.category_delay)

                except Exception as e:
                    logger.error(f"Error parsing category {category['name']}: {e}")
                    await self.telegram_bot.send_message(
                        f"❌ Error parsing category {category['name']}: {str(e)}"
                    )

            end_time = datetime.now()
            duration = end_time - start_time

            summary = (
                f"✅ Parse session completed!\n"
                f"📊 Categories parsed: {parsed_categories}/{len(categories)}\n"
                f"📝 Total listings found: {total_listings}\n"
                f"💾 Total stored: {self.storage.get_total_count()}\n"
                f"⏱️ Duration: {duration.total_seconds():.1f} seconds"
            )

            await self.telegram_bot.send_message(summary)
            logger.info(f"Parse session completed: {total_listings} listings in {duration}")

            return {
                'categories_parsed': parsed_categories,
                'total_listings': total_listings,
                'duration': duration.total_seconds(),
                'success': True
            }

        except Exception as e:
            logger.error(f"Full parse session failed: {e}")
            await self.telegram_bot.send_message(f"❌ Parse session failed: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    def get_statistics(self):
        """Get parsing statistics."""
        return self.storage.get_statistics()
        return self.storage.get_statistics()