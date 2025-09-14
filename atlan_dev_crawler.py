import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import hashlib
import json
import os
import time

class DeveloperAtlanCrawler:
    def __init__(self, seed_url, data_dir='developer_crawled_data'):
        self.seed_url = seed_url
        self.domain = urlparse(seed_url).netloc
        self.visited = set()
        self.frontier = [seed_url]
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    def get_url_id(self, url):
        return hashlib.md5(url.encode()).hexdigest()

    def save_page_data(self, url, url_id, text):
        data = {
            'url': url,
            'url_id': url_id,
            'text': text
        }
        filename = os.path.join(self.data_dir, f"{url_id}.json")
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def extract_target_text(self, soup):
        content_div = soup.find('div', class_='md-content', attrs={'data-md-component': 'content'})
        if content_div:
            text = content_div.get_text(separator=' ', strip=True)
            return ' '.join(text.split())
        return ""

    def get_links(self, html, base_url):
        soup = BeautifulSoup(html, 'html.parser')
        links = set()
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            joined_url = urljoin(base_url, href)
            parsed = urlparse(joined_url)
            # Limit crawl to the same domain
            if parsed.netloc == self.domain:
                normalized_url = parsed._replace(fragment='').geturl()
                links.add(normalized_url)
        return links

    def crawl(self):
        while self.frontier:
            url = self.frontier.pop(0)
            if url in self.visited:
                continue
            try:
                print(f"Crawling: {url}")
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                html = response.text

                soup = BeautifulSoup(html, 'html.parser')
                text = self.extract_target_text(soup)
                url_id = self.get_url_id(url)

                self.save_page_data(url, url_id, text)

                links = self.get_links(html, url)
                for link in links:
                    if link not in self.visited and link not in self.frontier:
                        self.frontier.append(link)

                self.visited.add(url)
                time.sleep(1)  # Politeness delay
            except Exception as e:
                print(f"Failed to crawl {url}: {e}")
                self.visited.add(url)

if __name__ == "__main__":
    seed_url = 'https://developer.atlan.com/'
    crawler = DeveloperAtlanCrawler(seed_url)
    crawler.crawl()
