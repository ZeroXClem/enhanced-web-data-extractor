import asyncio
import aiohttp
from bs4 import BeautifulSoup
import html2text
from urllib.parse import urlparse, urljoin
import os
import csv
import json
import xml.etree.ElementTree as ET
import streamlit as st
import tempfile
import zipfile
import re
import time
import logging
from typing import Generator, List, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RateLimiter:
    def __init__(self, calls: int, period: float):
        self.calls = calls
        self.period = period
        self.timestamps = []

    async def wait(self):
        now = time.time()
        self.timestamps = [t for t in self.timestamps if now - t < self.period]
        if len(self.timestamps) >= self.calls:
            sleep_time = self.period - (now - self.timestamps[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
        self.timestamps.append(time.time())

class WebDataExtractor:
    def __init__(self, base_url: str, max_depth: int = 3, keywords: List[str] = None, rate_limit: int = 10):
        self.base_url = base_url
        self.max_depth = max_depth
        self.keywords = keywords or []
        self.visited_urls = set()
        self.data = []
        self.rate_limiter = RateLimiter(rate_limit, 1.0)  # rate_limit requests per second

    def sanitize_filename(self, filename: str) -> str:
        return re.sub(r'[^\w\-_\. ]', '_', filename)

    async def scrape_page_async(self, url: str, depth: int, session: aiohttp.ClientSession) -> Dict[str, Any]:
        if depth > self.max_depth or url in self.visited_urls:
            return None

        self.visited_urls.add(url)

        try:
            await self.rate_limiter.wait()
            async with session.get(url, timeout=10) as response:
                html = await response.text()
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

        soup = BeautifulSoup(html, 'html.parser')
        title = soup.title.string if soup.title else "No title"
        content = html2text.html2text(html)

        if self.keywords and not any(keyword.lower() in content.lower() for keyword in self.keywords):
            return None

        links = [urljoin(url, link.get('href')) for link in soup.find_all('a', href=True)]
        links = [link for link in links if urlparse(link).netloc == urlparse(self.base_url).netloc]

        return {
            'url': url,
            'title': title,
            'content': content,
            'depth': depth,
            'links': links
        }

    async def run_scraper(self, max_pages: int) -> Generator[str, None, None]:
        total_pages = min(max_pages, 100)  # Limit to 100 pages for demonstration
        async with aiohttp.ClientSession() as session:
            to_visit = [(self.base_url, 0)]
            while to_visit and len(self.data) < total_pages:
                tasks = []
                for _ in range(min(10, total_pages - len(self.data))):
                    if to_visit:
                        url, depth = to_visit.pop(0)
                        if url not in self.visited_urls:
                            tasks.append(self.scrape_page_async(url, depth, session))
                
                results = await asyncio.gather(*tasks)
                for result in results:
                    if result:
                        self.data.append(result)
                        yield f"Scraped: {result['url']} (Depth: {result['depth']})"
                        to_visit.extend((link, result['depth'] + 1) for link in result['links'])

    def save_data(self, temp_dir: str) -> tuple:
        try:
            csv_path = os.path.join(temp_dir, "scraped_data.csv")
            with open(csv_path, mode='w', newline='', encoding='utf-8') as temp_file:
                fieldnames = ['url', 'title', 'content', 'depth']
                writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
                writer.writeheader()
                for item in self.data:
                    writer.writerow({k: item[k] for k in fieldnames})

            markdown_files = []
            for index, item in enumerate(self.data):
                safe_title = self.sanitize_filename(item['title'])
                filename = f"{index:03d}_{safe_title[:50]}.md"
                filepath = os.path.join(temp_dir, 'markdown', filename)
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(f"# {item['title']}\n\n")
                    f.write(f"**URL:** [{item['url']}]({item['url']})\n")
                    f.write(f"**Depth:** {item['depth']}\n\n")
                    f.write(item['content'])
                markdown_files.append(filepath)

            json_path = os.path.join(temp_dir, "scraped_data.json")
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)

            xml_root = ET.Element("web_data")
            for item in self.data:
                page = ET.SubElement(xml_root, "page")
                for key, value in item.items():
                    if key != 'links':
                        ET.SubElement(page, key).text = str(value)

            xml_path = os.path.join(temp_dir, "scraped_data.xml")
            tree = ET.ElementTree(xml_root)
            tree.write(xml_path, encoding='utf-8', xml_declaration=True)

            return csv_path, markdown_files, json_path, xml_path

        except Exception as e:
            logger.error(f"An error occurred while saving data: {str(e)}")
            return None, None, None, None

# Streamlit UI
st.title("Enhanced Web Data Extractor")

base_url = st.text_input("Base URL:")
max_pages = st.number_input("Max Pages:", min_value=1, value=10, max_value=100)
max_depth = st.number_input("Max Depth:", min_value=1, value=3, max_value=10)
keywords = st.text_input("Keywords (comma-separated):")
rate_limit = st.number_input("Rate Limit (requests per second):", min_value=1, value=10, max_value=60)
save_format = st.multiselect("Save Format:", ["csv", "markdown", "json", "xml"], default=["csv"])

if st.button("Start Scraping"):
    if not base_url:
        st.error("Please enter a URL")
    elif not urlparse(base_url).scheme:
        st.error("Please enter a valid URL with http:// or https://")
    else:
        keywords_list = [k.strip() for k in keywords.split(',') if k.strip()]
        extractor = WebDataExtractor(base_url, max_depth=max_depth, keywords=keywords_list, rate_limit=rate_limit)
        
        progress_bar = st.progress(0)
        status_text = st.empty()

        async def run_scraping():
            async for status in extractor.run_scraper(max_pages):
                status_text.text(status)
                progress = len(extractor.data) / min(max_pages, 100)
                progress_bar.progress(progress)

        with st.spinner("Scraping in progress..."):
            asyncio.run(run_scraping())

        status_text.text("Scraping completed. Saving data...")

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path, markdown_files, json_path, xml_path = extractor.save_data(temp_dir)
            
            file_paths = []
            if 'csv' in save_format:
                file_paths.append(csv_path)
            if 'markdown' in save_format:
                file_paths.extend(markdown_files)
            if 'json' in save_format:
                file_paths.append(json_path)
            if 'xml' in save_format:
                file_paths.append(xml_path)

            # Add download buttons for individual formats
            for format in save_format:
                if format == 'csv':
                    with open(csv_path, 'rb') as f:
                        st.download_button(
                            label="Download CSV",
                            data=f,
                            file_name="scraped_data.csv",
                            mime="text/csv"
                        )
                elif format == 'markdown':
                    st.write("Markdown files have been created. You can download them individually:")
                    for filepath in markdown_files:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            st.download_button(
                                label=f"Download {os.path.basename(filepath)}",
                                data=f.read(),
                                file_name=os.path.basename(filepath),
                                mime="text/markdown"
                            )
                elif format == 'json':
                    with open(json_path, 'rb') as f:
                        st.download_button(
                            label="Download JSON",
                            data=f,
                            file_name="scraped_data.json",
                            mime="application/json"
                        )
                elif format == 'xml':
                    with open(xml_path, 'rb') as f:
                        st.download_button(
                            label="Download XML",
                            data=f,
                            file_name="scraped_data.xml",
                            mime="application/xml"
                        )

            # Add a button to download all data in a zip file
            zip_path = os.path.join(temp_dir, "scraped_data.zip")
            try:
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for file_path in file_paths:
                        if file_path:  # Check if the file path is not None
                            arcname = os.path.relpath(file_path, temp_dir)
                            zipf.write(file_path, arcname)
            except Exception as e:
                logger.error(f"An error occurred while creating the zip file: {str(e)}")
                st.error(f"An error occurred while creating the zip file: {str(e)}")
                zip_path = None

            if zip_path:
                with open(zip_path, 'rb') as zip_file:
                    st.download_button(
                        label="Download All",
                        data=zip_file,
                        file_name="scraped_data.zip",
                        mime="application/zip"
                    )
            else:
                st.error("Unable to create zip file for download.")

        status_text.text("Data saved and ready for download!")
