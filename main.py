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

class WebDataExtractor:
    def __init__(self, base_url, max_depth=3, keywords=None):
        self.base_url = base_url
        self.max_depth = max_depth
        self.keywords = keywords or []
        self.visited_urls = set()
        self.data = []

    async def scrape_page_async(self, url, depth, session):
        if depth > self.max_depth or url in self.visited_urls:
            return None

        self.visited_urls.add(url)

        try:
            async with session.get(url) as response:
                html = await response.text()
        except Exception as e:
            print(f"Error fetching {url}: {e}")
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

# Streamlit UI
st.title("Enhanced Web Data Extractor")

base_url = st.text_input("Base URL:")
max_pages = st.number_input("Max Pages:", min_value=1, value=10)
max_depth = st.number_input("Max Depth:", min_value=1, value=3)
keywords = st.text_input("Keywords (comma-separated):")
save_format = st.selectbox("Save Format:", ["csv", "markdown", "json", "xml"])

if st.button("Start Scraping"):
    if not base_url:
        st.error("Please enter a URL")
    else:
        keywords_list = [k.strip() for k in keywords.split(',') if k.strip()]
        extractor = WebDataExtractor(base_url, max_depth=max_depth, keywords=keywords_list)
        
        progress_bar = st.progress(0)
        status_text = st.empty()

        async def run_scraper():
            total_pages = min(max_pages, 100)  # Limit to 100 pages for demonstration
            async with aiohttp.ClientSession() as session:
                to_visit = [(base_url, 0)]
                while to_visit and len(extractor.data) < total_pages:
                    tasks = []
                    for _ in range(min(10, total_pages - len(extractor.data))):
                        if to_visit:
                            url, depth = to_visit.pop(0)
                            if url not in extractor.visited_urls:
                                tasks.append(extractor.scrape_page_async(url, depth, session))
                    
                    results = await asyncio.gather(*tasks)
                    for result in results:
                        if result:
                            extractor.data.append(result)
                            status_text.text(f"Scraped: {result['url']} (Depth: {result['depth']})")
                            to_visit.extend((link, result['depth'] + 1) for link in result['links'])
                    
                    progress = len(extractor.data) / total_pages
                    progress_bar.progress(progress)

        asyncio.run(run_scraper())

        status_text.text("Scraping completed. Saving data...")

        if save_format == 'csv':
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', newline='', encoding='utf-8') as temp_file:
                fieldnames = ['url', 'title', 'content', 'depth']
                writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
                writer.writeheader()
                for item in extractor.data:
                    writer.writerow({k: item[k] for k in fieldnames})
            
            st.download_button(
                label="Download CSV",
                data=open(temp_file.name, 'rb'),
                file_name="scraped_data.csv",
                mime="text/csv"
            )
        elif save_format == 'markdown':
            with tempfile.TemporaryDirectory() as temp_dir:
                for item in extractor.data:
                    filename = f"{item['title'].replace(' ', '_')[:50]}.md"
                    filepath = os.path.join(temp_dir, filename)
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(f"# {item['title']}\n\n")
                        f.write(f"URL: {item['url']}\n")
                        f.write(f"Depth: {item['depth']}\n\n")
                        f.write(item['content'])
                
                st.write("Markdown files have been created. You can download them individually:")
                for filename in os.listdir(temp_dir):
                    with open(os.path.join(temp_dir, filename), 'r', encoding='utf-8') as f:
                        st.download_button(
                            label=f"Download {filename}",
                            data=f.read(),
                            file_name=filename,
                            mime="text/markdown"
                        )
        elif save_format == 'json':
            json_data = json.dumps(extractor.data, indent=2)
            st.download_button(
                label="Download JSON",
                data=json_data,
                file_name="scraped_data.json",
                mime="application/json"
            )
        elif save_format == 'xml':
            root = ET.Element("web_data")
            for item in extractor.data:
                page = ET.SubElement(root, "page")
                for key, value in item.items():
                    if key != 'links':
                        ET.SubElement(page, key).text = str(value)
            
            xml_data = ET.tostring(root, encoding='unicode', method='xml')
            st.download_button(
                label="Download XML",
                data=xml_data,
                file_name="scraped_data.xml",
                mime="application/xml"
            )

        status_text.text("Data saved and ready for download!")

st.write("Note: This app is for educational purposes only. Please respect websites' terms of service and robots.txt files.")
