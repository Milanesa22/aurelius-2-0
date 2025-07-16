import asyncio
import aiohttp
import json
from typing import Optional, Dict, List, Any
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import bleach
from config import config
from logging_setup import get_logger

logger = get_logger("scraping")

class WebScraper:
    """Asynchronous web scraper for market research and competitor analysis."""
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers=self.headers
            )
        return self.session
    
    async def close(self):
        """Close the aiohttp session."""
        if self.session and not self.session.closed:
            await self.session.close()
    
    def _sanitize_url(self, url: str) -> Optional[str]:
        """Sanitize and validate URL."""
        try:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                return None
            
            # Only allow http and https
            if parsed.scheme not in ['http', 'https']:
                return None
            
            return url.strip()
        except Exception as e:
            logger.error(f"Invalid URL format: {url} - {e}")
            return None
    
    def _extract_text_content(self, html: str) -> str:
        """Extract clean text content from HTML."""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Get text and clean it
            text = soup.get_text()
            
            # Clean up whitespace
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = ' '.join(chunk for chunk in chunks if chunk)
            
            return text
        except Exception as e:
            logger.error(f"Error extracting text content: {e}")
            return ""
    
    async def fetch_page(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch a web page and return structured data."""
        sanitized_url = self._sanitize_url(url)
        if not sanitized_url:
            logger.error(f"Invalid URL provided: {url}")
            return None
        
        try:
            session = await self._get_session()
            
            async with session.get(sanitized_url) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    # Parse with BeautifulSoup
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Extract structured data
                    data = {
                        'url': sanitized_url,
                        'status_code': response.status,
                        'title': soup.title.string.strip() if soup.title else '',
                        'meta_description': '',
                        'text_content': self._extract_text_content(html),
                        'links': [],
                        'images': [],
                        'headers': {}
                    }
                    
                    # Extract meta description
                    meta_desc = soup.find('meta', attrs={'name': 'description'})
                    if meta_desc:
                        data['meta_description'] = meta_desc.get('content', '')
                    
                    # Extract links
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        absolute_url = urljoin(sanitized_url, href)
                        if self._sanitize_url(absolute_url):
                            data['links'].append({
                                'url': absolute_url,
                                'text': link.get_text().strip()
                            })
                    
                    # Extract images
                    for img in soup.find_all('img', src=True):
                        src = img['src']
                        absolute_url = urljoin(sanitized_url, src)
                        data['images'].append({
                            'url': absolute_url,
                            'alt': img.get('alt', ''),
                            'title': img.get('title', '')
                        })
                    
                    # Extract headers
                    for i in range(1, 7):
                        headers = soup.find_all(f'h{i}')
                        if headers:
                            data['headers'][f'h{i}'] = [h.get_text().strip() for h in headers]
                    
                    logger.info(f"Successfully scraped: {sanitized_url}")
                    return data
                
                else:
                    logger.warning(f"HTTP {response.status} for URL: {sanitized_url}")
                    return {
                        'url': sanitized_url,
                        'status_code': response.status,
                        'error': f'HTTP {response.status}'
                    }
        
        except asyncio.TimeoutError:
            logger.error(f"Timeout while fetching: {sanitized_url}")
            return None
        except aiohttp.ClientError as e:
            logger.error(f"Client error while fetching {sanitized_url}: {e}")
            return None
        except Exception as e:
            logger.exception(f"Unexpected error while fetching {sanitized_url}: {e}")
            return None
    
    async def fetch_multiple_pages(self, urls: List[str], max_concurrent: int = 5) -> List[Dict[str, Any]]:
        """Fetch multiple pages concurrently."""
        if not urls:
            return []
        
        # Limit concurrency to avoid overwhelming servers
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(url: str) -> Optional[Dict[str, Any]]:
            async with semaphore:
                return await self.fetch_page(url)
        
        try:
            tasks = [fetch_with_semaphore(url) for url in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter out None results and exceptions
            valid_results = []
            for result in results:
                if isinstance(result, dict):
                    valid_results.append(result)
                elif isinstance(result, Exception):
                    logger.error(f"Exception in concurrent fetch: {result}")
            
            logger.info(f"Successfully fetched {len(valid_results)} out of {len(urls)} pages")
            return valid_results
        
        except Exception as e:
            logger.exception(f"Error in fetch_multiple_pages: {e}")
            return []
    
    async def search_content(self, pages_data: List[Dict[str, Any]], keywords: List[str]) -> List[Dict[str, Any]]:
        """Search for specific keywords in scraped content."""
        if not pages_data or not keywords:
            return []
        
        results = []
        keywords_lower = [kw.lower() for kw in keywords]
        
        for page in pages_data:
            if not isinstance(page, dict) or 'text_content' not in page:
                continue
            
            content = page.get('text_content', '').lower()
            title = page.get('title', '').lower()
            meta_desc = page.get('meta_description', '').lower()
            
            matches = []
            for keyword in keywords_lower:
                if keyword in content or keyword in title or keyword in meta_desc:
                    matches.append(keyword)
            
            if matches:
                results.append({
                    'url': page.get('url', ''),
                    'title': page.get('title', ''),
                    'matched_keywords': matches,
                    'relevance_score': len(matches) / len(keywords_lower)
                })
        
        # Sort by relevance score
        results.sort(key=lambda x: x['relevance_score'], reverse=True)
        
        logger.info(f"Found {len(results)} pages matching keywords")
        return results
    
    async def extract_competitor_info(self, competitor_urls: List[str]) -> Dict[str, Any]:
        """Extract competitor information from their websites."""
        if not competitor_urls:
            return {}
        
        pages_data = await self.fetch_multiple_pages(competitor_urls)
        
        competitor_info = {
            'total_competitors': len(competitor_urls),
            'successfully_scraped': len(pages_data),
            'competitors': []
        }
        
        for page in pages_data:
            if not isinstance(page, dict):
                continue
            
            competitor = {
                'url': page.get('url', ''),
                'title': page.get('title', ''),
                'meta_description': page.get('meta_description', ''),
                'content_length': len(page.get('text_content', '')),
                'num_links': len(page.get('links', [])),
                'num_images': len(page.get('images', [])),
                'headers': page.get('headers', {})
            }
            
            competitor_info['competitors'].append(competitor)
        
        logger.info(f"Extracted info for {len(competitor_info['competitors'])} competitors")
        return competitor_info
    
    async def monitor_mentions(self, brand_keywords: List[str], urls_to_monitor: List[str]) -> List[Dict[str, Any]]:
        """Monitor websites for brand mentions."""
        if not brand_keywords or not urls_to_monitor:
            return []
        
        pages_data = await self.fetch_multiple_pages(urls_to_monitor)
        mentions = await self.search_content(pages_data, brand_keywords)
        
        # Add timestamp and additional context
        import datetime
        for mention in mentions:
            mention['timestamp'] = datetime.datetime.utcnow().isoformat()
            mention['monitoring_keywords'] = brand_keywords
        
        logger.info(f"Found {len(mentions)} brand mentions")
        return mentions

# Global scraper instance
_scraper: Optional[WebScraper] = None

async def get_scraper() -> WebScraper:
    """Get scraper instance."""
    global _scraper
    if _scraper is None:
        _scraper = WebScraper()
    return _scraper

async def close_scraper():
    """Close scraper."""
    global _scraper
    if _scraper:
        await _scraper.close()
        _scraper = None

# Convenience functions
async def fetch_page(url: str) -> Optional[Dict[str, Any]]:
    """Fetch a single page."""
    scraper = await get_scraper()
    return await scraper.fetch_page(url)

async def fetch_multiple_pages(urls: List[str], max_concurrent: int = 5) -> List[Dict[str, Any]]:
    """Fetch multiple pages concurrently."""
    scraper = await get_scraper()
    return await scraper.fetch_multiple_pages(urls, max_concurrent)

async def extract_competitor_info(competitor_urls: List[str]) -> Dict[str, Any]:
    """Extract competitor information."""
    scraper = await get_scraper()
    return await scraper.extract_competitor_info(competitor_urls)

async def monitor_mentions(brand_keywords: List[str], urls_to_monitor: List[str]) -> List[Dict[str, Any]]:
    """Monitor for brand mentions."""
    scraper = await get_scraper()
    return await scraper.monitor_mentions(brand_keywords, urls_to_monitor)
