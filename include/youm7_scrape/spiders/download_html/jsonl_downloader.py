import scrapy
from scrapy.exceptions import CloseSpider
import json
import os

class JsonlDownloaderSpider(scrapy.Spider):
    name = "jsonl_downloader"    


    file_paths = {
            'arab': 'extracted_links/arab_article_links.jsonl',
            'art' : 'extracted_links/art_article_links.jsonl',
            'caricature' : 'extracted_links/caricature_article_links.jsonl',
            'economy' : 'extracted_links/economy_article_links.jsonl',
            'investigations' : 'extracted_links/investigations_article_links.jsonl',
            'television' : 'extracted_links/television_article_links.jsonl',
            'urgent' : 'extracted_links/urgent_article_links.jsonl',
            'your_horoscope_today' : 'extracted_links/your_horoscope_today_article_links.jsonl',
        }# اقتصاد و بورصة, تحقيقات, تلفزيون, عاجل
    
    
    category_mapping = {
        'arab': 'عرب', 'art': 'فن', 'caricature': 'كاريكاتير',
        'economy': 'اقتصاد و بورصة', 'investigations': 'تحقيقات',
        'television': 'تلفزيون', 'urgent': 'عاجل',
        'your_horoscope_today': 'حظك اليوم'
    }
    
    # I replaced this with dynamic initialization to allow passing the category key from the command line.
    # ---------------------------------------------------------
    # CONFIGURATION: Hardcode your specific JSONL file and category here
    # ---------------------------------------------------------
    # TARGET_FILE = file_paths['your_horoscope_today'] # Change this to the desired JSONL file
    # TARGET_FILE = r'G:\coding\python\web_scraping\Youm7\youm7_scrape\data\raw\missing_arab.jsonl' # Change this to the desired JSONL file
    # CATEGORY_NAME = category_mapping.get('arab')  # Change this to the corresponding category name for the file
    # TEST_LIMIT = None  # Set to a number (e.g., 3000) for testing
    # ---------------------------------------------------------



    def __init__(self, category_key=None, *args, **kwargs):
        super(JsonlDownloaderSpider, self).__init__(*args, **kwargs)
        
        # Validation: If no key is passed or the key isn't in our dictionary
        if not category_key or category_key not in self.file_paths:
            error_msg = f"CRITICAL ERROR: 'category_key' is missing or invalid. Received: '{category_key}'"
            self.logger.error(error_msg)
            raise CloseSpider(error_msg)

        # Set variables dynamically based on the passed key
        self.TARGET_FILE = self.file_paths[category_key]
        self.CATEGORY_NAME = self.category_mapping.get(category_key, 'Unknown')
        self.TEST_LIMIT = None 
        
        self.logger.info(f"Spider initialized for category: {self.CATEGORY_NAME}")


    def start_requests(self):
        if not os.path.exists(self.TARGET_FILE):
            self.logger.error(f"Target file not found: {self.TARGET_FILE}")
            return

        with open(self.TARGET_FILE, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                # Apply test limit if set
                if self.TEST_LIMIT and i >= self.TEST_LIMIT:
                    break
                
                line = line.strip()
                if not line:
                    continue

                try:
                    # Parse the line as a JSON object
                    data = json.loads(line)
                    # Extract the URL from the 'url' key
                    url = data.get('url')
                    
                    if url:
                        yield scrapy.Request(
                            url=url, 
                            callback=self.parse, 
                            meta={'category': self.CATEGORY_NAME}
                        )
                except json.JSONDecodeError:
                    self.logger.error(f"Failed to parse JSON on line {i+1}")
                    continue

    def parse(self, response):
        main_article = response.css('#divcont')
        if not main_article:
            return

        item = {
            'article_id': main_article.css('::attr(data-id)').get(),
            'url': response.url,
            'category': response.meta.get('category', 'unknown'),
            'title': (main_article.css('h1::text').get() or "").strip(),
            'publish_date': (main_article.css('.newsStoryDate::text').get() or "").strip(),
            'author': (main_article.css('.writeBy::text').get() or "").strip(),
            'content': " ".join([t.strip() for t in main_article.css('#articleBody ::text').getall() if t.strip()]),
            'tags': [tag.strip() for tag in main_article.css('.tags h3 a::text').getall()],
            'media': []
        }

        # Helper function to determine provider
        def get_provider(url):
            if not url: return 'unknown'
            return 'internal' if ('youm7.com' in url or 'static' in url) else 'external'

        # 1. IMAGES
        all_imgs = main_article.css('.img-cont img, #articleBody img, .imgcontainer img')
        for img in all_imgs:
            img_url = img.css('::attr(src)').get()
            if img_url:
                if not any(img_url == m['url'] for m in item['media']):
                    item['media'].append({
                        'type': 'image',
                        'url': img_url,
                        'alt': img.css('::attr(alt)').get(),
                        'caption': img.css('::attr(title)').get() or main_article.css('.img-cap::text').get(),
                        'provider': get_provider(img_url)
                    })

        # 2. VIDEOS (Iframes & Native)
        # Target common video embed sources
        video_urls = main_article.css('iframe::attr(src), video source::attr(src), video::attr(src)').getall()
        for v_url in video_urls:
            if v_url and not any(v_url == m['url'] for m in item['media']):
                item['media'].append({
                    'type': 'video',
                    'url': v_url,
                    'alt': None,
                    'caption': None,
                    'provider': get_provider(v_url)
                })

        # 3. AUDIO
        audio_sources = main_article.css('audio source::attr(src), audio::attr(src)').getall()
        audio_links = main_article.xpath('//a[contains(@href, ".mp3")]/@href').getall()
        for a_url in set(audio_sources + audio_links):
            full_audio_url = response.urljoin(a_url)
            if not any(full_audio_url == m['url'] for m in item['media']):
                item['media'].append({
                    'type': 'audio',
                    'url': full_audio_url,
                    'alt': None,
                    'caption': None,
                    'provider': get_provider(full_audio_url)
                })

        yield item