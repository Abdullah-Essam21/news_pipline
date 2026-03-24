import scrapy
from scrapy.exceptions import CloseSpider
import json
import os

class JsonlDownloaderSpider(scrapy.Spider):
    name = "jsonl_downloader"    

    # I replaced this with dynamic initialization to allow passing the category key from the command line.
    # # ---------------------------------------------------------
    # # CONFIGURATION: Hardcode your specific JSONL file and category here
    # # ---------------------------------------------------------
    # TARGET_FILE = file_paths['investigations'] # Change this to the desired JSONL file
    # CATEGORY_NAME = 'تحقيقات' # Change this to the corresponding category name for the file
    # TEST_LIMIT = None  # Set to a number (e.g., 3000) for testing
    # # ---------------------------------------------------------

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
        # Use the specific article container ID to avoid sidebar/footer noise
        # This ID is consistent across the provided samples
        main_article = response.css('#divcont')

        item = {
            'article_id': main_article.css('::attr(data-id)').get(),
            'url': response.url,
            'category': response.meta.get('category'),
            'title': main_article.css('h1::text').get(),
            'publish_date': main_article.css('.newsStoryDate::text').get(),
            'author': main_article.css('.writeBy::text').get(),
            
            # Extracts all paragraph text inside the body and joins them
            'content': " ".join(main_article.css('#articleBody p::text').getall()).strip(),
            
            # Captures all listed tags
            'tags': main_article.css('.tags h3 a::text').getall(),
            'images': []
        }

        # TARGETED IMAGE EXTRACTION
        # 1. Main Featured Image (Usually at the top)
        main_img_cont = main_article.css('.img-cont')
        if main_img_cont:
            item['images'].append({
                'url': main_img_cont.css('img::attr(src)').get(),
                'alt': main_img_cont.css('img::attr(alt)').get(),
                'caption': main_img_cont.css('.img-cap::text').get(),
                'type': 'featured'
            })

        # 2. In-Text & Gallery Images (The ones after the text)
        # These are often inside 'div.imgcontainer' or direct 'img' tags in the body
        body_images = main_article.css('#articleBody .imgcontainer img, #articleBody img')
        
        for img in body_images:
            img_url = img.css('::attr(src)').get()
            
            # Skip tracking pixels or small icons
            if not img_url or 'logo' in img_url.lower():
                continue
                
            # Deduplication: Don't add the main featured image again
            if any(img_url == existing['url'] for existing in item['images']):
                continue

            # For gallery images, the 'title' attribute is often used as a caption
            item['images'].append({
                'url': img_url,
                'alt': img.css('::attr(alt)').get(),
                'caption': img.css('::attr(title)').get(), # Captures the 'title' as secondary caption
                'type': 'body_gallery'
            })

        # Clean up whitespace for all string fields
        for key in ['title', 'author', 'publish_date']:
            if item[key]:
                item[key] = item[key].strip()

        yield item