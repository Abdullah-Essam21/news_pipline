import scrapy
import os

class TxtDownloaderSpider(scrapy.Spider):
    name = "txt_downloader"    

    # ---------------------------------------------------------
    # CONFIGURATION: Hardcode your specific file and category here
    # ---------------------------------------------------------
    TARGET_FILE = r'G:\coding\python\web_scraping\Youm7\youm7_scrape\data\raw\missing.txt'
    CATEGORY_NAME = 'تقارير مصرية'  # This is just a label for the category; it doesn't affect scraping
    TEST_LIMIT = None  # Set to None for a full run
    # ---------------------------------------------------------

    # custom_settings = {
    #     # This is a safety net: it closes the spider after 3000 items are scraped
    #     'CLOSESPIDER_ITEMCOUNT': 3000,
    #     'LOG_LEVEL': 'INFO',
    # }

    def start_requests(self):
        if not os.path.exists(self.TARGET_FILE):
            self.logger.error(f"Target file not found: {self.TARGET_FILE}")
            return

        with open(self.TARGET_FILE, 'r', encoding='utf-8') as f:
            # Enumerate allows us to stop exactly at the TEST_LIMIT
            for i, line in enumerate(f):
                if self.TEST_LIMIT and i >= self.TEST_LIMIT:
                    break
                
                url = line.strip()
                if url:
                    yield scrapy.Request(
                        url=url, 
                        callback=self.parse, 
                        meta={'category': self.CATEGORY_NAME}
                    )

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

    # def parse(self, response):
    #     # Use the specific article container ID to avoid sidebar/footer noise
    #     # This ID is consistent across the provided samples
    #     main_article = response.css('#divcont')

    #     item = {
    #         'article_id': main_article.css('::attr(data-id)').get(),
    #         'url': response.url,
    #         'category': response.meta.get('category'),
    #         'title': main_article.css('h1::text').get(),
    #         'publish_date': main_article.css('.newsStoryDate::text').get(),
    #         'author': main_article.css('.writeBy::text').get(),
            
    #         # Extracts all paragraph text inside the body and joins them
    #         'content': " ".join(main_article.css('#articleBody p::text').getall()).strip(),
            
    #         # Captures all listed tags
    #         'tags': main_article.css('.tags h3 a::text').getall(),
    #         'images': []
    #     }

    #     # TARGETED IMAGE EXTRACTION
    #     # 1. Main Featured Image (Usually at the top)
    #     main_img_cont = main_article.css('.img-cont')
    #     if main_img_cont:
    #         item['images'].append({
    #             'url': main_img_cont.css('img::attr(src)').get(),
    #             'alt': main_img_cont.css('img::attr(alt)').get(),
    #             'caption': main_img_cont.css('.img-cap::text').get(),
    #             'type': 'featured'
    #         })

    #     # 2. In-Text & Gallery Images (The ones after the text)
    #     # These are often inside 'div.imgcontainer' or direct 'img' tags in the body
    #     body_images = main_article.css('#articleBody .imgcontainer img, #articleBody img')
        
    #     for img in body_images:
    #         img_url = img.css('::attr(src)').get()
            
    #         # Skip tracking pixels or small icons
    #         if not img_url or 'logo' in img_url.lower():
    #             continue
                
    #         # Deduplication: Don't add the main featured image again
    #         if any(img_url == existing['url'] for existing in item['images']):
    #             continue

    #         # For gallery images, the 'title' attribute is often used as a caption
    #         item['images'].append({
    #             'url': img_url,
    #             'alt': img.css('::attr(alt)').get(),
    #             'caption': img.css('::attr(title)').get(), # Captures the 'title' as secondary caption
    #             'type': 'body_gallery'
    #         })

    #     # Clean up whitespace for all string fields
    #     for key in ['title', 'author', 'publish_date']:
    #         if item[key]:
    #             item[key] = item[key].strip()

    #     yield item









    ### Previous version for downloading raw HTML files; but it will take a lot of space so I used ETL instead ###
    
    # def start_requests(self):
    #     # Change this to your actual file path
    #     file_path = 'extracted_links/article_links.txt'
        
    #     if not os.path.exists(file_path):
    #         self.logger.error(f"File {file_path} not found!")
    #         return

    #     with open(file_path, 'r', encoding='utf-8') as f:
    #         for line in f:
    #             url = line.strip()
    #             if url:
    #                 # Use the ID at the end of the URL as a unique identifier for the file
    #                 page_id = url.split('/')[-1]
    #                 yield scrapy.Request(url=url, callback=self.parse, meta={'page_id': page_id})

    # def parse(self, response):
    #     page_id = response.meta['page_id']
    #     directory = 'downloaded_html/politics'
    #     os.makedirs(directory, exist_ok=True)
        
    #     filename = os.path.join(directory, f"{page_id}.html")
    #     with open(filename, 'wb') as f:
    #         f.write(response.body)