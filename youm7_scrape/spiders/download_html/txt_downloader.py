import scrapy
import os

class TxtDownloaderSpider(scrapy.Spider):
    name = "txt_downloader"    

    # ---------------------------------------------------------
    # CONFIGURATION: Hardcode your specific file and category here
    # ---------------------------------------------------------
    TARGET_FILE = 'extracted_links/reports_links.txt'
    CATEGORY_NAME = 'تقارير-مصرية'  # This is just a label for the category; it doesn't affect scraping
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