import scrapy

class DownloadNewArticlesJsonlSpider(scrapy.Spider):
    name = "download_new_articles_jsonl"
    allowed_domains = ["youm7.com"]

    category_mapping = {
        'arab': 'عرب', 'art': 'فن', 'caricature': 'كاريكاتير',
        'economy': 'اقتصاد و بورصة', 'investigations': 'تحقيقات',
        'television': 'تلفزيون', 'urgent': 'عاجل',
        'your_horoscope_today': 'حظك اليوم'
    }

    def start_requests(self):
        # Add as many categories as you want here
        urls = {
            "urgent": "https://www.youm7.com/Section/أخبار-عاجلة/65/1",
        }
        for category, url in urls.items():
            yield scrapy.Request(
                url=url, 
                callback=self.parse, 
                meta={'category': self.category_mapping.get(category, 'unknown')}
            )

    def parse(self, response):
        """Step 1: Extract links to all individual news stories from the section page."""
        # Find all story links (usually inside h3 or div within the list)
        story_links = response.css('h3 a::attr(href), .news-dev8 a::attr(href)').getall()
        
        for link in story_links:
            # Ensure the link is absolute
            full_url = response.urljoin(link)
            # Pass to the article parser
            yield scrapy.Request(
                url=full_url, 
                callback=self.parse_article, 
                meta={'category': response.meta['category']}
            )

    def parse_article(self, response):
        """Step 2: Extract data from the actual article page."""
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
                        'type': 'image', 'url': img_url,
                        'alt': img.css('::attr(alt)').get(),
                        'caption': img.css('::attr(title)').get() or main_article.css('.img-cap::text').get(),
                        'provider': get_provider(img_url)
                    })

        # 2. VIDEOS
        video_urls = main_article.css('iframe::attr(src), video source::attr(src), video::attr(src)').getall()
        for v_url in video_urls:
            if v_url and not any(v_url == m['url'] for m in item['media']):
                item['media'].append({
                    'type': 'video', 'url': v_url, 'alt': None, 'caption': None,
                    'provider': get_provider(v_url)
                })

        # 3. AUDIO
        audio_sources = main_article.css('audio source::attr(src), audio::attr(src)').getall()
        audio_links = main_article.xpath('//a[contains(@href, ".mp3")]/@href').getall()
        for a_url in set(audio_sources + audio_links):
            full_audio_url = response.urljoin(a_url)
            if not any(full_audio_url == m['url'] for m in item['media']):
                item['media'].append({
                    'type': 'audio', 'url': full_audio_url, 'alt': None, 'caption': None,
                    'provider': get_provider(full_audio_url)
                })

        yield item