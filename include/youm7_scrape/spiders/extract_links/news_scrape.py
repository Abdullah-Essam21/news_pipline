import scrapy
import os

# صفحة سياسة #

class NewsScrapeSpider(scrapy.Spider):
    name = "news_scrape"
    allowed_domains = ["youm7.com"]
    
    # Generate all 6946 page URLs
    start_urls = [f'https://www.youm7.com/Section/سياسة/319/{i}' for i in range(1, 6947)]

    def parse(self, response):
        # Using the selector from the 'سياسة - اليوم السابع.htm' file provided
        # The articles are inside 'div.bigOneSec'
        links = response.css('div.bigOneSec h3 a::attr(href)').getall()
        
        # Open file with explicit utf-8 encoding to fix your error
        with open("article_links.txt", "a", encoding="utf-8") as f:
            for link in links:
                # response.urljoin ensures relative links become full URLs
                full_url = response.urljoin(link)
                f.write(full_url + "\n")
        
        self.log(f"Saved {len(links)} links from {response.url}")
            
    #         # 3. Follow the link to download the actual article page
    #         yield scrapy.Request(full_url, callback=self.save_html)

    # def save_html(self, response):
    #     # Use the ID at the end of the URL as the filename
    #     page_id = response.url.split("/")[-1]
    #     filename = f'downloads/{page_id}.html'
        
    #     # Ensure the directory exists
    #     os.makedirs('downloads', exist_ok=True)
        
    #     # Save the full HTML content
    #     with open(filename, 'wb') as f:
    #         f.write(response.body)