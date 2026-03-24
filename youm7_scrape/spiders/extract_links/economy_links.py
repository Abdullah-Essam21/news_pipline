import scrapy

#صفحة اقتصاد و بورصة #

class EconomyLinksSpider(scrapy.Spider):
    name = "economy_links"
    allowed_domains = ["youm7.com"]

    # Generate all 9000 pages immediately
    # Scrapy will queue these and process them based on CONCURRENT_REQUESTS
    start_urls = [f'https://www.youm7.com/Section/اقتصاد-وبورصة/297/{i}' for i in range(1, 9222)]

    def parse(self, response):
        # Using the specific structure from your uploaded 'اقتصاد وبورصة' file
        # Selector: div.bigOneSec contains the h3 and the anchor tag
        links = response.css('div.bigOneSec h3 a::attr(href)').getall()
        
        for link in links:
            yield {
                'url': response.urljoin(link),
                'category': 'economy'
            }