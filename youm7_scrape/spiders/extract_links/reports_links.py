import scrapy
import asyncio

# class ReportsLinksSpider(scrapy.Spider):
#     name = "reports_links"
#     allowed_domains = ["youm7.com"]

#     # 15815 is the last page number for the 'اقتصاد وبورصة' section as of now
#     async def start(self):
#         base_url = 'https://www.youm7.com/Section/تقارير-مصرية/97/1'
#         for i in range(1, 15816):
#             url = f'{base_url}{i}'
#             yield scrapy.Request(url=url, callback=self.parse)


#     def parse(self, response):
#         # Using the specific structure from your uploaded 'تقارير مصرية' file
#         # Selector: div.bigOneSec contains the h3 and the anchor tag
#         links = response.css('div.bigOneSec h3 a::attr(href)').getall()
        
#         for link in links:
#             yield {
#                 'url': response.urljoin(link)
#             }


class ReportsLinksSpider(scrapy.Spider):
    name = "reports_links"
    allowed_domains = ["youm7.com"]

    # Generate all 6946 page URLs
    start_urls = [f'https://www.youm7.com/Section/تقارير-مصرية/97/{i}' for i in range(6947, 15816)]

    def parse(self, response):
        # Using the selector from the 'سياسة - اليوم السابع.htm' file provided
        # The articles are inside 'div.bigOneSec'
        links = response.css('div.bigOneSec h3 a::attr(href)').getall()
        
        # Open file with explicit utf-8 encoding to fix your error
        with open("extracted_links/reports_links.txt", "a", encoding="utf-8") as f:
            for link in links:
                # response.urljoin ensures relative links become full URLs
                full_url = response.urljoin(link)
                f.write(full_url + "\n")