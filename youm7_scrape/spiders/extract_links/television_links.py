import scrapy


class TelevisionLinksSpider(scrapy.Spider):
    name = "television_links"
    allowed_domains = ["youm7.com"]
    

    # 4756 is the last page number for the 'تليفزيون' section as of now
    async def start(self):
        base_url = 'https://www.youm7.com/Section/تليفزيون/251/'
        for i in range(1, 4757):
            url = f'{base_url}{i}'
            yield scrapy.Request(url=url, callback=self.parse)


    def parse(self, response):
        # Using the specific structure from your uploaded 'تليفزيون' file
        # Selector: div.bigOneSec contains the h3 and the anchor tag
        links = response.css('div.bigOneSec h3 a::attr(href)').getall()
        
        for link in links:
            yield {
                'url': response.urljoin(link)
            }
