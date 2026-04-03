import scrapy


class InvestigationsLinksSpider(scrapy.Spider):
    name = "investigations_links"
    allowed_domains = ["youm7.com"]
    

    # 8820 is the last page number for the 'تحقيقات-وملفات' section as of now
    async def start(self):
        base_url = 'https://www.youm7.com/Section/تحقيقات-وملفات/12/'
        for i in range(1, 8821):
            url = f'{base_url}{i}'
            yield scrapy.Request(url=url, callback=self.parse)


    def parse(self, response):
        # Using the specific structure from your uploaded 'تحقيقات-وملفات' file
        # Selector: div.bigOneSec contains the h3 and the anchor tag
        links = response.css('div.bigOneSec h3 a::attr(href)').getall()
        
        for link in links:
            yield {
                'url': response.urljoin(link)
            }
