import scrapy

class UrgentLinksSpider(scrapy.Spider):
    name = "urgent_links"
    allowed_domains = ["youm7.com"]
    

    # 16837 is the last page number for the 'أخبار-عاجلة' section as of now
    async def start(self):
        base_url = 'https://www.youm7.com/Section/أخبار-عاجلة/65/'
        for i in range(6000, 10000):
            url = f'{base_url}{i}'
            yield scrapy.Request(url=url, callback=self.parse)


    def parse(self, response):
        # Using the specific structure from your uploaded 'أخبار-عاجلة' file
        # Selector: div.bigOneSec contains the h3 and the anchor tag
        links = response.css('div.bigOneSec h3 a::attr(href)').getall()
        
        for link in links:
            yield {
                'url': response.urljoin(link)
            }



    
    