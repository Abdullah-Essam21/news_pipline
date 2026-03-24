import scrapy


class YourHoroscopeTodayLinksSpider(scrapy.Spider):
    name = "your_horoscope_today_links"
    allowed_domains = ["youm7.com"]

    # 1228 is the last page number for the 'حظك-اليوم' section as of now
    async def start(self):
        base_url = 'https://www.youm7.com/Section/حظك-اليوم/330/'
        for i in range(1, 1229):
            url = f'{base_url}{i}'
            yield scrapy.Request(url=url, callback=self.parse)


    def parse(self, response):
        # Using the specific structure from your uploaded 'حظك-اليوم' file
        # Selector: div.bigOneSec contains the h3 and the anchor tag
        links = response.css('div.bigOneSec h3 a::attr(href)').getall()
        
        for link in links:
            yield {
                'url': response.urljoin(link)
            }
