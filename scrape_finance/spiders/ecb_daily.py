import scrapy


class EcbDailySpider(scrapy.Spider):
    name = "ecb_daily"
    allowed_domains = ["www.ecb.europa.eu"]
    start_urls = ["https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html"]

    def parse(self, response):
        date = response.xpath("//div[@class = 'content-box']/h3/text()").get()
        print(date)
        headers = response.xpath("//table[@class='forextable']/thead/tr/th/text()").getall()[:2]

        usd_row = response.xpath("//table[@class='forextable']/tbody/tr[td[@id='USD']]")
        spot = usd_row.xpath("td[3]//span[@class='rate']/text()").get().strip()

        yield {"date": date,
               "value": spot}
        
        
