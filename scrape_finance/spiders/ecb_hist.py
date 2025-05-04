import scrapy

class EcbHistSpider(scrapy.Spider):
    name = 'ecb_hist'
    allowed_domains = ["www.ecb.europa.eu"]
    start_urls = ["https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/usd.xml"]

    def parse(self, response):
        namespaces = {
            'sdmx': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message',
            'exr': 'http://www.ecb.europa.eu/vocabulary/stats/exr/1'
        }
        group = response.xpath("//exr:Group", namespaces=namespaces).get()
        attrib = response.xpath("//exr:Group", namespaces=namespaces).attrib

        self.logger.info(f'grpoup attributes: {attrib}')

        observations = response.xpath("//exr:Obs", namespaces=namespaces)
        for obs in observations:
            date = obs.xpath("@TIME_PERIOD").get()
            spot = obs.xpath("@OBS_VALUE").get()

            yield {'date': date, 'value': spot}
        
        yield {'group attr': attrib}




       