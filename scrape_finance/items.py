# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CurrencyItem(scrapy.Item):
    # define the fields for your item here like:
    date = scrapy.Field()
    spot = scrapy.Field()
    
