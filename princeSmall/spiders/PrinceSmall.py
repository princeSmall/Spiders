#!/usr/bin/python
# -*- coding:utf-8 -*-

from scrapy.spiders import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import scrapy


from ..items import PrincesmallItem
import codecs
import json


# 获取princesmall的所有title，link，time
class princesmallSpider(Spider):
    name = "princesmall"

    download_delay = 1

    allowed_domains = ["princesmall.cn"]
    start_urls = [
        "http://princesmall.cn"
    ]

    def parse(self, response):
        sel = Selector(response)
        sites = sel.xpath('//header[@class="post-header"]')

        for site in sites:
            item = PrincesmallItem()
            title = site.xpath('h1/a/text()').extract()
            link = site.xpath('h1/a/@href').extract()
            time = site.xpath('div/span/time/text()').extract()
            item['title'] = [t.encode('utf-8') for t in title]
            item['link'] = [l.encode('utf-8') for l in link]
            item['time'] = [m.encode('utf-8') for m in time]
            yield item

        # title_prince = sel.xpath('//header[@class="post-header"]/h1/a/text()').extract()
        # url_prince = sel.xpath('//header[@class="post-header"]/h1/a/@href').extract()
        # time_prince = sel.xpath('//header[@class="post-header"]/div/span/time/text()').extract()
        #
        # item['title'] = [t.encode('utf-8') for t in title_prince]
        # item['link'] = [l.encode('utf-8') for l in url_prince]
        # item['time'] = [m.encode('utf-8') for m in time_prince]
        #
        # yield item

        print
        item, '------'
        urls = sel.xpath('//nav[@class="pagination"]/a[@class="extend next"]/@href').extract()
        for url in urls:
            url = "http://princesmall.cn" + url
            yield Request(url, callback=self.parse)


            # yield scrapy.Request(url,callback=self.parse)


# 获取w3cshoolxml的title，link，desc
class w3schoolSpider(Spider):
    name = "w3school"
    allowed_domains = ["w3school.com.cn"]
    start_urls = [
        "http://www.w3school.com.cn/xml/xml_syntax.asp"
    ]

    def parse(self, response):
        sel = Selector(response)
        sites = sel.xpath('//div[@id="navsecond"]/div[@id="course"]/ul[1]/li')
        items = []

        for site in sites:
            item = PrincesmallItem()

            title = site.xpath('a/text()').extract()
            link = site.xpath('a/@href').extract()
            desc = site.xpath('a/@title').extract()

            item['title'] = [t.encode('utf-8') for t in title]
            item['link'] = [l.encode('utf-8') for l in link]
            item['desc'] = [d.encode('utf-8') for d in desc]
            items.append(item)

        return items