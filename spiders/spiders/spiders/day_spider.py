from datetime import datetime
import json
import time
import requests
import scrapy
from dateutil.relativedelta import relativedelta    


# 与product_spider非常类似，故无注释


class day_spider(scrapy.Spider):
    name = 'day_spider'
    allowed_domains = ['nc.mofcom.gov.cn']

    def start_requests(self):
        date = datetime.now()
        body = f"queryDateType=0&timeRange={date.strftime('%Y-%m-%d')}~{date.strftime('%Y-%m-%d')}"
        yield scrapy.Request(url ='http://nc.mofcom.gov.cn/jghq/priceList',body=body,method='POST')

    def parse(self, response):
        date = datetime.now()
        response = json.loads(response.text)
        if response['totalCount'] == 0:
            return
        for pageNo in range(1,response['totalPageCount']+1):
            time.sleep(1)
            body = f"queryDateType=0&timeRange={date.strftime('%Y-%m-%d')}~{date.strftime('%Y-%m-%d')}&pageNo={pageNo}"
            yield scrapy.Request(url ='http://nc.mofcom.gov.cn/jghq/priceList',body=body,method='POST',callback=self.parse_product)
    
    def parse_product(self, response):
        data = json.loads(response.text)
        for product in data['result']:
            product['P_INDEX']= str(int(product['P_INDEX'])//100*100)
            product['GET_P_DATE']= product['GET_P_DATE']/1000   #修正时间戳
            yield {     #提取有效信息，并使信息名称与addr_spider中保持一致
                'price': product['AG_PRICE'],
                'varietyName': product['CRAFT_NAME'],
                'C_UNIT': product['C_UNIT'],
                'Date': time.strftime("%Y-%m-%d", time.localtime(product['GET_P_DATE']/1000)),
                'varietyCode': product['CRAFT_INDEX'],
                'M_ID': product['ID'],
                'C_INDEX':product['P_INDEX']
            }           