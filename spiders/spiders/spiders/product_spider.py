from datetime import datetime
import json
import time
import scrapy
from dateutil.parser import parse       #时间格式转化
from dateutil.relativedelta import relativedelta    #时间间隔


class product_spider(scrapy.Spider):
    name = 'product_spider'
    allowed_domains = ['nc.mofcom.gov.cn']
    def __init__(self, start_date='2021-3-10',end_date=None):   #起始时间为 2016-10-1,可根据需求更改
        self.start_date = start_date
        self.end_date = end_date

    def start_requests(self):   
        if self.end_date is not None:   #limit_date表示最早的可爬取日期，start_date表示一次爬取的起始日期，end_date表示一次爬取的结束日期
            end_date = parse(self.end_date,fuzzy = True)
        else:
            end_date = datetime.now()
        limit_date = parse(self.start_date,fuzzy = True)
        start_date = end_date - relativedelta(month=3)  #以三个月为一个爬取周期
        while (start_date-limit_date).days>=0:  #若还在可爬取范围内
            time.sleep(1)
            body = f"queryDateType=0&timeRange={start_date.strftime('%Y-%m-%d')}~{end_date.strftime('%Y-%m-%d')}"   #将起始日期和结束日期作为参数
            meta = {
                'start_date':start_date,
                'end_date':end_date
            }
            yield scrapy.Request(url ='http://nc.mofcom.gov.cn/jghq/priceList',body=body,method='POST',meta=meta)   #请求目标是获得该时间范围内的数据总数（数据页数）
            start_date = start_date - relativedelta(month=3)    
            end_date = start_date - relativedelta(day=1)

        if(end_date - limit_date).days >0:  #处理剩余不足三个月的农产品数据
            body = f"queryDateType=0&timeRange={limit_date.strftime('%Y-%m-%d')}~{end_date.strftime('%Y-%m-%d')}"   
            meta = {
                'start_date':limit_date,
                'end_date':end_date
            }
            yield scrapy.Request(url ='http://nc.mofcom.gov.cn/jghq/priceList',body=body,method='POST',meta=meta)   

    def parse(self, response):  #提取总页数，并对每一页数据发起请求
        meta = response.meta
        start_date = meta['start_date']
        end_date = meta['end_date']     #提取爬取时间范围
        response = json.loads(response.text)
        if response['totalCount'] == 0: #若没有数据
            return
        for pageNo in range(1,response['totalPageCount']+1):    #对每一页农产品数据发起请求
            time.sleep(1)
            body = f"queryDateType=0&timeRange={start_date.strftime('%Y-%m-%d')}~{end_date.strftime('%Y-%m-%d')}&pageNo={pageNo}"   #与start_request相比，增加了pageNo参数
            yield scrapy.Request(url ='http://nc.mofcom.gov.cn/jghq/priceList',body=body,method='POST',callback=self.parse_product)     #回调函数为parse_product
    
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