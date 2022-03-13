import scrapy
import json
import os
import pymysql

class variety_pipeline():
    def __init__(self):     #读取配置文件连接数据库
        path = os.path.dirname(os.path.abspath(__file__))
        self.settings = json.load(open(os.path.join(path,'settings.json')))
        self.conn = pymysql.connect(host = 'localhost',user = self.settings['user'],passwd=self.settings['passwd'],port = 3306,charset='utf8')
        self.cursor = self.conn.cursor()
        self.cursor.execute(f"use {self.settings['database_name']}")
    
    def process_item(self, item, spider):
        try:
            self.cursor.execute(f"INSERT INTO {self.settings['varietyTable_name']}(varietyCode,varietyName,name) VALUES({item['varietyCode']},'{item['varietyName']}','{item['name']}')")   #将种类数据存入数据库
            self.conn.commit()
        except Exception as e:
            print(e)
            self.conn.rollback()

    def close_spider(self,spider):
        self.cursor.close()
        self.conn.close()   


class variety_spider(scrapy.Spider):
    name = 'variety_spider'
    allowed_domains = ['nc.mofcom.gov.cn']  #允许域名
    custom_settings = {     # 自定义设置，连接pipeline和spider
        'ITEM_PIPELINES': {
            'spiders.spiders.variety_spider.variety_pipeline': 400
        }
    }

    def start_requests(self):
        return [scrapy.Request(url ='http://nc.mofcom.gov.cn/jghq/variety',method='GET')]

    def parse(self, response):
        response = json.loads(response.text)[1::]
        for upper_variety in response:
            for variety in upper_variety['variety']:
                yield {
                    'name':upper_variety['name'],
                    'varietyCode':variety['varietyCode'],
                    'varietyName':variety['varietyName']
                }   #例子：{'name': '水产品', 'varietyCode': '15052322', 'varietyName': '章鱼'}