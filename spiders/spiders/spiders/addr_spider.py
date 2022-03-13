import scrapy
import os
import json
import pymysql
import time
import requests

class addr_pipeline():  # 将处理地址（省份，城市，市场）的spider和pipeline写在同一个文件里
    def __init__(self):     #读取配置文件连接数据库
        path = os.path.dirname(os.path.abspath(__file__))
        self.settings = json.load(open(os.path.join(path,'settings.json')))
        self.conn = pymysql.connect(host = 'localhost',user = self.settings['user'],passwd=self.settings['passwd'],port = 3306,charset='utf8')
        self.cursor = self.conn.cursor()
        self.cursor.execute(f"use {self.settings['database_name']}")
    
    def process_item(self, item, spider):
        if item['name'] == 'market':
            try:
                self.cursor.execute(f"INSERT INTO {self.settings['marketTable_name']}(M_ID,M_NAME,M_ADDR,M_DESC,C_INDEX) VALUES({item['M_ID']},'{item['M_NAME']}','{item['M_ADDR']}','{item['M_DESC']}',{item['C_INDEX']})")   #将市场数据存入数据库
                self.conn.commit()
            except Exception as e:
                print(e,item)
                self.conn.rollback()
        else:
            try:
                self.cursor.execute(f"INSERT INTO {self.settings['provinceTable_name']}(P_INDEX,P_NAME) VALUES({item['P_INDEX']},'{item['P_NAME']}')")   #将省份数据存入数据库
                for city in item['CITIES']:
                    self.cursor.execute(f"INSERT INTO {self.settings['cityTable_name']}(C_INDEX,C_NAME,P_INDEX) VALUES ({city['P_INDEX']},'{city['P_NAME']}',{item['P_INDEX']})")
                # data = [(self.settings['cityTable_name'],city['P_INDEX'],city['P_NAME'],item['P_INDEX']) for city in item['CITIES']]
                # self.cursor.executemany("INSERT INTO %s(C_INDEX,C_NAME,P_INDEX) VALUES (%s,%s,%s)",data)   #将城市数据存入数据库,executemany:批量插入数据,此处因为 %s 会出现"",而表名不得有"",所以不可用
                self.conn.commit()   #执行
            except Exception as e:
                print(e,item)
                self.conn.rollback()

    def close_spider(self,spider):
        self.cursor.close()
        self.conn.close()   
        
class addr_spider(scrapy.Spider):
    name = 'addr_spider'    # 爬虫名称
    allowed_domains = ['nc.mofcom.gov.cn']  #允许域名
    custom_settings = {     # 自定义设置，连接pipeline和spider
        'ITEM_PIPELINES': {
            'spiders.spiders.addr_spider.addr_pipeline': 300,
        }
    }

    def start_requests(self):   #获取所有省份的index(P_INDEX)
        return [scrapy.Request(url='http://nc.mofcom.gov.cn/nc/qyncp/province',method='POST')]

    def parse(self, response):      #获取每个省份下的城市信息
        data = json.loads(response.text)
        for province in data:   
            body = 'pIndex='+province['P_INDEX']    #将P_INDEX作为访问网页的参数
            time.sleep(1)
            yield scrapy.Request(url='http://nc.mofcom.gov.cn/nc/qyncp/province',method='POST',body=body,callback=self.parse_city)  #回调函数为parse_city
    
    def parse_city(self, response):        #获取每个城市下的市场信息
        data = json.loads(response.text)
        yield {     #将省份城市信息返回给city_pipeline
            'name':'city',  #区分管道
            'P_INDEX':data[0]['P_INDEX'],
            'P_NAME':data[0]['P_NAME'],
            'CITIES':data[1::]
        }
        p_name = data[0]['P_NAME']
        p_index = data[0]['P_INDEX']
        for city in data[1::]:
            meta = {    #省份与城市信息规格化
                'C_INDEX':city['P_INDEX']
            }
            data = {
                'province':p_index,
                'city':city['P_INDEX']
            }
            time.sleep(1)
            res = requests.post(url="http://nc.mofcom.gov.cn/jghq/marketList",data=data)    #获取该城市下的市场信息概览
            res = json.loads(res.text)
            if res['totalCount'] == 0:  #如果没有市场，则停止本次循环
                continue
            for pageNo in range(1,res['totalPageCount']+1):     #对该城市下每一页的市场信息发起请求
                body = f'province={p_index}&city={meta["C_INDEX"]}&pageNo={pageNo}'
                time.sleep(1)
                yield scrapy.Request(url='http://nc.mofcom.gov.cn/jghq/marketList',method='POST',body=body,meta=meta,callback=self.parse_market)    #回调函数为parse_market

    def parse_market(self, response):
        meta = response.meta    #获取规格化后的省份城市信息
        response = json.loads(response.text)
        for market in response['result']:
            yield {     #将市场信息与省份城市信息组装在一起，交给pipeline处理
                'name':'market',    #区分处理的管道文件
                'C_INDEX':meta['C_INDEX'],
                'M_NAME':market['EUD_NAME'],
                'M_ID':market['ID'],
                'M_DESC':str(market['CONTENT']).replace('\'','’'),
                'M_ADDR':market['ADDR']
            }
        