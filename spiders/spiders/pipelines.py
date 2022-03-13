# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import os
import json
import pymysql
import pandas as pd

class SpidersPipeline:
    def __init__(self):     #读取配置文件连接数据库
        self.path = os.path.dirname(os.path.abspath(__file__))
        self.settings = json.load(open(os.path.join(self.path,'spiders\settings.json')))
        self.conn = pymysql.connect(host = 'localhost',user = self.settings['user'],passwd=self.settings['passwd'],port = 3306,charset='utf8')
        self.cursor = self.conn.cursor()
        self.cursor.execute(f"use {self.settings['database_name']}")
        self.C_INDEX_list = []
        self.M_ID_list = []
        self.varietyCode_list = []
        self.price_list = []
        self.C_UNIT_list = []
        self.Date_list = []
    
    def process_item(self, item, spider):
        try:
            self.cursor.execute(f"INSERT INTO {self.settings['productTable_name']}(C_INDEX,M_ID,varietyCode,price,C_UNIT,Date) VALUES({item['C_INDEX']},{item['M_ID']},{item['varietyCode']},{item['price']},'{item['C_UNIT']}','{item['Date']}')")   #将农产品数据存入数据库
            self.conn.commit()
        except Exception as e:  #将错误信息收集起来，人工处理
            print(e,item)
            self.C_INDEX_list.append(item['C_INDEX'])
            self.M_ID_list.append(item['M_ID'])
            self.varietyCode_list.append(item['varietyCode'])
            self.price_list.append(item['price'])
            self.C_UNIT_list.append(item['C_UNIT'])
            self.Date_list.append(item['Date'])
            self.conn.rollback()

    def close_spider(self,spider):
        self.cursor.close()
        self.conn.close()  
        frame = pd.DataFrame({
            'C_INDEX':self.C_INDEX_list,
            'M_ID':self.M_ID_list,
            'varietyCode':self.varietyCode_list,
            'price':self.price_list,
            'C_UNIT':self.C_UNIT_list,
            'Date':self.Date_list
        }) 
        frame.to_csv(os.path.join(self.path,'error.csv'),index=False)   #错误信息存储在当前文件夹下的error.csv中
