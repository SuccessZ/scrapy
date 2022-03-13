import os
import pymysql
import json
import pandas as pd

class productSpider():
    def __init__(self):     #连接数据库
        self.path = os.path.dirname(os.path.abspath(__file__))
        self.settings = json.load(open(os.path.join(self.path,'spiders\spiders\settings.json')))
        self.conn = pymysql.connect(host = 'localhost',user = self.settings['user'],passwd=self.settings['passwd'],port = 3306,charset='utf8')
        self.cursor = self.conn.cursor()

    def createDB(self):     #创建数据库和对应的表
        self.cursor.execute(f"DROP DATABASE {self.settings['database_name']}")  #删除之前重名的数据库
        self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.settings['database_name']}")  #创建数据库
        self.cursor.execute(f"use {self.settings['database_name']}")
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.settings['provinceTable_name']}(P_INDEX INT,P_NAME TEXT,CONSTRAINT PRIMARY KEY(P_INDEX))")  #省份表
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.settings['cityTable_name']}(C_INDEX INT,C_NAME TEXT,P_INDEX INT,CONSTRAINT PRIMARY KEY(C_INDEX),FOREIGN KEY (P_INDEX) REFERENCES {self.settings['provinceTable_name']}(P_INDEX) ON DELETE CASCADE ON UPDATE CASCADE)") #城市表
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.settings['marketTable_name']}(M_ID INT,M_NAME TEXT,M_ADDR TEXT,M_DESC TEXT,C_INDEX INT,CONSTRAINT PRIMARY KEY(C_INDEX,M_ID),FOREIGN KEY (C_INDEX) REFERENCES {self.settings['cityTable_name']}(C_INDEX) ON DELETE CASCADE ON UPDATE CASCADE)")    #市场表
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.settings['varietyTable_name']}(varietyCode INT,varietyName TEXT,name TEXT,CONSTRAINT PRIMARY KEY(varietyCode))")    #种类表
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.settings['productTable_name']}(C_INDEX INT,M_ID INT,varietyCode INT,price FLOAT,C_UNIT TEXT,Date DATE,FOREIGN KEY (C_INDEX,M_ID) REFERENCES {self.settings['marketTable_name']}(C_INDEX,M_ID) ON DELETE CASCADE ON UPDATE CASCADE,FOREIGN KEY (varietyCode) REFERENCES {self.settings['varietyTable_name']}(varietyCode) ON DELETE CASCADE ON UPDATE CASCADE)")    #农产品表

    def crawlSpider(self):  #开始爬虫
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        os.system("scrapy crawl addr_spider")   #爬取省份，城市，市场数据
        print('addr_spider finished -----')
        os.system("scrapy crawl variety_spider")    #爬取种类数据
        print('variety_spider finished -----')
        os.system("scrapy crawl product_spider")    #爬取农产品数据
        print('product_spider finished -----')

    def crawlOneDay(self):  # 只会获得当前日期的农产品数据，适用于需持续更新的情况
        os.system("scrapy crawl day_spider") 

    def process_error(self):    #将错误信息人工修改后，存入数据库
        error_data = pd.read_csv(os.path.join(self.path,'spiders\error.csv'))   #读取error信息
        for i,item in error_data.iterrows(): #存入数据库
            try:
                self.cursor.execute(f"INSERT INTO {self.settings['productTable_name']}(C_INDEX,M_ID,varietyCode,price,C_UNIT,Date) VALUES({item['C_INDEX']},{item['M_ID']},{item['varietyCode']},{item['price']},'{item['C_UNIT']}','{item['Date']}')")   #将农产品数据存入数据库
            except Exception as e:
                print(e,item)
        os.remove(os.path.join(self.path,'spiders\error.csv'))  #删除异常文件

if __name__ == '__main__':
    spider = productSpider()
    spider.createDB()
    spider.crawlSpider()
    # spider.process_error()  #手动修正异常数据后再运行
