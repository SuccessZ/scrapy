## 运行方法

#### 准备工作

1. 运行前请保证系统上安装有MySQL数据库，本项目使用数据库版本为 `mysql-8.0.27`		

2. 请修改 `./spiders/spiders/spiders/settings.json`文件，将本地数据库的账号密码正确输入其中，并根据自己喜好设置数据库与表的名称。

3. 默认设置为：
   1. 数据库：productData ；
   2. 市场表：market ；
   3. 城市表：city ；
   4. 省份表名：province ；
   5. 种类表名：variety ；
   6. 农产品表名：product ；

#### 运行文件

1. 运行文件为 `./spiders/main.py`
2. 直接运行该运行文件即可
3. 请自行在文件中添加/更改 `time.sleep()`，以免对爬取目标网页造成不利影响。

## 网页信息

```python
#农产品信息
# 获取时间段内的总页数，参数：start_date,end_date
url = 'http://nc.mofcom.gov.cn/jghq/priceList'
data = f"queryDateType=4&timeRange={start_date.strftime('%Y-%m-%d')}"f"+~+{end_date.strftime('%Y-%m-%d')}"
data = {
    'queryDateType':4,
    'timeRange':f"{start_date.strftime('%Y-%m-%d')}~{end_date.strftime('%Y-%m-%d')}"
}
headers = {
	"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"    
}
#获取总页数后，对每一页请求数据
url = 'http://nc.mofcom.gov.cn/jghq/priceList'
data = f"queryDateType=4&timeRange={start_date.strftime('%Y-%m-%d')}"f"~{end_date.strftime('%Y-%m-%d')}"f"&pageNo=3"
data={
    'queryDateType':4,
    'timeRange':f"{start_date.strftime('%Y-%m-%d')}~{end_date.strftime('%Y-%m-%d')}",
    'pageNo':3
}
headers = {
	"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"    
}
#最后从每一页的response提取农产品数据,并由引擎提交给pipeline


#省份与城市信息
# 获取省份编号 pIndex，POST
url = 'http://nc.mofcom.gov.cn/nc/qyncp/province'
#将省份编号作为参数，获取城市信息
url = "http://nc.mofcom.gov.cn/nc/qyncp/province"
headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
body="pIndex=" + item["P_INDEX"]	#body是scrapy.request的写法，requests.post是data


#市场信息
#通过省份与城市编号获得市场信息，与农产品类似，有很多页。此处可采用判断返回的 hasNext 属性的值来判断是否已遍历所有市场
url = "http://nc.mofcom.gov.cn/nc/qyncp/province",
headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
body=f"province{province['P_INDEX']}&city{city['P_INDEX']}"f"&isprod_mark=&par_craft_index=&pageNo={data['nextPage']}"


#农产品种类信息,GET
url = 'http://nc.mofcom.gov.cn/jghq/variety'
```

