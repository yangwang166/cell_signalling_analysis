# 增加一个API的步骤

1. 先确保在client下希望的sql可以执行, 明白自己要做什么  
2. 在`templates/index.html`中增加对应的测试项  
比如增加人群分布的测试:  
```html
<p><a href="/test_people_distribution">5 测试人基于数据量的分布</a></p>
```  
3. 在`api_platform.py`中增加测试项的相应api  
在Application的__init__中的handler中增加:  
```py
(r'/test_people_distribution',
TestPeopleDistributionHandler),
```
然后增加`TestPeopleDistributionHandler`的定义:  
```py
class TestPeopleDistributionHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_people_distribution.html')
```
4. 创建`templates/test_people_distribution.html`:
将需要传递给api的参数通过表单的形式传递给server:
```html
<!DOCTYPE html>
<html>
  <head><title>测试人基于数据量的分布</title></head>
  <body>
    <h1>输入参数</h1>
    <form method="post" action="/compute_people_distribution">
    <p>项目ID<br><input type="text" name="project_id"></p>
    <p>区间间隔<br><input type="text" name="interval_size"></p>
    <p>数据分区(日期)<br><input type="text" name="date_p"></p>
    <p>返回前n个数据<br><input type="text" name="top_n"></p>
    <input type="submit">
    </form>
  </body>
</html>
```
5. 在`api_platform.py`中增加api的相应Handler
注册handler:  
```py
(r'/compute_people_distribution',
  ComputePeopleDistributionHandler),
```
定义handler, 这个是api的主体部分, 后面会详细讲下怎么填充这两个方法:  
```py
class ComputePeopleDistributionHandler(tornado.web.RequestHandler, BaseHandler):
  def doComputePeopleDistribution(self, exe):
    pass
  def post(self):
    pass
```
6. `post(self)`实现
主体氛围四个步骤:
```py
def post(self):
  # 解析输入的参数
  self.project_id = self.get_argument('project_id');
  self.interval_size = self.get_argument('interval_size');
  self.date_p = self.get_argument('date_p');
  self.top_n = self.get_argument('top_n');
  # 构造阿里云上运行的sql
  sql = ('select t.r as interval, count(*) as count from ('
         'select uuid, floor((count(*)/' + self.interval_size + ')) as r '
         'from ' + self.project_id + '_spatio_temporal_raw_data '
         'where date_p=' + self.date_p + ' '
         'group by uuid) t '
         'group by t.r '
         'order by interval '
         'limit ' + self.top_n + ';')
  plog("sql: " + sql)
  # 调用阿里云执行sql
  BaseHandler.runCmd(self, sql, \
      "compute_people_distribution", self.doComputePeopleDistribution)
  # 渲染结果页面
  self.render('people_distribution_result.html',
              project_id = self.project_id,
              interval_size = self.interval_size,
              date_p = self.date_p,
              top_n = self.top_n,
              ret_msg = "Success")
```
7. `doComputePeopleDistribution()`实现
利用ODPS提供的py接口, 将sql提交到集群计算, 并随时拉取进度。并将进度存入本地数据库。计算结果也存入本地数据库(针对本api)。这个函数具体情况具体分析，不同的api的实现会不同。
8. 增加task_id定义  
```py
# 定义见文档: 任务进度查询.md
_task_id = {"create_customer_raw_data_table":1,
            "upload_customer_raw_data":2,
            "transform_to_spatio_temporal_raw_data":3,
            "compute_people_distribution":4
          }
```
9. 创建`templates/people_distribution_result.html`
```html
<!DOCTYPE html>
<html>
  <head><title>人基于数据量的分布</title></head>
  <body>
    <p>项目ID: {{project_id}}</p>
    <p>区间间隔: {{interval_size}}</p>
    <p>数据分区(日期): {{date_p}}</p>
    <p>返回前n个数据: {{top_n}}</p>
    <p>返回结果: {{ret_msg}}</p>
  </body>
</html>
```
