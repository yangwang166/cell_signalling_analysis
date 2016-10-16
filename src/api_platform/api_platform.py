#-*- coding:utf-8 –*-

import os.path
import subprocess
import shlex
import multiprocessing
import datetime
from pymongo import MongoClient

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from tornado.options import define, options

define("port", default=8000, help="run on the given port", type=int)

# Make odps client configurable
_odps_client = "/Users/willwywang-NB/"+\
         "github/cell_signalling_analysis/tool/odps/bin/odpscmd"

_task_id = {"create_customer_raw_data_table":1,
           "upload_customer_raw_data":2,
           "transform_to_raw_data":3
          }

def plog(msg):
  print "API: ", msg

class Application(tornado.web.Application):
  def __init__(self):
    handlers=[
              (r'/', 
                IndexHandler),
              (r'/test_upload', 
                TestUploadHandler), 
              (r'/test_create_customer_raw_data', 
                TestCreateCustomerRawDataTableHandler),
              (r'/test_request_task_progress', 
                TestRequestTaskProgressHandler),
              (r'/test_transform_to_inner_format', 
                TestTransformToInnerFormatHandler),
              (r'/upload_data', 
                UploadHandler),
              (r'/create_customer_raw_data', 
                CreateCustomerRawDataHandler),
              (r'/request_task_progress', 
                RequestTaskProgressHandler),
              (r'/transform_to_inner_format', 
                TransformToInnerFormatHandler),
             ]
    settings = dict(
      template_path=os.path.join(os.path.dirname(__file__), "templates"),
      debug=True,
      )
    tornado.web.Application.__init__(self, handlers, **settings)

class IndexHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('index.html')

class TestUploadHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_upload.html')

class TestCreateCustomerRawDataTableHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_create_customer_raw_data_table.html')

class TestRequestTaskProgressHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_request_task_progress.html')

class TestTransformToInnerFormatHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_transform_to_inner_format.html')

class BaseHandler():
  def runProcess(self, exe):    
    p = subprocess.Popen(exe, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while(True):
      retcode = p.poll() #returns None while subprocess is running
      line = p.stdout.readline()
      yield line
      if(retcode is not None):
        break
    plog("\nFinish process")
  def runCmd(self, inner, process_name, target_fuc):
    cmd = """
    %s -e "%s";
    """ % (_odps_client, inner)

    plog("cmd: " + cmd)
    arguments =  shlex.split(cmd)
    #plog("arguments: " + ' '.join(arguments))
    process = multiprocessing.Process(name = process_name,
                                      target = target_fuc, 
                                      args = (arguments,))
    process.start()

class UploadHandler(tornado.web.RequestHandler, BaseHandler):
  def doUpload(self, exe):
    name = multiprocessing.current_process().name
    plog(name+" "+"Starting")
    total = 0
    finished = 0
    progress = 0
    session_id = ""
    for line in BaseHandler.runProcess(self, exe):
      print line,
      # extract upload progess and total block
      if "Upload session" in line:
        session_id = line.split()[2]
        plog("session_id: " + session_id)
        db_client = MongoClient('localhost', 27017)
        db = db_client[self.project_id + "_db"]
        collection = db["task-progress"]
        result = collection.delete_many({
                 "project_id": self.project_id,
                 "task_id": _task_id["upload_customer_raw_data"]})
        result = collection.insert_one(
            { "project_id" : self.project_id,
              "task_id" : _task_id["upload_customer_raw_data"],
              "aliyun_sess_id" : session_id,
              "progress" : progress,
              "lastModified" : datetime.datetime.utcnow()
            }
        )
        plog("result.inserted_id: " + str(result.inserted_id))
      if "Split input to" in line:
        total = int(line.split()[5])
        plog("total: " + str(total))
      elif "upload block complete" in line:
        finished += 1
        plog("finished: " + str(finished))
        progress = int((1.0 * finished / total) * 100)
        plog("progress: " + str(progress) + "%")
        result = collection.update_one(
          {"project_id" : self.project_id, 
            "task_id" : _task_id["upload_customer_raw_data"] },
          {
            "$set": {
              "progress": progress 
            },
            "$currentDate": {"lastModified": True}
          }
        )
        plog("result.matched_count: "+str(result.matched_count))
        plog("result.modified_count: "+str(result.modified_count))
    plog(name+" Exiting")
    db_client.close()
  def post(self):
    self.project_id = self.get_argument('project_id')
    data_path = self.get_argument('data_path')
    #aliyun_table = self.get_argument('aliyun_table')
    aliyun_table = self.project_id + "_customer_raw_data"
    threads_num = self.get_argument('threads_num')
    row_delimiter = self.get_argument('row_delimiter')
    col_delimiter = self.get_argument('col_delimiter')
    upload_cmd = "tunnel upload %s %s -bs 10 -threads %s -s true" \
                 % (data_path, aliyun_table, threads_num)
    ## TODO: drop table and clean progress db before a new upload
    BaseHandler.runCmd(self, upload_cmd, "upload_process", self.doUpload)
    self.render('upload_result.html', 
                project_id = self.project_id,
                data_path = data_path,
                aliyun_table = aliyun_table,
                threads_num = threads_num,
                row_delimiter = row_delimiter,
                col_delimiter = col_delimiter)

class CreateCustomerRawDataHandler(tornado.web.RequestHandler, BaseHandler):
  def isValidType(self, f_type):
    valid_type = ["bigint", "double", "string", "datetime", "boolean"]
    if f_type in valid_type:
      return True
    else:
      return False
  def doCreateCustomerRaw(self, exe):
    name = multiprocessing.current_process().name
    plog(name + ": " + "Starting")
    for line in BaseHandler.runProcess(self, exe):
      print line,
      if "ID" in line:
        session_id = line.split()[2]
        plog("session_id: " + session_id)
      if "OK" in line:
        db_client = MongoClient('localhost', 27017)
        db = db_client[self.project_id + "_db"]
        collection = db["task-progress"]
        result = collection.delete_many({
                 "project_id": self.project_id,
                 "task_id": _task_id["create_customer_raw_data_table"]})
        result = collection.insert_one(
            { "project_id" : self.project_id,
              "task_id" : _task_id["create_customer_raw_data_table"],
              "aliyun_sess_id" : session_id,
              "progress" : 100,
              "lastModified" : datetime.datetime.utcnow()
            }
        )
        plog("result.inserted_id: " + str(result.inserted_id))
    plog(name + ": " + "Exiting")
    db_client.close()
  def post(self):
    fields_raw = self.get_argument('fields')
    fields = fields_raw.split(",")
    self.project_id = self.get_argument('project_id');
    plog("fields: " + ', '.join(fields))
    plog("project_id: " + self.project_id)
    is_valid = True # 类型是否完全合法
    #Eg: create table if not exists nanjing1_customer_raw_data (uuid string, 
    #    call_in bigint, call_out bigint, time bigint, cell_id bigint, 
    #    cell_name string, lon double, lat double, in_room bigint, 
    #    is_roam bigint);
    fields_list = ""
    for pairs in fields:
      (f_name, f_type) = pairs.split("#")
      plog("f_name: " + f_name + ", " + "f_type: " + f_type)
      # 检查类型是否合法
      if(not self.isValidType(f_type)):
        is_valid = False
    if is_valid:
      return_msg = "建表成功"
    else:
      return_msg = "类型不正确, 请仔细检查"
      self.render('create_customer_raw_data_result.html',
                  return_msg = return_msg)
      return
    fields_list = fields_raw.replace("#", " ")
    # TODO: 先drop掉再新建
    sql = "create table if not exists " + self.project_id + \
        "_customer_raw_data (" + fields_list + ")"
    BaseHandler.runCmd(self, sql, \
        "create_customer_raw_data_process", self.doCreateCustomerRaw)
    # 将客户原始有效字段存储到元数据中
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection = db["customer_fields"]
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["create_customer_raw_data_table"]})
    result = collection.insert_one(
        { "project_id" : self.project_id,
          "task_id" : _task_id["create_customer_raw_data_table"],
          "fields_raw" : fields_raw,
          "lastModified" : datetime.datetime.utcnow()
        }
    )
    db_client.close()
    self.render('create_customer_raw_data_result.html',
                return_msg = return_msg)

class RequestTaskProgressHandler(tornado.web.RequestHandler):
  def post(self):
    project_id = self.get_argument('project_id')
    task_id = self.get_argument('task_id')
    plog("project_id: " + project_id)
    plog("task_id: " + task_id)
    progress = -1
    db_client = MongoClient('localhost', 27017)
    db = db_client[project_id + "_db"]
    collection = db["task-progress"]
    cursor = collection.find({"project_id": project_id,
                             "task_id": int(task_id)})
    plog("cursor.count(): " + str(cursor.count()))
    if(cursor.count() == 1):
      progress = cursor.next()["progress"]
    else:
      plog("Warning: project_id: " + project_id + ", task_id: " + task_id
           + " has more than one progess record")
      progress = 0
      self.render('request_task_progress_result.html',
                  project_id = project_id,
                  task_id = task_id,
                  progress = progress,
                  ret_msg = "Error")
    db_client.close()
    self.render('request_task_progress_result.html',
                project_id = project_id,
                task_id = task_id,
                progress = progress,
                ret_msg = "Success")

class TransformToInnerFormatHandler(tornado.web.RequestHandler, BaseHandler):
  def extractValidFields(self, raw_fields):
    fields = []
    types = []
    valid_field = ["uuid", "time", "lon", "lat", "cell_id", "cell_name",
                   "is_roam", "in_room", "call_in", "call_out"]
    for pairs in raw_fields.split(","):
      (f_name, f_type) = pairs.split("#")
      if(f_name in valid_field):
        fields.append(f_name)
        types.append(f_type)
      else:
        continue
    return (fields, types)
  def doTransformToInnerFormat(self, exe):
    name = multiprocessing.current_process().name
    plog(name+" "+"Starting")
    total = 0
    finished = 0
    progress = 0
    session_id = ""
    for line in BaseHandler.runProcess(self, exe):
      print line,
      # TODO 解析
      """
      ID = 20161016172027922go1lobg8
      Log view:
      http://logview.odps.aliyun.com/logview/?h=http://service.odps.aliyun.com/api&p=tsnav_project&i=20161016172027922go1lobg8&token=MERHRjFGZXhNc2ppQSttbUNTYWs5RHlqSFNBPSxPRFBTX09CTzoxNDUzNTUxMjc4NDQwNjgzLDE0NzcyNDMyMjkseyJTdGF0ZW1lbnQiOlt7IkFjdGlvbiI6WyJvZHBzOlJlYWQiXSwiRWZmZWN0IjoiQWxsb3ciLCJSZXNvdXJjZSI6WyJhY3M6b2RwczoqOnByb2plY3RzL3RzbmF2X3Byb2plY3QvaW5zdGFuY2VzLzIwMTYxMDE2MTcyMDI3OTIyZ28xbG9iZzgiXX1dLCJWZXJzaW9uIjoiMSJ9
      OK
      
      ID = 20161016172031386gias37jc2
      Log view:
      http://logview.odps.aliyun.com/logview/?h=http://service.odps.aliyun.com/api&p=tsnav_project&i=20161016172031386gias37jc2&token=VXU4ZFBMR3QxSURKTUhHZHhhZkgxR0FQMS9RPSxPRFBTX09CTzoxNDUzNTUxMjc4NDQwNjgzLDE0NzcyNDMyMzIseyJTdGF0ZW1lbnQiOlt7IkFjdGlvbiI6WyJvZHBzOlJlYWQiXSwiRWZmZWN0IjoiQWxsb3ciLCJSZXNvdXJjZSI6WyJhY3M6b2RwczoqOnByb2plY3RzL3RzbmF2X3Byb2plY3QvaW5zdGFuY2VzLzIwMTYxMDE2MTcyMDMxMzg2Z2lhczM3amMyIl19XSwiVmVyc2lvbiI6IjEifQ==
      2016-10-17 01:20:57 M1_Stg1_job0:1/0/1[0%]
      Summary:
      resource cost: cpu 0.07 Core * Min, memory 0.13 GB * Min
      inputs:
               tsnav_project.nanjing1_customer_raw_data: 530441 (19062328 bytes)
      outputs:
               tsnav_project.nanjing1_raw_data: 530441 (16127760 bytes)
      Job run time: 4.000
      Job run mode: service job
      M1_Stg1:
               instance count: 1
               run time: 4.000
               instance time:
                 min: 4.000, max: 4.000, avg: 4.000
               input records:
                 input: 530441  (min: 530441, max: 530441, avg: 530441)
               output records:
                 M1_Stg1FS_17396543: 530441  (min: 530441, max: 530441, avg: 530441)
      
      OK
      """
    plog(name+" Exiting")
  def post(self):
    self.project_id = self.get_argument('project_id');
    plog("project_id: " + self.project_id)
    # TODO 用元数据记录客户原始表各个字段的类型, 因为时间类型不同处理方式不同
    #Eg: create table if not exists nanjing1_raw_data(uuid string,
    #    lon double, lat double, time bigint, cell_id bigint, 
    #    cell_name string, is_roam bigint, in_room bigint, call_in bigint,
    #    call_out bigint);
    #    insert overwrite table nanjing1_raw_data select uuid, lon, lat, time, 
    #    cell_id, cell_name, is_roam, in_room, call_in, call_out 
    #    from nj_raw_data;
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection = db["customer_fields"]
    task_id = _task_id["create_customer_raw_data_table"]
    cursor = collection.find({"project_id": self.project_id,
                       "task_id": task_id})
    if(cursor.count() == 1):
      fields_raw = cursor.next()["fields_raw"]
      plog("fields_raw: " + fields_raw)
      (fields, types) = self.extractValidFields(fields_raw)
      # TODO 根据字段的types区分转换
      # 合成需要取出的字段
      field_list = ",".join(fields)
      sql = "create table if not exists " + self.project_id + \
            "_raw_data(uuid string, lon double, lat double, time bigint, " + \
            "cell_id bigint, cell_name string, is_roam bigint, " + \
            "in_room bigint, call_in bigint, call_out bigint); " + \
            "insert overwrite table " + self.project_id + "_raw_data select " + \
            field_list + " from " + self.project_id + "_customer_raw_data;"
      plog("sql: " + sql)
      BaseHandler.runCmd(self, sql, \
          "transform_to_inner_format_process", self.doTransformToInnerFormat)
    else:
      plog("Warning: project_id: " + self.project_id + ", task_id: " 
          + task_id + " has more than one progess record")
      progress = 0
      self.render('transform_to_inner_format_result.html',
                  project_id = self.project_id,
                  task_id = task_id,
                  ret_msg = "Error")
    self.render('transform_to_inner_format_result.html',
                project_id = self.project_id,
                task_id = task_id,
                fields_raw = fields_raw,
                ret_msg = "Success")
    


if __name__ == '__main__':
  tornado.options.parse_command_line()
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(options.port)
  tornado.ioloop.IOLoop.instance().start()
