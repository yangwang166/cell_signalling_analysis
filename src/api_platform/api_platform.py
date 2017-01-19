#-*- coding:utf-8 –*-

import os.path
import subprocess
import shlex
import multiprocessing
import datetime
import time
from pymongo import MongoClient
from odps import ODPS

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from tornado.escape import json_encode
from tornado.options import define, options

define("port", default=8000, help="run on the given port", type=int)

# Make odps client configurable
#_odps_client = "/Users/willwywang-NB/"+\
#         "github/cell_signalling_analysis/tool/odps/bin/odpscmd"
_project_home = os.path.dirname(os.path.realpath(__file__))
print("Project Home: " + _project_home)

_odps_client = _project_home + "/../../tool/odps/bin/odpscmd"
print("ODPS Client: " + _odps_client)

# 定义见文档: 任务进度查询.md
_task_id = {"create_customer_raw_data_table":1,
            "upload_customer_raw_data":2,
            "transform_to_spatio_temporal_raw_data":3,
            "compute_raw_data_stat":4,
            "compute_people_distribution":5,
            "compute_base_station_info":6,
            "download_base_station_info":7,
            "filter_data_with_range":8,
            "compute_filtered_data_stat":9,
            "compute_base_station_hour_summary":10,
            "download_base_station_hour_summary":11,
            "compute_uuid_cell_hour":12,
            "delete_all_tables":13
          }

_download_folder = _project_home + "/downloads"
if not os.path.exists(_download_folder):
    os.makedirs(_download_folder)

access_id = "xO0RtfYLVQEnAuUN"
access_key = "imNsJiShzQlcNYmDvrEt2hiXsreDro"
project = "tsnav_project"
end_point = "http://service.odps.aliyun.com/api"
odps = ODPS(access_id, access_key, project, end_point)

def plog(msg):
  print "API: ", msg

class Application(tornado.web.Application):
  def __init__(self):
    handlers=[
              (r'/', 
                IndexHandler),

              (r'/test_create_customer_raw_data',          # 1
                TestCreateCustomerRawDataTableHandler),
              (r'/test_upload',                            # 2
                TestUploadHandler), 
              (r'/test_request_task_progress',             # 3
                TestRequestTaskProgressHandler),
              (r'/test_transform_to_inner_format',         # 4
                TestTransformToInnerFormatHandler),
              (r'/test_compute_raw_data_stat',             # 5
                TestComputeRawDataStatHandler),
              (r'/test_get_raw_data_stat',                 # 6
                TestGetRawDataStatHandler),
              (r'/test_compute_people_distribution',       # 7
                TestComputePeopleDistributionHandler),
              (r'/test_get_people_distribution',           # 8
                TestGetPeopleDistributionHandler),
              (r'/test_compute_base_station_info',         # 9
                TestComputeBaseStationInfoHandler),
              (r'/test_download_base_station_info',        # 10
                TestDownloadBaseStationInfoHandler),
              (r'/test_get_base_station_info',             # 11
                TestGetBaseStationInfoHandler),
              (r'/test_filter_data_with_range',            # 12
                TestFilterDataWithRangeHandler),
              (r'/test_compute_filtered_data_stat',        # 13
                TestComputeFilteredDataStatHandler),
              (r'/test_get_filtered_data_stat',            # 14
                TestGetFilteredDataStatHandler),
              (r'/test_compute_base_station_hour_summary', # 15
                TestComputeBaseStationHourSummaryHandler),
              (r'/test_download_base_station_hour_summary',# 16
                TestDownloadBaseStationHourSummaryHandler),
              (r'/test_get_base_station_hour_summary',     # 17
                TestGetBaseStationHourSummaryHandler),
              (r'/test_compute_uuid_cell_hour',            # 18
                TestComputeUuidCellHourHandler),
              (r'/test_get_uuid_cell_hour',                # 19
                TestGetUuidCellHourHandler),
              (r'/test_delete_all_tables',                 # 20
                TestDeleteAllTablesHandler),

              (r'/create_customer_raw_data',               # 1
                CreateCustomerRawDataHandler),
              (r'/upload_data',                            # 2
                UploadHandler),
              (r'/request_task_progress',                  # 3
                RequestTaskProgressHandler),
              (r'/transform_to_inner_format',              # 4
                TransformToInnerFormatHandler),
              (r'/compute_raw_data_stat',                  # 5
                ComputeRawDataStatHandler),
              (r'/get_raw_data_stat',                      # 6
                GetRawDataStatHandler),
              (r'/compute_people_distribution',            # 7
                ComputePeopleDistributionHandler),
              (r'/get_people_distribution',                # 8
                GetPeopleDistributionHandler),
              (r'/compute_base_station_info',              # 9
                ComputeBaseStationInfoHandler),
              (r'/download_base_station_info',             # 10
                DownloadBaseStationInfoHandler),
              (r'/get_base_station_info',                  # 11
                GetBaseStationInfoHandler),
              (r'/filter_data_with_range',                 # 12
                FilterDataWithRangeHandler),
              (r'/compute_filtered_data_stat',             # 13
                ComputeFilteredDataStatHandler),
              (r'/get_filtered_data_stat',                 # 14
                GetFilteredDataStatHandler),
              (r'/compute_base_station_hour_summary',      # 15
                ComputeBaseStationHourSummaryHandler),
              (r'/download_base_station_hour_summary',     # 16
                DownloadBaseStationHourSummaryHandler),
              (r'/get_base_station_hour_summary',          # 17
                GetBaseStationHourSummaryHandler),
              (r'/compute_uuid_cell_hour',                 # 18
                ComputeUuidCellHourHandler),
              (r'/get_uuid_cell_hour',                     # 19
                GetUuidCellHourHandler),
              (r'/delete_all_tables',                      # 20
                DeleteAllTablesHandler),
             ]
    settings = dict(
      template_path=os.path.join(os.path.dirname(__file__), "templates"),
      static_path=os.path.join(os.path.dirname(__file__), "static"),
      debug=True,
      )
    tornado.web.Application.__init__(self, handlers, **settings)

class IndexHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('index.html')

# 1
class TestCreateCustomerRawDataTableHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_create_customer_raw_data_table.html')

# 2
class TestUploadHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_upload.html')

# 3
class TestRequestTaskProgressHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_request_task_progress.html')

# 4
class TestTransformToInnerFormatHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_transform_to_inner_format.html')

# 5
class TestComputeRawDataStatHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_compute_raw_data_stat.html')

# 6
class TestGetRawDataStatHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_get_raw_data_stat.html')

# 7
class TestComputePeopleDistributionHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_compute_people_distribution.html')

# 8
class TestGetPeopleDistributionHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_get_people_distribution.html')

# 9
class TestComputeBaseStationInfoHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_compute_base_station_info.html')

# 10
class TestDownloadBaseStationInfoHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_download_base_station_info.html')

# 11
class TestGetBaseStationInfoHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_get_base_station_info.html')

# 12
class TestFilterDataWithRangeHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_filter_data_with_range.html')

# 13
class TestComputeFilteredDataStatHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_compute_filtered_data_stat.html')

# 14
class TestGetFilteredDataStatHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_get_filtered_data_stat.html')

# 15
class TestComputeBaseStationHourSummaryHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_compute_base_station_hour_summary.html')

# 16
class TestDownloadBaseStationHourSummaryHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_download_base_station_hour_summary.html')

# 17
class TestGetBaseStationHourSummaryHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_get_base_station_hour_summary.html')

# 18
class TestComputeUuidCellHourHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_compute_uuid_cell_hour.html')

# 19
class TestGetUuidCellHourHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_get_uuid_cell_hour.html')

# 20
class TestDeleteAllTablesHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_delete_all_tables.html')

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
  def runSQL(self, sql, process_name, target_fuc):
    process = multiprocessing.Process(name = process_name,
                                      target = target_fuc,
                                      args = (sql,))
    process.start()
  def runSQL2(self, sql1, sql2, process_name, target_fuc):
    process = multiprocessing.Process(name = process_name,
                                      target = target_fuc,
                                      args = (sql1,sql2,))
    process.start()

# 1
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
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection = db["task-progress"]
    for line in BaseHandler.runProcess(self, exe):
      print line,
      if "ID" in line:
        session_id = line.split()[2]
        plog("session_id: " + session_id)
      if "OK" in line:
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
      return_msg = "Create table successful"
    else:
      return_msg = "Type error, please check"
      #self.render('create_customer_raw_data_result.html',
      #            return_msg = return_msg)
      obj = {'return_msg' : return_msg}
      self.write(json_encode(obj))
      return
    fields_list = fields_raw.replace("#", " ")
    sql = "drop table if exists " + self.project_id + "_customer_raw_data; " + \
          "create table if not exists " + self.project_id + \
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
    #self.render('create_customer_raw_data_result.html',
    #            return_msg = return_msg)
    obj = {'return_msg' : return_msg}
    self.write(json_encode(obj))

# 2
class UploadHandler(tornado.web.RequestHandler, BaseHandler):
  def getTimeFieldType(self, raw_fields):
    for pairs in raw_fields.split(","):
      (f_name, f_type) = pairs.split("#")
      if(f_name == "time"):
        return f_type
  def doUpload(self, exe):
    name = multiprocessing.current_process().name
    plog(name+" "+"Starting")
    total = 0
    finished = 0
    progress = 0
    session_id = ""
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection = db["task-progress"]
    for line in BaseHandler.runProcess(self, exe):
      print line,
      # extract upload progess and total block
      if "Upload session" in line:
        session_id = line.split()[2]
        plog("session_id: " + session_id)
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
    # 解析输入的参数
    self.project_id = self.get_argument('project_id')
    data_path = self.get_argument('data_path')
    #aliyun_table = self.get_argument('aliyun_table')
    aliyun_table = self.project_id + "_customer_raw_data"
    threads_num = self.get_argument('threads_num')
    row_delimiter = self.get_argument('row_delimiter')
    col_delimiter = self.get_argument('col_delimiter')
    plog("aliyun_table: " + aliyun_table)
    plog("threads_num: " + threads_num)
    plog("row_delimiter: " + row_delimiter)
    plog("col_delimiter: " + col_delimiter)
    # 获取字段类型
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection = db["customer_fields"]
    task_id = _task_id["create_customer_raw_data_table"]
    cursor = collection.find({"project_id": self.project_id,
                       "task_id": task_id})
    if(cursor.count() == 1):
      fields_raw = cursor.next()["fields_raw"]
      plog("fields_raw: " + fields_raw)
      timeFieldType = self.getTimeFieldType(fields_raw)
      if(timeFieldType=="bigint"):
        # 构造阿里云上运行的sql
        upload_cmd = "truncate table " + aliyun_table + ";" + \
                     "tunnel upload " + data_path + " " + aliyun_table +\
                     " -bs 10 -threads " + threads_num + " -s true" +\
                     " -dbr true -mbr 999999999"
        # 调用阿里云执行sql
        BaseHandler.runCmd(self, upload_cmd, "upload_process", self.doUpload)
      elif(timeFieldType=="datetime"):
        # Time format: 2015-06-21 04:01:00
        # 构造阿里云上运行的sql
        upload_cmd = "truncate table " + aliyun_table + ";" +\
                     "tunnel upload " + data_path + " " + aliyun_table +\
                     " -bs 10 -threads " + threads_num + " -s true" +\
                     " -dbr true -mbr 999999999 -dfp 'yyyy-MM-dd HH:mm:ss'" 
        # 调用阿里云执行sql
        BaseHandler.runCmd(self, upload_cmd, "upload_process", self.doUpload)
      else:
        plog("Warning: project_id: " + self.project_id + ", task_id: " 
            + task_id + " time field format not support now.")
        #self.render('upload_result.html',
        #            project_id = self.project_id,
        #            data_path = data_path,
        #            aliyun_table = aliyun_table,
        #            threads_num = threads_num,
        #            row_delimiter = row_delimiter,
        #            col_delimiter = col_delimiter,
        #            ret_msg = "Time field format not support")
        obj = {'project_id' : project_id,
               'data_path' : data_path,
               'aliyun_table' : aliyun_table,
               'threads_num' : threads_num,
               'row_delimiter' : row_delimiter,
               'col_delimiter' : col_delimiter,
               'ret_msg' : "Time field format not support"}
        self.write(json_encode(obj))
        return
    else:
      plog("Warning: project_id: " + self.project_id + ", task_id: " 
          + task_id + " has more than one progess record")
      #self.render('upload_result.html',
      #            project_id = self.project_id,
      #            data_path = data_path,
      #            aliyun_table = aliyun_table,
      #            threads_num = threads_num,
      #            row_delimiter = row_delimiter,
      #            col_delimiter = col_delimiter,
      #            ret_msg = "Error")
      obj = {'project_id' : project_id,
             'data_path' : data_path,
             'aliyun_table' : aliyun_table,
             'threads_num' : threads_num,
             'row_delimiter' : row_delimiter,
             'col_delimiter' : col_delimiter,
             'ret_msg' : "Error"}
      self.write(json_encode(obj))
      return
    db_client.close()
    # 渲染结果页面
    #self.render('upload_result.html', 
    #            project_id = self.project_id,
    #            data_path = data_path,
    #            aliyun_table = aliyun_table,
    #            threads_num = threads_num,
    #            row_delimiter = row_delimiter,
    #            col_delimiter = col_delimiter,
    #            ret_msg = "Success")
    obj = {'project_id' : self.project_id,
           'data_path' : data_path,
           'aliyun_table' : aliyun_table,
           'threads_num' : threads_num,
           'row_delimiter' : row_delimiter,
           'col_delimiter' : col_delimiter,
           'ret_msg' : "Success"}
    self.write(json_encode(obj))


# 3
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
      #self.render('request_task_progress_result.html',
      #            project_id = project_id,
      #            task_id = task_id,
      #            progress = progress,
      #            ret_msg = "Error")
      obj = {'project_id' : project_id,
             'task_id' : task_id,
             'progress' : progress,
             'ret_msg' : "Error"}
      self.write(json_encode(obj))
      return
    db_client.close()
    #self.render('request_task_progress_result.html',
    #            project_id = project_id,
    #            task_id = task_id,
    #            progress = progress,
    #            ret_msg = "Success")
    obj = {'project_id' : project_id,
           'task_id' : task_id,
           'progress' : progress,
           'ret_msg' : "Success"}
    self.write(json_encode(obj))

# 4
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
  def getTimeFieldType(self, raw_fields):
    for pairs in raw_fields.split(","):
      (f_name, f_type) = pairs.split("#")
      if(f_name == "time"):
        return f_type
  def doTransformToInnerFormat(self, exe):
    name = multiprocessing.current_process().name
    plog(name + " " + "Starting")
    progress = 0
    session_id = ""
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection = db["task-progress"]
    count = 0;
    for line in BaseHandler.runProcess(self, exe):
      print line,
      if "ID" in line:
        session_id = line.split()[2]
        plog("session_id: " + session_id)
        result = collection.delete_many({
                 "project_id": self.project_id,
                 "task_id": _task_id["transform_to_spatio_temporal_raw_data"]})
        result = collection.insert_one(
            { "project_id" : self.project_id,
              "task_id" : _task_id["transform_to_spatio_temporal_raw_data"],
              "aliyun_sess_id" : session_id,
              "progress" : progress,
              "lastModified" : datetime.datetime.utcnow()
            }
        )
        plog("result.inserted_id: " + str(result.inserted_id))
      elif "OK" in line:
        progress = 100
        plog("progress: " + str(progress) + "%")
        result = collection.update_one(
          {"project_id" : self.project_id, 
            "task_id" : _task_id["transform_to_spatio_temporal_raw_data"] },
          {
            "$set": {
              "progress": progress 
            },
            "$currentDate": {"lastModified": True}
          }
        )
        plog("result.matched_count: "+str(result.matched_count))
        plog("result.modified_count: "+str(result.modified_count))
      elif "%" in line:
        count += 1
        l = line.rfind("[")
        r = line.rfind("%")
        progress = int(line[l+1:r]) # Not good look, use bellow
        progress = int((1.0 * count / (count + 1)) * 100)
        plog("progress: " + str(progress) + "%")
        result = collection.update_one(
          {"project_id" : self.project_id, 
            "task_id" : _task_id["transform_to_spatio_temporal_raw_data"] },
          {
            "$set": {
              "progress": progress 
            },
            "$currentDate": {"lastModified": True}
          }
        )
        plog("result.matched_count: "+str(result.matched_count))
        plog("result.modified_count: "+str(result.modified_count))
    db_client.close()
    plog(name+" Exiting")
  def post(self):
    self.project_id = self.get_argument('project_id');
    plog("project_id: " + self.project_id)
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
      timeFieldType = self.getTimeFieldType(fields_raw)
      # 合成需要取出的字段
      sql = ""
      if("uuid" in fields and "lon" in fields and "lat" in fields and
          "time" in fields):
        if(timeFieldType == "bigint"):
          sql = "create table if not exists " + self.project_id + \
                "_spatio_temporal_raw_data(uuid string, lon double, lat double, " + \
                "time bigint) partitioned by (date_p string); " + \
                "insert overwrite table " + self.project_id + \
                "_spatio_temporal_raw_data partition(date_p) select " + \
                "uuid, lon, lat, time" +\
                ", to_char(from_unixtime(time), 'yyyymmdd') as date_p "+\
                "from " + self.project_id + "_customer_raw_data " + \
                "where lat is not null and lon is not null and uuid is not null "+\
                "and time is not null and lat>0 and lon>0 and time>0 group by "+\
                "uuid, time, lat, lon;"
        elif(timeFieldType == "datetime"):
          # Time format: 2015-06-21 04:01:00
          field_list = "uuid, lon, lat, unix_timestamp(time)"
          sql = "create table if not exists " + self.project_id + \
                "_spatio_temporal_raw_data(uuid string, lon double, lat double, " + \
                "time bigint) partitioned by (date_p string); " + \
                "insert overwrite table " + self.project_id + \
                "_spatio_temporal_raw_data partition(date_p) select " + \
                "uuid, lon, lat, unix_timestamp(time)" +\
                ", to_char(time, 'yyyymmdd') as date_p "+\
                "from " + self.project_id + "_customer_raw_data " + \
                "where lat is not null and lon is not null and uuid is not null "+\
                "and time is not null and lat>0 and lon>0 and "+\
                "unix_timestamp(time)>0 group by "+\
                "uuid, time, lat, lon;"
        else:
          plog("Warning: project_id: " + self.project_id + ", task_id: " 
              + task_id + " time format not supported")
          progress = 0
          #self.render('transform_to_inner_format_result.html',
          #            project_id = self.project_id,
          #            task_id = task_id,
          #            ret_msg = "Time format not supported")
          obj = {'project_id' : project_id,
                 'task_id' : task_id,
                 'ret_msg' : "Time format not supported"}
          self.write(json_encode(obj))
          return
      plog("sql: " + sql)
      BaseHandler.runCmd(self, sql, \
          "transform_to_inner_format_process", self.doTransformToInnerFormat)
    else:
      plog("Warning: project_id: " + self.project_id + ", task_id: " 
          + task_id + " has more than one progess record")
      progress = 0
      #self.render('transform_to_inner_format_result.html',
      #            project_id = self.project_id,
      #            task_id = task_id,
      #            ret_msg = "Error")
      obj = {'project_id' : project_id,
             'task_id' : task_id,
             'ret_msg' : "Error"}
      self.write(json_encode(obj))
      return
    db_client.close()
    #self.render('transform_to_inner_format_result.html',
    #            project_id = self.project_id,
    #            task_id = task_id,
    #            fields_raw = fields_raw,
    #            ret_msg = "Success")
    obj = {'project_id' : self.project_id,
           'task_id' : task_id,
           'ret_msg' : "Success"}
    self.write(json_encode(obj))

# 5
class ComputeRawDataStatHandler(tornado.web.RequestHandler, BaseHandler):
  def processResult(self, raw):
    result = raw.split("\n")[1:-1]
    new_result = []
    for pair in result:
      date_p = pair.split(",")[0][1:-1]
      count = pair.split(",")[1]
      new_result.append("" + str(date_p) + "," + count)
    result_str = '#'.join(new_result)
    plog("Raw stat: " + str(result_str))
    return result_str
  def doComputeRawDataStat(self, sql):
    name = multiprocessing.current_process().name
    plog(name + " " + "Starting")
    progress = 0
    # 建立数据库连接
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection = db["task-progress"]
    count = 0;
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["compute_raw_data_stat"]})
    result = collection.insert_one(
        { "project_id" : self.project_id,
          "task_id" : _task_id["compute_raw_data_stat"],
          "progress" : progress,
          "lastModified" : datetime.datetime.utcnow()
        }
    )
    plog("result.inserted_id: " + str(result.inserted_id))
    instance = odps.run_sql(sql)
    while(not instance.is_successful()):
      count += 1
      progress = int((1.0 * count / (count + 1)) * 100)
      plog("progress: " + str(progress) + "%")
      result = collection.update_one(
        {"project_id" : self.project_id, 
          "task_id" : _task_id["compute_raw_data_stat"] },
        {
          "$set": {
            "progress": progress 
          },
          "$currentDate": {"lastModified": True}
        }
      )
      time.sleep(5) 
    raw_result = instance.get_task_result(instance.get_task_names()[0])
    plog("raw_result: " + raw_result)
    collection2 = db["local-result"]
    raw_stat = self.processResult(raw_result)
    result = collection2.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["compute_raw_data_stat"]})
    result = collection2.insert_one(
        { "project_id" : self.project_id,
          "task_id" : _task_id["compute_raw_data_stat"],
          "result" : raw_stat,
          "lastModified" : datetime.datetime.utcnow()
        }
    )
    progress = 100
    plog("progress: " + str(progress) + "%")
    result = collection.update_one(
      {"project_id" : self.project_id, 
        "task_id" : _task_id["compute_raw_data_stat"] },
      {
        "$set": {
          "progress": progress 
        },
        "$currentDate": {"lastModified": True}
      }
    )
    db_client.close()
    plog("result.matched_count: "+str(result.matched_count))
    plog("result.modified_count: "+str(result.modified_count))
  def post(self):
    # 解析输入的参数
    self.project_id = self.get_argument('project_id');
    # 构造阿里云上运行的sql
    # day
    #sql = ('select date_p, count(*) as count from ' 
    #       '' + self.project_id + '_spatio_temporal_raw_data ' 
    #       'group by date_p order by date_p limit 100')
    # hour
    sql = ('select to_char(from_unixtime(time), "yyyyMMddHH") as hour, '
           'count(*) as count from ' 
           '' + self.project_id + '_spatio_temporal_raw_data ' 
           'group by to_char(from_unixtime(time), "yyyyMMddHH") '
           'order by hour limit 2400')
    plog("sql: " + sql)
    # 调用阿里云执行sql
    BaseHandler.runSQL(self, sql, \
        "compute_raw_data_stat", self.doComputeRawDataStat)
    # 渲染结果页面
    #self.render('compute_raw_data_stat_result.html',
    #            project_id = self.project_id,
    #            ret_msg = "Success")
    obj = {'project_id' : self.project_id,
           'ret_msg' : "Success"}
    self.write(json_encode(obj))

# 6
class GetRawDataStatHandler(tornado.web.RequestHandler, BaseHandler):
  def post(self):
    # 解析输入的参数
    project_id = self.get_argument('project_id');
    result = ""
    # 查询本地数据库
    db_client = MongoClient('localhost', 27017)
    db = db_client[project_id + "_db"]
    collection = db["local-result"]
    cursor = collection.find({"project_id": project_id,
                             "task_id": _task_id["compute_raw_data_stat"]})
    plog("cursor.count(): " + str(cursor.count()))
    if(cursor.count() == 1):
      raw_stat = cursor.next()["result"]
    else:
      plog("Warning: project_id: " + project_id + ", task_id: " 
           + str(_task_id["compute_raw_data_stat"])
           + " has more than one progess record")
      raw_stat = ""
      #self.render('get_raw_data_stat_result.html',
      #            project_id = project_id,
      #            result = raw_stat,
      #            ret_msg = "Error")
      obj = {'project_id' : project_id,
             'result' : raw_stat,
             'ret_msg' : "Error"}
      self.write(json_encode(obj))
      return
    db_client.close()
    plog("Get raw stat: " + str(raw_stat))
    # 渲染结果页面
    #self.render('get_raw_data_stat_result.html',
    #            project_id = project_id,
    #            result = raw_stat,
    #            ret_msg = "Success")
    obj = {'project_id' : project_id,
           'result' : raw_stat,
           'ret_msg' : "Success"}
    self.write(json_encode(obj))

# 7
class ComputePeopleDistributionHandler(tornado.web.RequestHandler, BaseHandler):
  def processResult(self, raw):
    result = raw.split("\n")[1:-1]
    result_str = '#'.join(result)
    plog("People distribution: " + str(result_str))
    return result_str
  def doComputePeopleDistribution(self, sql):
    # 调用后台阿里云后, 这里处理其返回结果
    name = multiprocessing.current_process().name
    plog(name + " " + "Starting")
    progress = 0
    # 建立数据库连接
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection = db["task-progress"]
    count = 0;
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["compute_people_distribution"]})
    result = collection.insert_one(
        { "project_id" : self.project_id,
          "task_id" : _task_id["compute_people_distribution"],
          "progress" : progress,
          "lastModified" : datetime.datetime.utcnow()
        }
    )
    plog("result.inserted_id: " + str(result.inserted_id))
    instance = odps.run_sql(sql)
    while(not instance.is_successful()):
      #plog("is_successful(): " + str(instance.is_successful()))
      #plog("is_terminated(): " + str(instance.is_terminated()))
      count += 1
      progress = int((1.0 * count / (count + 1)) * 100)
      plog("progress: " + str(progress) + "%")
      result = collection.update_one(
        {"project_id" : self.project_id, 
          "task_id" : _task_id["compute_people_distribution"] },
        {
          "$set": {
            "progress": progress 
          },
          "$currentDate": {"lastModified": True}
        }
      )
      time.sleep(5) 
    #plog(instance.get_task_result(instance.get_task_names()[0]))
    raw_result = instance.get_task_result(instance.get_task_names()[0])
    # 把人基于数据量的分布写入本地数据库
    collection2 = db["local-result"]
    people_distribution = self.processResult(raw_result)
    result = collection2.update_one(
        {"project_id" : self.project_id, 
          "task_id" : _task_id["compute_people_distribution"] },
        { 
          "$set" : {
            "result" : people_distribution
          },
          "$currentDate": {"lstModified": True}
        }
    )
    progress = 100
    plog("progress: " + str(progress) + "%")
    result = collection.update_one(
      {"project_id" : self.project_id, 
        "task_id" : _task_id["compute_people_distribution"] },
      {
        "$set": {
          "progress": progress 
        },
        "$currentDate": {"lastModified": True}
      }
    )
    db_client.close()
    plog("result.matched_count: "+str(result.matched_count))
    plog("result.modified_count: "+str(result.modified_count))
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
    BaseHandler.runSQL(self, sql, \
        "compute_people_distribution", self.doComputePeopleDistribution)
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection2 = db["local-result"]
    result = collection2.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["compute_people_distribution"]})
    result = collection2.insert_one(
        { "project_id" : self.project_id,
          "task_id" : _task_id["compute_people_distribution"],
          "interval_size" : self.interval_size,
          "top_n" : self.top_n,
          "result" : "Not Computed",
          "lastModified" : datetime.datetime.utcnow()
        }
    )
    db_client.close()
    # 渲染结果页面
    #self.render('compute_people_distribution_result.html',
    #            project_id = self.project_id,
    #            interval_size = self.interval_size,
    #            date_p = self.date_p,
    #            top_n = self.top_n,
    #            ret_msg = "Success")
    obj = {'project_id' : self.project_id,
           'interval_size' : self.interval_size,
           'date_p' : self.date_p,
           'top_n' : self.top_n,
           'ret_msg' : "Success"}
    self.write(json_encode(obj))

# 8
class GetPeopleDistributionHandler(tornado.web.RequestHandler, BaseHandler):
  def post(self):
    # 解析输入的参数
    project_id = self.get_argument('project_id');
    # 查询本地数据库
    db_client = MongoClient('localhost', 27017)
    db = db_client[project_id + "_db"]
    collection = db["local-result"]
    cursor = collection.find({"project_id": project_id,
                             "task_id": _task_id["compute_people_distribution"]})
    plog("cursor.count(): " + str(cursor.count()))
    people_distribution = ""
    interval_size = ""
    top_n = ""
    if(cursor.count() == 1):
      res = cursor.next()
      people_distribution = res["result"]
      interval_size = res["interval_size"]
      top_n = res["top_n"]
    else:
      plog("Warning: project_id: " + project_id + ", task_id: " 
           + str(_task_id["compute_people_distribution"]) 
           + " has more than one progess record")
      people_distribution = ""
      #self.render('get_people_distribution_result.html',
      #            project_id = project_id,
      #            people_distribution = people_distribution,
      #            interval_size = interval_size,
      #            top_n = top_n,
      #            ret_msg = "Error")
      obj = {'project_id' : project_id,
             'people_distribution' : people_distribution,
             'interval_size' : interval_size,
             'top_n' : top_n,
             'ret_msg' : "Error"}
      self.write(json_encode(obj))
      return
    db_client.close()
    plog("Get people_distribution: " + str(people_distribution))
    # 渲染结果页面
    #self.render('get_people_distribution_result.html',
    #            project_id = project_id,
    #            people_distribution = people_distribution,
    #            interval_size = interval_size,
    #            top_n = top_n,
    #            ret_msg = "Success")
    obj = {'project_id' : project_id,
           'people_distribution' : people_distribution,
           'interval_size' : interval_size,
           'top_n' : top_n,
           'ret_msg' : "Success"}
    self.write(json_encode(obj))

# 9
class ComputeBaseStationInfoHandler(tornado.web.RequestHandler, BaseHandler):
  def doComputeBaseStationInfo(self, sql1, sql2):
    name = multiprocessing.current_process().name
    plog(name + " " + "Starting")
    progress = 0
    # 建立数据库连接
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection = db["task-progress"]
    count = 0;
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["compute_base_station_info"]})
    result = collection.insert_one(
        { "project_id" : self.project_id,
          "task_id" : _task_id["compute_base_station_info"],
          "progress" : progress,
          "lastModified" : datetime.datetime.utcnow()
        }
    )
    plog("result.inserted_id: " + str(result.inserted_id))
    instance1 = odps.run_sql(sql1)
    while(not instance1.is_successful()):
      time.sleep(1) 
    plog("sql1 runs Successful")
    instance2 = odps.run_sql(sql2)
    while(not instance2.is_successful()):
      count += 1
      progress = int((1.0 * count / (count + 1)) * 100)
      plog("progress: " + str(progress) + "%")
      result = collection.update_one(
        {"project_id" : self.project_id, 
          "task_id" : _task_id["compute_base_station_info"] },
        {
          "$set": {
            "progress": progress 
          },
          "$currentDate": {"lastModified": True}
        }
      )
      time.sleep(5) 
    progress = 100
    plog("progress: " + str(progress) + "%")
    result = collection.update_one(
      {"project_id" : self.project_id, 
        "task_id" : _task_id["compute_base_station_info"] },
      {
        "$set": {
          "progress": progress 
        },
        "$currentDate": {"lastModified": True}
      }
    )
    db_client.close()
    plog("result.matched_count: "+str(result.matched_count))
    plog("result.modified_count: "+str(result.modified_count))
  def post(self):
    # 解析输入的参数
    self.project_id = self.get_argument('project_id');
    # 构造阿里云上运行的sql
    sql1 = ('create table if not exists ' 
            '' + self.project_id + '_base_station_info'
            '(id bigint, lon double, lat double)') 
    sql2 = ('insert overwrite table ' 
            '' + self.project_id + '_base_station_info '
            'select row_number() over(partition by A.tmp order by A.tmp) as id,'
            'A.lon, A.lat '
            'from (select distinct lon, lat, 1 as tmp '
            'from ' + self.project_id + '_spatio_temporal_raw_data) A;')
    plog("sql1: " + sql1)
    plog("sql2: " + sql2)
    # 调用阿里云执行sql
    BaseHandler.runSQL2(self, sql1, sql2, \
        "compute_raw_data_stat", self.doComputeBaseStationInfo)
    # 渲染结果页面
    #self.render('compute_base_station_info_result.html',
    #            project_id = self.project_id,
    #            ret_msg = "Success")
    obj = {'project_id' : self.project_id,
           'ret_msg' : "Success"}
    self.write(json_encode(obj))

# 10
class DownloadBaseStationInfoHandler(tornado.web.RequestHandler, BaseHandler):
  def doDownloadBaseStationInfo(self, exe):
    name = multiprocessing.current_process().name
    plog(name + " " + "Starting")
    progress = 0
    count = 0
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection = db["task-progress"]
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["download_base_station_info"]})
    result = collection.insert_one(
        { "project_id" : self.project_id,
          "task_id" : _task_id["download_base_station_info"],
          "progress" : progress,
          "lastModified" : datetime.datetime.utcnow()
        }
    )
    for line in BaseHandler.runProcess(self, exe):
      print line,
      # extract upload progess and total block
      if "file" in line:
        count += 1
        progress = int((1.0 * count / (count + 1)) * 100)
        plog("progress: " + str(progress) + "%")
        result = collection.update_one(
          {"project_id" : self.project_id, 
            "task_id" : _task_id["download_base_station_info"] },
          {
            "$set": {
              "progress": progress 
            },
            "$currentDate": {"lastModified": True}
          }
        )
        plog("result.matched_count: "+str(result.matched_count))
        plog("result.modified_count: "+str(result.modified_count))
      elif "download OK" in line:
        progress = 100
        plog("progress: " + str(progress) + "%")
        result = collection.update_one(
          {"project_id" : self.project_id, 
            "task_id" : _task_id["download_base_station_info"] },
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
    # 解析输入的参数
    self.project_id = self.get_argument('project_id');
    download_cmd = ('tunnel download ' + self.project_id +
                    '_base_station_info ' + _download_folder + '/'
                    '' + self.project_id + '_base_station_info.csv ' 
                    '-h true;')
    plog("download_cmd: " + download_cmd)
    # 调用阿里云执行
    BaseHandler.runCmd(self, download_cmd,  \
        "download_base_station_info", \
        self.doDownloadBaseStationInfo)
    # 渲染结果页面
    #self.render('compute_base_station_info_result.html',
    #            project_id = self.project_id,
    #            ret_msg = "Success")
    obj = {'project_id' : self.project_id,
           'ret_msg' : "Success"}
    self.write(json_encode(obj))

# 11
class GetBaseStationInfoHandler(tornado.web.RequestHandler, BaseHandler):
  def post(self):
    # 解析输入的参数
    project_id = self.get_argument('project_id');
    # 渲染结果页面
    #self.render('get_raw_data_stat_result.html',
    #            project_id = project_id,
    #            result = _download_folder,
    #            ret_msg = "Success")
    obj = {'project_id' : project_id,
           'download_folder' : _download_folder,
           'ret_msg' : "Success"}
    self.write(json_encode(obj))

# 12
class FilterDataWithRangeHandler(tornado.web.RequestHandler, BaseHandler):
  def doFilterDataWithRange(self, sql1, sql2):
    plog("sql1: " + sql1)
    plog("sql2: " + sql2)
    name = multiprocessing.current_process().name
    plog(name + " " + "Starting")
    progress = 0
    # 建立数据库连接
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection = db["task-progress"]
    count = 0;
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["filter_data_with_range"]})
    result = collection.insert_one(
        { "project_id" : self.project_id,
          "task_id" : _task_id["filter_data_with_range"],
          "progress" : progress,
          "lastModified" : datetime.datetime.utcnow()
        }
    )
    plog("result.inserted_id: " + str(result.inserted_id))
    instance1 = odps.run_sql(sql1)
    while(not instance1.is_successful()):
      time.sleep(1) 
    plog("sql1 runs Successful")
    instance2 = odps.run_sql(sql2)
    while(not instance2.is_successful()):
      count += 1
      progress = int((1.0 * count / (count + 1)) * 100)
      plog("progress: " + str(progress) + "%")
      result = collection.update_one(
        {"project_id" : self.project_id, 
          "task_id" : _task_id["filter_data_with_range"] },
        {
          "$set": {
            "progress": progress 
          },
          "$currentDate": {"lastModified": True}
        }
      )
      time.sleep(5) 

    progress = 100
    plog("progress: " + str(progress) + "%")
    result = collection.update_one(
      {"project_id" : self.project_id, 
        "task_id" : _task_id["filter_data_with_range"] },
      {
        "$set": {
          "progress": progress 
        },
        "$currentDate": {"lastModified": True}
      }
    )
    db_client.close()
    plog("result.matched_count: "+str(result.matched_count))
    plog("result.modified_count: "+str(result.modified_count))
  def post(self):
    # 解析输入的参数
    self.project_id = self.get_argument('project_id');
    self.count_min = self.get_argument('count_min');
    self.count_max = self.get_argument('count_max');
    # 构造阿里云上运行的sql
    sql1 = ('create table if not exists ' 
           '' + self.project_id + '_filtered_raw_data '
           '(uuid string, lon double, lat double, bs_id bigint, '
           'time bigint, count bigint) '
           'partitioned by (date_p string);')
    sql2 = ('insert overwrite table ' + self.project_id + '_filtered_raw_data '
           'partition(date_p) '
           'select C.uuid, C.lon, C.lat, D.id as bs_id, C.time, '
           'C.count, C.date_p from '
           '  (select A.uuid, A.lon, A.lat, A.time, B.count, A.date_p '
           '  from ' + self.project_id + '_spatio_temporal_raw_data A '
           '  inner join '
           '  (select uuid, count(uuid) as count '
           '  from ' + self.project_id + '_spatio_temporal_raw_data '
           '  group by uuid '
           '  having count(uuid) > ' + self.count_min + ' '
           '  and count(uuid) < ' + self.count_max + ') B '
           '  on A.uuid = B.uuid) C '
           'left outer join ' + self.project_id + '_base_station_info D '
           'on C.lon = D.lon and C.lat = D.lat;')
    plog("sql1: " + sql1)
    plog("sql2: " + sql2)
    # 调用阿里云执行sql
    BaseHandler.runSQL2(self, sql1, sql2, \
        "filter_data_with_range", self.doFilterDataWithRange)
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection2 = db["local-result"]
    result = collection2.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["filter_data_with_range"]})
    result = collection2.insert_one(
        { "project_id" : self.project_id,
          "task_id" : _task_id["filter_data_with_range"],
          "count_min" : self.count_min,
          "count_max" : self.count_max,
          "result" : "None",
          "lastModified" : datetime.datetime.utcnow()
        }
    )
    db_client.close()
    # 渲染结果页面
    #self.render('filter_data_with_range_result.html',
    #            project_id = self.project_id,
    #            count_min = self.count_min,
    #            count_max = self.count_max,
    #            ret_msg = "Success")
    obj = {'project_id' : self.project_id,
           'count_min' : self.count_min,
           'count_max' : self.count_max,
           'ret_msg' : "Success"}
    self.write(json_encode(obj))

# 13
class ComputeFilteredDataStatHandler(tornado.web.RequestHandler, BaseHandler):
  def processResult(self, raw):
    result = raw.split("\n")[1:-1]
    new_result = []
    for pair in result:
      date_p = pair.split(",")[0][1:-1]
      count = pair.split(",")[1]
      new_result.append("" + str(date_p) + "," + count)
    result_str = '#'.join(new_result)
    plog("Raw stat: " + str(result_str))
    return result_str
  def doComputeFilteredDataStat(self, sql):
    name = multiprocessing.current_process().name
    plog(name + " " + "Starting")
    progress = 0
    # 建立数据库连接
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection = db["task-progress"]
    count = 0;
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["compute_filtered_data_stat"]})
    result = collection.insert_one(
        { "project_id" : self.project_id,
          "task_id" : _task_id["compute_filtered_data_stat"],
          "progress" : progress,
          "lastModified" : datetime.datetime.utcnow()
        }
    )
    plog("result.inserted_id: " + str(result.inserted_id))
    instance = odps.run_sql(sql)
    while(not instance.is_successful()):
      count += 1
      progress = int((1.0 * count / (count + 1)) * 100)
      plog("progress: " + str(progress) + "%")
      result = collection.update_one(
        {"project_id" : self.project_id, 
          "task_id" : _task_id["compute_filtered_data_stat"] },
        {
          "$set": {
            "progress": progress 
          },
          "$currentDate": {"lastModified": True}
        }
      )
      time.sleep(5) 
    raw_result = instance.get_task_result(instance.get_task_names()[0])
    collection2 = db["local-result"]
    filtered_stat = self.processResult(raw_result)
    result = collection2.update_one(
        {"project_id" : self.project_id, 
          "task_id" : _task_id["filter_data_with_range"] },
        {
          "$set": {
            "result": filtered_stat 
          },
          "$currentDate": {"lastModified": True}
        }
    )
    progress = 100
    plog("progress: " + str(progress) + "%")
    result = collection.update_one(
      {"project_id" : self.project_id, 
        "task_id" : _task_id["compute_filtered_data_stat"] },
      {
        "$set": {
          "progress": progress 
        },
        "$currentDate": {"lastModified": True}
      }
    )
    db_client.close()
    plog("result.matched_count: "+str(result.matched_count))
    plog("result.modified_count: "+str(result.modified_count))
  def post(self):
    # 解析输入的参数
    self.project_id = self.get_argument('project_id');
    # 构造阿里云上运行的sql
    sql = ('select date_p, count(*) as count from ' 
           '' + self.project_id + '_filtered_raw_data ' 
           'group by date_p order by date_p limit 100')
    plog("sql: " + sql)
    # 调用阿里云执行sql
    BaseHandler.runSQL(self, sql, \
        "compute_filtered_data_stat", self.doComputeFilteredDataStat)
    # 渲染结果页面
    #self.render('compute_filtered_data_stat_result.html',
    #            project_id = self.project_id,
    #            ret_msg = "Success")
    obj = {'project_id' : self.project_id,
           'ret_msg' : "Success"}
    self.write(json_encode(obj))

# 14
class GetFilteredDataStatHandler(tornado.web.RequestHandler, BaseHandler):
  def post(self):
    # 解析输入的参数
    project_id = self.get_argument('project_id');
    filtered_stat = ""
    # 查询本地数据库
    db_client = MongoClient('localhost', 27017)
    db = db_client[project_id + "_db"]
    collection = db["local-result"]
    cursor = collection.find({"project_id": project_id,
                             "task_id": _task_id["filter_data_with_range"]})
    plog("cursor.count(): " + str(cursor.count()))
    if(cursor.count() == 1):
      filtered_stat = cursor.next()["result"]
    else:
      plog("Warning: project_id: " + project_id + ", task_id: " 
           + str(_task_id["filter_data_with_range"])
           + " has more than one progess record")
      filtered_stat = ""
      #self.render('get_filtered_data_stat_result.html',
      #            project_id = project_id,
      #            result = filtered_stat,
      #            ret_msg = "Error")
      obj = {'project_id' : project_id,
             'filtered_stat' : filtered_stat,
             'ret_msg' : "Error"}
      self.write(json_encode(obj))
      return
    db_client.close()
    # 渲染结果页面
    #self.render('get_filtered_data_stat_result.html',
    #            project_id = project_id,
    #            result = filtered_stat,
    #            ret_msg = "Success")
    obj = {'project_id' : project_id,
           'filtered_stat' : filtered_stat,
           'ret_msg' : "Success"}
    self.write(json_encode(obj))

# 15
class ComputeBaseStationHourSummaryHandler(tornado.web.RequestHandler, 
                                           BaseHandler):
  def doComputeBaseStationHourSummary(self, sql1, sql2):
    name = multiprocessing.current_process().name
    plog(name + " " + "Starting")
    progress = 0
    # 建立数据库连接
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection = db["task-progress"]
    count = 0;
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["compute_base_station_hour_summary"]})
    result = collection.insert_one(
        { "project_id" : self.project_id,
          "task_id" : _task_id["compute_base_station_hour_summary"],
          "progress" : progress,
          "lastModified" : datetime.datetime.utcnow()
        }
    )
    plog("result.inserted_id: " + str(result.inserted_id))
    instance1 = odps.run_sql(sql1)
    while(not instance1.is_successful()):
      time.sleep(1) 
    plog("sql1 runs Successful")
    instance2 = odps.run_sql(sql2)
    while(not instance2.is_successful()):
      count += 1
      progress = int((1.0 * count / (count + 1)) * 100)
      plog("progress: " + str(progress) + "%")
      result = collection.update_one(
        {"project_id" : self.project_id, 
          "task_id" : _task_id["compute_base_station_hour_summary"] },
        {
          "$set": {
            "progress": progress 
          },
          "$currentDate": {"lastModified": True}
        }
      )
      time.sleep(5) 
    db_client.close()
    progress = 100
    plog("progress: " + str(progress) + "%")
    result = collection.update_one(
      {"project_id" : self.project_id, 
        "task_id" : _task_id["compute_base_station_hour_summary"] },
      {
        "$set": {
          "progress": progress 
        },
        "$currentDate": {"lastModified": True}
      }
    )
    plog("result.matched_count: "+str(result.matched_count))
    plog("result.modified_count: "+str(result.modified_count))
  def post(self):
    # 解析输入的参数
    self.project_id = self.get_argument('project_id');
    # 构造阿里云上运行的sql
    sql1 = ('create table if not exists ' 
            '' + self.project_id + '_base_station_hour_summary '
            '(lon double, lat double, bs_id bigint, hour bigint, count bigint)'
            'partitioned by (date_p string);')
    sql2 = ('insert overwrite table '
            '' + self.project_id + '_base_station_hour_summary '
            'partition(date_p) '
            'select lon, lat, bs_id, '
            'datepart(from_unixtime(time), "HH") as hour,'
            'count(*) as count, date_p '
            'from ' + self.project_id + '_filtered_raw_data '
            'group by lon, lat, bs_id, datepart(from_unixtime(time), "HH"), '
            'date_p;')
    plog("sql1: " + sql1)
    plog("sql2: " + sql2)
    # 调用阿里云执行sql
    BaseHandler.runSQL2(self, sql1, sql2, \
        "compute_base_station_hour_summary", \
        self.doComputeBaseStationHourSummary)
    # 渲染结果页面
    #self.render('compute_base_station_hour_summary_result.html',
    #            project_id = self.project_id,
    #            ret_msg = "Success")
    obj = {'project_id' : self.project_id,
           'ret_msg' : "Success"}
    self.write(json_encode(obj))

# 16
class DownloadBaseStationHourSummaryHandler(tornado.web.RequestHandler, 
                                           BaseHandler):
  def doDownloadBaseStationHourSummary(self, exe):
    name = multiprocessing.current_process().name
    plog(name + " " + "Starting")
    progress = 0
    count = 0
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection = db["task-progress"]
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["download_base_station_hour_summary"]})
    result = collection.insert_one(
        { "project_id" : self.project_id,
          "task_id" : _task_id["download_base_station_hour_summary"],
          "progress" : progress,
          "lastModified" : datetime.datetime.utcnow()
        }
    )
    for line in BaseHandler.runProcess(self, exe):
      print line,
      # extract upload progess and total block
      if "file" in line:
        count += 1
        progress = int((1.0 * count / (count + 1)) * 100)
        plog("progress: " + str(progress) + "%")
        result = collection.update_one(
          {"project_id" : self.project_id, 
            "task_id" : _task_id["download_base_station_hour_summary"] },
          {
            "$set": {
              "progress": progress 
            },
            "$currentDate": {"lastModified": True}
          }
        )
        plog("result.matched_count: "+str(result.matched_count))
        plog("result.modified_count: "+str(result.modified_count))
      elif "download OK" in line:
        progress = 100
        plog("progress: " + str(progress) + "%")
        result = collection.update_one(
          {"project_id" : self.project_id, 
            "task_id" : _task_id["download_base_station_hour_summary"] },
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
    # 解析输入的参数
    self.project_id = self.get_argument('project_id');
    # 构造阿里云上运行的cmd
    download_cmd = ('tunnel download ' + self.project_id +
                    '_base_station_hour_summary ' + _download_folder + '/'
                    '' + self.project_id + '_base_station_hour_summary.csv ' 
                    '-h true;')
    plog("download_cmd: " + download_cmd)
    # 调用阿里云执行
    BaseHandler.runCmd(self, download_cmd,  \
        "download_base_station_hour_summary", \
        self.doDownloadBaseStationHourSummary)
    # 渲染结果页面
    #self.render('download_base_station_hour_summary_result.html',
    #            project_id = self.project_id,
    #            ret_msg = "Success")
    obj = {'project_id' : self.project_id,
           'ret_msg' : "Success"}
    self.write(json_encode(obj))

# 17
class GetBaseStationHourSummaryHandler(tornado.web.RequestHandler, BaseHandler):
  def post(self):
    # 解析输入的参数
    project_id = self.get_argument('project_id');
    # 渲染结果页面
    #self.render('get_base_station_hour_summary_result.html',
    #            project_id = project_id,
    #            result = _download_folder,
    #            ret_msg = "Success")
    obj = {'project_id' : project_id,
           'download_folder' : _download_folder,
           'ret_msg' : "Success"}
    self.write(json_encode(obj))

# 18
class ComputeUuidCellHourHandler(tornado.web.RequestHandler, BaseHandler):
  def doComputeUuidCellHour(self, sql1, sql2):
    name = multiprocessing.current_process().name
    plog(name + " " + "Starting")
    progress = 0
    # 建立数据库连接
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection = db["task-progress"]
    count = 0;
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["compute_uuid_cell_hour"]})
    result = collection.insert_one(
        { "project_id" : self.project_id,
          "task_id" : _task_id["compute_uuid_cell_hour"],
          "progress" : progress,
          "lastModified" : datetime.datetime.utcnow()
        }
    )
    plog("result.inserted_id: " + str(result.inserted_id))
    instance1 = odps.run_sql(sql1)
    while(not instance1.is_successful()):
      time.sleep(1) 
    plog("sql1 runs Successful")
    instance2 = odps.run_sql(sql2)
    while(not instance2.is_successful()):
      count += 1
      progress = int((1.0 * count / (count + 1)) * 100)
      plog("progress: " + str(progress) + "%")
      result = collection.update_one(
        {"project_id" : self.project_id, 
          "task_id" : _task_id["compute_uuid_cell_hour"] },
        {
          "$set": {
            "progress": progress 
          },
          "$currentDate": {"lastModified": True}
        }
      )
      time.sleep(5) 
    db_client.close()
    progress = 100
    plog("progress: " + str(progress) + "%")
    result = collection.update_one(
      {"project_id" : self.project_id, 
        "task_id" : _task_id["compute_uuid_cell_hour"] },
      {
        "$set": {
          "progress": progress 
        },
        "$currentDate": {"lastModified": True}
      }
    )
    plog("result.matched_count: "+str(result.matched_count))
    plog("result.modified_count: "+str(result.modified_count))
  def post(self):
    # 解析输入的参数
    self.project_id = self.get_argument('project_id');
    # 构造阿里云上运行的sql
    sql1 = ('create table if not exists ' 
            '' + self.project_id + '_uuid_cell_hour '
            '(uuid string, cell_hour string)'
            'partitioned by (date_p string);')
    sql2 = ('insert overwrite table '
            '' + self.project_id + '_uuid_cell_hour '
            'partition(date_p) '
            'select uuid, '
            'agg_cell_hour(bs_id, datepart(from_unixtime(time), "HH")) as ' 
            'cell_hour, date_p from ' + self.project_id + '_filtered_raw_data '
            'group by uuid, date_p')
    plog("sql1: " + sql1)
    plog("sql2: " + sql2)
    # 调用阿里云执行sql
    BaseHandler.runSQL2(self, sql1, sql2, \
        "compute_uuid_cell_hour", \
        self.doComputeUuidCellHour)
    # 渲染结果页面
    #self.render('compute_uuid_cell_hour_result.html',
    #            project_id = self.project_id,
    #            ret_msg = "Success")
    obj = {'project_id' : self.project_id,
           'ret_msg' : "Success"}
    self.write(json_encode(obj))

# 19
class GetUuidCellHourHandler(tornado.web.RequestHandler, BaseHandler):
  def post(self):
    # 解析输入的参数
    project_id = self.get_argument('project_id');
    # 渲染结果页面
    #self.render('get_uuid_cell_hour_result.html',
    #            project_id = project_id,
    #            result = project_id + "_uuid_cell_hour",
    #            ret_msg = "Success")
    obj = {'project_id' : project_id,
           'odps_table' : project_id + "_uuid_cell_hour",
           'ret_msg' : "Success"}
    self.write(json_encode(obj))

# 20
class DeleteAllTablesHandler(tornado.web.RequestHandler, BaseHandler):
  def doDeleteAllTables(self, exe):
    name = multiprocessing.current_process().name
    plog(name + ": " + "Starting")
    db_client = MongoClient('localhost', 27017)
    db = db_client[self.project_id + "_db"]
    collection = db["task-progress"]
    progress = 0
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["delete_all_tables"]})
    result = collection.insert_one(
        { "project_id" : self.project_id,
          "task_id" : _task_id["delete_all_tables"],
          "progress" : progress,
          "lastModified" : datetime.datetime.utcnow()
        }
    )
    count = 0
    for line in BaseHandler.runProcess(self, exe):
      print line,
      if "OK" in line:
        count += 1 
      if count == 5:
        progress = 100
        result = collection.update_one(
          {"project_id" : self.project_id, 
            "task_id" : _task_id["delete_all_tables"] },
          {
            "$set": {
              "progress": progress 
            },
            "$currentDate": {"lastModified": True}
          }
        )
    plog(name + ": " + "Exiting")
    #delete all local meta data
    ## task-progress
    collection = db["task-progress"]
    ### create_customer_raw_data_table
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["create_customer_raw_data_table"]})
    ### upload_customer_raw_data
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["upload_customer_raw_data"]})
    ### transform_to_spatio_temporal_raw_data
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["transform_to_spatio_temporal_raw_data"]})
    ### compute_raw_data_stat
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["compute_raw_data_stat"]})
    ### compute_people_distribution
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["compute_people_distribution"]})
    ### compute_base_station_info
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["compute_base_station_info"]})
    ### download_base_station_info
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["download_base_station_info"]})
    ### filter_data_with_range
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["filter_data_with_range"]})
    ### compute_filtered_data_stat
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["compute_filtered_data_stat"]})
    ### compute_base_station_hour_summary
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["compute_base_station_hour_summary"]})
    ### download_base_station_hour_summary
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["download_base_station_hour_summary"]})
    ### compute_uuid_cell_hour
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["compute_uuid_cell_hour"]})
    ### delete_all_tables (remained)

    ## customer_fields
    collection = db["customer_fields"]
    ### create_customer_raw_data_table
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["create_customer_raw_data_table"]})

    ## local-result
    collection = db["local-result"]
    ### compute_raw_data_stat
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["compute_raw_data_stat"]})
    ### compute_people_distribution
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["compute_people_distribution"]})
    ### filter_data_with_range
    result = collection.delete_many({
             "project_id": self.project_id,
             "task_id": _task_id["filter_data_with_range"]})
    db_client.close()

  def post(self):
    self.project_id = self.get_argument('project_id');
    plog("project_id: " + self.project_id)
    sql = "drop table if exists " + self.project_id + "_customer_raw_data; " + \
          "drop table if exists " + self.project_id + "_spatio_temporal_raw_data; " + \
          "drop table if exists " + self.project_id + "_base_station_info; " + \
          "drop table if exists " + self.project_id + "_base_station_hour_summary; " + \
          "drop table if exists " + self.project_id + "_uuid_cell_hour; " 
    BaseHandler.runCmd(self, sql, \
        "delete_all_tables", self.doDeleteAllTables)
    obj = {'return_msg' : "Success"}
    self.write(json_encode(obj))

if __name__ == '__main__':
  tornado.options.parse_command_line()
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(options.port)
  tornado.ioloop.IOLoop.instance().start()
