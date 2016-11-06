#!/usr/local/bin/python

from odps import ODPS
import time

access_id = "xO0RtfYLVQEnAuUN"
access_key = "imNsJiShzQlcNYmDvrEt2hiXsreDro"
project = "tsnav_project"
end_point = "http://service.odps.aliyun.com/api"
odps = ODPS(access_id, access_key, project, end_point)

instance = odps.run_sql('select t.r as interval, count(*) as count from (select uuid, floor((count(*)/10)) as r from nanjing1_spatio_temporal_raw_data where date_p=20151229 group by uuid) t group by t.r order by interval limit 20;') 

while(not instance.is_successful()):
  print("is_successful(): " + str(instance.is_successful()))
  print("is_terminated(): " + str(instance.is_terminated()))
  time.sleep(2) 

instance.get_task_result(instance.get_task_names()[0])
