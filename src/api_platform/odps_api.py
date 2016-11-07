#!/usr/local/bin/python

from odps import ODPS
import time

access_id = "xO0RtfYLVQEnAuUN"
access_key = "imNsJiShzQlcNYmDvrEt2hiXsreDro"
project = "tsnav_project"
end_point = "http://service.odps.aliyun.com/api"
odps = ODPS(access_id, access_key, project, end_point)

#instance = odps.run_sql('select t.r as interval, count(*) as count from (select uuid, floor((count(*)/10)) as r from nanjing1_spatio_temporal_raw_data where date_p=20151229 group by uuid) t group by t.r order by interval limit 20;') 
instance = odps.run_sql('Create table if not exists nanjing1_filtered_raw_data (uuid string, lon double, lat double, time bigint, count bigint) partitioned by (date_p string); insert overwrite table nanjing1_filtered_raw_data partition(date_p) select A.uuid, A.lon, A.lat, A.time, B.count, A.date_p from nanjing1_spatio_temporal_raw_data A inner join (select uuid, count(uuid) as count from nanjing1_spatio_temporal_raw_data group by uuid having count(uuid) > 10 and count(uuid) < 300) B on A.uuid = B.uuid;')
#instance = odps.run_sql('Create table if not exists nanjing1_filtered_raw_data (uuid string, lon double, lat double, time bigint, count bigint) partitioned by (date_p string);')
#instance = odps.run_sql('insert overwrite table nanjing1_filtered_raw_data partition(date_p) select A.uuid, A.lon, A.lat, A.time, B.count, A.date_p from nanjing1_spatio_temporal_raw_data A inner join (select uuid, count(uuid) as count from nanjing1_spatio_temporal_raw_data group by uuid having count(uuid) > 10 and count(uuid) < 300) B on A.uuid = B.uuid;')

while(not instance.is_successful()):
  print("is_successful(): " + str(instance.is_successful()))
  print("is_terminated(): " + str(instance.is_terminated()))
  time.sleep(2) 

#instance.get_task_result(instance.get_task_names()[0])
#instance.get_task_result(instance.get_task_names()[1])
