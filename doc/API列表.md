# API list

## /upload_data [ok]

上传客户原始格式数据

参数:
1. data_path
2. threads_num
3. row_delimiter
4. col_delimiter
5. project_id

## /create_customer_raw_data [ok]

创建客户原始格式数据表

参数:
1. fields
2. project_id

## /request_task_progress [ok]

获取各种任务执行进度

参数:
1. project_id
2. task_id

## /transform_to_inner_format [ok]

将客户格式数据转换到内部时空格式  
同时:
1. 对数据进行按照日期的分区,
2. 在(uuid, time, lat, lon)粒度上的去重
3. 和初级的过滤, 各字段null, 0等异常值去除

参数:
1. project_id

# TODO
## 过滤
### /filter_data
启动数据过滤
## 人口统计
### /demographic
启动计算每个人的数据条数等
## 基站信息
### /basestation
启动基站信息抽取
## 天粒度基站热力
### /base_station_summary
启动天粒度基站热力
### /get_base_station_summary
获取天粒度基站热力进度
## 小时粒度基站热力
### /base_station_hour_summary
启动天粒度基站热力
### /get_base_station_hour_summary
获取天粒度基站热力进度
## 最细粒度人时空信息表
### /uuid_cell_hours
启动计算人的时空聚合信息
### /request_uuid_cell_hours_stat
获取人的时空信息统计
