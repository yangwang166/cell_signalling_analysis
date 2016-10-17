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

## /get_raw_data_stat
获取每天的数据条数, 可知道有几天数据, 每天的数据条数
参数:
1. project_id

## /obtain_people_with_range
通过设置人一天产生数据条数的范围, 获取符合这个范围的人群表
参数:
1. project_id
2. count_min
3. count_max

## /people_with_range_stat
获得指定数据条数范围内的人的个数
参数:
1. project_id
2. count_min
3. count_max

## /filtered_data_with_range
利用obtain_people_with_range的到的人群表, 过滤数据
参数:
1. project_id
2. count_min
3. count_max

## /get_filtered_data_stat
获取过滤后的每天数据条数
参数:
1. project_id
2. count_min
3. count_max

### /base_station_info
启动基站信息抽取

### /get_base_station_info
获取基站信息

### /base_station_summary
启动天粒度基站热力

### /get_base_station_summary
获取天粒度基站热力

### /base_station_hour_summary
启动小时粒度基站热力

### /get_base_station_hour_summary
获取小时粒度基站热力进度

### /uuid_cell_hours
启动计算人的时空聚合信息

### /request_uuid_cell_hours_stat
获取人的时空信息统计
