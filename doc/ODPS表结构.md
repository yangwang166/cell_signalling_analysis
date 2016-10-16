# 手机信令表结构

## 阿里云的表结构

{$project} 表示项目名称

### 原始表

```sql
create table if not exists {$project}_raw_data
(uuid string, call_in bigint, call_out bigint, time bigint,
cell_id bigint, cell_name string, lon double, lat double,
in_room bigint, is_roam bigint);
```

### 数据去重1

去掉完全相同的数据

```sql
create table if not exists {$project}_raw_data_no_repeat
(uuid string, call_in bigint, call_out bigint, time bigint,
cell_id bigint, cell_name string, lon double, lat double,
in_room bigint, is_roam bigint);
```

### 数据去重2+过滤

根据uuid, time, lon, lat聚合做一次聚合(即去重)  
去掉lon, lat是NULL  
去掉lat, lon是0
去掉异性uuid

```sql
create table if not exists {$project}_raw_data_no_repeat_core
(uuid string, time bigint, lat double, lon double,
in_room bigint, is_roam bigint, count bigint)
partitioned by (date_p string);
```

### 原始数据的分区表

```sql
create table if not exists {$project}_raw_data_no_repeat_core_partition
(uuid string, call_in bigint, call_out bigint, time bigint,
cell_id bigint, cell_name string, lon double, lat double,
in_room bigint, is_roam bigint)
partitioned by (date_p string);
```

### 人数据条数表

按天生成每个人的数据条数

```sql
create table if not exists {$project}_uuid_count
(uuid string, count bigint)
partitioned by (date_p string);
```

### 目标人群表

根据每人的数据条数, 选取置信人群

```sql
create table if not exists {$project}_uuid_count_from_{$a}_to_{$b}
(uuid string, count bigint)
partitioned by (date_p string);
```

### 目标人群的最细粒度表

根据目标人群, 抽取全局数据中对应的人群数据

```sql
create table if not exists
{$project}_raw_data_no_repeat_partition_core_from_{$a}_to{$b}
(uuid string, time bigint, cell_id bigint,
in_room bigint, is_roam bigint, count bigint, hour bigint)
partitioned by (date_p string);
```

### 基站信息表

```sql
create table if not exists {$project}_raw_base_station
(cell_id bigint, cell_raw_id bigint, cell_name string, lat double, lon double);
```

### 天粒度的基站聚合表

```sql
create table {$project}_base_station_summary
(cell_id bigint, count bigint)
partitioned by(date_p string);
```

### 小时粒度的基站聚合表

```sql
create table {$project}_base_station_summary_hour
(cell_id bigint, hour bigint, count bigint)
partitioned by(date_p string);
```

### 最细粒度人时空信息表

人的时空信息: uuid string, cellhours string  

cellhours字段解释: 基站id@开始小时#逗留时间

例子:

```
000008271c767a90617ae958ce9f49f7, 14258@0#1|14319@1#7|14201@8#1|14319@9#9|14201@18#1|14208@19#1|14319@20#4
```

```sql
create table {$project}_uuid_cellhours
(uuid string, cellhours string)
partitioned by(date_p string);
```
