# 增加UDF

1 上传jar: 

```
add jar /Users/willwywang-NB/HQJY/src/udf/Minizone/target/AggCellHour.jar
```

2 检查资源:

```
list resources;
```

3 加载udf:

```
create function agg_cell_hour as com.tsnav.udf.AggCellHour using AggCellHour.jar
```

4 检查加载udf:

```
list functions;
```

5 删除udf

```
drop function xxx
drop resource xxx;
```
