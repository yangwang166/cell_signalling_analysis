# cell_signalling_analysis
手机信令分析

## 依赖
mongodb  
tornado  
odps console  


## mongodb

### 安装方法

参考<https://docs.mongodb.com/manual/tutorial/install-mongodb-on-os-x/>

### 启动

创建用于存储db的目录  
mkdir -p /mongodb/data/storage/path
eg: mkdir -p /Users/willwywang-NB/github/cell_signalling_analysis/db

启动db  
mongod --dbpath /mongodb/data/storage/path
eg: mongod --dbpath /Users/willwywang-NB/github/cell_signalling_analysis/db

启动db shell  
在启动了db后, `mongo`用于启动shell
