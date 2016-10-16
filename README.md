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

#### mongodb基本命令
show dbs:显示数据库列表 
show collections：显示当前数据库中的集合（类似关系数据库中的表） 
show users：显示用户
use <db name>：切换当前数据库，这和MS-SQL里面的意思一样 
db.help()：显示数据库操作命令，里面有很多的命令 
db.foo.help()：显示集合操作命令，同样有很多的命令，foo指的是当前数据库下，一个叫foo的集合，并非真正意义上的命令 
db.foo.find()：对于当前数据库中的foo集合进行数据查找（由于没有条件，会列出所有数据） 
db.foo.find( { a : 1 } )：对于当前数据库中的foo集合进行查找，条件是数据中有一个属性叫a，且a的值为1
