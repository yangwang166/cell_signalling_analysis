
<!-- toc orderedList:0 -->

- [api平台demo](#api平台demo)
	- [基于tornado](#基于tornado)
	- [命令行执行方法](#命令行执行方法)
	- [通过api调用命令行](#通过api调用命令行)
		- [api输入参数:](#api输入参数)
			- [数据地址:](#数据地址)
			- [目标表:](#目标表)
			- [线程数:](#线程数)
			- [行分隔符:](#行分隔符)
			- [列分隔符:](#列分隔符)
			- [隐藏了的参数:](#隐藏了的参数)
		- [api使用post形式提交](#api使用post形式提交)

<!-- tocstop -->

# api平台demo

## 基于tornado
使用python语言的tornado作为api server的平台

## 命令行执行方法

上传原始数据:
`python test_cmd.py cmd/upload.cmd`

执行sql:
`python test_cmd.py cmd/limit3.sql`

test_cmd.py代码:

```python
import subprocess
import shlex
import sys

def runProcess(exe):
    p = subprocess.Popen(exe, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while(True):
      retcode = p.poll() #returns None while subprocess is running
      line = p.stdout.readline()
      yield line
      if(retcode is not None):
        break
    print "\nFinish process"

# Make it configable
client = "/Users/willwywang-NB/github/cell_signalling_analysis/tool/odps/bin/odpscmd"

file_name = sys.argv[1]

# use sql file
cmd = """
%s -f "%s";
""" % (client, file_name)

print "cmd: ",cmd
args =  shlex.split(cmd)
print "args: ",args

for line in runProcess(args):
    print line,
```

## 通过api调用命令行

### api输入参数:

暴露出来如下参数, 其余的使用内置参数(为了尽可能简单)

#### 数据地址:
例如:  
/Users/willwywang-NB/github/cell_signalling_analysis/data/test/50m  

注意事项:  
数据是已经按照指定格式处理过的, 列数要跟创建的阿里云表一致。

#### 目标表:
例如: test_20160828

#### 线程数:
例如: 8

#### 行分隔符:
例如: "\n"

#### 列分隔符:
例如: ","

#### 隐藏了的参数:
1. -bs <ARG>: block大小, 单位MB, 默认100
2. -c <ARG>: 设置file的编码格式, **默认不改变编码, 使用raw**
3. -cp <ARG>: 压缩, 默认true
4. -dbr <ARG>: 是否丢弃异常数据, 默认false
5. -dfp <ARG>: 设置日期格式, 默认"yyyy-MM-dd HH:mm:ss"
7. -h <ARG>: 是否有表头, 默认false
8. -mbr <ARG>: 最多允许多少条异常数据, 默认1000
9. -ni <ARG>: 设置null表示成的字符串, 默认""
11. -s <ARG>: 是否先扫描文件再上传, 默认true, only时只扫描
12. -sd <ARG>: 设置session目录, 默认null, **作用?**
13. -te <ARG>: 设置tunnel endpoint
15. -tz <ARG>: 设置时区, 默认本地Asia/Shanghai

### api使用post形式提交

后台测试期间, 前端我简单的设计一个提交窗口用于输入参数
