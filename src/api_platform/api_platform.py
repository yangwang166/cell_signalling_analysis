import os.path
import subprocess
import shlex
import multiprocessing
import datetime
from pymongo import MongoClient

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from tornado.options import define, options

define("port", default=8888, help="run on the given port", type=int)

# Make odps client configurable
client = "/Users/willwywang-NB/"+\
         "github/cell_signalling_analysis/tool/odps/bin/odpscmd"

def plog(msg):
  print "API: ", msg

class Application(tornado.web.Application):
  def __init__(self):
    handlers=[(r'/test_upload', TestUploadHandler), 
              (r'/upload_data', UploadHandler)
             ]
    settings = dict(
      template_path=os.path.join(os.path.dirname(__file__), "templates"),
      debug=True,
      )
    #Make mongodb host/port configurable
    client = MongoClient('localhost', 27017)
    #Make database and table configurable
    self.db = client['test-database']
    self.collection = self.db['test-collection']
    tornado.web.Application.__init__(self, handlers, **settings)

class TestUploadHandler(tornado.web.RequestHandler):
  def get(self):
    self.render('test_upload.html')

class UploadHandler(tornado.web.RequestHandler):

  def runProcess(self, exe):    
    p = subprocess.Popen(exe, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while(True):
      retcode = p.poll() #returns None while subprocess is running
      line = p.stdout.readline()
      yield line
      if(retcode is not None):
        break
    plog("\nFinish process")

  def doUpload(self, exe):
    name = multiprocessing.current_process().name
    plog(name+" "+"Starting")
    total = 0
    finished = 0
    progress = 0
    session_id = ""
    for line in self.runProcess(exe):
      print line,
      # extract upload progess and total block
      if "Upload session" in line:
        session_id = line.split()[2]
        plog("session_id: "+session_id)
        result = self.application.collection.delete_many({"project_id": 0,
                                                          "task_id": 1})
        result = self.application.collection.insert_one(
            { "project_id" : 0,
              "task_id" : 1,
              "aliyun_sess_id" : session_id,
              "progress" : progress,
              "lastModified" : datetime.datetime.utcnow()
            }
        )
        plog("result.inserted_id: "+str(result.inserted_id))
      if "Split input to" in line:
        total = int(line.split()[5])
        plog("total: " + str(total))
      elif "upload block complete" in line:
        finished += 1
        plog("finished: " + str(finished))
        progress = int((1.0*finished/total)*100)
        plog("progress: " + str(progress) + "%")
        result = self.application.collection.update_one(
          {"project_id" : 0,"task_id" : 1 },
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

  def post(self):
    data_path = self.get_argument('data_path')
    aliyun_table = self.get_argument('aliyun_table')
    threads_num = self.get_argument('threads_num')
    row_delimiter = self.get_argument('row_delimiter')
    col_delimiter = self.get_argument('col_delimiter')

    upload_cmd = "tunnel upload %s %s -bs 10 -threads %s -s true" \
                 % (data_path, aliyun_table, threads_num)

    plog("upload_cmd: " + upload_cmd)

    cmd = """
    %s -e "%s";
    """ % (client, upload_cmd)

    plog("cmd: " + cmd)
    arguments =  shlex.split(cmd)
    plog("arguments: " + ' '.join(arguments))

    # TODO: drop table and clean progress db before a new upload
    upload_process = multiprocessing.Process(name='upload_service', 
                                             target=self.doUpload, 
                                             args=(arguments,))
    upload_process.start()

    self.render('upload_result.html', 
                data_path=data_path,
                aliyun_table=aliyun_table,
                threads_num=threads_num,
                row_delimiter=row_delimiter,
                col_delimiter=col_delimiter)


if __name__ == '__main__':
  tornado.options.parse_command_line()
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(options.port)
  tornado.ioloop.IOLoop.instance().start()

