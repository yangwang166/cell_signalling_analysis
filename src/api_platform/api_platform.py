import os.path

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

from tornado.options import define, options
define("port", default=8888, help="run on the given port", type=int)

class TestUploadHandler(tornado.web.RequestHandler):
	def get(self):
		self.render('test_upload.html')

class UploadHandler(tornado.web.RequestHandler):
	def post(self):
		data_path = self.get_argument('data_path')
		aliyun_table = self.get_argument('aliyun_table')
		threads_num = self.get_argument('threads_num')
		row_delimiter = self.get_argument('row_delimiter')
		col_delimiter = self.get_argument('col_delimiter')

		self.render('upload_result.html', 
                data_path=data_path,
				        aliyun_table=aliyun_table,
                threads_num=threads_num,
                row_delimiter=row_delimiter,
                col_delimiter=col_delimiter)


if __name__ == '__main__':
	tornado.options.parse_command_line()
	app = tornado.web.Application(
		handlers=[(r'/test_upload', TestUploadHandler), 
              (r'/upload', UploadHandler)],
		template_path=os.path.join(os.path.dirname(__file__), "templates")
	)
	http_server = tornado.httpserver.HTTPServer(app)
	http_server.listen(options.port)
	tornado.ioloop.IOLoop.instance().start()

