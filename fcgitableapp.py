#!/usr/bin/python
# encoding : utf-8

from flup.server.fcgi import WSGIServer

from tableapp  import app

if __name__  == '__main__':
   WSGIServer(app,bindAddress=('127.0.0.1',8008)).run()
