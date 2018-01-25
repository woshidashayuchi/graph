# -*- coding: utf-8 -*-
# Author: bill-jack<xfwangaw@isoftstone.com>

from flup.server.fcgi import WSGIServer
from restapi_register import app
from common.logs import logging as log
from time import sleep

if __name__  == '__main__':
    while True:
        try:
            WSGIServer(app,bindAddress=('127.0.0.1',8008)).run()
        except Exception, e:
            log.error('start the graph server error, reason is: %s' % e)
        time.sleep(8)
