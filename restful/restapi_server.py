# -*- coding: utf-8 -*-
# Author: bill-jack<xfwangaw@isoftstone.com>

import sys
path1 = sys.path[0] + '/..'
sys.path.append(path1)
from common.logs import logging as log
from time import sleep
from restapi_register import rest_app_run
from common.db.mysql_base import DBInit


def start_server(server_name):
    while True:
        try:
            rest_app_run()
        except Exception, e:
            log.error('start the server %s failed, '
                      'reason is: %s' % (server_name, e))

        sleep(8)


if __name__ == '__main__':
    start_server('Graph')
