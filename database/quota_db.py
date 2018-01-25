# -*-coding: utf-8 -*-
# Author: bill-jack<xfwangaw@isoftstone.com>
from common.logs import logging as log
from common.db.mysql_base import MysqlInit
import time
from uuid import uuid4


class QuotaDB(MysqlInit):
    def __init__(self):
        super(QuotaDB, self).__init__()

    def add_quota(self, data_dict):
        u_id = str(uuid4())
        file_id = data_dict.get('file_id')
        q_name = data_dict.get('q_name')
        q_value = data_dict.get('q_value')
        q_label = data_dict.get('q_label')
        q_img = data_dict.get('q_img')
        
        sql = "insert into quota(id, file_id, q_name, q_value, q_label," \
              "q_img) values('%s', '%s', '%s', '%s', %d, '%s')" % (u_id, file_id, q_name,
                                                                   q_value, q_label, q_img)

        return super(QuotaDB, self).exec_update_sql(sql)

    def all_quotas(self, file_id):
        sql = "select id, file_id, q_name, q_value, q_label," \
              "q_img from quota where file_id='%s'" % file_id

        return super(QuotaDB, self).exec_select_sql(sql)

