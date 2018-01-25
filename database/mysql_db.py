# -*- coding: utf-8 -*-
# Author: bill-jack<xfwangaw@isoftstone.com>
from common.logs import logging as log
from common.db.mysql_base import MysqlInit
import time
from uuid import uuid4


class BaseMessageDB(MysqlInit):
    def __init__(self):
        super(BaseMessageDB, self).__init__()

    def select_user_password(self, username):
        sql = "select password from graph_user where username='%s'" % username
        return super(BaseMessageDB, self).exec_select_sql(sql)

    def get_sessionid(self, username):
        sql = "select sessions from user_session where username='%s'" % username
        log.info('>>>>>>sql>>>>>:%s' % sql)
        return super(BaseMessageDB, self).exec_select_sql(sql)

    def update_cookie(self, username, sessionid):
        # sessionid = str(uuid4())
        sql = "update user_session set sessions='%s' where username='%s'" % (sessionid, username)
        return super(BaseMessageDB, self).exec_update_sql(sql)
    
    def insert_cookie(self, username, sessionid):
        u_s_id = str(uuid4())
        sql = "insert into user_session(id, username, sessions) values('%s', '%s', '%s') " % (u_s_id, username, sessionid)
        return super(BaseMessageDB, self).exec_update_sql(sql)

    def get_create_time(self, file_id):
        sql = "select create_time from file_message where id = '%s'" % file_id
        return super(BaseMessageDB, self).exec_select_sql(sql)

    def upload_data(self, dict_data):
        log.info('get the upload data is: %s ' % dict_data)
        # id = uuid4()
        file_id = dict_data.get('file_id')
        title = dict_data.get('title')
        comment = dict_data.get('comment')
        local_name = dict_data.get('local_name')
        upload_name = local_name
        sql = "insert into file_message(id, title, comment, upload_name, " \
              "local_name) values ('%s','%s','%s','%s','%s')" % (file_id, title,
                                                                 comment,
                                                                 upload_name,
                                                                 local_name)

        return super(BaseMessageDB, self).exec_update_sql(sql)
    
    def get_titles(self):
        sql = "select title from file_message"
        return super(BaseMessageDB, self).exec_select_sql(sql)
    
    def get_details(self, file_id):
        sql = "select create_time from file_message where id='%s'" % file_id
        return super(BaseMessageDB, self).exec_select_sql(sql)

    def get_all_files(self):
        sql = "select id, title, comment, upload_name, local_name, " \
              "data_total, data_nodes, data_relations from file_message"

        return super(BaseMessageDB, self).exec_select_sql(sql)

    def update_nums_nodes_relations(self, data_nodes, data_relations, file_id):
        sql = "update file_message f set data_nodes=%d,data_relations=%d " \
              "WHERE f.id = '%s'" % (data_nodes, data_relations, file_id)

        return super(BaseMessageDB, self).exec_update_sql(sql)

    def get_resource_message(self):

        sql = "select title, upload_name, local_name, data_total," \
              "data_nodes, data_relations, create_time from file_message"

        return super(BaseMessageDB, self).exec_select_sql(sql)

    @staticmethod
    def time_diff(create_time):
        dates = time.strftime('%Y-%m-%d %H:%M:%S',
                              time.strptime(str(create_time),
                                            "%Y-%m-%d %H:%M:%S"))
        return dates

    @staticmethod
    def time_diff_1(create_time):
        dates = time.strftime('%Y-%m-%d %H:%M:%S', create_time.timetuple())
        return dates
     
    def delete_all(self, file_id):
        sql = "delete from file_message where id='%s'" % file_id
        return super(BaseMessageDB, self).exec_update_sql(sql) 
