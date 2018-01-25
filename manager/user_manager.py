# -*- coding: utf-8 -*-
# Author: bill-jack<xfwangaw@isoftstone.com>
from common.logs import logging as log
from common.request_result import request_result
from database.mysql_db import BaseMessageDB


class UserManager(object):
    def __init__(self):
        self.db = BaseMessageDB()
    
    
    def user_password_manager(self, username):
        try:
            db_result = self.db.select_user_password(username)
            log.info('get the database result is: %s' % db_result)
            password = db_result[0][0]
            log.info(password)
            return request_result(200, password)
        except Exception, e:
            log.error('get the password by username from db error, reason is: %s' % e)
            return request_result(403)

    def get_sessionid(self, username):
        try:
            db_result = self.db.get_sessionid(username)
            log.info('get the database result is:')
            log.info(db_result)
            log.info('type is: %s' % type(db_result))
            sessionid = ''
            if len(db_result) != 0:
                sessionid = db_result[0][0]
            return request_result(200, sessionid)
        except Exception, e:
            log.error('get the password by username from db error, reason is: %s' % e)
            return request_result(403)

    def change_sessionid(self, stype, username, sessionid):
        try:
            if stype == 'insert':
                self.db.insert_cookie(username, sessionid)
            if stype == 'update':
                self.db.update_cookie(username, sessionid)
            return request_result(200, 'ok')
        except Exception, e:
            log.error('update the sessionid error, reason is: %s' % e)
            return request_result(402)
