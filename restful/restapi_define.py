# -*- coding: utf-8 -*-
# Author: bill-jack<xfwangaw@isoftstone.com>
import uuid
import os
import time
from uuid import uuid4
from common.logs import logging as log
from common.request_result import request_result
from flask_restful import Resource
from flask import request, url_for, redirect, jsonify
from manager.file_manager import FileManager
from manager.user_manager import UserManager
from manager.quota_manager import QuotaManager
from werkzeug.utils import secure_filename
from hashlib import md5


class Graph(Resource):
    def __init__(self):
        self.manager = FileManager()

    # 文件上传与文件内容入库（基本信息存入mysql、文件内容存入neo4j）
    def post(self):
        # 上传文件
        # f = request.files['file']
        file_list = []
        file_id = str(uuid4()) 
        basepath = os.path.dirname(__file__)  # 当前文件所在路径
        day_time = time.strftime("%Y-%m-%d")
        # timestamp = time.strftime("%Y-%m-%d-%H%M%S")
        timestamp = time.strftime("%Y-%m-%d-")
        try:
            is_dir = os.listdir('/home/cmcc/graph/tools/neo4j-community-3.2.3/import/upload/')
            if len(is_dir) == 0 or (day_time not in is_dir):
                os.mkdir('/home/cmcc/graph/tools/neo4j-community-3.2.3/import/upload/%s/' % day_time)
            else:
                file_list = os.listdir('/home/cmcc/graph/tools/neo4j-community-3.2.3/import/upload/%s/' % day_time)
        except Exception, e:
            log.error('check the direct error, reason is: %s' % e)
            return request_result(202)
        for f in request.files.getlist('file'):
            if secure_filename(timestamp+f.filename) in file_list:
                return request_result(300)
        for f in request.files.getlist('file'):
            upload_path = os.path.join(basepath, '../../tools/neo4j-community-3.2.3/import/upload/%s/' % day_time,
                                       secure_filename(file_id + '.' + f.filename))
            
            try:
                f.save(upload_path)
            except Exception, e:
                log.error('write the msg into file error, reason is: %s' % e)
                return request_result(201)
        title = request.form["title"]
        comment = request.form['comment']
        local_name = 'test'
        log.info('get the parameters: title is: %s, comment is: %s' % (title, comment))
        dict_data = {'title': title, 'comment': comment, 'local_name':
                     local_name, 'file_id': file_id}

        try:
            result = self.manager.file_upload_manager(dict_data)
            if result is not None:
                return result
        except Exception, e:
            log.error('insert the data to mysql failed, reason is: %s' % e)
            return request_result(401)
        # file_position = 'PersonRel_Format.csv'
        # file_type = 'relationship'
        # try:
        #     self.manager.neo4j_upload_manager(file_position, file_type)
        # except Exception, e:
        #     log.error('neo4j storage failed, reason is: %s' % e)
        #     return request_result(501)

        return request_result(200)

    # 获取信息
    def get(self):
        file_id = request.args.get('file_id')
        if file_id is not None:
            try:
                result = self.manager.read_detail(file_id)
                return request_result(200, result)
            except Exception, e:
                log.error('get the detail message error, reason is: %s' % e)
                return request_result(403)
        try:
            result = self.manager.files_list_manager()
        except Exception, e:
            log.error('get the all files error, reason is: %s' % e)
            return request_result(403)

        return request_result(200, result)
    
    def delete(self):
        file_id = request.args.get('file_id')
        if file_id is None:
            return request_result(101)
        try:
            self.manager.delete_all(file_id)
        except Exception, e:
            log.error('delete the file error, reason is: %s' % e)
            return request_result(404)
        return request_result(200, 'delete sucessed')


class FileDetail(Resource):
    def __init__(self):
        self.manager = FileManager()
    
    def get(self):
        file_id = request.args.get('file_id')
        try:
            m_result = self.manager.read_detail(file_id)
            return request_result(200, m_result)
        except Exception, e:
            log.error('get the detail message error, reason is: %s' % e)
            return request_result(402)
class Nodes(Resource):
    def __init__(self):
        self.manager = FileManager()

    # 获取neo4j里面所有的nodes
    def get(self):
        try:
            result = self.manager.neo4j_get_nodes()
            return request_result(200, result)
        except Exception, e:
            log.error('get the neo4j nodes failed, reason is: %s' % e)
            return request_result(503)


class Relationship(Resource):
    def __init__(self):
        self.manager = FileManager()

    # 获取neo4j里面的relationship
    def get(self):
        from_id = request.args.get('from_id')
        to_id = request.args.get('to_id')
        shortest = request.args.get('shortest')
        # 获取所有relationship
        if from_id is None and to_id is None:
            try:
                result = self.manager.neo4j_get_relationships()
                return request_result(200, result)
            except Exception, e:
                log.error('get the neo4j rel failed, reason is: %s' % e)
                return request_result(503)
        # 获取关系深度最短的relationship
        elif from_id is not None and to_id is not None and \
                        shortest is not None:
            try:
                int(from_id)
                int(to_id)
                int(shortest)
            except Exception, e:
                log.error('parameters error, reason is: %s' % e)
                return request_result(101)
            if int(shortest) == 1:
                log.info('--------------------shortest:%s' % shortest)
                try:
                    result = self.manager.neo4j_shortest_relationship(from_id,
                                                                      to_id)
                    return request_result(200, result)
                except Exception, e:
                    log.eror('get the shortest rel failed, reason is: %s' % e)
                    return request_result(503)
        # 获取两个node之间的路径个数
        elif from_id is not None and to_id is not None:
            try:
                int(from_id)
                int(to_id)
            except Exception, e:
                log.error('parameters error, reason is: %s' % e)
                return request_result(101)
            try:
                result = self.manager.neo4j_get_design_relationships(from_id,
                                                                     to_id)
                return request_result(200, result)
            except Exception, e:
                log.error('get the neo4j one to one rel failed, '
                          'reason is: %s' % e)
                return request_result(503)
        else:
            return request_result(101)


class RelationDiscover(Resource):
    def __init__(self):
        self.manager = FileManager()

    # 关系和node数量的发现
    def post(self):
        try:
            file_id = request.args.get('file_id')
        except Exception, e:
            log.error('parameters error')
            return request_result(101)
        try:
            result = self.manager.storage_nums_node_and_rel(file_id)
            if result is not True:
                return request_result(402)
        except Exception, e:
            log.error('discover the relations and nodes failed, '
                      'reason is: %s' % e)
            return request_result(402)

        return request_result(200)

    def get(self):
        try:
            result = self.manager.source_message()
        except Exception, e:
            log.error('query the base message error, reason is: %s' % e)
            return request_result(403)

        return request_result(200, result)


class CommunityExplain(Resource):
    def __init__(self):
        self.manager = FileManager()

    def post(self):
        try:
            self.manager.community_ab()
            return request_result(200, 'ok')
        except Exception, e:
            log.error('community error, reason is: %s' % e)
            return request_result(502)

    def get(self):
        try:
            result = self.manager.get_all_community()
            return result
        except Exception, e:
            log.error('get the community error, reason is: %s' % e)
            return request_result(503)


class Quota(Resource):
    def __init__(self):
        self.quota = QuotaManager()

    def get(self):
        file_id = request.args.get('file_id')
        if file_id is None:
            return request_result(101)
        try:
            result = self.quota.all_quotas(file_id)
        except Exception, e:
            return request_result(403)
        return request_result(200, result)
    
    def post(self):
        quota = request.args.get('quota')
        file_id = request.args.get('file_id')
        if file_id is None:
            return request_result(101)
        file_name = "/home/cmcc/graph/tools/neo4j-community-3.2.3/import/programe/lpa.txt"
        if quota is None:
            return request_result(101)
        try:
            # 平均度
            if quota == 'ave_degree':
                return self.quota.ave_degree(file_id)
            # 直径
            if quota == 'diameter':
                result = self.quota.diameter(file_id)
                return result
            # 图密度
            if quota == 'density':
                return self.quota.density(file_id)
            # 模块度
            if quota == 'modular':
                return self.quota.modular(file_id)
            # pageranke
            if quota == 'pagerank':
                return self.quota.pagerank(file_id)
            # 全网聚类系数
            if quota == 'clustering':
                return self.quota.clustering(file_id)
            # 平均路径长度
            if quota == 'ave_path_len':
                return self.quota.ave_path_len(file_id)
            # HITS
            if quota == 'hits':
                return self.quota.pagerank_hits(file_id)
            # 特征向量中心
            if quota == 'eigenvector':
                return self.quota.eigenvector(file_id)
        except Exception, e:
            log.error('calculate error, reason is: %s' % e)
            return request_result(504)


class UserAbout(Resource):
    def __init__(self):
        self.user_manager = UserManager()
    
    def get(self):
        username = request.cookies.get('username')
        sessionid = request.cookies.get('sessionid')
        cookie_ab = self.user_manager.get_sessionid(username)
        if cookie_ab.get('status') == 200:
            if cookie_ab.get('result') == sessionid:
                log.info('check the cookie correct, login...')
                return jsonify({'status': 200})
            else:
                log.warn('check the cookie incorrect')
                return jsonify({'status': 400})
        else:
            log.warn('login check the cookie error')
            return jsonify({'status': 400})
    def post(self):
        sessionid = str(uuid4())
        username = request.form.get("username").encode("utf8")
        password = request.form.get("password").encode("utf8")
        md5_obj = md5()
        md5_obj.update(password + "tupu2017")
        password = md5_obj.hexdigest()
        log.info('get the password from login html is: %s' % password)
        # user_manager = UserManager()
        log.info('aaaaaaaaaaa')
        password1 = self.user_manager.user_password_manager(username)
        if password1.get('status') != 200:
            return passworld1
        password1 = password1.get('result')
        stype = ''
        if password1 == password:
            cookie_ab = self.user_manager.get_sessionid(username)
            if cookie_ab.get('status') == 200:
                if cookie_ab.get('result') != '':
                    stype = 'update'
                else:
                    stype = 'insert'
                self.user_manager.change_sessionid(stype, username, sessionid)
            if cookie_ab.get('status') == 403:
                return cookie_ab
            response = jsonify({"status":200})
            response.set_cookie("username", username, max_age=180)
            response.set_cookie("sessionid", sessionid, max_age=180)
            return response
        else:
            return jsonify({"status": 400})

