# -*- coding: utf-8 -*-
# Author: bill-jack<xfwangaw@isoftstone.com>
import os
from common.logs import logging as log
from common.request_result import request_result
from database.mysql_db import BaseMessageDB
from common.db.neo4j_base import Neo4jBase, Neo4jAlgorithm
from common.conf import file_path


class FileManager(object):
    def __init__(self):
        self.base_message = BaseMessageDB()
        self.neo4j_message = Neo4jBase()
        self.neo4j_al = Neo4jAlgorithm()

    @staticmethod
    def explain_neo4j_result(dict_data, ret_type):
        try:
            if len(dict_data.get('errors')) != 0:
                raise Exception('have errors')
            else:
                if ret_type == 'nodes':
                    result = dict_data.get('results')[0].get('data')[0].get('row')[0]
                else:
                    result = dict_data.get('results')[0].get('data')
                return result
        except Exception, e:
            log.error('explain the neo4j result error, reason: %s' % e)
            raise Exception('explain the neo4j result happened exception')

    def file_upload_manager(self, dict_data):
        try:
            titles = self.base_message.get_titles()
            for i in titles:
                log.info('>>>>>>>>>>>>>>>>>>>')
                log.info(i)
                log.info(i[0])
                if dict_data.get('title') == i[0]:
                    return request_result(302)
        except Exception, e:
            log.error('check the title if exist error, reason is: %s' % e)
            raise Exception(e)
        try:
            db_result = self.base_message.upload_data(dict_data)
            if db_result is not None:
                raise Exception('db insert error')
        except Exception, e:
            log.error('add file base message into database failed,'
                      'reason is: %s' % e)
            raise Exception(e)

        # if len(db_result) is not None:
        #     raise Exception('mysql operator failed')
    
    def files_list_manager(self):
        result = []
        try:
            db_ret = self.base_message.get_all_files()
            for f in db_ret:
                id = f[0]
                title = f[1]
                comment = f[2]
                upload_name = f[3]
                local_name = f[4]
                data_total = f[5]
                data_nodes = f[6]
                data_relations = f[7]
                ret = {'id': id, 'title': title, 'comment': comment,
                       'upload_name': upload_name, 'local_name': local_name,
                       'data_total': data_total, 'data_nodes': data_nodes,
                       'data_relations': data_relations}
                result.append(ret)
        except Exception, e:
            log.error('get the file lists error, reason is: %s' % e)
            raise Exception(e)

        return result

    def file_details(self, file_id):
        try:
            db_ret = self.base_massage.get_details(file_id)
        except Exception, e:
            log.error('get the file detail error, reason is: %s' % e)
            raise Exception(e)
        log.info('>>>>>>>>>>>>>>>>>>>')
        log.info('get the create time is: %s' % db_ret)
        diff_time = self.base_message.time_diff(db_ret[0][0]).split(' ')[0]
        
        file_list = os.listdir('/home/cmcc/graph/tools/neo4j-community-3.2.3/import/upload/%s/' % diff_time)
        
        return diff_time

    def neo4j_upload_manager(self, file_position, file_type):
        try:
            self.neo4j_message.execute_create_cql(file_position, file_type)
        except Exception, e:
            log.error('storage the file message into neo4j failed, '
                      'file_type is: %s, reason is: %s' % (file_type, e))
            raise Exception(e)

    def neo4j_get_nodes(self):
        try:
            result = self.neo4j_message.execute_nodes_cql()
            result = self.explain_neo4j_result(result, 'nodes')
        except Exception, e:
            log.error('get the node message failed, reason is: %s' % e)
            raise Exception(e)

        return result

    def neo4j_get_relationships(self):
        try:
            result = self.neo4j_message.execute_rel_cql()
            # result = self.explain_neo4j_result(result, 'relationships')
            result = result.get('results')[0].get('data')[0].get('row')[1]
        except Exception, e:
            log.error('get the rel message failed, reason is: %s' % e)
            raise Exception(e)

        return result

    def neo4j_get_design_relationships(self, from_id, to_id):
        try:
            result = self.neo4j_message.rel_route(from_id, to_id)
            result = result.get('results')[0].get('data')[0].get('row')[0]
        except Exception, e:
            log.error('get the one to one rel failed, reason is: %s' % e)
            raise Exception(e)

        return result

    def neo4j_shortest_relationship(self, from_id, to_id):
        response = []
        try:
            result = self.neo4j_message.shortest_rel_route(from_id, to_id)
        except Exception, e:
            log.error('get the shortest rel failed, reason is: %s' % e)
            raise Exception(e)
        try:
            ret = []
            inner_str = ''
            for i in result.get('results')[0].get('data'):
                a = i.get('meta')[0]
                ret.append(a)
            for relations in ret:
                for r in relations:
                    if r.get('type') == 'node':
                        inner_str = inner_str + '->' + str(r.get('id'))
                    if r.get('type') == 'relationship':
                        pass
                b = inner_str[2:]
                response.append(b)
                inner_str = ''
        except Exception, e:
            log.error('explain shortest relation result error, '
                      'reason is: %s' % e)
            raise Exception(e)

        return response

    def read_detail(self, file_id):
        result = dict()
        line_num = 1
        try:
            db_result = self.base_message.get_create_time(file_id)
        except Exception, e:
            log.error('get the create time form db error, reason is: %s' % e)
            return request_result(403)
        create_time = self.base_message.time_diff(db_result[0][0]).split(' ')[0]
        log.info("<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        file_list = os.listdir(file_path+create_time)
        for file in file_list:
            if str(file_id+'total') in file:
                f = open(file_path+create_time+'/'+file)
                for line in f.readlines()[0:100]:
                    result[line_num] = line
                    line_num += 1
                f.close()
                
        return result
    
    def combine_all_files(self, file_id):
        result = dict()
        result_total = dict()
        b = ['3.csv','4.csv','5.csv','6.csv','7.csv','8.csv','9.csv','10.csv','11.csv','12.csv']
        files_num = 1
        # 读取数据库，获取所需合并文件的上传日期
        try:
            db_result = self.base_message.get_create_time(file_id)
        except Exception, e:
            log.error('get the create time form db error, reason is: %s' % e)
            return request_result(403)
        create_time = self.base_message.time_diff(db_result[0][0]).split(' ')[0]
        # 获取文件夹下的问价列表
        file_list = os.listdir(file_path+create_time)
        for file in file_list:
            if file_id in file:
                if '3.csv' in file:
                    files_num = 3
                if '6.csv' in file:
                    files_num = 6
                if '9.csv' in file:
                    file_num = 9
                if '12.csv' in file:
                    file_num = 12
                f = open(file_path+create_time+'/'+file)
                for line in f.readlines():
                    # a = [0 for x in range(12)]
                    if str(line.split(',')[0])+'-'+str(line.split(',')[1]) in result.keys():
                        result[str(line.split(',')[0])+'-'+str(line.split(',')[1])] = str(int(result[str(line.split(',')[0])+'-'+str(line.split(',')[1])]) + int(line.split(',')[2]))
                    else:
                        result[str(line.split(',')[0])+'-'+str(line.split(',')[1])] = line.split(',')[2]

                    if str(line.split(',')[0])+'-'+str(line.split(',')[1]) in result_total.keys():
                        result_total[str(line.split(',')[0])+'-'+str(line.split(',')[1])] = str(result_total[str(line.split(',')[0])+'-'+str(line.split(',')[1])]) +',' + str(line.split(',')[2].strip('\r\n'))
                    else:
                        result_total[str(line.split(',')[0])+'-'+str(line.split(',')[1])] = line.split(',')[2].strip('\r\n')
                f.close()
        if os.path.isfile(file_path+create_time+'/'+file_id+'.csv') is True:
            pass
        else:
            os.mknod(file_path+create_time+'/'+file_id+'.csv')

        if os.path.isfile(file_path+create_time+'/'+file_id+'total.csv') is True:
            pass
        else:
            os.mknod(file_path+create_time+'/'+file_id+'total.csv')
        with open(file_path+create_time+'/'+file_id+'.csv', 'w') as fout:
            for key, value in result.items():
                line = key.split('-')[0] + ',' + key.split('-')[1] + ',' + str(int(value)/files_num) + '\n'
                fout.write(line)
        fout.close()
        log.info('>>>>>>>>>>>>>>>>>>>>>>>%s' % result_total)
        with open(file_path+create_time+'/'+file_id+'total.csv', 'w') as tout:
            for key, value in result_total.items():
                line = key.split('-')[0] + ',' + key.split('-')[1] + ',' + value + '\n'
                tout.write(line)
        tout.close()
        log.info('write the file over')
        return True

    # 将发现的nodes数量和relationship存入mysql
    def storage_nums_node_and_rel(self, file_id):
        try:
            if self.combine_all_files(file_id) is False:
                return False
        except Exception, e:
            log.error('expalin the file error. reason is: %s' % e)
            raise Exception(e)
        nodes_list = []
        f = open('/home/cmcc/graph/pyapp/algorithm/lpa.txt', 'r')
        detail = f.readlines()
        relations = len(detail)
        for line in detail:
            node1 = line.split(',')[0]
            node2 = line.split(',')[1]
            if node1 not in nodes_list:
                nodes_list.append(node1)
            if node2 not in nodes_list:
                nodes_list.append(node2)
        f.close()
        nodes = len(nodes_list)
        log.info('explain the file ,get the relations is: %d, nodes is: %d' % (relations, nodes))
        # neo4j实现过程
        # try:
        #     relations = self.neo4j_get_relationships()
        #     nodes = self.neo4j_get_nodes()
        #     log.info('get the relations is: %s and nodes is: %s' % (relations,
        #                                                             nodes))
        #     # relations_num = relations[0].get('row')[1]
        # except Exception, e:
        #     log.error('explain the data failed, reason is: %s' % e)
        #     raise Exception(e)

        # 存入mysql数据库
        try:
            self.base_message.update_nums_nodes_relations(nodes, relations, file_id)
        except Exception, e:
            log.error('update the mysql db error, reason is: %s' % e)
            raise Exception(e)

        return True

    def source_message(self):
        ret = dict()
        try:
            result = self.base_message.get_resource_message()
            log.info('from database get the result message is: %s' % result)
            for i in result:
                ret['title'] = i[0]
                ret['upload_name'] = i[1]
                ret['local_name'] = i[2]
                ret['data_total'] = i[3]
                ret['data_nodes'] = i[4]
                ret['data_relations'] = i[5]
                ret['create_time'] = self.base_message.time_diff(i[6])
            return ret
        except Exception, e:
            log.error('get the message error, reason is: %s' % e)
            raise Exception(e)

    # 调取社团划分算法
    def community_ab(self):
        json_inner = {'nodes': [], 'links': []}
        try:
            self.neo4j_al.neo4j_community()
        except Exception, e:
            log.error('community the graph error, reason is: %s' % e)
            raise Exception(e)

        try:
            # 获取划分社团后的社团分组
            result = self.neo4j_al.get_all_community()
            # log.info('from neo4j get the community result is:%s' % result)
            # 查找关系属性
            rel_ret = self.neo4j_message.execute_rel_detail()
        except Exception, e:
            log.error('get the community message error, reason is: %s' % e)
            raise Exception(e)
        log.info('++++++++++++++++++++++++:%s' % rel_ret)
        if len(result.get('errors')) != 0 or len(rel_ret.get('errors')) != 0:
            raise Exception('get the community error')
        # 构建json串
        for i in result.get('results')[0].get('data'):
            ret = i.get('row')
            group = ret[0]  # int
            community = ret[1]  # list
            for c in community:
                nodes = {'id': c, 'group': group}
                json_inner['nodes'].append(nodes)

        for r in rel_ret.get('results')[0].get('data'):
            ret = r.get('row')
            from_id = ret[0].get('id')
            relationship = ret[1].get('property1')
            to_id = ret[2].get('id')
            links = {"source": from_id, "value": "abc",
                     "target": to_id, "relation": relationship}
            json_inner['links'].append(links)

        log.info('the json will into json file is: %s' % json_inner)

        # 将json串存入json文件
        with open('C:\Users\hp\Desktop\hat.json', 'w') as f:
            f.write(str(json_inner))
        return result

    def delete_all(self, file_id):
        try:
            db_result = self.base_message.get_create_time(file_id)
        except Exception, e: 
            log.error('get the create time form db error, reason is: %s' % e)
            return request_result(403)
        create_time = self.base_message.time_diff(db_result[0][0]).split(' ')[0]
        # 获取文件夹下的问价列表
        file_list = os.listdir(file_path+create_time)
        for file in file_list:
            if file_id in file:
                os.remove(file_path+create_time+'/'+file)
        try:
            self.base_message.delete_all(file_id)
        except Exception, e:
            log.error('delete the database file error, reason is: %s' % e)
            raise Exception(e)
