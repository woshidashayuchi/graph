# -*- coding: utf-8 -*-
# Author: bill-jack<xfwangaw@isoftstone.com>
import sys
path1 = sys.path[0] + '/..'
sys.path.append(path1)
import os
import networkx as nx
from database.quota_db import QuotaDB
from database.mysql_db import BaseMessageDB
from common.logs import logging as log
from common.conf import file_path as import_file_path
from common.conf import base_img_dir
import string
import community
# import matplotlib.pyplot as plt


vector_dict = {}
edge_dict = {}
nodes_list = []
edges_list = []

def init_graph(file_path):
    # conf = SparkConf().setAppName('lpa')
    # sc = SparkContext(conf=conf)
    # textfile = sc.textFile(file_path)
    # liness = ' '.join(textfile.collect())
    
    # a = []
    # a.append(liness)
    # distData = sc.parallelize(a)
    # a = distData.map(lambda x: loadData(x)).collect()
    f = open(file_path)
    lines = f.readlines()
    a = loadData(lines)
    nodes = a[0]
    edges = a[1]
    for k, v in nodes.items():
        nodes_list.append(k)
    for k, v in edges.items():
        for relation in v:
            edges_list.append((k, relation.split(':')[0]))
    return nodes_list, edges_list


def loadData(liness):
    global vector_dict
    for line in liness:
        lines = line.strip().split(",")
        for i in xrange(2):
            if lines[i] not in vector_dict:
                vector_dict[lines[i]] = string.atoi(lines[i])
                edge_list = []
                if len(lines) == 3:
                    edge_list.append(lines[1-i]+":"+lines[2])
                else:
                    edge_list.append(lines[1-i]+":"+"1")
                edge_dict[lines[i]] = edge_list
            else:
                edge_list = edge_dict[lines[i]]
                if len(lines) == 3:
                    edge_list.append(lines[1-i]+":"+lines[2])
                else:
                    edge_list.append(lines[1-i]+":"+"1")
                edge_dict[lines[i]] = edge_list
    # 这样处理完成后，节点就没有重复项了
    return [vector_dict, edge_dict]


def nx_graph(file_path):
    nodes_list, edges_list = init_graph(file_path)
    G = nx.Graph()
    for node in nodes_list:
        G.add_node(node)
    for edge in edges_list:
        G.add_edge(edge[0], edge[1])
    return G

def main():
    quota = QuotaManager()
    # ave_degree = quota.ave_degree('/home/cmcc/graph/tools/neo4j-community-3.2.3/import/programe/lpa.txt')
    # print '>>>>>>>平均度>>>>>>>>>'
    # print ave_degree
    
    # diam = quota.diameter('lpa.txt')
    # print '>>>>>>>直径>>>>>>>>>>'
    # print diam
    
    # density = quota.density('lpa.txt')
    # print '>>>>>>>>图密度>>>>>>>>>'
    # print density
    
    # md = quota.modular('lpa.txt')
    # print '>>>>>>>>模块度>>>>>>>>>'
    # print md
    
    # 需要作图:见neo_test
    # pr = quota.pagerank('lpa.txt')
    # print '>>>>>>>>>pagerank>>>>>>'
    # print pr
    
    # 需要作图?
    # cl = quota.clustering('lpa.txt')
    # print '>>>>>>>>>>聚类系数>>>>>>'
    # print cl
    
    # apl = quota.ave_path_len('lpa.txt')
    # print '>>>>>>>>>平均长度>>>>>>>'
    # print apl
    
    result = quota.all_quotas('None')
    print result


def draw_img(x, y, img_name):
    try:
        img_dir = base_img_dir + img_name
        plt.loglog(x, y, color="blue", linewidth=2)  # 在双对数坐标轴上绘制度分布曲线
        plt.savefig(os.path.join(img_dir))
    except Exception, e:
        log.error('drow photo error, reason is: %s' % e)
        raise Exception(e)
    

class QuotaManager(object):
    def __init__(self):
        self.db = QuotaDB()
        self.base_message = BaseMessageDB()
    
    def c_file_path(self, file_id):
        try:
            db_result = self.base_message.get_create_time(file_id)
        except Exception, e: 
            log.error('get the create time form db error, reason is: %s' % e)
            raise Exception(e)
        create_time = self.base_message.time_diff(db_result[0][0]).split(' ')[0]
        # 获取文件夹下的问价列表
        file_list = os.listdir(import_file_path+create_time)
        for file in file_list:
            if file_id+'.csv' == file:
                return import_file_path+create_time+'/'+file_id+'.csv'

    # 平均度
    def ave_degree(self, file_id):
        file_path = self.c_file_path(file_id)
        G = nx_graph(file_path)
        # 度分布
        degree = nx.degree_histogram(G)
        img_name = '%s-degree.png' % file_id
        img_dir = base_img_dir + img_name
        # 保存图片
        # x = range(len(degree))  # 生成x轴序列，从1到最大度
        # y = [z / float(sum(degree)) for z in degree]  # 将频次转换为频率
        # draw_img(x, y, img_name)
        # 平均度计算
        s_sum = 0
        for i in range(len(degree)):
            s_sum =s_sum+i*degree[i]
        result = s_sum/nx.number_of_nodes(G)
        data_dict = {'file_id': file_id, 'q_name': 'ave_degree', 'q_value': result, 'q_img': img_dir, 'q_label': 1}
        try:
            self.db.add_quota(data_dict)
        except Exception, e:
            raise Exception(e)
        
        return result
    
    # 直径:很耗时(2*60)
    def diameter(self, file_id):
        file_path = self.c_file_path(file_id)
        G = nx_graph(file_path)
        result = nx.diameter(G)
        log.info('>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        log.info(result)
        log.info(type(result))
        data_dict = {'file_id': file_id, 'q_name': 'diameter', 'q_value': 'ok', 'q_label': 1}
        try:
            self.db.add_quota(data_dict)
        except Exception, e:
            raise Exception(e)
        
        return result
    
    # 图密度
    def density(self, file_id):
        file_path = self.c_file_path(file_id)
        G = nx_graph(file_path)
        result =  nx.degree(G)
        data_dict = {'file_id': file_id, 'q_name': 'density', 'q_value': 'ok', 'q_label': 1}
        try:
            self.db.add_quota(data_dict)
        except Exception, e:
            raise Exception(e)
        return result

    # 模块度
    def modular(self, file_id):
        file_path = self.c_file_path(file_id)
        G = nx_graph(file_path)
        part = community.best_partition(G)
        result = community.modularity(part, G)
        data_dict = {'file_id': file_id, 'q_name': 'modular', 'q_value': result, 'q_label': 1}
        try:
            self.db.add_quota(data_dict)
        except Exception, e:
            raise Exception(e)
        return result
    
    # pagerank
    def pagerank(self, file_id):
        file_path = self.c_file_path(file_id)
        G = nx_graph(file_path)
        result = nx.pagerank(G)
        
        img_name = '%s-pagerank.png'
        img_dir = base_img_dir + img_name
        # x_len = range(len(pr.keys()))  # 计算x轴长度
        # y_values = pr.values()
        # draw_img(hub_x_len, hub_y_values, img_name)
        data_dict = {'file_id': file_id, 'q_name': 'pagerank', 'q_value': 'ok', 'q_img': img_dir, 'q_label': 1}
        try:
            self.db.add_quota(data_dict)
        except Exception, e:
            raise Exception(e)
        return result

    # 全网聚类系数
    def clustering(self, file_id):
        file_path = self.c_file_path(file_id)
        G = nx_graph(file_path)
        result = nx.clustering(G)
        data_dict = {'file_id': file_id, 'q_name': 'clustering', 'q_value': 'ok', 'q_label': 1}
        try:
            self.db.add_quota(data_dict)
        except Exception, e:
            raise Exception(e)
        return result
    
    # 平均路径长度    
    def ave_path_len(self, file_id):
        file_path = self.c_file_path(file_id)
        pathlengths = []
        G = nx_graph(file_path)
        for v in G.nodes():  
            spl=nx.single_source_shortest_path_length(G,v)  
            for p in spl.values():  
                pathlengths.append(p)
        result = sum(pathlengths)/len(pathlengths)
        data_dict = {'file_id': file_id, 'q_name': 'ave_path_len', 'q_value': result, 'q_label': 1}
        try:
            self.db.add_quota(data_dict)
        except Exception, e:
            raise Exception(e)
        return result
    
    # HITS
    def pagerank_hits(self, file_id):
        file_path = self.c_file_path(file_id)
        G = nx_graph(file_path)
        hub, auth = nx.hits(G)
        img_name1 = '%s-hits-hub.png' % file_id
        img_name2 = '%s-hits-auth.png' % file_id
        
        img_dir1 = base_img_dir + img_name1
        img_dir2 = base_img_dir + img_name2
        img_dir = str([img_dir1, img_dir2])

        # 保存图片
        # hub_x_len = range(len(hub.keys()))  # 计算x轴长度
        # hub_y_values = hub.values()
        # draw_img(hub_x_len, hub_y_values, img_name1)
        
        # auth_x_len = range(len(auth.keys()))
        # auth_y_values = auth.values()
        # draw_img(auth_x_len, auth_y_values, img_name2)

        data_dict = {'file_id': file_id, 'q_name': 'hits', 'q_value': 'ok', 'q_img': img_dir, 'q_label': 1}
        try:
            self.db.add_quota(data_dict)
        except Exception, e:
            raise Exception(e)

        return 'ok'
        # for h in sorted(hub.items(), key=lambda x: x[1], reverse=True)[:100]:
        #     print h[0], h[1]
        # print '\nauth top 100:\n'
        # for a in sorted(auth.items(), key=lambda x: x[1], reverse=True)[:100]:
        #     print a[0], a[1]

    # 特征向量中心
    def eigenvector(self, file_id):
        file_path = self.c_file_path(file_id)
        G = nx_graph(file_path)
        eig = nx.eigenvector_centrality(G)
        data_dict = {'file_id': file_id, 'q_name': 'eigenvector', 'q_value': 'ok', 'q_label': 1}
        try:
            self.db.add_quota(data_dict)
        except Exception, e:
            raise Exception(e)

        return 'ok'
        # eigs = [(v,k) for k,v in eig.iteritems()]
        # eigs.sort()
        # eigs.reverse()
        # print eigs[1]
    
    def all_quotas(self, file_id):
        result = []
        try:
            resu = self.db.all_quotas(file_id)
        except Exception, e:
            log.error('get the all quotas error, reason is: %s' % e)
            raise Exception(e)
        n = 0
        print ">>>>>>>>>>>>>>>>>>>"
        print resu
        for i in resu:
            ret = dict()
            print n
            ret['id'] = resu[n][0]
            ret['file_id'] = resu[n][1]
            ret['q_name'] = resu[n][2]
            ret['q_value'] = resu[n][3]
            ret['q_label'] = resu[n][4]
            ret['q_img'] = resu[n][5]
            result.append(ret)
            n += 1
            if n == len(resu):
                break;
        
        return result

if __name__ == '__main__':
    main()
