# -*- coding: UTF-8 -*-
import os
import networkx as nx
import time
import datetime
from logs import logging as log
from pyspark import SparkConf, SparkContext, SQLContext
nodes_list = []
edges_list = []

def load_data(liness):
    # f = open(filePath)
    vector_dict = {}  # 导航字典
    edge_dict = {}
    for line in liness.split(' '):
        lines = line.strip().split(",")  # 根据换行符分割内容,最后让每一行的内容都变成数组 [1,2,3]
        for i in xrange(2):  # 0,1
            if lines[i] not in vector_dict:  # 如果被检测行不在vector_dict里, 数组前两个为nodes，第三个为关系
                #put the vector into the vector_dict
                vector_dict[lines[i]] = True  # 那么在vector_dict里添加文件内容
                #put the edges into the edge_dict
                edge_list = []
                if len(lines) == 3:
                    edge_list.append(lines[1-i]+":"+lines[2]) # 如果每一行有三个元素，那么就在边的信息里添加：
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
    # 最终结果，把nodes放到一个dict里，键就是nodes的名字，值就是True
    # 最终结果，把edge放到一个dict里，键就是nodes的名字，值就是与这个nodes有关系的relation（无向）
    return [vector_dict, edge_dict]

def init_graph(vector_dict, edge_dict):
    for k, v in vector_dict.items():
        nodes_list.append(k)
    for k, v in edge_dict.items():
        for relation in v:
            edges_list.append((k, relation.split(':')[0], 1))
    return nodes_list, edges_list

def graph_build(a, conn):
    vector_dict = a[0]
    edge_dict = a[1]
    nodes_list, edges_list = init_graph(vector_dict, edge_dict)
    G = nx.Graph()
    for node in nodes_list:
        G.add_node(node)

    # G.add_edges_from(edges_list)
    G.add_weighted_edges_from(edges_list)
    # print ">>>>>>>>>G>>>>>>>>"
    # print "nodes: ", G.number_of_nodes()
    # print "edges: ", G.number_of_edges()
    result = BGLL(G, conn)
    return result


def calculateQ(IN, TOT, m):
    Q =IN/m-((TOT/(2*m))**2)
    return Q

def phases_one(G):
    ##用来存社区编号和其节点
    community_dict={}
    ##用来存节点所对应的社区编号
    community_node_in_dict={}
    ##用来保存边两头节点所在社区
    edge_dict={}
    ##存Tot和In
    Tot={}
    In={}

    ##初始化上述指标
    for node in G.nodes():
        community_dict[node]=[node]
        community_node_in_dict[node]=node
        #初始化Tot
        T=0.
        for nbrs in G.neighbors(node):
            if node==nbrs:
                T += (2*G.get_edge_data(nbrs, node).values()[0])
            else:
                T+=G.get_edge_data(nbrs,node).values()[0]
        Tot[node]=T
        #初始化In
        if G.get_edge_data(node,node)==None:
            In[node]=0.
        else:
            In[node]=G.get_edge_data(node,node).values()[0]

    #初始化M
    M=0.0
    for edge in G.edges():
        M+=G.get_edge_data(*edge).values()[0]
        edge_dict[(edge[0],edge[1])]=(community_node_in_dict[edge[0]],community_node_in_dict[edge[1]])
    index=True
    ##一直遍历直到收敛
    while index==True:
        index=False
        ##遍历所有节点
        for node in G.nodes():
            ##保存节点之前所在社区
            old_community=community_node_in_dict[node]
            # 用来保存节点node的总权值
            ki = 0
            # 保存其所有邻居社区
            nbrs_community={}
            # 保存该节点移动到某社区所带来的In增益
            kiin_dict={}
            # 保存其离开自己所在社区的的In减少量
            kiout=0.
            # 目标社区
            max_community=-1
            max_detaQ=-1
            max_nbrs=-1
            # 临时保存edge_dict变化
            edge_dict_tmp={}
            for nbrs in G.neighbors(node):
                weight=G.get_edge_data(node,nbrs).values()[0]
                if nbrs==node:
                    ki+=(2*weight)
                else:
                    ki+=weight
                current_community=community_node_in_dict[nbrs]
                if current_community==old_community:
                    kiout+=weight
                    continue

                if nbrs_community.has_key(current_community):
                    kiin_dict[current_community]+=weight
                else :
                    nbrs_community[current_community]=current_community
                    kiin_dict[current_community]=weight
                    if G.has_edge(node, node):
                        kiin_dict[current_community] += G.get_edge_data(node, node).values()[0]
            #计算它离开自己社区的detaQ
            detaQ1=calculateQ(In[old_community]-kiout,Tot[old_community]-ki,M)-calculateQ(In[old_community],Tot[old_community],M)
            #计算将要加入的社区的detaQ
            for com in nbrs_community:
                detaQ2=calculateQ(In[com]+kiin_dict[com],Tot[com]+ki,M)-calculateQ(In[com],Tot[com],M)
                Q=detaQ2+detaQ1
                if Q>max_detaQ and Q>0:
                    max_detaQ=Q
                    max_community=com
                    max_nbrs=nbrs
            #如果该社区使网络模块度增加，则加进去
            if max_detaQ>0:
                index=True
                #更新节点社区编号
                community_node_in_dict[node]=max_community
                #更新所去的社区的In和Tot值
                In[max_community]=In[max_community] + kiin_dict[max_community]
                Tot[max_community]=Tot[max_community]+ki
                #更新原先的社区值
                In[old_community]=In[old_community]-kiout
                Tot[old_community]=Tot[old_community]-ki
                #更新社区
                community_dict[max_community].append(node)
                community_dict[old_community].remove(node)
                #原社区没有节点了就删除
                if community_dict[old_community].__len__()==0:
                    community_dict.__delitem__(old_community)
                    In.__delitem__(old_community)
                    Tot.__delitem__(old_community)
                #更新edge
                if edge_dict.has_key((node,max_nbrs)):
                    edge_dict[(node,max_nbrs)]=(max_community,max_community)
                else:
                    edge_dict[(max_nbrs, node)] = (max_community, max_community)

    nowQ = 0
    for com in community_dict:
        nowQ+=calculateQ(In[com], Tot[com] , M)
    return G,community_node_in_dict,community_dict,nowQ,Tot,In



def phase_two(now_G,community_node_dict):
    super_G=nx.Graph()
    super_edge={}
    for edge in now_G.edges():
        super_node1=community_node_dict[edge[0]]
        super_node2=community_node_dict[edge[1]]
        if (super_node1,super_node2) in super_edge or (super_node2,super_node1) in super_edge:
            if super_node1!=super_node2:
                super_edge[(super_node1,super_node2)]+=now_G.get_edge_data(*edge).values()[0]
                super_edge[(super_node2, super_node1)] += now_G.get_edge_data(*edge).values()[0]
            else:
                super_edge[(super_node1, super_node1)] += now_G.get_edge_data(*edge).values()[0]
        else:
            super_edge[(super_node1, super_node2)] = now_G.get_edge_data(*edge).values()[0]
            super_edge[(super_node2, super_node1)] = now_G.get_edge_data(*edge).values()[0]

    for edge in super_edge:
        if super_G.has_edge(*edge):
            continue
        super_G.add_edge(*edge,weight=super_edge[edge])
    for node in now_G.nodes():
        if now_G.degree(node)==0:
            super_G.add_node(node)
    return super_G

def merge_community_node(next_community, tmp_community, community_node_in_dict):
    communitys = {}
    for i in next_community.keys():
        communitys[i] = []
        for node in next_community[i]:
            for n in tmp_community[node]:
                community_node_in_dict[n]=i
                communitys[i].append(n)
    return communitys


def BGLL(a, sc, conn):
    vector_dict = a[0][0]
    edge_dict = a[0][1]
    nodes_list, edges_list = init_graph(vector_dict, edge_dict)
    G = nx.Graph()
    for node in nodes_list:
        G.add_node(node)

    # G.add_edges_from(edges_list)
    G.add_weighted_edges_from(edges_list)
    ##进行第一次迭代
    # t1 = time.clock()
    now_G, community_node_dict, now_community, now_Q, now_Tot, now_In = phases_one(G)
    community_node_in_dict = community_node_dict
    # print "迭代第一次时间：", time.clock() - t1
    # print "社区总数：", now_community.__len__()
    tmp_community = now_community
    # print "Q:", now_Q, '\n'
    times = 1
    while True:
        # times += 1
        ##根据now_edge_dict形成超级节点图
        t2 = time.clock()
        super_G = phase_two(now_G, community_node_dict)
        # print "第", times, "个图生成的时间:", time.clock() - t2
        ##用超级节点形成的图去迭代
        t2 = time.clock()
        now_G, community_node_dict, now_community, next_Q, next_Tot, next_In = phases_one(super_G)
        print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        print "迭代第", times, "次的时间：", time.clock() - t2
        print "社区总数：", now_community.__len__()
        send_message(conn, 'already finished %d,the num of community is: %d' % (times, now_community.__len__()))
        ##如果当前社区的nextQ没有增加就返回
        # print "Q:", next_Q, '\n'
        if next_Q <= now_Q:
            break
        now_Q = next_Q
        now_Tot, now_In = next_Tot, next_In
        tmp_community = merge_community_node(now_community, tmp_community, community_node_in_dict)
        times += 1
    if os.path.exists('fast_unfolding_num'):
        return
    nodes = []
    links = []
    n = 1
    for k,v in tmp_community.items():
        for i in v:
            nodes.append({'id': i, 'group': n})
        n += 1
    for k,v in edge_dict.items():
        links.append({'source': k, 'target': v[0].split(':')[0], 'relation': v[0].split(':')[1]})
    alls = [nodes, links]
    newfiledata = sc.parallelize(alls)
    # coalesce(1, True)是为了将不同的task执行的文件集中保存为一个文件
    newfiledata.coalesce(1, True).saveAsTextFile('fast_unfolding_num')
    # return tmp_community,community_node_in_dict


def send_message(conn, message):
    # global connectionlist
    message_utf_8 = message.encode('utf-8')
    # log.info('connectlist array is: %s' % conn)
    # for connection in connectionlist.values():
    back_str = []
    back_str.append('\x81')
    data_length = len(message_utf_8)

    if data_length <= 125:
        back_str.append(chr(data_length))
    elif data_length <= 65535:
        back_str.append(struct.pack('b', 126))
        back_str.append(struct.pack('>h', data_length))
        # back_str.append(chr(data_length >> 8))
        # back_str.append(chr(data_length & 0xFF))
        # a = struct.pack('>h', data_length)
        # b = chr(data_length >> 8)
        # c = chr(data_length & 0xFF)
    elif data_length <= (2^64-1):
        # back_str.append(chr(127))
        back_str.append(struct.pack('b', 127))
        back_str.append(struct.pack('>q', data_length))
        # back_str.append(chr(data_length >> 8))
        # back_str.append(chr(data_length & 0xFF))
    else:
        print ('太长了')
    msg = ''
    for c in back_str:
        msg += c
    back_str = str(msg) + message_utf_8  # encode('utf-8')
    # connection.send(str.encode(str(u"\x00%s\xFF\n\n" % message))) #这个是旧版
    # print (u'send message:' +  message)
    if back_str is not None and len(back_str) > 0:
        print (back_str)
        try:
            conn.send(back_str)
        except Exception, e:
            log.error('send error, reason is: %s' % e)

def unfold(conn):
    send_message(conn, 'prepare spark env...')
    conf = SparkConf().setAppName('textfile_dir')
    sc = SparkContext(conf=conf)
    send_message(conn, 'spark env is ready!')
    starttime = datetime.datetime.now()
    send_message(conn, 'load files...')
    textfile = sc.textFile("lpa.txt")
    liness = ' '.join(textfile.collect())
    a = []
    a.append(liness)
    distData = sc.parallelize(a)
    a = distData.map(lambda x: load_data(x)).collect()
    send_message(conn, "file loads completed!")
    send_message(conn, "fast-unfolding begin!")
    distData = sc.parallelize(a)
    # result = distData.map(lambda x: graph_build(x)).collect()
    result = distData.map(BGLL(a, sc, conn))
    send_message(conn, "fast-unfolding completed!")
    sc.stop()
    endtime = datetime.datetime.now()
    # print 'all use time: ', (endtime-starttime).seconds
    send_message(conn, "use time: %d seconds" % (endtime - starttime).seconds)
    # print result
