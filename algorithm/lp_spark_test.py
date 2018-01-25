# -*- coding: utf-8 -*-
# Author: bill-jack<xfwangaw@isoftstone.com>
import os
from logs import logging as log
import string
import datetime
from db_redis import connect
from pyspark import SparkConf, SparkContext, SQLContext


def send_message(conn, message):
    # global connectionlist
    message_utf_8 = message.encode('utf-8')
    log.info('connectlist array is: %s' % conn)
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

vector_dict = {}
edge_dict = {}
def loadData(liness):
    global vector_dict
    for line in liness.split(' '):
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
    return [vector_dict,edge_dict]


def get_max_community_label(vector_dict, adjacency_node_list):
    label_dict = {}
    for node in adjacency_node_list:
        node_id_weight = node.strip().split(":")
        node_id = node_id_weight[0]
        node_weight = string.atoi(node_id_weight[1])
        if vector_dict[node_id] not in label_dict:
            label_dict[vector_dict[node_id]] = node_weight
        else:
            label_dict[vector_dict[node_id]] += node_weight
    # find the max label
    sort_list = sorted(label_dict.items(), key = lambda d: d[1], reverse=True)
    
    return sort_list[0][0]

def check(vector_dict, edge_dict):
    for node in vector_dict.keys():
        adjacency_node_list = edge_dict[node]
        node_label = vector_dict[node]
        label_check = {}
        for ad_node in adjacency_node_list:
            node_id_weight = ad_node.strip().split(":")
            node_id = node_id_weight[0]
            if vector_dict[node_id] not in label_check:
                label_check[vector_dict[node_id]] = 1
            else:
                label_check[vector_dict[node_id]] += 1

        sort_list = sorted(label_check.items(), key = lambda d: d[1], reverse=True)

        if node_label == sort_list[0][0]:
            continue
        else:
            return 0

    return 1

def label_propagation(res, sc, conn):
    result = {}
    vector_dict = res[0][0]
    edge_dict = res[0][1]
    t = 0
    n = 0
    r = connect()
    while True:
        checks = check(vector_dict, edge_dict)
        if (checks == 0):
            t = t+1
            for node in vector_dict.keys():
                adjacency_node_list = edge_dict[node]
                vector_dict[node] = get_max_community_label(vector_dict, adjacency_node_list)
            n += 1
            for k, v in vector_dict.items():
                if v not in result.keys():
                    result[v]=[k]
                else:
                    result[v].append(k)
            send_message(conn, 'already finished %d,the num of community is: %d ' % (n, len(result.keys())))
            print '已完成第%d次计算周边最丰富标签并完成检查工作...' % n
        else:
            for k, v in vector_dict.items():
                if v not in result.keys():
                    result[v]=[k]
                else:
                    result[v].append(k)
            send_message(conn, 'the endle num of community is: %d' % len(result.keys()))
            print '已完成对所有数据的lpa算法分析，共经历%d步' % n
            break
        result = {}
    if os.path.exists('lpa_num'):
        return
    nodes = []
    links = []
    n = 1
    for k,v in result.items():
        for i in v:
            nodes.append({'id': i, 'group': n})
        n += 1
    for k,v in edge_dict.items():
        links.append({'source': k, 'target': v[0].split(':')[0], 'relation': v[0].split(':')[1]})
    alls = [nodes, links]
    newfiledata = sc.parallelize(alls)
    # coalesce(1, True)是为了将不同的task执行的文件集中保存为一个文件
    newfiledata.coalesce(1, True).saveAsTextFile('lpa_num')
    return result

def test(conn):
    send_message(conn, 'prepare spark env...')
    conf = SparkConf().setAppName('lpa')
    sc = SparkContext(conf=conf)
    send_message(conn, 'spark env is ready!')
    starttime = datetime.datetime.now()
    send_message(conn, "load files...")
    textfile = sc.textFile("lpa.txt")
    liness = ' '.join(textfile.collect())
    a = []
    a.append(liness)
    distData = sc.parallelize(a)
    a = distData.map(lambda x: loadData(x)).collect()
    send_message(conn, "file loads completed!")
    send_message(conn, "lpa begin!")
    distData = sc.parallelize(a)
    #result = distData.map(lambda x: label_propagation(x)).collect()
    result = distData.map(label_propagation(a, sc, conn))
    # print "all commucity num is ", len(result.keys())
    print "all commucity num is:", result.reduceByKey("0")
    endtime = datetime.datetime.now()
    #send_message(conn, "共划a社区数: %d,用时：%d秒" % (len(result[0].keys()), (endtime - starttime).seconds))
    send_message(conn, "lpa completed!")
    # send_message(conn, ("all commucity num is:" , result.reduceByKey(0)))
    send_message(conn, "use time: %d seconds" % (endtime - starttime).seconds)
    sc.stop()
    #conn.close()


if __name__ == "__main__":
    test()
    # t = WebSocketServer()
    # t.begin()
