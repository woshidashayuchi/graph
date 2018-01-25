# -*- coding: utf-8 -*-
# Author: bill-jack<xfwangaw@isoftstone.com>
from common.logs import logging as log
# from py2neo import Graph, Node, Relationship
import base64
from common import conf
import requests
import json
from py2neo import Graph
#from igraph import Graph as IGraph


class Neo4jBase(object):
    def __init__(self):
        self.neo4j_auth = base64.b64encode(conf.neo4j_auth)
        self.url = conf.neo4j_url
        self.header = {"Authorization": self.neo4j_auth}

    @staticmethod
    def cql_structure(cql_type):
        statements = ""
        if cql_type == 'nodes':
            statements = "match (n) return count(n)"
        if cql_type == 'rel':
            statements = "match (m)-[r]->(n)  return type(r), count(*)"
        cql = {
            "statements": [
                  {
                      "statement": "%s" % statements
                  }
            ]
        }
        return cql

    def cql_execute(self, dict_data):
        try:
            result = json.loads(requests.post(self.url, json.dumps(dict_data),
                                              headers=self.header,
                                              timeout=5).text)
            log.info('get the nodes result is:%s,type is:%s' % (result,
                                                                type(result)))

        except Exception, e:
            log.error('request the neo4j api error, reason is: %s' % e)
            raise Exception(e)
        log.info('get all nodes result is: %s' % result)

        return result

    # 传入文件路径与文件类型（是作为关系文件还是节点文件）即可进行创建创建操作
    def execute_create_cql(self, file_position, file_type):
        if file_type == 'node':
            dict_data = {
                "statements": [
                    {
                        "statement": "LOAD CSV WITH HEADERS  "
                                     "FROM \"file:///%s\" AS line  "
                                     "CREATE (p:person{id:line.id,"
                                     "name:line.name,age:line.age,"
                                     "sex:line.sex})" % file_position
                    },
                ]
            }

            result = json.loads(requests.post(self.url, json.dumps(dict_data),
                                              headers=self.header,
                                              timeout=5).text)
            log.info('inner the neo4j, the result is: %s' % result)
            if len(result.get('errors')) != 0:
                raise Exception('neo4j operator exception')

            return result

        if file_type == 'relationship':
            dict_data = {
                "statements": [
                    {
                        "statement": "LOAD CSV WITH HEADERS FROM "
                                     "\"file:///%s\" AS line "
                                     "match (from:person{id:line.from_id}),"
                                     "(to:person{id:line.to_id}) "
                                     "merge "
                                     "(from)-[r:rel{property1:line.property1,"
                                     "property2:line.property2}]->(to)" % file_position
                    },
                ]
            }

            result = json.loads(requests.post(self.url, json.dumps(dict_data),
                                              headers=self.header,
                                              timeout=5).text)
            log.info('inner the neo4j, the result is: %s' % result)
            if len(result.get('errors')) != 0:
                raise Exception('neo4j operator exception')
            return result

        else:
            raise Exception('neo4j operator happened exception')

    # 查询节点总数
    def execute_nodes_cql(self):
        dict_data = self.cql_structure('nodes')
        try:
            result = json.loads(requests.post(self.url, json.dumps(dict_data),
                                              headers=self.header,
                                              timeout=5).text)
            log.info('get the nodes result is:%s,type is:%s' % (result,
                                                                type(result)))

        except Exception, e:
            log.error('request the neo4j api error, reason is: %s' % e)
            raise Exception(e)
        log.info('get all nodes result is: %s' % result)

        return result

    # 查询关系总数
    def execute_rel_cql(self):
        dict_data = self.cql_structure('rel')
        try:
            result = self.cql_execute(dict_data)
        except Exception, e:
            log.error('execute the cql error, reason is: %s' % e)
            raise Exception(e)

        return result

    def execute_rel_detail(self):
        dict_data = {"statements": [
                    {
                        "statement": "match (p1:person)-[r:rel]->(p2:person) "
                                     "return p1,r,p2"
                    },
                ]}
        try:
            result = self.cql_execute(dict_data)
        except Exception, e:
            log.error('execute the cql error, reason is: %s' % e)
            raise Exception(e)
        return result

    # 两个节点之间的路径
    def rel_route(self, from_id, to_id):
        cql = {
            "statements": [
                {
                    "statement": "MATCH path = (n)-[*]->(m) "
                                 "WHERE id(n)=%d and id(m)=%d "
                                 "RETURN count(path)" % (int(from_id),
                                                         int(to_id))
                    #"statement": "MATCH path = (n:person)-[:rel]->(m:person) "
                    #             "WHERE n.uid='%d' and m.uid='%d' "
                    #             "RETURN count(path)" % (int(from_id),
                    #                                     int(to_id))
                }
            ]
        }
        try:
            result = self.cql_execute(cql)
        except Exception, e:
            log.error('execute the cql error, reason is: %s' % e)
            raise Exception(e)

        return result

    # 获取最短路径
    def shortest_rel_route(self, from_id, to_id):
        cql = {
            "statements": [
                {
                    "statement": "START d=node(%d), e=node(%d)"
                    "MATCH p=allShortestPaths((d)-[*1..]->(e))"
                    "RETURN p" % (int(from_id), int(to_id))
                    # "statement": "MATCH p = shortestPath((n:person)-[*1..]->(m:person)) " \
                    #             "WHERE n.uid='%d' and m.uid='%d' " \
                    #             "RETURN p" % (int(from_id), int(to_id))
                }
            ]
        }
        try:
            result = self.cql_execute(cql)
        except Exception, e:
            log.error('execute the cql error, reason is: %s' % e)
            raise Exception(e)

        return result


class Neo4jAlgorithm(object):
    def __init__(self):
        self.neo4j = Neo4jBase()
        self.graph = Graph(
            conf.neo4j_url_py2neo,
            username=conf.neo4j_username_py2neo,
            password=conf.neo4j_pwd_py2neo)

    # 利用随机游走算法实现社团发现（node上标注community）
    def neo4j_community(self):
        query = '''
            MATCH (c1:person)-[r:rel]->(c2:person)
            RETURN c1.name, c2.name, r.property1 AS weight
            '''
        try:
            ig = IGraph.TupleList(self.graph.run(query), weights=True)
            log.info('ig is: type(%s)' % type(ig))
            clusters = IGraph.community_walktrap(ig,
                                                 weights=None).as_clustering()
        except Exception, e:
            log.error('error.....:%s' % e)
            raise Exception(e)

        try:
            nodes = [{"name": node["name"]} for node in ig.vs]
            for node in nodes:
                idx = ig.vs.find(name=node["name"]).index
                node["community"] = clusters.membership[idx]
            write_clusters_query = '''
                UNWIND {nodes} AS n
                MATCH (c:person) WHERE c.name = n.name
                SET c.community = toInt(n.community)
                '''
            self.graph.run(write_clusters_query, nodes=nodes)
        except Exception, e:
            log.error('explain nodes or insert into neo4j except, '
                      'reason is: %s' % e)
            raise Exception(e)

    def get_all_community(self):
        statement = " MATCH (c:person) WITH c.community AS cluster, " \
                    "collect(c.name) AS  members RETURN cluster, " \
                    "members ORDER BY cluster ASC"
        cql = {
            "statements": [
                {
                    "statement": statement
                }
            ]
        }

        try:
            result = self.neo4j.cql_execute(cql)
            log.info('get the community result is: %s' % result)
        except Exception, e:
            log.error('execute the cql error, reason is: %s' % e)
            raise Exception(e)

        return result
