# -*- coding: utf-8 -*-
# Author: bill-jack<xfwangaw@isoftstone.com>
from logs import logging as log
from py2neo import Graph
from igraph import Graph as IGraph
graph = Graph(
    "http://localhost:7474",
    username="neo4j",
    password="qwe123")
query = '''
MATCH (c1:person)-[r:rel]->(c2:person)
RETURN c1.name, c2.name, r.property1 AS weight
'''
ig = IGraph.TupleList(graph.run(query), weights=True)

# 社区发现
if __name__ == '__main__':
    try:
        log.info('ig is: type(%s)' % type(ig))
        clusters = IGraph.community_walktrap(ig, weights=None).as_clustering()
    except Exception, e:
        log.error('error.....:%s' % e)
        raise
    nodes = [{"name": node["name"]} for node in ig.vs]
    for node in nodes:
        idx = ig.vs.find(name=node["name"]).index
        node["community"] = clusters.membership[idx]
    write_clusters_query = '''
    UNWIND {nodes} AS n
    MATCH (c:person) WHERE c.name = n.name
    SET c.community = toInt(n.community)
    '''
    graph.run(write_clusters_query, nodes=nodes)
    # 在Neo4j中查询有多少个社区以及每个社区的成员数：
    # '''
    # MATCH (c:person)
    # WITH c.community AS cluster, collect(c.name) AS  members
    # RETURN cluster, members ORDER BY cluster ASC
    # '''

# pageRanks
# if __name__ == '__main__':
#     pg = ig.pagerank()
#     pgvs = []
#     for p in zip(ig.vs, pg):
#         print(p)
#         pgvs.append({"name": p[0]["name"], "pg": p[1]})
#
#     write_clusters_query = '''
#     UNWIND {nodes} AS n
#     MATCH (c:Character) WHERE c.name = n.name
#     SET c.pagerank = n.pg
#     '''
#     graph.run(write_clusters_query, nodes=pgvs)