# -*- coding: utf-8 -*-
# Author: bill-jack<xfwangaw@isoftstone.com>
from common.logs import logging as log
import json
import requests
import base64
'''
    节点的载入
    LOAD CSV WITH HEADERS  FROM "file:///aaa.csv" AS line  
    MERGE (p:person{id:line.id,name:line.name,age:line.age,sex:line.sex})
    关系文件的载入
    LOAD CSV WITH HEADERS FROM "file:///PersonRel_Format.csv" AS line  
    match (from:person{id:line.from_id}),(to:person{id:line.to_id})  
    merge (from)-[r:rel{property1:line.property1}]->(to)  
'''


def neo4j_http_test():
    url = "http://localhost:7474/db/data/transaction/commit"
    a = "neo4j:qwe123"
    a = base64.b64encode(a)
    log.info('________________a:%s' % a)
    h = {"Authorization": a}
    dict_data = {
        "statements": [
            {
                "statement": "LOAD CSV WITH HEADERS  FROM \"file:///aaa.csv\" AS line  MERGE (p:person{id:line.id,name:line.name,age:line.age,sex:line.sex})"
            },
            # {
            #     "statement": "CREATE (n {props}) RETURN n",
            #     "parameters": {
            #     "props": {
            #         "name": "My Node"
            #     }
            #     }
            # }
        ]
    }
    result = json.loads(requests.post(url, json.dumps(dict_data), headers=h, timeout=5).text)
    log.info('the request result is: %s' % result)


if __name__ == '__main__':
    try:
        neo4j_http_test()
    except Exception, e:
        log.error('test the neo4j restful api error, reason is: %s' % e)

