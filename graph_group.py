#! /usr/bin/env python
#coding=utf-8
#! /usr/bin/env python
#coding=utf-8
import os,json,time
from pyspark.sql import SparkSession
from pyspark import SQLContext
import pymysql
import os,json,time
from pyspark.sql import SparkSession
from pyspark import SQLContext
os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 pyspark-shell")
spark = SparkSession.builder.getOrCreate()
from graphframes import *

host = 'localhost'
user = 'root'
passwd = 'cmcc1234'
db = 'tupu'
charset = 'utf8'
port = 3306        
print "初始化变量"
while 1:      
    con = pymysql.connect(host=host,user=user,passwd=passwd,db=db,charset=charset,port=port)
    cur = con.cursor()
    cur.execute('select label from target where target="community" and finish = 0')     
    list1 = cur.fetchone()
    if list1 != None:
        os.system('echo "初始化spark环境  "'+time.strftime("%Y-%m-%d %H:%M:%S")+'>>spark.log')
        sc,sqlContext,v,e=None,None,None,None
        sc = spark.sparkContext
        sqlContext = SQLContext(sc)
        os.system('echo "获取原数据文本  "'+time.strftime("%Y-%m-%d %H:%M:%S")+'>>spark.log')
        lines = sc.textFile("facebook_combined.txt")
        os.system('echo "处理原数据文本，提取数据  "'+time.strftime("%Y-%m-%d %H:%M:%S")+'>>spark.log')
        list2 = lines.map(lambda x:(x.split(" ")[0],x.split(" ")[1])).collect()
        list1 = list(set(lines.flatMap(lambda x:x.split(" ")).collect()))
        list3=[(i,"0")for i in list1]
        os.system('echo "构建GraphFrame关系  "'+time.strftime("%Y-%m-%d %H:%M:%S")+'>>spark.log')
        v = sqlContext.createDataFrame(list3, ["id", "name"])
        e = sqlContext.createDataFrame(list2, ["src", "dst"])
        g = GraphFrame(v, e)
        os.system('echo "计算labelPropagation  "'+time.strftime("%Y-%m-%d %H:%M:%S")+'>>spark.log')
        list5 = g.labelPropagation(5).collect()
        os.system('echo "构建nodes和links的数据结构  "'+time.strftime("%Y-%m-%d %H:%M:%S")+'>>spark.log')
        model = {}
        model["nodes"] = []
        model["links"] = []
        for i in list5:
            model["nodes"].append({"id":i[0],"group":i[2]})
        for i in list2:
            model["links"].append({"source":i[0],"value":"abc","target":i[1],"relation":"数据"})
        os.system('echo "保存数据结构到json文件  "'+time.strftime("%Y-%m-%d %H:%M:%S")+'>>spark.log')
        with open("/var/www/html/json/facebookv4.json",'wb') as json_file:
                json_file.write('{"nodes":[')   
                list1 = ""
                for i in model["nodes"]:
                    list1 = list1 + '{"id":"'+str(i["id"])+'","group":'+str(i["group"])+"},"
                json_file.write(list1[:-1])
                json_file.write('],"links":[')
                list2 = ""
                for i in model["links"]:
                    list2 = list2 +'{"source":"'+str(i["source"])+'","target":"'+str(i["target"])+'","value":"a","relation":"a"},'
                json_file.write(list2[:-1])
                json_file.write(']}')
        os.system('echo "终止社区发现的任务  "'+time.strftime("%Y-%m-%d %H:%M:%S")+'>>spark.log')
        cur.execute('update target set finish =1 where target="community"')
        con.commit()  
        os.system('echo "完成任务  "'+time.strftime("%Y-%m-%d %H:%M:%S")+'>>spark.log')   
    cur.close()
    con.close()
    time.sleep(2)



