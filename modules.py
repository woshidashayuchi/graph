#! /usr/bin/env python
#coding=utf-8

import time,os,uuid,re,csv,json
import pymysql,datetime,json,calendar
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from hashlib import md5    
import networkx as nx
import re


class tupu_sql():
    def __init__(self):
        #类的初始化变量参数，生成con链接和cur
        host = '127.0.0.1'
        user = 'root'
        passwd = 'cmcc1234'
        db = 'tupu'
        charset = 'utf8'
        port = 3306              
        #self.con = mysql.self.connector.self.connect(host=host,user=user,passwd=passwd,db=db,charset=charset,port=port)
        self.con = pymysql.connect(host=host,user=user,passwd=passwd,db=db,charset=charset,port=port)
        self.cur = self.con.cursor() 
        #logtime = logtime #形式["2017","12","01"]年，月，日。

    def sqlclose(self):
        #关闭连接的方法
        self.cur.close()    
        self.con.close()
        
    def insert_insert_file(self,local_name,upload_name,timestamp,title,comment,label):
        #插入文本的sql语句，包含本地服务器名称，上传原始名称，时间戳，标题，注释，label标签
        sql = "insert into insert_file(local_name,upload_name,timestamp,title,comment,label)values('%s','%s','%s','%s','%s','%s')"\
        %(local_name,upload_name,timestamp,title,comment,label)
        print sql
        self.cur.execute(sql)
    def update_data_total(self,label):
        #根据upload下的文本，获取总行数-1，算做文本的总数据量，存到insert_file表里
        output = open("upload/"+label+"/"+label+".txt")
        data_total = len(output.readlines())-1
        output.close() 
        sql = "update insert_file set data_total='%s',data_jiedian=0,data_bian=0 where label='%s'"%(data_total,label)
        self.cur.execute(sql)
        
    def showall_file(self):
        #查询insert_file表里的不同的名称，注释，时间戳等基础信息
        sql1 = "select distinct title,comment,date_format(timestamp,'%Y-%m-%d %H:%i:%s'),label,data_total,data_jiedian,data_bian from insert_file"
        self.cur.execute(sql1)
        data0 = self.cur.fetchall()
        return data0
    
    def insert_insert_data(self,file_dict,label):
        #创建一个总文本，然后根据提交的文本列表，写入到总文本里面
        output = open("upload/"+label+"/"+label+".txt",'a')  
        for key in file_dict:   
            print key,file_dict[key]
            data = open("upload/"+label+"/"+file_dict[key])
            for i in data.readlines():
                output.write(i.replace("\n","")+"\t"+key+"\n")
        output.write(reduce(lambda a,b:a+","+b,file_dict.keys())+"\n")
        output.close()
  
    def select_data(self,label):
        #根据label获取文本对应的数据，和label对应的本地名称
        output = open("upload/"+label+"/"+label+".txt")
        data1 = []
        print "select_data2"
        for i in output.readlines():
            i_list = i.replace("\n","").split("\t")
            data1.append((i_list[0],i_list[1],i_list[2],i_list[3]))
        sql = "select distinct local_name from insert_file where label = '%s'"%(label)
        self.cur.execute(sql)
        data2 = self.cur.fetchall()
        return data1,data2
    def insert_format_data(self,dict1,label):
        #根据计算好的字典dict1,写入到format_data表里
        for key_list in dict1:
            str = ""
            for key,value in key_list[2].items():
                str=str+value+"_"+key+","
            sql = "insert into format_data(src,dest,summariz,label)values('%s','%s','%s','%s')"%(key_list[0],key_list[1],str[:-1],label)
            self.cur.execute(sql)
        self.con.commit()
    def select_format_data(self,label,page):
        #数据表格浏览页面，根据提交的page和label标签，返回查询每次30个
        sql = "select src,dest,summariz from format_data where label = '%s' "%(label)
        if page == "all":
            sql = sql
        else:
            sql = "".join((sql,"limit ",str((int(page)-1)*30),",30"))
        print sql
        self.cur.execute(sql)
        return self.cur.fetchall()
    def select_insert_file(self,label):
        #根据labelh获取对应的基础信息，注释，时间，所有数据量，名称
        sql1 = "".join(("select comment,date_format(timestamp,'%Y-%m-%d %H:%i:%s'),data_total,data_jiedian,title from insert_file where label = '",label,"'"))
        self.cur.execute(sql1)
        data1 = self.cur.fetchone()
        dict1 = {"comment":data1[0],"timestamp":data1[1],"all":data1[2],"user":data1[3],"title":data1[4]}
        return dict1
    def rm_data(self,label):
        #删除写入的数据，格式化的数据
        sql1 = "delete from insert_file where label = '%s'"%(label)
        sql2 = "delete from format_data where label = '%s'"%(label)
        self.cur.execute(sql1)
        self.cur.execute(sql2)
    def select_title_label(self,label):
        #根据label，单纯返回对应的名称
        sql = "select title from insert_file where label = '%s'"%(label)
        print sql
        self.cur.execute(sql)
        return self.cur.fetchone()
    def select_user(self,username):
        #根据用户名获取对应的密码
        sql = "select password from user where username = '%s'"%(username)
        print sql
        self.cur.execute(sql)
        return self.cur.fetchone()
    def insert_cookie(self,username):
        #根据用户名查询登录表里面的session会话，如果存在则是重新登录更新处理，不存在则是直接写入
        sql = "select sessionid from login_session where username = '%s'"%(username)
        self.cur.execute(sql)
        data = self.cur.fetchone()
        sessionid = str(uuid.uuid4()).replace("-", "")
        if data is None :
            sql = "insert into login_session(username,sessionid,session_time)values('%s','%s','%s')"%(username,sessionid,time.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            sql = "update login_session set sessionid = '%s',session_time = '%s' where username = '%s'"%(sessionid,time.strftime("%Y-%m-%d %H:%M:%S"),username)
        self.cur.execute(sql)
        return sessionid
               
    def select_user_password(self,username):
        #根据用户名获取对应的密码
        sql = "select password from user where username = '%s'"%(username)
        print sql
        self.cur.execute(sql)
        return self.cur.fetchone()
    def select_user_cookie(self,username):
        #查询根据用户名，和会话串
        sql = "select sessionid from login_session where username = '%s'"%(username)
        print sql
        self.cur.execute(sql)
        return self.cur.fetchone()
    def format_data_jiedian_bian(self,label):
        #根据节点，和边的计算，更新到insert_file表里
        sql1 = "select count(1) from format_data where label = '%s'"%(label)
        self.cur.execute(sql1)
        data_bian = self.cur.fetchone()[0]
        sql2 = "select count(1) from (select DISTINCT src from format_data where label = '%s' union select DISTINCT dest from format_data where label = '%s') as a"%(label,label)
        self.cur.execute(sql2)
        data_jiedian = self.cur.fetchone()[0]
        sql3 = "update insert_file set data_bian='%s',data_jiedian='%s' where label='%s'"%(data_bian,data_jiedian,label)      
        print sql1
        print sql2
        print sql3
        self.cur.execute(sql3)
        self.con.commit()
    
    def format_data(self,label):
        #数据处理databrows.html页面，点击关系发现按钮，执行spark获取label对应的文本，计算后存到表里
        print "format_data,1"
        sc = SparkContext("local","Page Rank")
        textFile = sc.textFile("/usr/local/nginx/pygraph/upload/"+label+"/"+label+".txt")
        dict1 = textFile.mapPartitions(format_main).collect()
        print dict1 #列表格式，('20014073', '20014070', {'all': '2', '5': '2', 'average': '2'})]
        sc.stop()
        print "format_data,3"
        self.insert_format_data(dict1,label)
        self.format_data_jiedian_bian(label)
        print "format_data,4"
        self.con.commit()

    def itemedit_data_select(self,label):
        #在insert_file表里，根据label标签，查询标题，注释，节点总量，边总量
        sql = "select title,comment,data_jiedian,data_bian from insert_file where label = '%s'"%(label)
        print sql
        self.cur.execute(sql)
        return self.cur.fetchone()
    def itemedit_data_update(self,label,title,comment):
        #netedit.html页面，更新标题，注释
        sql = "update insert_file set title = '%s',comment = '%s' where label = '%s'"%(title,comment,label)
        self.cur.execute(sql) 
        self.con.commit()
        
    def idxreport_begin(self,label,target):
        #community_layout.html先判断，是否存在需要处理的任务
        #首先查询当前是否有0的标记，然后就去执行。。如果有，则返回，当前有任务在执行，请稍后。。
        self.cur.execute("select label from target where label = '%s' and target='%s'"%(label,target)) 
        label1 = self.cur.fetchone()
        if label1 != None:
            self.cur.execute("update target set finish=0 where label = '%s'and target='%s'"%(label,target)) 
        else:
            sql = 'insert target (label,target,finish)values("%s","%s",0)'%(label,target)
            self.cur.execute(sql)
        self.con.commit()
        
    def idxreport_finish(self,label,target):
        #community_layout.html页面更新完处理完成状态
        self.cur.execute('select finish from target where label = "%s" and target="%s"'%(label,target))     
        finish = self.cur.fetchone()[0]
        return finish
        
    def netlayout(self,label):
        #community_layout.html判断是否完成处理
        self.cur.execute('select target from target where label = "%s" and finish is null'%(label))     
        finish = self.cur.fetchall()
        return finish
        
    def netlayoutresult(self,result,label,target):
        #netlayout.html写入新的任务
        self.cur.execute('insert into target (result,label,target)values("%s","%s","%s")'%(result,label,target))     
        self.con.commit()
    def netlayoutresultget(self,label,target):
        #netlayout.html查询结果，如果是单个的数值
        self.cur.execute('select result from target where label = "%s" and target="%s"'%(label,target))     
        finish = self.cur.fetchone()[0]
        return finish
    
def format_main(iterator):
    #tupu类的format_data使用的处理单行数据方法
    data1 = []
    for i in iterator:
        i_list = i.replace("\n","").split("\t")
        if len(i_list)>3:
            data1.append((i_list[0],i_list[1],i_list[2],i_list[3]))
        else:
            data2 = i_list[0].split(",")
    print data2
    format_data_class = format_data()
    dict1 = format_data_class.get_result(data1,data2)
    dict1 = format_data_class.get_averge(dict1)
    dict1 = format_data_class.get_list(dict1)
    return dict1

class analysis():
    #测试neof4j代码，并未使用。
    #关于Neo4j和Cypher批量更新和批量插入优化的5个建议 - CSDN博客 http://blog.csdn.net/hwz2311245/article/details/60963383
    #neo4j图数据库入门 - AinUser的博客 - CSDN博客 http://blog.csdn.net/AinUser/article/details/72123316?locationNum=3&fps=1
    def format_data(self,data1,data2):
        uri = "bolt://192.168.1.144:7687"  
        driver = GraphDatabase.driver(uri, auth=("neo4j", "0")) 
        str1 = "" 
        str2 = ""
        for i in data2:
            str0 = "".join(("(",str(i[0]),"tel {name:'",str(i[0]),"'}),"))
            str1 = str1 + str0
        for i in data1:
            str0 = "".join(("(",str(i[0]),")-[:LOVES]->(",str(i[1]),"),"))
            str2 = str2 + str0
        cypher = "".join(("create ",str1,str2))
        print cypher
#        cypher = """ 
#                    create (Neo:Crew {name:'Neo'}), 
#                           (Morpheus:Crew {name: 'Morpheus'}), 
#                           (Trinity:Crew {name: 'Trinity'}), 
#                           (Cypher:Crew:Matrix {name: 'Cypher'}), 
#                           (Smith:Matrix {name: 'Agent Smith'}), 
#                           (Architect:Matrix {name:'The Architect'}), 
#         
#                           (Neo)-[:KNOWS]->(Morpheus), 
#                           (Neo)-[:LOVES]->(Trinity), 
#                           (Morpheus)-[:KNOWS]->(Trinity), 
#                           (Morpheus)-[:KNOWS]->(Cypher), 
#                           (Cypher)-[:KNOWS]->(Smith), 
#                           (Smith)-[:CODED_BY]->(Architect) 
#                 """    # "cypher from <a target="_blank" href="http://console.neo4j.org/">http://console.neo4j.org/</a>"  
        with driver.session() as session:  
            with session.begin_transaction() as tx:  
                tx.run(cypher) 
        
    
    
        
class format_data():
    #给format_main方法使用，处理文本数据
    def get_data(self,data1,local_name):
        dict2 = {}
        for i in data1:
            if i[3] == local_name:
                dict2[str(i[0])+"_"+str(i[1])] = {str(i[3]).split(".")[0]:str(i[2])}
        return dict2
        
    def get_result(self,data1,data2):
        #新的字典和初始dict1字典匹配，如果新的字典的key在初始字典里面存在，则把值添加到初始字典对应的指列表里面，
        #否则直接给初始字典添加一对新的值
        dict1 = self.get_data(data1,data2[0])
        for i in data2[1:]:
            dict2 = self.get_data(data1,i)
            for key in dict2.keys():
                if dict1.has_key(key):
                    dict1[key][dict2[key].keys()[0]] = dict2[key].values()[0]
                else:
                    dict1[key] = dict2[key]
        return dict1 
                
    def get_averge(self,dict1):
        for key in dict1:
            value = dict1[key]
            int_values = [int(x)for x in value.values()] #把原始的['3_1', '6_2', '7_3', '2_5']格式化为数字列表[3,6,7,2]
            all_values = reduce(lambda x,y:x+y,int_values)  #计算数字列表全部数量
            averge_value = all_values/len(int_values)   #平均是按出现的月份数量还是固定的12个月？
            dict1[key]["all"]=str(all_values)  #dict1的key对应的value列表添加，两个指标总数和平均数。
            dict1[key]["average"]=str(averge_value)
        return dict1
    def get_list(self,dict1):
        list1 = [(key.split("_")[0],key.split("_")[1],dict1[key])for key in dict1]
        return list1

    
    
def get_rowslist(data):
    #根据data的每个元素，生成每个项目的具体信息json格式，标签，注释，时间，标签，数据总量，节点量，边量
    list = []
    for i in data:
        list.append({"title":i[0],"comment":i[1],"timestamp":i[2],"label":i[3],"data_total":i[4],\
        "data_jiedian":i[5],"data_bian":i[6]})
    return list


def new_file():
    #随机生成新的label标签，当前的时间戳+uuid字符串
    return datetime.datetime.now().strftime("%Y%m%d%H%M%S")+str(uuid.uuid4()).replace("-","")


def csvwrite(data0,data,csvname):
    #data0为基本信息统计，data为每条数据统计，生成csv的文本
    #data = [('小河', '25', '1234567'),('小芳', '18', '789456')] 数据格式
    print csvname
    csvfile = file(csvname,'wb')
    writer = csv.writer(csvfile)
    for x in data0:
        writer.writerow([x]) #
    writer.writerow(['序号','主叫','被叫','频率-M1','频率-M2','频率-M3','频率-M4','频率-M5',\
    '频率-M6','频率-M7','频率-M8','频率-M9','频率-M10','频率-M11','频率-M12','总频率','平均频率'])
    writer.writerows(data)
    csvfile.close()
