#! /usr/bin/env python
#coding=utf-8
from flask import Flask, render_template,request,send_from_directory
from flask import jsonify,make_response
import time,os,uuid,re,csv,json
import pymysql,datetime,json,calendar
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from hashlib import md5    
import networkx as nx
import re
from modules import csvwrite,format_main,get_rowslist,new_file,analysis,format_data,tupu_sql


app = Flask(__name__)
app.config.from_object(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT']=1
app.config['UPLOAD_FOLDER'] = 'upload'


    

    

@app.route('/app/uploads', methods=['GET', 'POST'])
def upload_file():
    print "upload_file"
    tupu_class = tupu_sql()
    if request.method == 'POST':
        title = request.form["name1"]
        comment = request.form["name2"]
        label = str(uuid.uuid4()).replace("-","")
        os.mkdir("upload/"+label)
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        file_dict = {}
        for filename in request.files.getlist('file'):
            local_name = filename.filename
            upload_name = new_file()+".txt"
            filename.save(os.path.join(app.config['UPLOAD_FOLDER'],label+"/"+upload_name))
            tupu_class.insert_insert_file(local_name,upload_name,timestamp,title,comment,label)
            file_dict[local_name] = upload_name
        tupu_class.insert_insert_data(file_dict,label)
        tupu_class.update_data_total(label)
        tupu_class.con.commit()
        tupu_class.sqlclose()
        return "上传成功！"

@app.route('/app/showall', methods=['GET'])
def showall_file():
    tupu_class = tupu_sql()
    data = tupu_class.showall_file()
    tupu_class.sqlclose()
    list = get_rowslist(data)
    return jsonify({"status":200,"rows":list})

@app.route('/app/selectdata', methods=['GET'])
def select_data():
    label = request.args.get("label").encode("utf8")
    page = request.args.get("page").encode("utf8")
    print label,page,"select_data"
    tupu_class = tupu_sql()
    data = tupu_class.select_format_data(label,page)
    dict1 = tupu_class.select_insert_file(label)
    tupu_class.sqlclose()
    list = []
    for i in data:
        list.append({"src":i[0],"dest":i[1],"total":i[2]})
    return jsonify({"status":200,"page":3,"rows":list,"title":dict1})
    
@app.route('/app/rm', methods=['GET'])
def rm_data():
    label = request.args.get("label").encode("utf8")
    print label,"rm_data"
    tupu_class = tupu_sql()
    tupu_class.rm_data(label)
    tupu_class.con.commit()
    tupu_class.sqlclose()    
    return jsonify({"status":200})

@app.route('/app/get_data', methods=['GET'])
def get_data():
    label = request.args.get("label").encode("utf8")
    tupu_class = tupu_sql()
    title = tupu_class.select_title_label(label)
    print label,title,"get_data"
    title_label_name = "".join((title[0],"_",label,".csv"))
    dirname = os.path.join('../html/download',title_label_name)
    if os.path.isfile(dirname):
        tupu_class.sqlclose() 
        return jsonify({"status":200,"dirname":title_label_name}) 
    else:
        data = tupu_class.select_format_data(label,"all")
        dict1 = tupu_class.select_insert_file(label)
        tupu_class.sqlclose() 
        n = 1
        import sys 
        reload(sys) 
        sys.setdefaultencoding('utf-8')
        list0 = ["说明："+dict1["comment"],"用户数："+str(dict1["user"]),\
        "记录数："+str(dict1["all"]),"状态："+str(dict1["timestamp"])+"导入成功"]
        list1 = []
        for i in data: 
            list2 = [str(n),i[0],i[1]]
            for m in ["_1","_2","_3","_4","_5","_6","_7","_8","_9","_10","_11","_12","_all","_average"]:
                if m in i[2]:
                    list2.append(re.search("(\d+)"+m,i[2]).group(1))
                else:
                    list2.append("0")
            list1.append(tuple(list2))
            n = n + 1
        csvwrite(list0,list1,dirname)
        print "get_data,ok"
        #return send_from_directory('upload',title_label_name,as_attachment=True)
        return jsonify({"status":200,"dirname":title_label_name}) 

@app.route('/app/login',methods=['POST'])
def login():
    username = request.form.get("username").encode("utf8")
    password = request.form.get("password").encode("utf8")
    md5_obj = md5()
    md5_obj.update(password+"tupu2017")
    password = md5_obj.hexdigest()
    tupu_class = tupu_sql()
    password1 = tupu_class.select_user_password(username)
    print username,password,password1,"login"
    if password1[0] == password:
        sessionid = tupu_class.insert_cookie(username)
        tupu_class.con.commit()
        tupu_class.sqlclose()      
        response = jsonify({"status":200})
        response.set_cookie("username",username)
        response.set_cookie("sessionid",sessionid)
        return response
    else:
        return jsonify({"status":400})
   
@app.route('/app/logintest',methods=['get'])
def logintest():
    username = request.cookies.get('username')
    sessionid = request.cookies.get('sessionid')
    tupu_class = tupu_sql()
    sessionid1 = tupu_class.select_user_cookie(username)
    print username,sessionid,sessionid1,"logintest"
    tupu_class.sqlclose()
    if sessionid1 != None and sessionid1[0] == sessionid:
        print "logintest,ok"
        return jsonify({"status":200})
    else:
        print "logintest,false"
        return jsonify({"status":400})

@app.route('/app/formatdata', methods=['GET'])
def formatdata():
    label = request.args.get("label").encode("utf8")
    tupu_class = tupu_sql()
    tupu_class.format_data(label)
    tupu_class.sqlclose()    
    return jsonify({"status":200})
    

@app.route('/app/test1')
def test1():
    ok = request.cookies.get('Name')
    return ok
    
@app.route("/app/idxreport_begin")
#社群发现页面
def idxreport_data1(): 
    os.system('echo "开始任务  "'+time.strftime("%Y-%m-%d %H:%M:%S")+'>spark.log')
    label = request.args.get("label").encode("utf8")
    target = request.args.get("idxreport").encode("utf8")
    print label,target,"idxreport_begin"
    tupu_class = tupu_sql()
    tupu_class.idxreport_begin(label,target)
    tupu_class.sqlclose() 
    return jsonify({"status":200})
@app.route("/app/idxreport_finish")
def idxreport_data2(): 
    label = request.args.get("label").encode("utf8")
    target = request.args.get("idxreport").encode("utf8")
    tupu_class = tupu_sql()
    finish = tupu_class.idxreport_finish(label,target)
    tupu_class.sqlclose() 
    return jsonify({"status":200,"finish":finish})
    #前端循环读取，等finish为0,

    
@app.route("/app/sparklog")
def sparklog():
    log1 = open("spark.log")
    loglist= log1.readlines()
    log1.close()
    log = reduce(lambda x, y:"<p>"+x+"</p><p>"+y+"<p>",loglist[-10:])
    print "sparklog,ok"
    return jsonify({"status":200,"log":log}) 

    
@app.route("/app/itemeditdataselect")
#netedit.html
def itemedit_data_select():
    label = request.args.get("label").encode("utf8")
    tupu_class = tupu_sql()
    title,comment,jiedian,bian = tupu_class.itemedit_data_select(label)
    tupu_class.sqlclose() 
    print "itemedit_data_select,ok"
    return jsonify({"status":200,"title":title,"comment":comment,"jiedian":jiedian,"bian":bian}) 
    
@app.route("/app/itemeditdataupdate")
def itemedit_data_update():
    label = request.args.get("label").encode("utf8")
    title = request.args.get("title").encode("utf8")
    comment = request.args.get("comment").encode("utf8")
    print title,comment,label
    tupu_class = tupu_sql()
    tupu_class.itemedit_data_update(label,title,comment)
    tupu_class.sqlclose() 
    print "itemedit_data_update,ok"
    return jsonify({"status":200})

@app.route("/app/netlayoutselect")
def netlayoutselect():
    label = request.args.get("label").encode("utf8")
    tupu_class = tupu_sql()
    finish = tupu_class.netlayout(label)
    tupu_class.sqlclose() 
    print "netlayout,ok" 
    return jsonify({"status":200,"finish":finish})
    
@app.route("/app/netlayouttarget")
def netlayouttarget():
    label = request.args.get("label").encode("utf8")
    idxreport = request.args.get("idxreport").encode("utf8")
    try:
        sc.stop()
    except:
        pass
    sc = SparkContext("local","Page Rank")
    sqlContext = SQLContext(sc)
    df = sqlContext.read.format("jdbc").options(url="jdbc:mysql://localhost:3306/tupu?user=root&password=cmcc1234",dbtable="format_data").load()
    df.registerTempTable("temp1")
    list1 = sqlContext.sql('select src,dest,summariz from temp1 where label="abcdefg"').collect()
    list2 = [(i["src"],i["dest"]) for i in list1]
    #list3 =[(i["src"],i["dest"],int(re.search(r"(\d+)_all",i["summariz"]).group(1))) for i in list1]
    G = nx.Graph(list2)
    if idxreport in ["coefficient","weightedaverage","diameter","clusteringcoefficient"]:
        if idxreport == "diameter":
            a1=nx.diameter(G)#返回图G的直径（最长最短路径的长度）
        elif idxreport == "coefficient":
            a1=nx.average_clustering(G)#平均聚类系数,     一个
        elif idxreport == "clusteringcoefficient":
            a1=nx.average_shortest_path_length(G) # 所有节点间平均最短路径长度    一个
        sc.stop()
        print a1
        tupu_class = tupu_sql()
        finish = tupu_class.netlayoutresult(a1,label,idxreport)
        tupu_class.sqlclose() 
        return  jsonify({"status":200})
    elif idxreport == "density":
        #加权平均度
        a3=nx.degree(G)#图密度
    elif idxreport == "hits":
        a3=nx.hits(G)[0]#HITS
    elif idxreport == "pagerank":
        a3=nx.pagerank(G)#PageRank
    elif idxreport == "friendship":
        a3=nx.closeness_centrality(G)#关系亲密度
    elif idxreport == "eigenvector":
        a3=nx.eigenvector_centrality(G)#特征向量中心度
    elif idxreport == "average":
        a3=G.degree()
    print a3
    value1=[value for key,value in a3.items()]
    dict01 = {}
    for item in value1:
        dict01.update({item:value1.count(item)})
    valuey = [value for key,value in dict01.items()]
    valuex = [key for key,value in dict01.items()]
    import matplotlib as mpl 
    mpl.use('Agg') 
    import matplotlib.pyplot as plt
    plt.plot(valuex,valuey,"o")
    plt.savefig('/var/www/html/pic/'+label+idxreport+".png",dpi=150)
    print "ok"
    sc.stop()
    tupu_class = tupu_sql()
    finish = tupu_class.netlayoutresult(0,label,idxreport)
    tupu_class.sqlclose()
    return jsonify({"status":200})
    
@app.route("/app/netlayoutresultget")
def netlayoutresultget():
    label = request.args.get("label").encode("utf8")
    idxreport = request.args.get("idxreport").encode("utf8")
    tupu_class = tupu_sql()
    finish = tupu_class.netlayoutresultget(label,idxreport)
    tupu_class.sqlclose()
    return jsonify({"status":200,"finish":finish})

if __name__ == '__main__':
    app.run(port=8001,host="0.0.0.0",debug=True)


