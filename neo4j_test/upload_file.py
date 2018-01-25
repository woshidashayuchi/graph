# -*- coding: utf-8 -*-
# Author: bill-jack<xfwangaw@isoftstone.com>
from logs import logging as log

# coding:utf-8

from flask import Flask,render_template,request,redirect,url_for
from werkzeug.utils import secure_filename
import os

app = Flask(__name__)


@app.route('/upload', methods=['POST', 'GET'])
def upload():

    if request.method == 'POST':
        f = request.files['file']
        basepath = os.path.dirname(__file__)  # 当前文件所在路径
        upload_path = os.path.join(basepath, 'static\uploads',
                                    secure_filename(f.filename))
        # upload_path = os.path.join('C:\Users\hp\Documents\Neo4j\default.graphdb\import')
        log.info('go+++++++++++++++++++++++:%s' % upload_path)
        # upload_path = os.path.join(basepath)
        # 注意：没有的文件夹一定要先创建，不然会提示没有该路径
        try:
            f.save(upload_path)
        except Exception, e:
            log.error('write the msg into file error, reason is: %s' % e)
            return '写入失败，因为:%s' % e
        return redirect(url_for('upload'))
    return 'ok'


if __name__ == '__main__':
    app.run(debug=True)