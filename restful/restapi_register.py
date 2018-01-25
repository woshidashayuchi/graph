# -*- coding: utf-8 -*-
# Author: bill-jack<xfwangaw@isoftstone.com>
from common.logs import logging as log
from flask import Flask
from flask_cors import CORS
from flask_restful import Api
import restapi_define


def rest_app_run():
    app = Flask(__name__)
    CORS(app=app)
    api = Api(app)
    app.config.from_object(__name__)
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 1
    app.config['UPLOAD_FOLDER'] = 'upload'

    api.add_resource(restapi_define.Graph,
                     '/api/v1/files')
    api.add_resource(restapi_define.Nodes,
                     '/api/v1/nodes')
    api.add_resource(restapi_define.Relationship,
                     '/api/v1/relationships')

    api.add_resource(restapi_define.RelationDiscover,
                     '/api/v1/sources')
    api.add_resource(restapi_define.CommunityExplain,
                     '/api/v1/community')
    api.add_resource(restapi_define.UserAbout,
                     '/api/v1/user')
    api.add_resource(restapi_define.Quota,
                    '/api/v1/quota')


    app.run(host='0.0.0.0', port=9999, debug=True, threaded=True)
