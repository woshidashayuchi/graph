# -*- coding: utf-8 -*-
# Author: bill-jack<xfwangaw@isoftstone.com>

status_code = {
    200: 'ok',
    101: 'parameters error',
    201: 'file upload error',
    300: 'file has been exist, please change the file name',
    301: 'login exception',
    302: 'title has exist',
    401: 'mysql database insert error',
    402: 'mysql database update error',
    403: 'mysql database select error',
    404: 'mysql database delete error',
    501: 'neo4j insert error',
    502: 'neo4j update error',
    503: 'neo4j select error',
    504: 'neo4j delete error'
}


def request_result(code, ret={}):
    result = {
        "status": code,
        "msg": status_code[code],
        "result": ret
    }
    return result
