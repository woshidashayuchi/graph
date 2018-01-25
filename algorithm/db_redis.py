# -*- coding: utf-8 -*-
# Author: bill-jack<xfwangaw@isoftstone.com>

import redis

def connect():
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    
    return r

def test():
    r = connect()
    r.set(1,'hello')
    print r.get(1)

if __name__ == '__main__':
    test()
