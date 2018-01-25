# -*- coding: utf-8 -*-
# Author: wang-xf <it-wangxf@all-reach.com>
# Date: 17/8/18 下午7:08

import hashlib
import threading
import time
import struct
from socket import *
from base64 import b64encode
from logs import logging as log
from lp_spark_test import test
from test_unfolding import unfold


connectionlist = {}
g_code_length = 0
g_header_length = 0
HOST = '127.0.0.1'
PORT = 4567
BUFSIZ = 1024
ADDR = (HOST, PORT)


def hex2dec(string_num):
    return str(int(string_num.upper(), 16))


def get_datalength(msg):
    global g_code_length
    global g_header_length

    print (len(msg))
    g_code_length = ord(msg[1]) & 127
    received_length = 0
    if g_code_length == 126:
        # g_code_length = msg[2:4]
        # g_code_length = (ord(msg[2])<<8) + (ord(msg[3]))
        g_code_length = struct.unpack('>H', str(msg[2:4]))[0]
        g_header_length = 8
    elif g_code_length == 127:
        # g_code_length = msg[2:10]
        g_code_length = struct.unpack('>Q', str(msg[2:10]))[0]
        g_header_length = 14
    else:
        g_header_length = 6
    g_code_length = int(g_code_length)
    return g_code_length


def parse_data(msg):
    global g_code_length
    g_code_length = ord(msg[1]) & 127
    # received_length = 0
    if g_code_length == 126:
        g_code_length = struct.unpack('>H', str(msg[2:4]))[0]
        masks = msg[4:8]
        data = msg[8:]
    elif g_code_length == 127:
        g_code_length = struct.unpack('>Q', str(msg[2:10]))[0]
        masks = msg[10:14]
        data = msg[14:]
    else:
        masks = msg[2:6]
        data = msg[6:]

    i = 0
    raw_str = ''

    for d in data:
        raw_str += chr(ord(d) ^ ord(masks[i%4]))
        i += 1

    # print (u"总长度是：%d" % int(g_code_length))
    return raw_str


def socket_client(data):
    client = socket(AF_INET, SOCK_STREAM)
    client.connect(ADDR)

    try:
        client.send(data.encode('utf8'))
    except Exception, e:
        log.error("send message error, reason is : %s" % e)


    try:
        data = client.recv(BUFSIZ)
    except Exception, e:
        log.error("recv the message error, reason is: %s" % e)

    log.info(data.decode('utf8'))

    return data


# 广播用
def broadcast_message(message):
    global connectionlist
    message_utf_8 = message.encode('utf-8')
    log.info('connectlist array is: %s' % connectionlist)
    for connection in connectionlist.values():
        back_str = []
        back_str.append('\x81')
        data_length = len(message_utf_8)

        if data_length <= 125:
            back_str.append(chr(data_length))
        elif data_length <= 65535:
            back_str.append(struct.pack('b', 126))
            back_str.append(struct.pack('>h', data_length))
            # back_str.append(chr(data_length >> 8))
            # back_str.append(chr(data_length & 0xFF))
            # a = struct.pack('>h', data_length)
            # b = chr(data_length >> 8)
            # c = chr(data_length & 0xFF)
        elif data_length <= (2^64-1):
            # back_str.append(chr(127))
            back_str.append(struct.pack('b', 127))
            back_str.append(struct.pack('>q', data_length))
            # back_str.append(chr(data_length >> 8))
            # back_str.append(chr(data_length & 0xFF))
        else:
            print ('太长了')
        msg = ''
        for c in back_str:
            msg += c
        back_str = str(msg) + message_utf_8  # encode('utf-8')
        # connection.send(str.encode(str(u"\x00%s\xFF\n\n" % message))) #这个是旧版
        # print (u'send message:' +  message)
        if back_str is not None and len(back_str) > 0:
            print (back_str)
            try:
                connection.send(back_str)
            except Exception, e:
                log.error('send error, reason is: %s' % e)


def send_message(conn, message):
    # global connectionlist
    message_utf_8 = message.encode('utf-8')
    log.info('connectlist array is: %s' % conn)
    # for connection in connectionlist.values():
    back_str = []
    back_str.append('\x81')
    data_length = len(message_utf_8)

    if data_length <= 125:
        back_str.append(chr(data_length))
    elif data_length <= 65535:
        back_str.append(struct.pack('b', 126))
        back_str.append(struct.pack('>h', data_length))
        # back_str.append(chr(data_length >> 8))
        # back_str.append(chr(data_length & 0xFF))
        # a = struct.pack('>h', data_length)
        # b = chr(data_length >> 8)
        # c = chr(data_length & 0xFF)
    elif data_length <= (2^64-1):
        # back_str.append(chr(127))
        back_str.append(struct.pack('b', 127))
        back_str.append(struct.pack('>q', data_length))
        # back_str.append(chr(data_length >> 8))
        # back_str.append(chr(data_length & 0xFF))
    else:
        print ('太长了')
    msg = ''
    for c in back_str:
        msg += c
    back_str = str(msg) + message_utf_8  # encode('utf-8')
    # connection.send(str.encode(str(u"\x00%s\xFF\n\n" % message))) #这个是旧版
    # print (u'send message:' +  message)
    if back_str is not None and len(back_str) > 0:
        print (back_str)
        try:
            conn.send(back_str)
        except Exception, e:
            log.error('send error, reason is: %s' % e)


def deleteconnection(item):
    global connectionlist
    del connectionlist['connection'+item]


class WebSocket(threading.Thread):  # 继承Thread

    GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

    def __init__(self, conn, index, name, remote, path="/"):
        threading.Thread.__init__(self)  # 初始化父类Thread
        self.conn = conn  # 套接字
        self.index = index  # 1
        self.name = name  # 127.0.0.1
        self.remote = remote  # (127.0.0.1, 57047)
        self.path = path
        self.buffer = ""
        self.buffer_utf8 = ""
        self.length_buffer = 0

    def run(self):  # 重载Thread的run
        print('Socket%s Start!' % self.index)
        headers = {}
        self.handshaken = False

        while True:

            if self.handshaken is False:  # 握手
                log.info('Socket%s Start Handshaken with %s!' % (self.index, self.remote))

                self.buffer += bytes.decode(self.conn.recv(1024))
                log.info('buffer data is: %s' % self.buffer)

                if self.buffer.find('\r\n\r\n') != -1:
                    header, data = self.buffer.split('\r\n\r\n', 1)
                    for line in header.split("\r\n")[1:]:
                        key, value = line.split(": ", 1)
                        headers[key] = value

                    headers["Location"] = ("ws://%s%s" % (headers["Host"], self.path))
                    key = headers['Sec-WebSocket-Key']
                    token = b64encode(hashlib.sha1(str.encode(str(key + self.GUID))).digest())

                    handshake = "HTTP/1.1 101 Switching Protocols\r\n"\
                        "Upgrade: websocket\r\n"\
                        "Connection: Upgrade\r\n"\
                        "Sec-WebSocket-Accept: "+bytes.decode(token)+"\r\n"\
                        "WebSocket-Origin: "+str(headers["Origin"])+"\r\n"\
                        "WebSocket-Location: "+str(headers["Location"])+"\r\n\r\n"

                    log.info('handshake is: %s' % handshake)
                    self.conn.send(str.encode(str(handshake)))
                    self.handshaken = True
                    log.info('Socket %s Handshaken with %s success!' % (self.index, self.remote))
                    # send_message(self.conn, 'Welcome, ' + self.name + ' !')
                    # sendMessage(socket_client(self.name))
                    send_message(self.conn, 'connect the server successed!')
                    self.buffer_utf8 = ""
                    g_code_length = 0

            else:
                global g_code_length
                global g_header_length
                mm = self.conn.recv(1024)
                if len(mm) <= 0:
                    continue
                if g_code_length == 0:
                    get_datalength(mm)

                # 接受的长度
                self.length_buffer = self.length_buffer + len(mm)
                self.buffer = self.buffer + mm
                if self.length_buffer - g_header_length < g_code_length:
                    continue
                else:
                    self.buffer_utf8 = parse_data(self.buffer)  # utf8
                    msg_unicode = str(self.buffer_utf8).decode('utf-8', 'ignore')  # unicode
                    if msg_unicode == 'quit':
                        print ('Socket%s Logout!' % self.index)
                        nowTime = time.strftime('%H:%M:%S', time.localtime(time.time()))
                        # send_message('%s %s say: %s' % (nowTime, self.remote, self.name+' Logout'))
                        send_message(self.conn, socket_client('Logout'))
                        deleteconnection(str(self.index))
                        self.conn.close()
                        break  # 退出线程
                    else:
                        # print (u'Socket%s Got msg:%s from %s!' % (self.index, msg_unicode, self.remote))
                        nowTime = time.strftime(u'%H:%M:%S', time.localtime(time.time()))
                        # send_message(self.conn, u'%s %s say: %s' % (nowTime, self.remote, msg_unicode))
                        # if msg_unicode != 'lpa' and msg_unicode != 'fast_unfolding':
                        #     send_message(self.conn, 'parameters error')
                        #     break
                        try:
                            if 'lpa' in msg_unicode:
                                test(self.conn)
                                self.conn.close()
                                break
                            if 'fast' in msg_unicode:
                                unfold(self.conn)
                                self.conn.close()
                                break
                        except Exception, e:
                            print "error..., reason is: %s" % e
                            self.conn.close()
                            break
                        # finally:
                        #     self.conn.close()
                        # send_message(self.conn, socket_client(msg_unicode))
                    # 重置buffer和bufferlength
                    self.buffer_utf8 = ""
                    self.buffer = ""
                    g_code_length = 0
                    self.length_buffer = 0
            self.buffer = ""
           


class WebSocketServer(object):
    def __init__(self):
        self.socket = None

    def begin(self):
        print('WebSocketServer Start!')
        self.socket = socket(AF_INET, SOCK_STREAM)
        self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.socket.bind(("0.0.0.0", 4567))
        self.socket.listen(50)

        global connectionlist

        i = 0
        while True:
            connection, address = self.socket.accept()
            log.info('the new socket is: %s, and link to the client address is: %s' % (connection, address))
            username = address[0]
            newSocket = WebSocket(connection, i, username, address)
            newSocket.start()  # 开始线程,执行run函数
            connectionlist['connection'+str(i)] = connection
            i += 1

if __name__ == "__main__":
    server = WebSocketServer()
    server.begin()
    time.sleep(5)
