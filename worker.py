# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('UTF-8')
import pika
import json
import base64
import logging
import os
from config_mgr import get_config
from sqlalchemy import *
from sqlalchemy.orm import create_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.exc import *
from sqlalchemy import desc
from model import *;


PROJECT_DIR = os.getcwd();

##########################################init logger#######################################################
LOG_DIR = "./worker_log"
if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)

log_file_path=os.path.join(LOG_DIR, str(os.getpid())+".log")
logging.basicConfig(filename = log_file_path , level=logging.INFO)
##########################################init logger#######################################################

##########################################init rabbitmq######################################################
print "start of pika init"
credentials = pika.PlainCredentials(get_config("pika_id"), get_config("pika_pwd"))
parameters  = pika.ConnectionParameters(host=get_config("pika_ip"),
                                        port=int(get_config("pika_port")),
                                        credentials=credentials)
connection  = pika.BlockingConnection(parameters)
channel     = connection.channel()

queue_name = get_config("queue_name")
exchange_name = get_config("exchange_name")
channel.queue_declare(queue=queue_name, durable=True)
channel.queue_bind(exchange =exchange_name, queue = queue_name)
print "end of pika init"
##########################################init rabbitmq######################################################


##########################################init pidpath######################################################
#make file for identity my pid
pid_path = "/var/run/zoco-workers/"

#파일이 경로가존재하지 않으면 경로를 만든다.
if not os.path.exists(pid_path) :
    os.mkdir(pid_path)

#step 1 : get current screen name
m_argv = sys.argv
filename = None

if len(m_argv) == 1:
    filename = 'root'

else :
    filename=m_argv[1]

filename = filename + ".pid"

pid_file_path = pid_path + filename

#if file is already existed, remove this file
if os.path.exists(pid_file_path):
    os.remove(pid_file_path)

#filename을 지정해 파일 생성
pid_file=open(pid_file_path, "w+")

#create file
pid_file.write(str(os.getpid()))
pid_file.close()

print " [*] Waiting for messages. To exit press CTRL+C"

def save_login(data_body):
    session.add(User(email=data_body['email'], univ=data_body['univ']));
    session.flush();
def save_book(data_body):
    book=Book(
        email=data_body['email'],
        isbn=data_body['isbn'],
        author=data_body['author'],
        ori_price=data_body['ori_price'],
        price=data_body['price'],
        scribble=data_body['scribble'],
        check_answer=data_body['check_answer'],
        has_answer=data_body['has_answer'],
        state=data_body['selling']
        )
    session.add(Book());
    session.flush();

def callback(ch, method, properties,body):

    #해당 연산을 하면 python dictionary으로 만들어지게 된다.
    firstData = json.loads(body,encoding='utf-8')

    #tag정보와 body정보로 구분한다. tag정보는 데이터를 분류할 떄 사용한다.
    try:
        tag = firstData['tag']
        data_body = firstData['data']
    except Exception as e:
        print "cannot parsing data from firstData"
        print e
        return

    if tag == 'login':
        save_login(data_body);

    elif tag == 'receive_exception':
        save_book(data_body);

def finalize():
    connection.close()
    sys.exit(1)

if __name__ == '__main__':
    try :
        try:
            channel.basic_consume(callback, queue=queue_name, no_ack=True)
            channel.start_consuming()
        except Exception as e:
            print e
            logging.error(e)
            channel.stop_consuming()
    finally :
        finalize()