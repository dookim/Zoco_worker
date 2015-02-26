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
from sqlalchemy.exc import IntegrityError
from model import *;
import hashlib
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

state_selling = 'selling';
state_sold = 'sold'
email = 'email';
isbn = 'isbn';
author = 'author';
ori_price = 'ori_price';
price = 'price';
scribble = 'scribble';
check_answer = 'check_answer'
have_answer = 'have_answer'
img_str = 'img_str'
title = 'title'

def save_user(data_body):
    password = data_body['password']
    enc = hashlib.sha1()
    enc.update(password)
    password=enc.hexdigest()
    session.add(User(email=data_body['email'], univ=data_body['univ'],provider=data_body['provider'],password=password));
    session.flush();
#값의 변동을 유지하는 함수
def change_boolean(data_body):
    if data_body[scribble] == True:
        data_body[scribble] = 1
    else :
        data_body[scribble] = 0

    if data_body[check_answer] == True:
        data_body[check_answer] = 1
    else :
        data_body[check_answer] = 0

    if data_body[have_answer] == True:
        data_body[have_answer] = 1
    else :
        data_body[have_answer] = 0

def save_book(data_body):
    #change_boolean(data_body);

    #in order to maintain one transacetion, we should use one flush method
    book=Book(
        title=data_body[title],
	email=data_body[email],
        isbn=data_body[isbn],
        author=data_body[author],
        ori_price=data_body[ori_price],
        price=data_body[price],
        scribble=data_body[scribble],
        check_answer=data_body[check_answer],
        have_answer=data_body[have_answer],
        state=state_selling
        )
    session.add(book);
    session.flush();
    #book image 저장
    bookimage = Bookimage(
        isbn=data_body[isbn],
	bookimage=base64.b64decode(data_body['img_str'])
        )
    session.add(bookimage);
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

    print data_body
    try:
        if tag == 'register':
            save_user(data_body);

        elif tag == 'register_book':
            save_book(data_body);
    except IntegrityError as e:
         print e;
	 return
    except Exception as e:
         print e;
	 return;

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

