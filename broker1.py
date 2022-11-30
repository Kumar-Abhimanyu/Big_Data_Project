from flask import Flask, request, jsonify
import os
import json
from time import strftime
import shutil
import requests
import socket

# Broker 1

app = Flask(__name__)

def I_am_leader():
    f = open('leaders.txt')
    data = f.read()
    f.close()
    return data == '100'

@app.after_request
def after_request(response):
    timestamp = strftime('[%Y-%b-%d %H:%M]')
    f = open(os.getcwd() + '\\Data\\Broker1\\' + 'log.txt', 'a')
    f.write(' '.join([timestamp, request.host, request.remote_addr, request.method, request.scheme, request.full_path, response.status]) + '\n')
    f.close()
    return response

@app.get('/poll')
def confirm_running_status():
    return '1'

def insert_data(folder_path, data):

    list_of_partitions = [name for name in os.listdir(folder_path)]
    print(list_of_partitions)
    no_of_partitions = len(list_of_partitions)
    print("No of partitions: ", no_of_partitions)

    if (no_of_partitions == 0):
        no_of_partitions += 1
    
    i = 0
    
    folder_path += '\\'
    
    f = open(folder_path + f'p{no_of_partitions}.txt', 'a+')
    f.seek(0)
    in_file_data_str = f.read()
    f.truncate(0)
    in_file_data = []

    if (in_file_data_str != ''):
        in_file_data = json.loads(in_file_data_str)

    while (i < len(data) and len(in_file_data) < 10):
        in_file_data.append(data[i])
        i += 1
    
    while (i < len(data)):
        f.write(json.dumps(in_file_data))
        f.close()
        no_of_partitions += 1
        f = open(folder_path + f'p{no_of_partitions}.txt', 'w')
        in_file_data = []
        while (i < len(data) and len(in_file_data) < 10):
            in_file_data.append(data[i])
            i += 1

    f.write(json.dumps(in_file_data))
    f.close()
    return 'success'


@app.route('/topic_data', methods = ["POST"])
def topic_data():
    # request should contain three fields - producer_id, topic, and data
    # it would help if the consumer id is the same as its port number
    obj = request.json
    print(obj)

    topic = obj["topic"]
    data = obj["data"]
    producer_id = obj["producer_id"]

    # register the producer
    f = open('active_producers.txt', 'a+')
    f.seek(0)
    active_producers = f.read()
    f.seek(0)
    f.truncate()
    list_of_active_producers = []
    if (active_producers != ''):
        list_of_active_producers = json.loads(active_producers)
    
    if producer_id not in list_of_active_producers:
        list_of_active_producers.append(producer_id)

    f.write(json.dumps(list_of_active_producers))
    f.close()

    print("Consumer registered")

    if (I_am_leader()):
        data_to_send = {'topic' : topic, 'data' : data, "producer_id" : producer_id}
        headers = {'Content-type' : 'application/json'}
        sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result1 = sock1.connect_ex(('127.0.0.1', 5001))
        if result1 == 0:
            requests.post('http://localhost:5001/topic_data', json = data_to_send, headers = headers)
        sock1.close()
        sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result2 = sock2.connect_ex(('127.0.0.1', 5002))
        if result2 == 0:
            requests.post('http://localhost:5002/topic_data', json = data_to_send, headers = headers)
        sock2.close()

        # we can probably send the data to the consumers (if any) here
        f = open('active_consumers.txt', 'r')
        active_consumers = f.read()
        if (active_consumers != ''):
            active_consumers = json.loads(active_consumers)
            for consumer, subscribed_topic in active_consumers:
                if (subscribed_topic == topic):
                    data_to_send = {"data": data, "topic" : topic}
                    headers = {'Content-Type' : 'application/json'}
                    # check if port is free before sending
                    requests.post('http://localhost:%s/receive_extra_data'%consumer, json = data_to_send, headers = headers)

        f.close()

    topic_folder_path = os.getcwd() + '\\Data\\Broker1\\' + topic
    print(topic_folder_path)

    if (not os.path.isdir(topic_folder_path)):
        os.mkdir(topic_folder_path)

    return insert_data(topic_folder_path, data)

@app.route('/delete_topic', methods = ["POST"])
def delete_topic():
    topic_to_delete = request.json['topic_to_delete']

    if (I_am_leader()):
        data_to_send = {'topic_to_delete' : topic_to_delete}
        headers = {'Content-type' : 'application/json'}
        sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result1 = sock1.connect_ex(('127.0.0.1',5001))
        if result1 == 0:
            requests.post('http://localhost:5001/delete_topic', json = data_to_send, headers = headers)
        sock1.close()
        sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result2 = sock2.connect_ex(('127.0.0.1',5002))
        if result2 == 0:
            requests.post('http://localhost:5002/delete_topic', json = data_to_send, headers = headers)
        sock2.close()

    topic_folder_path = os.getcwd() + '\\Data\\Broker1\\' + topic_to_delete
    if (not os.path.isdir(topic_folder_path)):
        return 'Topic Does Not Exist'
    else:
        shutil.rmtree(topic_folder_path)
        return 'Topic Deleted'

@app.route('/dereg_producer', methods = ["POST"])
def dereg_producer():
    # request should contain the id of the producer that wants to be de-registered
    obj = request.json
    producer_id = obj["producer_id"]

    f = open('active_producers.txt', 'r+')
    producers = json.loads(f.read())
    print(producers)
    producers = [element for element in producers if element != producer_id]
    f.seek(0)
    f.truncate()
    f.write(json.dumps(producers))
    f.close()
    return 'Success'

def get_all_data_from_topic(folder_path):
    data_to_send = []
    list_of_partitions = [name for name in os.listdir(folder_path)]

    for file in list_of_partitions:
        f = open(folder_path + '\\' + file, 'r')
        data_from_file = json.loads(f.read())
        for element in data_from_file:
            data_to_send.append(element)
        f.close()
    
    return data_to_send


@app.route('/register_consumer', methods = ["POST"])
def register_consumer():
    # request sould contain the id of the consumer and the topic they are registering for
    # it would help if the request id of the consumer is the same as its port number
    obj = request.json
    consumer_id = obj["consumer_id"]
    topic = obj["topic"]
    from_beginning = obj["from_beginning"]
    print("Inside /register_consumer")
    # register the consumer
    f = open('active_consumers.txt', 'a+')
    f.seek(0)
    consumers = json.loads(f.read())
    f.seek(0)
    f.truncate()
    consumers.append((consumer_id, topic))
    f.write(json.dumps(consumers))
    f.close()

    print("Consumer Registered")

    # send data from the topic to the consumer
    folder_path = os.getcwd() + '\\Data\\Broker1\\' + topic
    if (os.path.isdir(folder_path) and from_beginning == 'True'):
        data = get_all_data_from_topic(folder_path)
        data_to_send = {"data": data}
        headers = {'Content-Type' : 'application/json'}
        print(data_to_send)
        return json.dumps(data_to_send)
        # requests.post('http://localhost:%s/sub/output'%consumer_id, json = data_to_send, headers = headers)
        # print("post request sent")
    else:
        os.mkdir(folder_path)

    return 'Success'

@app.route('/unsub', methods = ["POST"])
def unsub():
    # request contains consumer_id
    consumer_id = request.json["consumer_id"]
    print(consumer_id)
    print("Inside /unsub")

    f = open('active_consumers.txt', 'a+')
    f.seek(0)
    consumers = json.loads(f.read())
    f.seek(0)
    f.truncate()
    consumers = [element for element in consumers if element[0] != consumer_id]
    print(consumers)
    f.write(json.dumps(consumers))
    f.close()

    return 'Success'

if (__name__ == '__main__'):
    app.run(debug = True, port = 5000)