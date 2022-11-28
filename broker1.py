from flask import Flask, request, jsonify
import os
import json
from time import strftime
import shutil
import requests

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
    f.write(' '.join([timestamp, request.remote_addr, request.method, request.scheme, request.full_path, response.status]) + '\n')
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
    obj = request.json
    print(obj)

    topic = obj["topic"]
    data = obj["data"]

    if (I_am_leader()):
        data_to_send = {'topic' : topic, 'data' : data}
        headers = {'Content-type' : 'application/json'}
        requests.post('http://localhost:5001/topic_data', json = data_to_send, headers = headers)
        requests.post('http://localhost:5002/topic_data', json = data_to_send, headers = headers)

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
        requests.post('http://localhost:5001/delete_topic', json = data_to_send, headers = headers)
        requests.post('http://localhost:5002/delete_topic', json = data_to_send, headers = headers)

    topic_folder_path = os.getcwd() + '\\Data\\Broker1\\' + topic_to_delete
    if (not os.path.isdir(topic_folder_path)):
        return 'Topic Does Not Exist'
    else:
        shutil.rmtree(topic_folder_path)
        return 'Topic Deleted'

if (__name__ == '__main__'):
    app.run(debug = True, port = 5000)