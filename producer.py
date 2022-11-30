from flask import Flask, request, render_template, redirect
import requests
import json
import logging
import sys

def get_leader_port():
    f = open('leaders.txt', 'r')
    leader = f.read().strip()
    if (leader == '100'):
        return 5000
    elif (leader == '010'):
        return 5001
    else:
        return 5002

port = int(sys.argv[1])
print(port)

headers = {'Content-Type' : 'application/json'}

app = Flask(__name__)

@app.route('/')
def index():
    return render_template("index_p.html")

@app.route('/unsub', methods= ["GET", "POST"])
def unsubscribe():
    leader_port = get_leader_port()
    data = {'producer_id':f'{port}'}
    requests.post('http://localhost:%d/dereg_producer'%leader_port,json = data, headers=headers)
    return render_template("unsub_producer.html")

@app.route('/unsub/serv',methods=["GET","POST"])
def unsub_serv():
    leader_port = get_leader_port()
    topic = request.form["topic"]
    data = {'topic_to_delete':topic}
    x = requests.post('http://localhost:%d/delete_topic'%leader_port,json = data, headers= headers)
    return x.text 

@app.route('/send_data', methods=["GET","POST"])
def send_data():
    return render_template("data_send.html")

@app.route('/send_data/serv',methods = ["GET","POST"])
def data_send_serv():
    topic = request.form["topic"]
    data = request.form["data"]
    leader_port = get_leader_port()
    data_to_send = {'topic':topic,'data':json.loads(data),'producer_id':f'{port}'}
    requests.post('http://localhost:%d/topic_data'%leader_port,json = data_to_send,headers=headers)
    return "Data Sent"


app.run(debug = True, port = port)