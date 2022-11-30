from flask import Flask,render_template,request,redirect
import requests
from time import sleep
import json
import logging
import sys

app = Flask(__name__)

port = int(sys.argv[1])

def get_leader_port():
    f = open('leaders.txt', 'r')
    leader = f.read().strip()
    if (leader == '100'):
        return 5000
    elif (leader == '010'):
        return 5001
    else:
        return 5002


leader_port = get_leader_port()
sub_list = []

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

@app.route('/')
def index():
    return render_template("index.html")

@app.route('/unsub',methods = ["GET","POST"])
def unsubscribe():
    data = {'consumer_id':f'{port}'}
    requests.post('http://localhost:%d/unsub'%leader_port,json = data)

    return render_template("unsub.html")

@app.route('/sub',methods = ["GET","POST"])
def subscribe():
    return render_template("subscribe.html")

@app.route('/sub/server',methods = ["GET","POST"])
def subscribe_server():
    topic = request.form['topic']
    data = {'topic':topic,'consumer_id':f'{port}'}
    headers = {'Content-Type' : 'application/json'}
    x = requests.post('http://localhost:%d/register_consumer'%leader_port,json = data,headers = headers)
    print(x.json()["data"])
    return 'Check Terminal'

@app.route('/receive_extra_data', methods = ["POST"])
def receive_extra_data():
    # request should contain only data and topic
    # print("Inside /receive_extra_data")
    obj = request.json
    topic = obj["topic"]
    data = obj["data"]

    print(data)

    return "Success"

@app.route('/sub/output',methods = ["POST","GET"])
def subscribe_output():
    if request.method == 'POST':
        obj = request.json
        data1 = obj['data']
        return render_template("subscribe.html", data = json.dumps(data1))

app.run(debug = True, port = port)