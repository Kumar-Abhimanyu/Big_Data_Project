from flask import Flask, request, render_template, redirect
import requests
import json
import logging

port = 3000
leader_port = 5000
headers = {'Content-Type' : 'application/json'}

app = Flask(__name__)

@app.route('/')
def index():
    return render_template("index_p.html")

@app.route('/unsub', methods= ["GET", "POST"])
def unsubscribe():
    data = {'producer_id':f'{port}'}
    requests.post('http://localhost:%d/dereg_producer'%leader_port,json = data, headers=headers)
    return render_template("unsub_producer.html")

@app.route('/unsub/serv',methods=["GET","POST"])
def unsub_serv():
    topic = request.form["topic"]
    data = {'topic':topic}
    requests.post('http://localhost:%d/delete_topic'%leader_port,json = data, headers= headers)
    return "Topic Deleted"

@app.route('/send_data', methods=["GET","POST"])
def send_data():
    return render_template("data_send.html")

@app.route('/send_data/serv',methods = ["GET","POST"])
def data_send_serv():
    topic = request.form["topic"]
    data = request.form["data"]
    data_to_send = {'topic':topic,'data':data,'producer_id':port}
    requests.post('http://localhost:%d/topic_data'%leader_port,json = data_to_send,headers=headers)
    return "Data Sent"


app.run()