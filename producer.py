from flask import Flask, request
import requests
app = Flask(__name__)

@app.route("/")
def send_msg():
    f = open('leaders.txt')
    data = f.read()
    if(data=='100'):
        port_num = 5000
    elif(data=='010'):
        port_num = 5001
    elif(data=='001'):
        port_num = 5002
    msg = input("Enter your message = ")
    msg_json = {"message":msg}
    port_num = "http://localhost:" + str(port_num) + "/test" 
    res = requests.post(port_num,json=msg_json)
    return str(res)

if __name__ == "__main__":
    app.run(host="localhost", port=5003, debug=True)


    
