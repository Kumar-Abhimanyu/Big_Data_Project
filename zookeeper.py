import socket
from time import sleep

# sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# result = sock.connect_ex(('127.0.0.1',5000))
# if result == 0:
#    print("Port is open")
# else:
#    print("Port is not open")
# sock.close()
f = open('leaders.txt',"r")
leader = f.read()
f.close()

while True:
    sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result1 = sock1.connect_ex(('127.0.0.1',5000))
    if result1:
        if leader == "100":
            leader = "010"
            f = open('leaders.txt',"w")
            f.write(leader)
            f.close()
    sock1.close()
    print(leader)

    sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result2 = sock2.connect_ex(('127.0.0.1',5001))
    if result2:
        if leader == "010":
            leader = "001"
            f = open('leaders.txt',"w")
            f.write(leader)
            f.close()
    sock2.close()
    print(leader)

    sock3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result3 = sock3.connect_ex(('127.0.0.1',5002))
    if result3:
        if leader == "001":
            leader = "100"
            f = open('leaders.txt',"w")
            f.write(leader)
            f.close()
    sock3.close()
    print(leader)
    sleep(5)
