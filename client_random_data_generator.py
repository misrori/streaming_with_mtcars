#!/home/mihaly/hadoop/anaconda3/bin/python
import socket

soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
soc.connect(("localhost", 9000))

#clients_input = input("What you want to proceed my dear client?\n")  
#soc.send(clients_input.encode("utf8")) # we must encode the string to bytes  
i = 0
while i<1000:
    result_bytes = soc.recv(4096) # the number means how the response can be in bytes  
    result_string = result_bytes.decode("utf8") # the return will be in bytes, so decode
    print(result_string)  
    i+=1


