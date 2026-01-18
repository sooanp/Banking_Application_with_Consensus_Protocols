import xmlrpc.client





def printDB():
    # output should be all 9 servers
    for i in range(1,10):
        proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+i}", allow_none=True)
        reply = proxy.printDB()
        print("Server ", i, ": ",reply)

def printBalance(client_id):
    for i in range(1,10):
        proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+i}", allow_none=True)
        reply = proxy.printBalance(client_id)
        print("Server ",i, ": ",reply)

def printView(i):
    proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+i}", allow_none=True)
    reply = proxy.printView()
    print(reply)



# TestCas2 1
# print(" Result for Balance 1001")
# printBalance(1001)
# print(" Result for Balance 1002")
# printBalance(1002)
# print(" Result for Balance 3001")
# printBalance(3001)
# print(" Result for Balance 3002")
# printBalance(3002)
# print(" Result for Balance 6001")
# printBalance(6001)
# print(" Result for Balance 6002")
# printBalance(6002)



# printDB()

# Test 3
# printView(7)
# Test 7
# printView(9)

# Test8
# printView(1)
# printView(4)
# printView(6)
# printView(7)
# printView(9)