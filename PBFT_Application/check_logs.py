import xmlrpc.client


db_list = [1] # test case 1
# db_list = [5,6] # test case 2
# db_list = [2,5] # test case 3
# db_list = [1,6] # test case 4
# db_list = [2,6] # test case 5
# db_list = [6,7] # test case 6
# db_list = [1,3] # test case 7
# db_list = [4,5] # test case 8
# db_list = [1,3] # test case 9
# db_list = [1,7] # test case 10
print("DBs \n")
for i in db_list:
    """TestCase 1: n1

        TestCase 2: n5, n6

        TestCase 3: n2, n5

        TestCase 4: n1, n6

        TestCase 5: n2, n6

        TestCase 6: n6, n7

        TestCase 7: n1, n3

        TestCase 8: n4, n5

        TestCase 9: n1, n3

        TestCase 10: n1, n7
        """
    try:
        proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+i}", allow_none=True)
        reply = proxy.printDB()
        print(f"[Server {i}]: {reply}")
    except Exception as e:
        print()


print("PrintView: ")
replica_id = 1
proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+replica_id}", allow_none=True)
reply = proxy.printView()
print(reply['actual_new_view'])
print()

print("Seq Num Status for 4\n")
for i in range(1,8):
    try:
        proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+i}", allow_none=True)
        reply = proxy.printStatus(4)
        print(f"[Server {i}]: {reply}")
    except Exception as e:
        print()


# byzantine:
# Test case 1: none
# Test case 2: none
# Test case 3: n2
# byz_node = [2]
# # Test case 4: n1
# # Test case 5: n1
# # Test case 6: n1
# byz_node = [1]
# # Test case 7: n3
# byz_node = [3]
# # Test case 8: n1
# byz_node = [1]
# # Test case 9: n1, n2
# byz_node = [1,2]
# # Test case 10: n7
# byz_node = [7]

# log for byz_nodes
print("=============================== Log for Byzantine Node(s) =====================================")
# for replica_id in byz_node:
#     proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+replica_id}", allow_none=True)
#     reply = proxy.printLog()
#     print(reply['logs'])
