from multiprocessing import Process
from server_nodes import ServerNode
from client_nodes import ClientNode
from input_parser import parse_csv_file
import xmlrpc.client
import commands
import time
import threading
import queue
import sys


def wait_for_enter(q):
    input()
    q.put("next")

def run_transactions(transactions, clients, names):
    q = queue.Queue()
    t = threading.Thread(target=wait_for_enter, args=(q,))
    t.daemon = True
    t.start()

    for transaction in transactions:

        if not q.empty():
            print("\n ================ Skipping to next sequence ===========\n")
            break

        if len(transaction) == 3:
            reformatted = (transaction[0], transaction[1], int(transaction[2]))
            sender = clients[names[transaction[0]]]
            reply = sender.send_request(transaction=reformatted)
        elif transaction[0] == "LF":
            print("\n\n=================== LF OCCURRED ======================\n\n")
            for node in nodes_set:
                commands.fail_leader(server_names[node])
        time.sleep(0.05)

def run_node(node_id):
    node = ServerNode(node_id)
    # print(node.info())
    node.initialize_server()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass

def generate_nodes(n):
    processes = []
    for i in range(1,n+1):
        p = Process(target=run_node, args=(i,))
        p.start()
        processes.append(p)
    return processes



if __name__ == "__main__":

    processes = generate_nodes(5)
    print("Launched 5 server nodes...")
    time.sleep(0.1)  #


    names = {"A":0, "B":1, "C":2, "D":3, "E":4, "F":5, "G":6, "H":7, "I":8, "J":9}
    server_names = {"n1": 1, "n2":2, "n3": 3, "n4": 4, "n5": 5}
    nodes_set = ["n1", "n2", "n3", "n4", "n5"]
    clients = []
    for name in names:
        client = ClientNode(name=name, id=names[name])
        clients.append(client)
    print("Initialized 10 clients.")



    all_sequence = parse_csv_file()
    
    for sequence_set in all_sequence:
        test_sequence_num = sequence_set[0]
        transactions = sequence_set[1]
        node_participation = sequence_set[2]
        # if int(test_sequence_num) <=5:
            # continue
        user_input = input(f"Simply press Enter key to resume the transactions on test sequence number: {test_sequence_num} \n")



        # disconnect the nodes NOT participating in current set
        for node in nodes_set:
            if node not in node_participation:
                commands.fail_node(server_names[node])
            if node in node_participation:
                commands.revive(server_names[node])


                


        run_transactions(transactions, clients, names)
        # for transaction in transactions:
        #     if len(transaction) == 3:
        #         reformatted = (transaction[0], transaction[1], int(transaction[2]))
        #         sender = clients[names[transaction[0]]]
        #         reply = sender.send_request(transaction=reformatted)
        #     elif transaction[0] == "LF": # leader failure
        #         print("\n\n=================== LF OCCURRED ======================\n\n")
        #         # Just send to fail the leader to all participating nodes in current set
        #         for node in nodes_set:
        #             commands.fail_leader(server_names[node])


        print(f"========== Test Sequence {test_sequence_num} transaction done. Printing DB for each nodes ==========")
        for node in nodes_set:
            print(f"[Node {node}]'s client balance DB log: ")
            print(commands.PrintDB(server_names[node]))

      



    # all_transactions = parse_csv_file()
    # i = 0
    # total = 0
    # for transaction in all_transactions:
    #     # time.sleep(0.1)
    #     i += 1
    #     if len(transaction) == 3:
    #         reformatted = (transaction[0], transaction[1], int(transaction[2]))
    #         sender = clients[names[transaction[0]]]
    #         total += 1
    #         reply = sender.send_request(transaction=reformatted)
    #     if i == 37:
    #         processes[0].terminate()
    #     if i == 61:
    #         processes[1].terminate()
    
    # # time.sleep(25)
    # print("ALL TRANSACTIONS SENT")
    # print("total is: ", total)
    # for i in range(3,6):
    #     commands.PrintDB(i)

    # for client in clients:
    #     print(client.name)
    #     print(client.received_reply)



    # time.sleep(15)

    # # Shut everything down
    # print("\nShutting down servers...")
    # for p in processes:
    #     p.terminate()
    #     p.join()
    # print("All nodes killed.")