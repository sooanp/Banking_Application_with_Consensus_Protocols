from multiprocessing import Process
from server_nodes import ServerNode
from client_nodes import ClientNode
from input_parser import parse_csv_file
import xmlrpc.client
import time
import threading
import queue
import sys




def run_server_node(node_id):
    node = ServerNode(node_id)

    node.initialize_server()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass

def run_client_node(node_id):
    names = {1:"A", 2:"B", 3:"C", 4:"D", 5:"E", 6:"F", 7:"G", 8:"H", 9:"I", 10:"J"}
    node = ClientNode(names[node_id], node_id)
    node.initialize_server()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass

def restart_system():
    global server_processes, client_processes

    if server_processes:
        for p in server_processes:
            if p.is_alive():
                p.terminate()
                p.join()
        server_processes = []

    if client_processes:
        for p in client_processes:
            if p.is_alive():
                p.terminate()
                p.join()
        client_processes = []

    server_processes = generate_nodes(7)
    time.sleep(0.1)
    client_processes = generate_nodes(10, node_type="client")
    time.sleep(3)

    for i in range(1,11):
        try:
            proxy = xmlrpc.client.ServerProxy(f"http://localhost:{9000+i}", allow_none=True)
            proxy.get_public_keys()
        except Exception as e:
            print(f"Error initializing public keys for client {i}: {e}")

    for i in range(1,8):
        try:
            proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+i}", allow_none=True)
            proxy.exchange_replica_keys()
        except Exception as e:
            print(f"Error initializing public keys for server {i}: {e}")

def generate_nodes(n, node_type="server"):
    processes = []
    for i in range(1,n+1):
        if node_type == "server":
            p = Process(target=run_server_node, args=(i,))
        else:
            p = Process(target=run_client_node, args=(i,))
        p.start()
        processes.append(p)
    return processes


if __name__ == "__main__":

    server_processes = generate_nodes(7)
    print("Launched 7 server nodes...")
    # time.sleep(0.1)

    
    server_names = {"n1": 1, "n2":2, "n3": 3, "n4": 4, "n5": 5, "n6": 6, "n7": 7}


    client_names = {"A":1, "B":2, "C":3, "D":4, "E":5, "F":6, "G":7, "H":8, "I":9, "J":10}

    client_processes = generate_nodes(10, node_type="client")

    print("Initializing 10 clients...")
    restart_system()

    time.sleep(3)

    print("Exchanging public keys between all Nodes...")
    for i in range(1,11):
        try:
            proxy = xmlrpc.client.ServerProxy(f"http://localhost:{9000+i}", allow_none=True)
            proxy.get_public_keys()
        except Exception as e:
            print(f"Error initializing public keys for client {i}: {e}")
    
    for i in range(1,8):
        try:
            proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+i}", allow_none=True)
            proxy.exchange_replica_keys()
        except Exception as e:
            print(f"Error initializing public keys for server {i}: {e}")
    

    all_nodes = ['n1', 'n2', 'n3', 'n4', 'n5', 'n6', 'n7']
    test_set = parse_csv_file()
    for single_set in test_set: # will be (setNum, List: Transactions, List: LiveNodes, List: ByzNodes, List: ByzType)
        set_sequence_num = single_set[0]
        if int(set_sequence_num) < 9:
            continue
        transactions = single_set[1]
        live_nodes = single_set[2]
        byz_nodes = single_set[3]
        byz_types = single_set[4]
        print(f"====================Current set number: {set_sequence_num}==============================================")
        time.sleep(2.0)

        
        # Disconnect not-included nodes first
        for server in all_nodes:
            if server not in live_nodes:
                proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+server_names[server]}", allow_none=True)
                proxy.exclude_node()
        time.sleep(2.0)
        
        # Setup Byzantine
        for attack in byz_types:
            for malicious_node in byz_nodes:
                if attack == 'crash':
                    proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+server_names[malicious_node]}", allow_none=True)
                    proxy.crash_attack()
                if attack == 'time':
                    proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+server_names[malicious_node]}", allow_none=True)
                    proxy.timing_attack()
                if attack == 'sign':
                    proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+server_names[malicious_node]}", allow_none=True)
                    proxy.invalid_signature_attack()
                if attack.startswith('dark'): # 'dark(n6)'
                    victim_nodes = []
                    if 'n1' in attack:
                        victim_nodes.append(server_names['n1'])
                    if 'n2' in attack:
                        victim_nodes.append(server_names['n2'])
                    if 'n3' in attack:
                        victim_nodes.append(server_names['n3'])
                    if 'n4' in attack:
                        victim_nodes.append(server_names['n4'])
                    if 'n5' in attack:
                        victim_nodes.append(server_names['n5'])
                    if 'n6' in attack:
                        victim_nodes.append(server_names['n6'])
                    if 'n7' in attack:
                        victim_nodes.append(server_names['n7'])
                    proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+server_names[malicious_node]}", allow_none=True)
                    proxy.in_dark_attack(victim_nodes)
                if attack.startswith('equivocation'): # 'equivocation(n6,n7)'
                    proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+server_names[malicious_node]}", allow_none=True)
                    victim_nodes = []
                    if 'n1' in attack:
                        victim_nodes.append(server_names['n1'])
                    if 'n2' in attack:
                        victim_nodes.append(server_names['n2'])
                    if 'n3' in attack:
                        victim_nodes.append(server_names['n3'])
                    if 'n4' in attack:
                        victim_nodes.append(server_names['n4'])
                    if 'n5' in attack:
                        victim_nodes.append(server_names['n5'])
                    if 'n6' in attack:
                        victim_nodes.append(server_names['n6'])
                    if 'n7' in attack:
                        victim_nodes.append(server_names['n7'])
                    proxy.equivocation_attack(victim_nodes)
        time.sleep(3.0)
        print(f"==================================================================")

        for transaction in transactions:
            op = (transaction[0], transaction[1], int(transaction[2])) if len(transaction) > 1 else (transaction[0])
            proxy = xmlrpc.client.ServerProxy(f"http://localhost:{9000+client_names[op[0]]}", allow_none=True)
            print(f"Client sending transaction {op} to primary server")
            proxy.send_to_server(op)


        


        input()
        time.sleep(1.5)
        print(f"====================Current Client Balance: ==============================================")
        for i in range(1,8):
            try:
                proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+i}", allow_none=True)
                reply = proxy.printDB()
                print(f"[Server {i}]: {reply}")
            except Exception as e:
                print(f"Error printing DB for server {i}: {e}")
        
        print(f"==================================================================")



        print(f"\nTo proceed to next test set and restart the system, press Enter again to continue...")
        input()
        restart_system()



