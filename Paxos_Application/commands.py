import xmlrpc.client

# PrintLog
def PrintLog(node):
    # node input should be the server node ID
    try:
        with xmlrpc.client.ServerProxy(f"http://localhost:{9000+node}", allow_none=True) as proxy:
            reply = proxy.printLog()
            return reply["all_logs"]
    except:
        pass
    return
# PrintDB
def PrintDB(node):
    try:
        with xmlrpc.client.ServerProxy(f"http://localhost:{9000+node}", allow_none=True) as proxy:
            reply = proxy.printDB()
            

            return reply
    except:
        pass
    return

# PrintStatus
# A: Accepted, C: Commited, E: Executed, X: No Status
def PrintStatus(seq_num=1):
    status = ['X', 'X', 'X', 'X', 'X']
    for node in range(1,6):
        try:
            with xmlrpc.client.ServerProxy(f"http://localhost:{9000+node}", allow_none=True) as proxy:
                reply = proxy.printStatus()

                status[node-1]= reply[str(seq_num)]
        except:
            pass
    return status

    

# for killing and reviving the nodes or leader failure
def fail_node(node_id):
    try:
        with xmlrpc.client.ServerProxy(f"http://localhost:{9000+node_id}", allow_none=True) as proxy:
            proxy.node_failure()
    except:
        pass

def fail_leader(node_id):
    try:
        with xmlrpc.client.ServerProxy(f"http://localhost:{9000+node_id}", allow_none=True) as proxy:
            proxy.leader_failure()
    except:
        pass

def revive(node_id):
    try:
        with xmlrpc.client.ServerProxy(f"http://localhost:{9000+node_id}", allow_none=True) as proxy:
            proxy.revive_node()
    except:
        pass