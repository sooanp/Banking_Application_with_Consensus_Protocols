from multiprocessing import Process
import xmlrpc.client
import asyncio
import time
from client_nodes import ClientNode
from server_nodes import ServerNode
from input_parser import parse_csv_file
from sqlitedict import SqliteDict



def run_server_node(node_id: int):
    node = ServerNode(node_id)
    node.initialize_server()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass


def generate_server_nodes(n):
    processes = []
    for i in range(1, n + 1):
        p = Process(target=run_server_node, args=(i,))
        p.start()
        processes.append(p)
    return processes


async def async_send(client: ClientNode, txn, sem: asyncio.Semaphore):
    # global semaphore ensures only one RPC runs at a time across all clients
    async with sem:
        return await asyncio.to_thread(client.send_to_server, txn)



def initialize_clients(num=9000):
    return {client_id: ClientNode(client_id) for client_id in range(1, num + 1)}


def fail(node_id: int):
    proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+node_id}", allow_none=True)
    proxy.fail_node()


def revive(node_id: int):
    proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+node_id}", allow_none=True)
    proxy.recover_node()

def initialize_db():
    for j in range(1,10):
        db = SqliteDict(f"bank_db_{j}.sqlite", autocommit=False)
        db.clear()
        for i in range(0, 3001):
            db[((j-1) // 3)*3000 + i] = 10
        db.commit()
    db.close()

async def main():

    server_processes = generate_server_nodes(9)
    time.sleep(1)

    clients = initialize_clients()
    sem = asyncio.Semaphore(1)


    def restart_system():

        nonlocal server_processes, clients
        print("\n========== RESTARTING SYSTEM ==========")

        for p in server_processes:
            p.terminate()
        for p in server_processes:
            p.join()

        initialize_db()

        server_processes = generate_server_nodes(9)
        time.sleep(1)

        clients = initialize_clients()

        sem = asyncio.Semaphore(1)

        print("========== SYSTEM RESTARTED ==========\n")
    

    print("===================== Starting Bank Application ==========================")
    
    sequences = parse_csv_file()
    

    for seq in sequences:
        total_t = 0
        TOTAL = 0
        all_txns = []
        tasks = seq[1]
        live_nodes = [int(node[-1]) for node in seq[2]]

        for ln in live_nodes:
            revive(ln)
        for fn in range(1,10):
            if fn not in live_nodes:
                fail(fn)
        print(f"\nTo proceed to next test set, press Enter")
        input()
        restart_system()
        start_t = time.time()
        for task in tasks:

            if len(task) > 1:
                # write request
                s = int(task[0])
                r = int(task[1])
                amt = int(task[2])
                txn = (s, r, amt)
                TOTAL += 1

            elif len(task) == 1:

                if task[0][0] == "F":
                    # fail node
                    node_id = int(task[0][-1])
                    fail(node_id)

                elif task[0][0] == "R":
                    # revive node
                    node_id = int(task[0][-1])
                    revive(node_id)
                else:
                    s = int(task[0])
                    txn = (s)
                    TOTAL += 1

            client = clients[s]
            all_txns.append(asyncio.create_task(async_send(client, txn, sem)))

        results = await asyncio.gather(*all_txns, return_exceptions=True)
        end_t = time.time()
        total_t += (end_t - start_t)

        



    


    print("All transactions processed.")
    actual_results = []
    for clin in clients.values():
        actual_results.extend(clin.all_received_reply.values())

    
    valid_replies = 0
    for r in actual_results:
        if isinstance(r, dict) and r.get("type") == "REPLY":
            valid_replies += 1
    

    def Performance():
        all_latencies = []
        for cli in clients.values():
            all_latencies.extend(cli.latencies)
        throughput = valid_replies / total_t
        avg_latency = sum(all_latencies) / len(all_latencies) * 1000
        print("Throughput: ", throughput)
        print("Average latency: ", avg_latency, "ms")
    Performance()
    print("Total transactions sent: ", TOTAL)
    print("Valid replies: ", valid_replies)
    print("Total time for all transactions: ", total_t)
    
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:

        for p in server_processes:
            p.terminate()
        for p in server_processes:
            p.join()



if __name__ == "__main__":
    initialize_db()
    asyncio.run(main())
