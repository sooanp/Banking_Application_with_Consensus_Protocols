import xmlrpc.client
import threading
from cluster_info import CLUSTER_1, CLUSTER_2, CLUSTER_3
import time

class ClientNode:

    def __init__(self, client_id):
        self.client_id = client_id
        self.tau = 1

        self.server_proxies = {8000 + i: xmlrpc.client.ServerProxy(f"http://localhost:{8000+i}", allow_none=True) 
                               for i in range(1, 10)}


        self.last_sent_txn = None
        self.last_tau = None

        self.cluster_1_primary = 8001
        self.cluster_2_primary = 8004
        self.cluster_3_primary = 8007


        self.all_received_reply = {}  # tau -> reply or None

        self.client_timer = {}

        self.latencies = []   
        self.start_times = {} # start_time for each txn


    def send_to_server(self, txn, retry=False):
        
        if not retry:
            self.last_sent_txn = txn
            self.last_tau = self.tau
            self.tau += 1
        

        start_t = time.time()
        self.start_times[self.tau] = start_t
        msg = {
            "type": "REQUEST",
            "txn": self.last_sent_txn,
            "tau": self.last_tau,
            "c": self.client_id
        }

        if self.last_tau not in self.client_timer or self.client_timer[self.last_tau] is None:
            timer = threading.Timer(0.3, self.broadcast_request, args=(msg,))
            timer.daemon = True
            self.client_timer[self.last_tau] = timer
            timer.start()

        target_port = self.get_primary_port(self.client_id)
        try:
            proxy = self.server_proxies[target_port]
            reply = proxy.process_msg(msg)
            end_t = time.time()
            latency = end_t - start_t

            self.latencies.append(latency)
            if reply and reply.get("type") == "REPLY":

                if self.client_timer[self.last_tau]:
                    self.client_timer[self.last_tau].cancel()
                    self.client_timer[self.last_tau] = None

                self.all_received_reply[self.last_tau] = reply

                if reply['ballot_num'][1] + 8000 != target_port:
                    new_primary = reply['ballot_num'][1] + 8000
                    if self.client_id in CLUSTER_1:
                        self.cluster_1_primary = new_primary
                    elif self.client_id in CLUSTER_2:
                        self.cluster_2_primary = new_primary
                    elif self.client_id in CLUSTER_3:
                        self.cluster_3_primary = new_primary

                return

        except Exception as e:
            return None

    def broadcast_request(self, msg):
        cluster_ports = []
        if self.client_id in CLUSTER_1:
            cluster_ports = [8001, 8002, 8003]
        elif self.client_id in CLUSTER_2:
            cluster_ports = [8004, 8005, 8006]
        elif self.client_id in CLUSTER_3:
            cluster_ports = [8007, 8008, 8009]


        for port in cluster_ports:
            try:
                proxy = self.server_proxies[port]
                reply = proxy.process_msg(msg)
                if reply and reply.get("type") == "REPLY":
                    if self.client_timer[msg['tau']]:
                        self.client_timer[msg['tau']].cancel()
                        self.client_timer[msg['tau']] = None

                    self.all_received_reply[msg['tau']] = reply

                    if reply['ballot_num'][1] + 8000 != port:
                        new_primary = reply['ballot_num'][1] + 8000
                        if self.client_id in CLUSTER_1:
                            self.cluster_1_primary = new_primary
                        elif self.client_id in CLUSTER_2:
                            self.cluster_2_primary = new_primary
                        elif self.client_id in CLUSTER_3:
                            self.cluster_3_primary = new_primary

                    return reply

            except Exception as e:
                continue

    def get_primary_port(self, client_id):
        if client_id in CLUSTER_1:
            return self.cluster_1_primary
        elif client_id in CLUSTER_2:
            return self.cluster_2_primary
        elif client_id in CLUSTER_3:
            return self.cluster_3_primary
