import xmlrpc.client
import threading
import time

class ClientNode:
    def __init__(self, name, id):
        self.name = name
        self.id = id
        self.local_clock = 0
        self.curr_leader_id = 1

        self.stop_event = threading.Event()


        self.received_reply = {}

    def send_request(self, transaction):
        self.stop_event.clear()
        self.update_time()
        tau = self.local_clock

        msg = {
            "type": "REQUEST",
            "transaction": transaction,
            "tau": tau,
            "client": self.name
        }

        def broadcast_all_nodes():
            while not self.stop_event.is_set():
                print(f"[Client {self.name}] timeout for tau={tau}, broadcasting to all nodes...")
                for nid in range(1, 6):
                    if self.stop_event.is_set():
                        return
                    try:
                        with xmlrpc.client.ServerProxy(f"http://localhost:{9000+nid}", allow_none=True) as proxy:
                            r = proxy.process_message(msg)

                            if r:
                                self.received_reply[tau] = r
                                self.curr_leader_id = r["ballot"][1]
                                self.stop_event.set()
                                return
                    except:
                        continue
                time.sleep(2.0 + self.id * 0.3)

        try:
            with xmlrpc.client.ServerProxy(f"http://localhost:{9000+self.curr_leader_id}", allow_none=True) as proxy:
                r = proxy.process_message(msg)
                if r:
                    self.received_reply[tau] = r
                    self.curr_leader_id = r["ballot"][1]
                    self.stop_event.set()
        except:
            pass

        def leader_timeout():
            if not self.stop_event.is_set():

                threading.Thread(target=broadcast_all_nodes, daemon=True).start()

        leader_timer = threading.Timer(2.0 + self.id * 0.2, leader_timeout)
        leader_timer.daemon = True
        leader_timer.start()

        waited = 0
        while not self.stop_event.is_set() and waited < 60:
            time.sleep(0.01)
            waited += 0.05

        leader_timer.cancel()
        self.stop_event.set()

        if tau in self.received_reply:
            print(f"[Client {self.name}] final reply for tau={tau}: {self.received_reply[tau]}")
            return self.received_reply[tau]
        else:
            print(f"[Client {self.name}] received nothing for tau={tau}")
            return None


    def update_time(self):
        self.local_clock += 1
