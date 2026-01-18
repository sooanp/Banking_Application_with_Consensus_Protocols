import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
import threading
import time
from socketserver import ThreadingMixIn
import hashlib
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes, serialization
import json

class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

class ClientNode:
    def __init__(self, name, id):
        self.name = name
        self.id = id

        self.curr_primary = 3
        self.time_stamp = 0

        self.private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.my_public_key = self.private_key.public_key()

        self.replica_public_key_phonebook = {}

        # self.stop_event = {}
        self.client_timer = {}
        self.total_received_reply_msg = 0
        self.sent_request_type = None

        self.received_reply_msg = {} # timestamp: [msg list]

    # Upon initialization, exchange public keys with all server replicas
    def get_public_keys(self):

        client_pubkey_pem = self.my_public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode('utf-8')

        for i in range(1, 8):  # assuming servers at ports 8001–8007
            try:
                proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+i}", allow_none=True)

                proxy.receive_client_public_key(self.name, client_pubkey_pem)
                server_pubkey_pem = proxy.get_public_key()
                server_pubkey = serialization.load_pem_public_key(server_pubkey_pem.encode('utf-8'))
                self.replica_public_key_phonebook[i] = server_pubkey



            except Exception as e:
                print(f"Error exchanging public key with server {i}: {e}")
        

    def initialize_server(self):
        # 9000+id for client nodes
        server = ThreadedXMLRPCServer(("localhost", 9000+self.id), allow_none=True)
        server.register_instance(self)
        server.register_function(self.send_to_server, 'send_to_server')
        server.register_function(self.get_public_keys, 'get_public_keys')
        server.register_function(self.receive_msg, 'receive_msg')
        print(f"Server initialized and listening on port {9000+self.id}...")
        server.serve_forever()
    
    def digest_msg(self, msg):
        msg_bytes = json.dumps(msg, sort_keys=True).encode('utf-8')
        digest = hashlib.sha256(msg_bytes).digest()
        return digest
    
    def sign_msg(self, msg, private_key):
        signature = private_key.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        return signature

    def verify_signature(self, signature, computed_digest, public_key):
        try:
            public_key.verify(
                signature,
                computed_digest,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except:
            return False
        
    def send_to_server(self, operation):
        self.time_stamp += 1
        request_msg = {
            "type": "REQUEST",
            "op": operation,
            "c": self.name,
            "timestamp": self.time_stamp
        }
        if len(operation) > 1:
            self.sent_request_type = "write"
        elif len(operation) == 1:
            self.sent_request_type = "read"
        request_msg_digest = self.digest_msg(request_msg)
        request_msg_signature = self.sign_msg(request_msg_digest, self.private_key)

        full_request_msg = {
            "signature": xmlrpc.client.Binary(request_msg_signature),
            "actual_msg": request_msg
        }
        self.received_reply_msg[self.time_stamp] = []

        
        
        

        def request_time_out(full_request_msg):
            
            if len(self.received_reply_msg[full_request_msg['actual_msg']['timestamp']]) >= 3:

                if self.client_timer[full_request_msg['actual_msg']['timestamp']]:
                    self.client_timer[full_request_msg['actual_msg']['timestamp']].cancel()
                    # self.stop_event[full_request_msg['actual_msg']['timestamp']].set()
                    self.client_timer[full_request_msg['actual_msg']['timestamp']] = None
                return

            # if not self.stop_event[full_request_msg['actual_msg']['timestamp']].is_set():
            # broadcast first
            print(f"[CLIENT {self.name}] Timer expired, broadcasting message.")
            for i in range(1,8):
                try:
                    proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+i}", allow_none=True)
                    proxy.process_msg(full_request_msg)
                except Exception as e:
                    print(f"[client {self.name}] Error sending request to [Server {i}]: {e}")
            self.client_timer[full_request_msg['actual_msg']['timestamp']] = threading.Timer(1.0 + self.id * 0.2, request_time_out,[full_request_msg])
            self.client_timer[full_request_msg['actual_msg']['timestamp']].daemon = True
            self.client_timer[full_request_msg['actual_msg']['timestamp']].start()


        
        # if self.client_timer:
        #     self.client_timer.cancel()
        # self.client_timer = threading.Timer(5.0 + self.id * 0.2, request_time_out,[full_request_msg])
        # self.client_timer.daemon = True
        # self.client_timer.start()
        
        # self.stop_event.wait()
        # self.stop_event.clear()
        self.client_timer[request_msg["timestamp"]] = threading.Timer(5.0 + self.id * 0.2, request_time_out,[full_request_msg])
        self.client_timer[request_msg["timestamp"]].daemon = True
        self.client_timer[request_msg["timestamp"]].start()

        try:
            proxy = xmlrpc.client.ServerProxy(f"http://localhost:{8000+self.curr_primary}", allow_none=True)
            proxy.process_msg(full_request_msg)
        except Exception as e:
            print(f"Error sending request to server {self.curr_primary}: {e}")
        


    def receive_msg(self, msg):
        # Reply will be: {"signature": sign, "actual_msg": {"type": "REPLY", "v": v, "t": t, "c": c, "i": i, "r": result} }}
        actual_reply_msg = msg['actual_msg']
        actual_reply_msg_bytes = json.dumps(actual_reply_msg, sort_keys=True).encode('utf-8')
        replica_id = actual_reply_msg['i']
        is_reply_valid = self.verify_signature(msg['signature'].data, actual_reply_msg_bytes, self.replica_public_key_phonebook[replica_id])
        if is_reply_valid:
            print(f"[Client {self.name}] Received reply from [Server {replica_id}].\n---msg: {msg}")
            if msg['actual_msg']['v'] != self.curr_primary: # if primary is different, update primary
                self.curr_primary = msg['actual_msg']['v']
            rm_list = self.received_reply_msg[msg['actual_msg']['t']]
            if msg['actual_msg'] not in rm_list:
                self.received_reply_msg[msg['actual_msg']['t']].append(msg['actual_msg'])

        else:
            print(f"[Client {self.name}] Invalid reply signature from server")
        
        if self.sent_request_type == "write":
            if len(self.received_reply_msg[msg['actual_msg']['t']]) >= 3: # f+1
                self.client_timer[msg['actual_msg']['t']].cancel()
                # self.stop_event[msg['actual_msg']['t']].set()
                print(f"[Client {self.name}] Received {len(self.received_reply_msg[msg['actual_msg']['t']])} valid reply messages for timestamp : {msg['actual_msg']['t']}.")
        elif self.sent_request_type == "read":
            if len(self.received_reply_msg[msg['actual_msg']['t']]) >= 5:
                self.client_timer[msg['actual_msg']['t']].cancel()
                print(f"[Client {self.name}] Received {len(self.received_reply_msg[msg['actual_msg']['t']])} valid reply messages for timestamp : {msg['actual_msg']['t']}.")
                # self.stop_event.set()
