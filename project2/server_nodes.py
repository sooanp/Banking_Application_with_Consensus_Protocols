from xmlrpc.server import SimpleXMLRPCServer
from socketserver import ThreadingMixIn
import hashlib
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes, serialization
import json
import xmlrpc.server
import xmlrpc.client
import time
import threading
import queue



class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


class ServerNode:
    def __init__(self, id):
        self.id = id
        self.client_phonebook = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5,
                                 "F": 6, "G": 7, "H": 8, "I": 9, "J": 10}
        
        self.client_balance = {"A": 10, "B": 10, "C": 10, "D": 10, "E": 10,
                                 "F": 10, "G": 10, "H": 10, "I": 10, "J": 10}

        self.private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.my_public_key = self.private_key.public_key()

        self.replica_public_key_phonebook = {}
        self.client_public_key_phonebook = {}

        self.all_logs = ''
        self.accepted_logs = {} # key: (view, seq_num, msg_type), value: msg
        self.all_status = {} # key: (view, seq_num), value: status
        self.client_request_status = {} # key: (c, t), val: status
        self.all_reply_to_client = {} # key: (c,t), val: reply_msg

        self.curr_view = 1
        self.seq_num = 1


        self.backup_replica_waiting = False
        self.timer = None # timer is going to ONLY run on backup servers
        self.pending_requests = queue.Queue()

        # holder variables for checkpoints
        self.received_checkpoint_msgs = []
        self.stable_checkpoint = 0
        self.stable_checkpoint_proof = []

        # view-change holders
        self.received_view_change_msgs = []
        self.view_change_phase = False
        self.all_new_view_msg = []

        self.excluded = False

        self.invalid_sign = False
        self.is_crashed = False
        self.to_not_send_id = []
        self.delay_t = 0.00 # 10ms-70ms when timing attack activated.
        self.equivocation_replica_ids = []

        self.nv_timer = None

    def initialize_server(self):
        server = ThreadedXMLRPCServer(("localhost", 8000+self.id), allow_none=True)
        server.register_function(self.process_msg, 'process_msg')
        server.register_function(self.printDB, "printDB")
        server.register_function(self.get_public_key, "get_public_key")
        server.register_function(self.receive_client_public_key, "receive_client_public_key")
        server.register_function(self.exchange_replica_keys, "exchange_replica_keys")
        server.register_function(self.printStatus, 'printStatus')
        server.register_function(self.exclude_node, 'exclude_node')
        server.register_function(self.include_node, 'include_node')
        server.register_function(self.printLog, 'printLog')
        server.register_function(self.crash_attack, 'crash_attack')
        server.register_function(self.timing_attack, 'timing_attack')
        server.register_function(self.invalid_signature_attack, 'invalid_signature_attack')
        server.register_function(self.in_dark_attack, 'in_dark_attack')
        server.register_function(self.equivocation_attack, 'equivocation_attack')
        server.register_function(self.printView, 'printView')
        print(f"Server initialized and listening on port {8000+self.id}...")
        server.serve_forever()


    def printView(self):
        return {'actual_new_view': self.all_new_view_msg[0] if self.all_new_view_msg != [] else None}
    
    def exchange_replica_keys(self):

        for i in range(1,8):
            if i != self.id:
                try:
                    with xmlrpc.client.ServerProxy(f"http://localhost:{8000+i}", allow_none=True) as proxy:
                        server_pubkey_pem = proxy.get_public_key()
                        server_pubkey = serialization.load_pem_public_key(server_pubkey_pem.encode('utf-8'))
                        self.replica_public_key_phonebook[i] = server_pubkey
                except Exception as e:
                    print("")

    def get_public_key(self):

        pub_pem = self.my_public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        return pub_pem.decode('utf-8')

    def receive_client_public_key(self, client_name, client_pub_pem):

        try:
            pub_key = serialization.load_pem_public_key(client_pub_pem.encode('utf-8'))
            self.client_public_key_phonebook[client_name] = pub_key
            return True
        except Exception as e:
            print("")
            return False

    def printDB(self):

        return self.client_balance
    
    def printStatus(self, seq_num):
        for (_, b), val in self.all_status.items():
            if b == seq_num:
                return {'status': val}
        
        return {'status': "X"} # nothing if no transaction at that sequence

    def printLog(self):

        return {"logs": self.all_logs}


    def send_msg_to_replica(self, node_id, msg):
        time.sleep(self.delay_t)
        if self.curr_view == self.id and self.is_crashed:
            # can't send anything, but to keep on listening
            self.all_logs += f'\n--[Server {self.id}] Failed to send anything! Crashed!'
            return
        else:
            try:
                with xmlrpc.client.ServerProxy(f"http://localhost:{8000+node_id}", allow_none=True) as proxy:
                    reply = proxy.process_msg(msg)
                    return reply
            except:
                pass

    def send_reply_to_client(self, client_id, msg):
        time.sleep(self.delay_t)
        if self.curr_view == self.id and self.is_crashed:
            # can't send anything, but to keep on listening
            self.all_logs += f'\n--[Server {self.id}] Failed to send anything! Crashed!'
            return
        else:
            try:
                with xmlrpc.client.ServerProxy(f"http://localhost:{9000+client_id}", allow_none=True) as proxy:
                    proxy.receive_msg(msg)

                    return
            except:
                pass
    
    def digest_msg(self, msg):
        msg_bytes = json.dumps(msg, sort_keys=True).encode('utf-8')
        digest = hashlib.sha256(msg_bytes).digest()
        return digest

    def check_digest(self, d, msg):
        msg_bytes = json.dumps(msg, sort_keys=True).encode('utf-8')
        computed_digest = hashlib.sha256(msg_bytes).digest()
        return computed_digest == d
    
    # https://cryptography.io/en/latest/hazmat/primitives/asymmetric/rsa/
    # below two functions are referenced from cryptography library documentation
    def sign_msg(self, msg, private_key):
        signature = private_key.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        if self.invalid_sign:
            return signature + b'1' # just add 1 and still will create invalid signature
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
        
    def execute_operation(self, sender, receiver, amt):

        execution_status = False
        if self.client_balance[sender] >= amt:
            self.client_balance[sender] -= amt
            self.client_balance[receiver] += amt
            execution_status = True
        else:
            execution_status = False
        return execution_status
        
    def update_sequence_number(self):
        self.seq_num = len(self.accepted_logs) + 1
        return self.seq_num

    def reset_timer(self):
        if self.timer:
            self.timer.cancel()
            self.timer = None
        self.timer = threading.Timer(0.8 + self.id * 0.08, self.start_view_change, [self.curr_view]) #
        self.timer.daemon = True
        self.timer.start()
    
    def check_pending_requests(self):
        if self.excluded:
            if self.timer:
                self.timer.cancel()
                self.timer = None
            return
        if self.id == self.curr_view:
            while self.pending_requests.qsize() > 0:
                msg = self.pending_requests.get()
                self.process_msg(msg)
        if self.id != self.curr_view:
            while self.pending_requests.qsize() > 0:
                msg = self.pending_requests.get()
                if self.client_request_status[msg['actual_msg']['c'], msg['actual_msg']['timestamp']] == "E":
                    if self.timer:
                        self.timer.cancel()
                        self.timer = None
                    continue
                self.reset_timer()
                self.process_msg(msg)
        
        if self.id != self.curr_view:
            self.timer.cancel()
            self.timer = None
        

            
    def start_view_change(self, view):
        # Function that will run when backup replica's timer has expired
        # VIEW-msg will be {'signature': sign, 'actual_msg':{<VIEW-CHANGE,v+1,n,C,P,i>}}
        # C is a set of 2f+1 valid checkpoint messages proving the correctness of s
        # P is a set containing a set Pm for each request m that prepared at i with a sequence number higher than n
        if self.timer:
            self.timer.cancel()
            self.timer = None
        if self.curr_view > view:
            return

        set_P = []
        if len(self.all_status) > 0:
            for (v,n) in self.all_status.keys():
                if n > self.stable_checkpoint and self.all_status[(v,n)] == ("PREPARED" or "COMMITTED" or "EXECUTED"):
                    print("\n\n\nself AC LP", self.accepted_logs)
                    set_P.append(self.accepted_logs[(v,n,"PP")])
        

        view_msg = {'type': "VIEW-CHANGE",
                    'v': (view % 7) + 1,
                    'n': self.stable_checkpoint, #<seq_num of last stable checkpoint>,
                    'C': [{'actual_msg':cp_msg['actual_msg']} for cp_msg in self.stable_checkpoint_proof], #<valid Checkpoint msg, to PROVE n>,
                    'P': [{'actual_msg':p_msg['actual_msg']} for p_msg in set_P],#<all_prepared_msgs>,
                    'i': self.id
                    }
        
        self.view_change_phase = True
        view_msg_bytes = json.dumps(view_msg, sort_keys=True).encode('utf-8')
        signed_view_msg = self.sign_msg(view_msg_bytes, self.private_key)
        full_view_change_msg = {'signature': xmlrpc.client.Binary(signed_view_msg), 'actual_msg': view_msg}
        self.received_view_change_msgs.append(full_view_change_msg)
        
        self.all_logs += f"\n--[Server {self.id}] Created VIEW-CHANGE message and broadcasting to all Replicas.\n--msg: {full_view_change_msg}"
        for replica_id in range(1,8):
            if replica_id == self.id or replica_id in self.to_not_send_id:
                continue
            if replica_id != self.id:
                self.send_msg_to_replica(replica_id, full_view_change_msg)

        self.reset_timer_for_new_view(view)
            
    def reset_timer_for_new_view(self,view):
        if self.nv_timer:
            self.nv_timer.cancel()
            self.nv_timer = None
        self.nv_timer = threading.Timer(3.6 + self.id * 0.08, self.start_view_change, [view+1]) # 480 ms to 960 ms
        self.nv_timer.daemon = True
        self.nv_timer.start()
    
    def send_check_point(self,s, d):
        # <CHECKPOINT,n,d,i>
        # msg will be: {'signature': sign, 'actual_msg': {<CHECKPOINT,n,d,i>}
        check_point_msg = {
            'type': 'CHECKPOINT',
            'n': s,
            'd': d,
            'i': self.id
        }
        check_point_msg_bytes = json.dumps(check_point_msg, sort_keys=True).encode('utf-8')
        signed_checpoint_msg = self.sign_msg(check_point_msg_bytes, self.private_key)
        full_checkpoint_msg = {'signature': xmlrpc.client.Binary(signed_checpoint_msg), 'actual_msg': check_point_msg}

        self.all_logs += f"\n--[Server {self.id}] Broadcasting CHECKPOINT msg to all other nodes.\n---msg: {check_point_msg}"
        for replica_id in range(1,8):
            if replica_id == self.id or replica_id in self.to_not_send_id:
                continue
            if replica_id != self.id:
                self.send_msg_to_replica(replica_id, full_checkpoint_msg)
            
        # So, to enable checkpoint msg, replica store its' last created checkpoint msg.
        # In the process_msg function, implement the case for receiving CHECKPOINT msg from other replicas.
        # store the received CHECKPOINT msg, compare with already stored CHECKPOINT msgs, if there are 2f+1 matching, it becomes stable checkpoint
        self.received_checkpoint_msgs.append(full_checkpoint_msg)


    
    def process_msg(self, msg):
        # for test case where node is not included in the test set
        if self.excluded:
            if self.timer:
                self.timer.cancel()
                self.timer = None
            return None
        if not self.excluded and not self.view_change_phase:
            # Handling REQUEST msg from client
            if msg['actual_msg']['type'] == "REQUEST": # input will be <signed(D(msg)), msg>

                if self.curr_view != self.id and not self.timer and (msg['actual_msg']['c'], msg['actual_msg']['timestamp']) in self.client_request_status:
                    self.all_logs += f"\n--[Server {self.id}] Already executed the transaction from [Client {msg['actual_msg']['c']}] with timestamp {msg['actual_msg']['timestamp']}. Replying the result."
                    self.send_reply_to_client(self.client_phonebook[msg['actual_msg']['c']], self.all_reply_to_client[(msg['actual_msg']['c'], msg['actual_msg']['timestamp'])])
                    return

                # also, timer should only start if it is not ALREADY running
                if self.curr_view != self.id and not self.timer: # meaning I am the backup, and REQUEST msg is sent directly from client braodcast
                    self.all_logs += f"\n--[Server {self.id}] Received a REQUEST msg, but I am not the leader. Relaying the message to the current primary and resetting the timer.\n---msg: {msg}"
                    self.send_msg_to_replica(self.curr_view, msg)
                    self.reset_timer()
                    return None

                if self.curr_view != self.id and self.timer and (msg['actual_msg']['c'], msg['actual_msg']['timestamp']) in self.client_request_status:

                    self.all_logs += f"\n--[Server {self.id}] Already executed the transaction from [Client {msg['actual_msg']['c']}] with timestamp {msg['actual_msg']['timestamp']}. Replying the result."
                    self.send_reply_to_client(self.client_phonebook[msg['actual_msg']['c']], self.all_reply_to_client[(msg['actual_msg']['c'], msg['actual_msg']['timestamp'])])
                    return
  
                if self.curr_view != self.id and self.timer:
                    # in this case, backup replica i is working on another execution but I received a REQUEST.
                    # either I can send this directly to primary and it will store it in its queue or I can store it in my queue.
                    # What if primary is malicious? -> do both
                    # if I do both, my code MIGHT execute same transaction twice.
                    self.pending_requests.put(msg)
                    self.send_msg_to_replica(self.curr_view, msg)
                    return None

                # if current server is working on another request, put it to queue to process request later
                if self.backup_replica_waiting:

                    self.all_logs += f"\n--[Server {self.id}] Received msg: {msg} But I am working right now. Shifting request to queue."
                    self.pending_requests.put(msg)
                    self.client_request_status[(msg['actual_msg']['c'], msg['actual_msg']['timestamp'])] = "PENDING"
                    return None
                
                    

                # msg will be: <REQUEST, o, t, c>
                computed_request_digest = self.digest_msg(msg['actual_msg'])
                is_request_valid = self.verify_signature(msg['signature'].data, computed_request_digest, self.client_public_key_phonebook[msg['actual_msg']['c']])
                if is_request_valid:
                    # process the request msg
                    timestamp = msg['actual_msg']['timestamp']

                    # exactly-once semantic: is (c, t)'s status unseen or has been processed?
                    if (msg['actual_msg']['c'], timestamp) in self.client_request_status:
                        if self.client_request_status[(msg['actual_msg']['c'], timestamp)] == "E":

                            self.all_logs += f"\n--[Server {self.id}] Already executed the transaction from [Client {msg['actual_msg']['c']}] with timestamp {timestamp}. Replying the result."
                            self.send_reply_to_client(self.client_phonebook[msg['actual_msg']['c']], self.all_reply_to_client[(msg['actual_msg']['c'], timestamp)])
                            self.backup_replica_waiting = False
                            return
                        else:
                            # just wait since request is on process or pending
                            # Also, since replica has already seen the (c, t) request, it should not be processed again
                            return
                        
                    self.backup_replica_waiting = True
                    op = msg['actual_msg']['op'] # op will be either (A,B, amt) or (B)
                    if len(op) == 3: # read-write operation

                        # assign sequence number first
                        seq_num = self.update_sequence_number()

                        # create PRE-PREPARE msg : <<PRE-PREPARE, v, n, d>𝜎L , m>
                        pre_prepare_msg = {
                            "type": "PRE-PREPARE",
                            "view": self.curr_view,
                            "seq": seq_num,
                            "d": computed_request_digest.hex()
                        }

                        equivocation_pre_prepare_msg = {
                            "type": "PRE-PREPARE",
                            "view": self.curr_view,
                            "seq": seq_num+1,
                            "d": computed_request_digest.hex()
                        }
                        pre_prepare_msg_bytes = json.dumps(pre_prepare_msg, sort_keys=True).encode('utf-8')
                        equi_pre_pre_msg_bytes = json.dumps(equivocation_pre_prepare_msg, sort_keys=True).encode('utf-8')
                        pre_prepare_signed = self.sign_msg(pre_prepare_msg_bytes, self.private_key)
                        equi_prep_signed = self.sign_msg(equi_pre_pre_msg_bytes, self.private_key)

                        full_pre_prepare_msg = {"signature": xmlrpc.client.Binary(pre_prepare_signed),
                                                "actual_msg": pre_prepare_msg,
                                                "client_request_msg": msg['actual_msg']}
                        full_equi_prepare_msg = {"signature": xmlrpc.client.Binary(equi_prep_signed),
                                                "actual_msg": equivocation_pre_prepare_msg,
                                                "client_request_msg": msg['actual_msg']}
                        # Broadcast with RPC
                        all_prepare_replies = []
                        self.all_logs += f'\n--[Server {self.id}] Broadcasting PRE-PREPARE msgs.\n----msg: {full_pre_prepare_msg}'
                        for replica_id in range(1,8):

                            if replica_id == self.id or replica_id in self.to_not_send_id:
                                continue
                            if replica_id != self.id and replica_id not in self.equivocation_replica_ids:
                                reply = self.send_msg_to_replica(replica_id, full_pre_prepare_msg)

                                self.all_logs += f"\n--[Server {self.id}] Received reply msg from [Server {replica_id}]\n---Reply msg: {reply}"
                                # Check validity of PREPARE reply messages first
                                if reply: # Reply will be in form <signed(D(msg)), msg> where msg is 'actual_msg': <PREPARE, v, n, d, i> and i is the replica i
                                    reply_msg_bytes = json.dumps(reply['actual_msg'], sort_keys=True).encode('utf-8')
                                    is_reply_valid = self.verify_signature(reply['signature'].data, reply_msg_bytes, self.replica_public_key_phonebook[reply['actual_msg']['i']])
                                    if is_reply_valid:
                                        all_prepare_replies.append(reply)

                            if replica_id != self.id and replica_id in self.equivocation_replica_ids:
                                reply = self.send_msg_to_replica(replica_id, full_equi_prepare_msg)

                                self.all_logs += f"\n--[Server {self.id}] Received reply msg from [Server {replica_id}]\n---Reply msg: {reply}"
                                # Check validity of PREPARE reply messages first
                                if reply: # Reply will be in form <signed(D(msg)), msg> where msg is 'actual_msg': <PREPARE, v, n, d, i> and i is the replica i
                                    reply_msg_bytes = json.dumps(reply['actual_msg'], sort_keys=True).encode('utf-8')
                                    is_reply_valid = self.verify_signature(reply['signature'].data, reply_msg_bytes, self.replica_public_key_phonebook[reply['actual_msg']['i']])
                                    if is_reply_valid:
                                        all_prepare_replies.append(reply)

                        # For PREPARE, need n-f-1 =4 is valid enough since primary does not participate
                        if len(all_prepare_replies) >= 4: # f=2, n = 7, n-f-1 = 4
                            self.accepted_logs[(self.curr_view, self.seq_num, "PP")] = full_pre_prepare_msg
                            # Update (view, seq_num) status to PREPARED
                            self.all_status[(self.curr_view, seq_num)] = "PREPARED"
                            self.client_request_status[(msg['actual_msg']['c'], timestamp)] = "P"

                            # Now, broadcast collected PREPARE message to all replicas in a single message
                            # Now, the process will be 1) primary send collected PREPARE to all replicas
                            # 2) each replica verify the collected PREPARE messages
                            # 3) each replica return COMMIT message to the collector (primary)
                            all_commit_replies = []
                            SINGLE_PREPARE_MSG = {"actual_msg": {"type": "COLLECTED_PREPARE", "prepare_msgs": all_prepare_replies}}
                            for replica_id in range(1,8):
                                if replica_id == self.id or replica_id in self.to_not_send_id:
                                    continue
                                if replica_id != self.id:
                                    # reply is going to be  <COMMIT,v,n,D(m),i>𝜎i hence {signature: sign, actual_msg: <COMMIT,v,n,D(m),i>}

                                    self.all_logs += f"\n--[Server {self.id}] Sending COLLECTED_PREPARE msgs to [Server {replica_id}]\n---COLLECTED_PREPARE msg: {SINGLE_PREPARE_MSG}"
                                    reply = self.send_msg_to_replica(replica_id, SINGLE_PREPARE_MSG)

                                    if reply: 

                                        self.all_logs += f"\n--[Server {self.id}] Received reply msg from [Server {replica_id}]\n---Reply msg: {reply}"
                                        
                                        reply_msg_bytes = json.dumps(reply['actual_msg'], sort_keys=True).encode('utf-8')
                                        is_reply_valid = self.verify_signature(reply['signature'].data, reply_msg_bytes, self.replica_public_key_phonebook[reply['actual_msg']['i']])
                                        if is_reply_valid:
                                            all_commit_replies.append(reply)
                            # Include primary's own commit msg too
                            # commit_msg = "signature": signed(D(cmmit_msg)), "actual_msg": <COMMIT, v, n, d, i>
                            primary_commit_msg = {"type": "COMMIT", "v": self.curr_view, "n": seq_num, "d": computed_request_digest.hex(), "i": self.id}
                            primary_commit_msg_bytes = json.dumps(primary_commit_msg, sort_keys=True).encode('utf-8')
                            primary_commit_signed = self.sign_msg(primary_commit_msg_bytes, self.private_key)
                            full_primary_commit_msg = {"signature": xmlrpc.client.Binary(primary_commit_signed), "actual_msg": primary_commit_msg}
                            all_commit_replies.append(full_primary_commit_msg)

                            # For commit, need 5 since primary participates
                            if len(all_commit_replies) >= 5: # Now collector sends the collected commit message to all replicas
                                self.all_status[(self.curr_view, seq_num)] = "COMMITTED"
                                SINGLE_COMMIT_MSG = {"actual_msg":  {"type": "COLLECTED_COMMIT", "commit_msgs": all_commit_replies}}
                                for replica_id in range(1,8):
                                    if replica_id == self.id or replica_id in self.to_not_send_id:
                                        continue
                                    if replica_id != self.id:
                                        # Now send collected commit msgs to all replicas, but reply is not needed. We reply to client after execution.
                                        self.send_msg_to_replica(replica_id, SINGLE_COMMIT_MSG)

                                # Since primary has collected enough commit msgs, execute the operation and reply to client
                                sender = op[0]
                                receiver = op[1]
                                amt = op[2]
                                execution_status = self.execute_operation(sender, receiver, amt)
                                self.client_request_status[(msg['actual_msg']['c'], timestamp)] = "E"
                                self.all_status[(self.curr_view, self.seq_num)] = "EXECUTED"
                                # reply: <REPLY, v, t, c, i, r> where r is the result
                                reply_msg = {"type": "REPLY",
                                            "v": self.curr_view,
                                            "t": timestamp,
                                            "c": msg['actual_msg']['c'],
                                            "i": self.id,
                                            "r": "SUCCESS" if execution_status else "FAIL"
                                            }
                                
                                reply_msg_bytes = json.dumps(reply_msg, sort_keys=True).encode('utf-8')
                                reply_msg_signed = self.sign_msg(reply_msg_bytes, self.private_key)
                                full_reply_msg = {"signature": xmlrpc.client.Binary(reply_msg_signed), "actual_msg": reply_msg}
                                self.all_logs += f"\n--[Server {self.id}] Sending reply msg to [Client {msg['actual_msg']['c']}]\n----msg: {msg}]"
                                self.send_reply_to_client(self.client_phonebook[msg['actual_msg']['c']], full_reply_msg)
                                self.all_reply_to_client[(msg['actual_msg']['c'], timestamp)] = full_reply_msg
                                self.backup_replica_waiting = False
                                self.check_pending_requests() # this function will also STOP the timer if there's no pending request
                                
                        
                    # read-only operation
                    elif len(op) == 1: 
                        self.all_logs += f'\n--[Server {self.id}] Received a read-only request from [Client {op[0]}]. Replaying with the client balance.'
                        reply_msg = {"type": "REPLY",
                                    "v": self.curr_view,
                                    "t": timestamp,
                                    "c": op[0],
                                    "i": self.id,
                                    "r": self.client_balance[op[0]]
                                    }
                        reply_msg_bytes = json.dumps(reply_msg, sort_keys=True).encode('utf-8')
                        reply_msg_signed = self.sign_msg(reply_msg_bytes, self.private_key)
                        full_reply_msg = {"signature": xmlrpc.client.Binary(reply_msg_signed), "actual_msg": reply_msg}
                        self.all_logs += f"\n--[Server {self.id}] Sending reply msg to [Client {msg['actual_msg']['c']}]\n----msg: {msg['actual_msg']}"
                        for replica_id in range(1,8):
                            if replica_id == self.id or replica_id in self.to_not_send_id:
                                continue
                            if replica_id != self.id:
                                self.send_msg_to_replica(replica_id, full_reply_msg)
                        self.send_reply_to_client(self.client_phonebook[msg['actual_msg']['c']], full_reply_msg)
                        self.all_reply_to_client[(msg['actual_msg']['c'], timestamp)] = full_reply_msg
                        self.backup_replica_waiting = False
                        return
                        
                        
                else:

                    self.all_logs += f"\n--[Server {self.id}] Invalid signature received from [Client {msg['actual_msg']['c']}]"
            # process to return PREPARE when receiving PRE-PREPARE
            if msg['actual_msg']['type'] == "PRE-PREPARE":


                self.all_logs += f"\n--[Server {self.id}] Received PRE-PREPARE msg from [Server {msg['actual_msg']['view']}]\n--PRE-PREPARE msg: {msg}"
                # Also, timer should start since PRE-PREPARE is received in backups by the primary
                if self.id != self.curr_view and not self.timer:
                    self.reset_timer()


                # msg will be: 'signature': signed(D(actual_msg)), 'actual_msg': <PRE-PREPARE, v, n, d>, 'client_request_msg': <REQUEST, o, t, c>
                pre_prepare_msg_bytes = json.dumps(msg['actual_msg'], sort_keys=True).encode('utf-8')

                is_pre_prepare_valid = self.verify_signature(msg['signature'].data, pre_prepare_msg_bytes, self.replica_public_key_phonebook[msg['actual_msg']['view']]) # view since the current view is the primary

                if is_pre_prepare_valid:
                    # process the PRE-PREPARE msg: 'actual_msg': <PRE-PREPARE, v, n, d>
                    # check view first
                    if msg['actual_msg']['view'] == self.curr_view:
                        


                        # Then, check if it has not accepted a pre-prepare message for view v and seq_num n containing different digest
                        if (self.curr_view, msg['actual_msg']['seq'],"PP") not in self.accepted_logs:


                            # Then, also check if the seq_num is between low and high watermarks
                            # seq_num should be between low water mark h and high water mark H
                            # I set k as 10 and ckpt every 5 sequence, since we won't be working with thousands of transactions in the project
                            if msg['actual_msg']['seq'] >= self.stable_checkpoint+10 or msg['actual_msg']['seq'] < self.stable_checkpoint:
                                # Just ignore msg
                                return None

                            # Also, now, accept the PRE-PREPARE msg to the log
                            self.accepted_logs[(self.curr_view, msg['actual_msg']['seq'], "PP")] = msg # now, msg is {'signature': signed(D(actual_msg)), 'actual_msg': <PRE-PREPARE, v, n, d>, 'client_request_msg': <REQUEST, o, t, c>}
                            # ===== Hence, retrieve the client request msg from self.accepted_logs when needed!! =======

                            # <PREPARE, v, n, d, i>
                            prepare_msg = {
                                "type": "PREPARE",
                                "v": self.curr_view,
                                "n": msg["actual_msg"]["seq"],
                                "d": msg["actual_msg"]["d"],
                                "i": self.id
                            }

                            prepare_msg_bytes = json.dumps(prepare_msg, sort_keys=True).encode('utf-8')

                            prepare_signed = self.sign_msg(prepare_msg_bytes, self.private_key)

                            full_prepare_msg = {"signature": xmlrpc.client.Binary(prepare_signed), 'actual_msg': prepare_msg}

                            self.all_logs += f"\n--[Server {self.id}] Sending PREPARE msg back to [Server {msg['actual_msg']['view']}]\n---PREPARE msg: {full_prepare_msg}"
                            # return PREPARE msg since this part runs by RPC
                            return full_prepare_msg
                        elif (self.curr_view, msg['actual_msg']['seq'],"PP") in self.accepted_logs:
                            accepted_pre_prepare_msg = self.accepted_logs[(self.curr_view, msg['actual_msg']['seq'])]
                            actual_appm = accepted_pre_prepare_msg['actual_msg']
                            if actual_appm['d'] != msg['actual_msg']['d']:

                                self.all_logs += f"\n--[Server {self.id}] Same v,n PRE-PREPARE with different digest found"
                                return None
                            elif actual_appm['d'] == msg['actual_msg']['d']:
                                # already accepted the same PRE-PREPARE msg

                                return None

            # process to return COMMIT msg when receiving collection of PREPARE
            if msg['actual_msg']['type'] == "COLLECTED_PREPARE":
                prepare_msgs = msg['actual_msg']['prepare_msgs']

                self.all_logs += f"\n--[SERVER {self.id}] Received COLLECTED_PREPARE msgs\n---COLLECTED_PREPARE msgs: {prepare_msgs}"
                valid_prepare_msgs = []
                invalid_prepare_msgs = []
                # single prepare_msg = {'signature': signed(D(msg)), 'actual_msg': <PREPARE, v, n, d, i>}
                for prepare_msg in prepare_msgs:
                    if prepare_msg['actual_msg']['i'] == self.id:
                        valid_prepare_msgs.append(prepare_msg)
                        continue
                    prepare_msg_bytes = json.dumps(prepare_msg['actual_msg'], sort_keys=True).encode('utf-8')
                    is_prepare_valid = self.verify_signature(prepare_msg['signature'].data, prepare_msg_bytes, self.replica_public_key_phonebook[prepare_msg['actual_msg']['i']])
                    if is_prepare_valid and valid_prepare_msgs != []:

                        if prepare_msg['actual_msg']['v'] == valid_prepare_msgs[0]['actual_msg']['v'] and prepare_msg['actual_msg']['n'] == valid_prepare_msgs[0]['actual_msg']['n']:
                            valid_prepare_msgs.append(prepare_msg)  
                        else:
                            invalid_prepare_msgs.append(prepare_msg)
                    elif is_prepare_valid and valid_prepare_msgs == []:
                        valid_prepare_msgs.append(prepare_msg)
                    else:
                        invalid_prepare_msgs.append(prepare_msg)
                # Now, check if quorum

                if len(valid_prepare_msgs) >= 4:

                    self.all_status[(self.curr_view, valid_prepare_msgs[0]['actual_msg']['n'])] = "PREPARED"

                    # Now, return COMMIT msg
                    commit_msg = {
                        "type": "COMMIT",
                        "v": valid_prepare_msgs[0]['actual_msg']['v'],
                        "n": valid_prepare_msgs[0]['actual_msg']['n'],
                        "d": valid_prepare_msgs[0]['actual_msg']['d'],
                        "i": self.id
                    }
                    commit_msg_bytes = json.dumps(commit_msg, sort_keys=True).encode('utf-8')

                    commit_signed = self.sign_msg(commit_msg_bytes, self.private_key)

                    full_commit_msg = {"signature": xmlrpc.client.Binary(commit_signed), "actual_msg": commit_msg}

                    return full_commit_msg
            
            if msg['actual_msg']['type'] == "COLLECTED_COMMIT": # Going to be {"actual_msg": {"type": "COLLECTED_COMMIT", "commit_msgs": all_commit_replies}}

                self.all_logs += f"\n--[SERVER {self.id}] Received COLLECTED_COMMIT msgs\n---COLLECTED_COMMIT msgs: {msg}"
                commit_msgs = msg['actual_msg']['commit_msgs']
                valid_commit_msgs = []
                invalid_commit_msgs = []
                for commit_msg in commit_msgs:


                    if commit_msg['actual_msg']['i'] == self.id:
                        valid_commit_msgs.append(commit_msg)
                        continue

                    commit_msg_bytes = json.dumps(commit_msg['actual_msg'], sort_keys=True).encode('utf-8')

                    is_commit_valid = self.verify_signature(commit_msg['signature'].data, commit_msg_bytes, self.replica_public_key_phonebook[commit_msg['actual_msg']['i']])

                    if is_commit_valid and valid_commit_msgs != []:
                        if commit_msg['actual_msg']['v'] == valid_commit_msgs[0]['actual_msg']['v'] and commit_msg['actual_msg']['n'] == valid_commit_msgs[0]['actual_msg']['n'] and self.all_status[(commit_msg['actual_msg']['v'], commit_msg['actual_msg']['n'])] == "PREPARED":
                            valid_commit_msgs.append(commit_msg)
                        else:
                            invalid_commit_msgs.append(commit_msg)
                    elif is_commit_valid and valid_commit_msgs == []:
                        valid_commit_msgs.append(commit_msg)
                    else:
                        invalid_commit_msgs.append(commit_msg)
                        
                # Now, check if quorum
                if len(valid_commit_msgs) >= 5:

                    self.all_status[(self.curr_view, valid_commit_msgs[0]['actual_msg']['n'])] = "COMMITTED"
                    # Now, execute the operation
                    accepted_log = self.accepted_logs[(valid_commit_msgs[0]['actual_msg']['v'], valid_commit_msgs[0]['actual_msg']['n'], "PP")]
                    client_request_msg = accepted_log['client_request_msg']
                    op = client_request_msg['op']
                    sender = op[0]
                    receiver = op[1]
                    amt = op[2]
                    execution_status = self.execute_operation(sender, receiver, amt)
                    self.all_status[(self.curr_view, valid_commit_msgs[0]['actual_msg']['n'])] = "EXECUTED"
                    time_stamp = client_request_msg['timestamp']
                    client = client_request_msg['c']
                    self.client_request_status[(client, time_stamp)] = "E"
                    reply_msg = {"type": "REPLY",
                                "v": self.curr_view,
                                "t": time_stamp,
                                "c": client,
                                "i": self.id,
                                "r": "SUCCESS" if execution_status else "FAIL"
                                }
                    reply_msg_bytes = json.dumps(reply_msg, sort_keys=True).encode('utf-8')
                    reply_msg_signed = self.sign_msg(reply_msg_bytes, self.private_key)
                    full_reply_msg = {"signature": xmlrpc.client.Binary(reply_msg_signed), "actual_msg": reply_msg}
                    self.all_logs += f"\n--[Server {self.id}] Sending reply msg to [Client {client}]. \n----msg: {reply_msg}]"
                    self.send_reply_to_client(self.client_phonebook[client], full_reply_msg)
                    self.all_reply_to_client[(client, time_stamp)] = full_reply_msg
                    self.backup_replica_waiting = False
                    if self.timer:
                        self.timer.cancel()
                        self.timer = None
                    # CHECKPOINT MECHANISM EVERY 5 SEQUENCE
                    # s is seq num, d is digest
                    if valid_commit_msgs[0]['actual_msg']['n'] % 5 == 0 and self.stable_checkpoint != valid_commit_msgs[0]['actual_msg']['n']:

                        self.all_logs += f"\n--[Server {self.id}] Sequence number reached multiple of 5, sending out CHECKPOINT msgs."
                        self.send_check_point(valid_commit_msgs[0]['actual_msg']['n'],valid_commit_msgs[0]['actual_msg']['d'])

            
                    self.check_pending_requests() # Now, in the check_pending_requests, if there is no pending request, stop timer. Otherwise, restart timer.
            # case for read-only request
            if msg['actual_msg']['type'] == "REPLY":
                self.all_logs += f'\n--[Server {self.id}] Received a read-only request from [Server {self.curr_view}]. Replying with the client balance.'
                reply_msg = {"type": "REPLY",
                                    "v": self.curr_view,
                                    "t": msg['actual_msg']['t'],
                                    "c": msg['actual_msg']['c'],
                                    "i": self.id,
                                    "r": self.client_balance[msg['actual_msg']['c']]
                                    }
                reply_msg_bytes = json.dumps(reply_msg, sort_keys=True).encode('utf-8')
                reply_msg_signed = self.sign_msg(reply_msg_bytes, self.private_key)
                full_reply_msg = {"signature": xmlrpc.client.Binary(reply_msg_signed), "actual_msg": reply_msg}
                self.all_logs += f"\n--[Server {self.id}] Sending reply msg to [Client {msg['actual_msg']['c']}]\n----msg: {msg['actual_msg']}"
                self.send_reply_to_client(self.client_phonebook[msg['actual_msg']['c']], full_reply_msg)
                self.all_reply_to_client[(msg['actual_msg']['c'], msg['actual_msg']['t'])] = full_reply_msg
                self.backup_replica_waiting = False
                if self.timer:
                    self.timer.cancel()
                    self.timer = None
                return

        # process for CHECKPOINT msg
        if msg['actual_msg']['type'] == "CHECKPOINT":
            ckpt_msg_bytes = json.dumps(msg['actual_msg'], sort_keys=True).encode('utf-8')
            is_ckpt_valid = self.verify_signature(msg['signature'].data, ckpt_msg_bytes, self.replica_public_key_phonebook[msg['actual_msg']['i']])
            if is_ckpt_valid:
                self.received_checkpoint_msgs.append(msg)
                # for ckpt_msg in self.received_checkpoint_msgs:
                    # if there are 2f+1 ckpt_msg that has equal value for ckpt_msg['actual_msg']['n'] and ckpt_msg['actual_msg']['d']
                count_map = {}
                for ckpt_msg in self.received_checkpoint_msgs:
                    key = (ckpt_msg['actual_msg']['n'], ckpt_msg['actual_msg']['d'])
                    count_map[key] = count_map.get(key, 0) + 1
                
                valid_ckpts = []
                for (n, d), count in count_map.items():
                    if count >= 5:
                        valid_ckpts.append((n,d))
                new_stable_ckpt = max(valid_ckpts, key=lambda x: x[0])

                self.all_logs += f"\n--[Server {self.id}] New stable checkpoint will be sequence: {new_stable_ckpt[0]}"
                self.stable_checkpoint = new_stable_ckpt[0]
                # reset previous proofs
                self.stable_checkpoint_proof = []
                for ckpt_msg in self.received_checkpoint_msgs:
                    if (ckpt_msg['actual_msg']['n'], ckpt_msg['actual_msg']['d']) == new_stable_ckpt:
                        self.stable_checkpoint_proof.append(ckpt_msg)
                # reset previous received ckpt msgs
                self.received_checkpoint_msgs = []
        
        if msg['actual_msg']['type'] == "VIEW-CHANGE":
            view_msg_bytes = json.dumps(msg['actual_msg'], sort_keys=True).encode('utf-8')
            is_view_valid = self.verify_signature(msg['signature'].data, view_msg_bytes, self.replica_public_key_phonebook[msg['actual_msg']['i']])
            if is_view_valid and msg['actual_msg']['v'] != self.curr_view:
                self.all_logs += f"\n--[Server {self.id}] Received VIEW-CHANGE message from [Server {msg['actual_msg']['i']}]\n--msg: {msg}"
                self.received_view_change_msgs.append(msg)
                # for view_msg in self.received_view_change_msgs:
                    # if there are 2f+1 ckpt_msg that has equal value for ckpt_msg['actual_msg']['n'] and ckpt_msg['actual_msg']['d']
                count_map = {}
                for view_msg in self.received_view_change_msgs:
                    key = view_msg['actual_msg']['v']
                    count_map[key] = count_map.get(key, 0) + 1
                valid_next_view = []
                for k, count in count_map.items():
                    if count >= 5:
                        valid_next_view.append(k)
                if valid_next_view != []:
                    next_view = max(valid_next_view)

                    self.all_logs += f"\n--[Server {self.id}] Next view is: {next_view}"
                    
                    valid_view_change_msgs = []

                    for view_msg in self.received_view_change_msgs:
                        if view_msg['actual_msg']['v']:
                            valid_view_change_msgs.append(view_msg)
                    self.received_view_change_msgs = []

                    if next_view == self.id:
                        # Since I am the next primary, create and send out NEW-VIEW msg
                        # one view-change msg: {'signature', 'actual_msg':{'type': "VIEW-CHANGE",
                        # 'v': (self.curr_view % 7) + 1,
                        # 'n': self.stable_checkpoint, #<seq_num of last stable checkpoint>,
                        # 'C': self.stable_checkpoint_proof, #<valid Checkpoint msg, to PROVE n>,
                        # 'P': set_P,#<all_prepared_msgs>,
                        # 'i': self.id
                        # }}
                        # and set_P has full_pre_pre_msg
                        self.curr_view = next_view
                        min_s = max(vc_msg['actual_msg']['n'] for vc_msg in valid_view_change_msgs)

                        all_prepared_seqs = []
                        for vc_msg in valid_view_change_msgs:
                            prepared_set = vc_msg['actual_msg'].get('P', [])
                            if isinstance(prepared_set, list):
                                for p in prepared_set:
                                    if isinstance(p, dict):
                                        seq = p.get('actual_msg', {}).get('seq')
                                        if seq is not None:
                                            all_prepared_seqs.append(seq)
                            elif isinstance(prepared_set, dict):
                                seq = prepared_set.get('actual_msg', {}).get('seq')
                                if seq is not None:
                                    all_prepared_seqs.append(seq)
                        
                        max_s = max(all_prepared_seqs) if all_prepared_seqs else min_s
                        o_component = []
                        d_null = {'actual_msg': "no-op"}


                        d_null_digest = self.digest_msg(d_null)
                        for se in range(min_s, max_s+1):
                            digest = d_null_digest
                            is_null_digest = True
                            for vc_msg in valid_view_change_msgs:
                                for pp_vc_msg in vc_msg['actual_msg']['P']:
                                    if pp_vc_msg['actual_msg']['seq'] == se:

                                        digest = pp_vc_msg['actual_msg']['d']
                                        is_null_digest = False

                            new_pre_prepare_msg = {'type': "PRE-PREPARE",
                                                'view': self.curr_view,
                                                'seq': se,
                                                'd': digest.hex() if is_null_digest else digest
                                                }
                            o_component.append(new_pre_prepare_msg)
                        self.received_view_change_msgs = []
                        
                        new_view_msg = {
                            'type': "NEW-VIEW",
                            'v': self.curr_view,
                            'valid_v': [vv['actual_msg'] for vv in valid_view_change_msgs],
                            'o': o_component
                        }
                        if self.nv_timer:
                            self.nv_timer.cancel()
                            self.nv_timer = None
                        new_view_msg_bytes = json.dumps(new_view_msg, sort_keys=True).encode('utf-8')
                        new_view_msg_signed = self.sign_msg(new_view_msg_bytes, self.private_key)
                        full_new_view_msg = {"signature": xmlrpc.client.Binary(new_view_msg_signed), "actual_msg": new_view_msg}
                        self.view_change_phase = False
                        self.all_new_view_msg.append(full_new_view_msg)
                        self.all_logs += f'\n--[Server {self.id}] Created and broadcasting NEW-VIEW message.\n--NEW-VIEW msg: {new_view_msg}'
                        for replica_id in range(1,8):
                            if replica_id == self.id or replica_id in self.to_not_send_id:
                                continue
                            if replica_id != self.id:
                                self.send_msg_to_replica(replica_id, full_new_view_msg)
                    

        if msg['actual_msg']['type'] == "NEW-VIEW":
            if self.nv_timer:
                self.nv_timer.cancel()
                self.nv_timer = None
            new_view_msg_bytes = json.dumps(msg['actual_msg'], sort_keys=True).encode('utf-8')
            is_new_view_valid = self.verify_signature(msg['signature'].data, new_view_msg_bytes, self.replica_public_key_phonebook[msg['actual_msg']['v']])
            if is_new_view_valid and msg['actual_msg']['v'] > self.curr_view:
                self.all_logs += f'\n--[Server {self.id}] Received a valid NEW-VIEW msg.\n----NEW-VIEW Msg: {msg}'
                self.curr_view = msg['actual_msg']['v']
                self.received_view_change_msgs = []
                self.view_change_phase = False
                self.all_new_view_msg.append(msg)





    # attacks to simulate
    def invalid_signature_attack(self):
        self.invalid_sign = True
    
    def crash_attack(self):
        # IF REPLICA is the primary:
        #   1) refrain from changing its status to PREPARED and neglects to send PREPARE msgs to other replicas
        #   2) does NOT send a new-view msg during a view-change, while still recording any received view-change msgs
        #   3) does NOT send REPLY msg back to clients
        # IF REPLICA is the backup:
        #   1) fails to send any PREPARE msgs or update its status to PREPARED
        #   2) does NOT send REPLY msg back to clients
        self.is_crashed = True
    
    def in_dark_attack(self, to_not_send): # input as list

        self.to_not_send_id = to_not_send


    def timing_attack(self):
        self.delay_t = 0.01

    def equivocation_attack(self, subset_replica_ids):
        self.equivocation_replica_ids = subset_replica_ids
    


    def exclude_node(self):
        self.excluded = True
        self.view_change_phase = False
        if self.timer:
            self.timer.cancel()
            self.timer = None
        
    def include_node(self):
        self.excluded = False
        
    

