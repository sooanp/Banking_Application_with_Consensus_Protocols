import os, psutil
import xmlrpc.server
import xmlrpc.client
import threading
import queue
import time
from xmlrpc.server import SimpleXMLRPCServer
from socketserver import ThreadingMixIn

class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

class ServerNode:
    def __init__(self, id):
        self.id = id
        self.pid = os.getpid()
        self.memory = psutil.Process(self.pid).memory_info().rss / 1024 / 1024

        self.currLeader = None # Could be None, or a node id
        self.ballot_num = (0, 0)
        self.received_messages = []

        # AcceptLog is a triplet (AcceptNum, AcceptSeq, AcceptVal)
        self.acceptLog = []
     
        self.last_reply = [[],[],[],[],[],[],[],[],[],[]] # timestamp of last reply to client (discard requests with lower timestamp - implement later)
        self.client_phonebook = {"A": 0, "B": 1, "C": 2, "D": 3, "E": 4,
                                 "F": 5, "G": 6, "H": 7, "I": 8, "J": 9}
        self.client_balance = {"A": 100, "B": 100, "C": 100, "D": 100, "E": 100,
                                 "F": 100, "G": 100, "H": 100, "I": 100, "J": 100}

        self.seq_num = 0
        self.seq_tau_pair = [] # (seq,tau) to check if certain request is already handled and ASSIGNED a sequence num
        # Client's request should be FIFO
        self.client_request = queue.Queue()

        self.executed_logs = [] # (seq, transaction)

        # timer for threading
        self.timer = None

        # heartbeat # Heartbeat Idea from Piazza Post @60
        self.heartBeat = None
        self.prepare_logs = [] # ballots

        self.mutex = threading.Lock()

        self.is_alive = True # to simulate node failure

        self.seq_status = {}
        self.election_running = False

        self.all_logs_strings = ''


    def info(self):
        return f"Node {self.id} (PID {self.pid}) using ~{self.memory:.2f} MB"

    
    def initialize_server(self):
        # server = xmlrpc.server.SimpleXMLRPCServer(('localhost', 9000 + self.id), allow_none=True, logRequests=False)
        server = ThreadedXMLRPCServer(('localhost', 9000 + self.id), allow_none=True, logRequests=False)
        server.register_function(self.printStatus, 'printStatus')
        server.register_function(self.process_message, 'process_message')
        server.register_function(self.printDB, "printDB")
        server.register_function(self.leader_failure, "leader_failure")
        server.register_function(self.node_failure, "node_failure")
        server.register_function(self.revive_node, "revive_node")
        server.register_function(self.printLog, "printLog")
        print(f"Node {self.id} listening on port {9000 + self.id}...")
        self.reset_timer()
        server.serve_forever()

    def reset_timer(self):
        
        if self.timer:
            self.timer.cancel()
        self.timer = threading.Timer(2.0 + (self.id-1)*2.0, self.elect_leader) # 1, 1.5, 2, 2.5, 3s for each node
        self.timer.daemon = True
        self.timer.start()


    # Function that returns a reply when it receives "type" of certain message
    def process_message(self, msg):
        
        # if node is dead, don't process any message
        if not self.is_alive:
            return None


        # wheneber node receives a message from leader
        if msg["type"] == "PREPARE" or msg["type"] == "ACCEPT" or msg["type"] == "COMMIT" or msg["type"] == "HEARTBEAT" or msg["type"] == "NEW-VIEW":
            # Should not start timer if I am the leader myself
            
            if self.currLeader is not None and self.currLeader != self.id:
                self.reset_timer()

            
            # case when node is revived and does not know current leader
            if self.currLeader is None and msg["type"] == "HEARTBEAT":
                self.currLeader = msg["ballot"][1]
                self.ballot_num = msg["ballot"]

            self.election_running = False
            self.reset_timer()

        # Phase 1 - Leader Election
        if msg["type"] == "PREPARE":
            self.all_logs_strings += (f'\n-- Received [PREPARE] msg from leader. \n-- msg: {msg}')
            # push prepare msg to the prepare log
            self.prepare_logs.append(msg["ballot"]) # handling further in elect_leader func
            self.election_running = True
            if msg["ballot"][0] > self.ballot_num[0]:
                self.ballot_num = msg["ballot"]
                self.currLeader = msg["ballot"][1]

                # TODO: forward queue here because backup node could have received and queued up requests while leader is not yet elected

                reply_msg = {"type": "ACK", "ballot": self.ballot_num, "AcceptLog": self.acceptLog}
                # (n1, p1) > (n2, p2)
                # if n1 > n2
                # or n1 == n2 and p1 > p2
                self.prepare_logs = []
                self.election_running = False
                self.all_logs_strings += (f'\n-- The Ballot number is higher than mine. I will accept it my ACK msg. \n-- msg: {reply_msg}')
                return reply_msg

            elif msg["ballot"][0] == self.ballot_num[0]:
                if msg["ballot"][1] > self.ballot_num[1]:
                    self.ballot_num = msg["ballot"]
                    self.currLeader = msg["ballot"][1]
                    reply_msg = {"type": "ACK", "ballot": self.ballot_num, "AcceptLog": self.acceptLog}
                    self.prepare_logs = []
                    # TODO: Forward queue here
                    self.all_logs_strings += (f'\n-- The Ballot number is higher than mine. I will accept it my ACK msg. \n-- msg: {reply_msg}')
                    return reply_msg
                
            elif msg["ballot"][0] < self.ballot_num[0]:
                self.all_logs_strings += ('\n-- The Ballot number is lower than mine. I will not return anything.')
                return None
            return #
        
        
        # Phase 2 - Replication (ACCEPT)
        if msg["type"] == "ACCEPT":
            # accept msg : <ACCEPT, b, s, m>
            self.all_logs_strings += (f'\n-- Received [ACCEPT] msg from leader. \n-- msg: {msg}')
            if msg["ballot"] == self.ballot_num:
                
                self.acceptLog.append((msg["ballot"], msg["seq"], msg["m"]))
                # reply : <ACCEPTED, b, s, m, n_b>
                reply_msg = {"type": "ACCEPTED", "ballot": self.ballot_num, "seq": msg["seq"], "m": msg["m"], "node": self.id}
                self.seq_status[msg["seq"]] = "A"
                # print(f"[Node {self.id}] SEQ_STATUS: {self.seq_status}")
                highest_seq = max([acceptVal[1] for acceptVal in self.acceptLog])
                if highest_seq in self.seq_status:
                    if self.seq_status[highest_seq] == "C" or self.seq_status[highest_seq] == "E":
                        self.seq_num = highest_seq + 1
                    else:        
                        self.seq_num = highest_seq
                self.all_logs_strings += (f'\n-- Returning [ACCEPTED] msg to leader for \n-- msg: {msg}\n-- Replying with {reply_msg}')
                return reply_msg

            elif msg["ballot"][0] > self.ballot_num[0] or (msg["ballot"][0] == self.ballot_num[0] and msg["ballot"][1] > self.ballot_num[1]):
                
                # update later Oct 04
                self.ballot_num = msg["ballot"]

                self.acceptLog.append((msg["ballot"], msg["seq"], msg["m"]))
                # reply : <ACCEPTED, b, s, m, n_b>
                reply_msg = {"type": "ACCEPTED", "ballot": self.ballot_num, "seq": msg["seq"], "m": msg["m"], "node": self.id}
                self.seq_status[msg["seq"]] = "A"
                print(f"[Node {self.id}] SEQ_STATUS: {self.seq_status}")
                highest_seq = max([acceptVal[1] for acceptVal in self.acceptLog])
                if highest_seq in self.seq_status:
                    if self.seq_status[highest_seq] == "C" or self.seq_status[highest_seq] == "E":
                        self.seq_num = highest_seq + 1
                    else:        
                        self.seq_num = highest_seq
                self.all_logs_strings += (f'\n-- Returning [ACCEPTED] msg to leader for \n-- msg: {msg}\n-- Replying with {reply_msg}')
                return reply_msg
            
            elif msg["ballot"][0] < self.ballot_num[0]:
                self.all_logs_strings += (f'\n-- Returning nothing because the ballot is lower than current ballot: {self.ballot_num}')
                return None # Discard?
            
        # Phase 3 - Decision
        if msg["type"] == "COMMIT":
            # commit msg : <COMMIT, b, s, m>
            self.all_logs_strings += (f'\n-- Received Commit msg from leader. \n-- msg: {msg}')
            if msg["ballot"] == self.ballot_num or msg["ballot"][0] > self.ballot_num[0] or (msg["ballot"][0] == self.ballot_num[0] and msg["ballot"][1] > self.ballot_num[1]):
                # what if I already executed this transaction
                if msg["seq"] in [s for s, t in self.executed_logs]:
                    self.all_logs_strings += ('\n-- Already executed the corresponding sequence number transaction. Skipping.')
                    return
                self.seq_status[msg["seq"]] = "C"
                self.all_logs_strings += (f'\n-- Executing the transaction for msg: {msg}')
                self.execute_transaction(msg["m"], msg["seq"])

                return

        # TODO: new-view, then make timer to wait longer to catch up with the repair
        # Also, repair the logs
        # Also, update the new leader
        # if msg["type"] == "NEW-VIEW":
        if msg["type"] == "NEW-VIEW":
            self.all_logs_strings += (f'\n-- Received NEW-VIEW msg from the new leader. \n-- msg: {msg}')



        # Handle client requests
        if msg["type"] == "REQUEST":

            
            client_id = self.client_phonebook[msg["client"]]

            if self.last_reply[client_id] != []: # 이부분도 문제있는듯 1번 고칠점
                # This should be a list of replies, since client may ask for a reply of a request in tau 2 but current latest could be tau 3
                
                
                # + Also implement the case where "If the request has already been processed, simply resend the message"
                for reply_msg in self.last_reply[client_id]:
                    if reply_msg["tau"] == msg["tau"]:

                        # + Also implement the case where request tau is lower than last reply's tau
                        # if msg["tau"] < self.last_reply[client_id][-1]['tau'] and reply_msg[""] != msg:
                        #     print(f"[Node {self.id}] Discarding old request from {msg['client']} with tau={msg['tau']}")
                        #     return None

                        self.all_logs_strings += (f"\n-- Resending previous reply to [Client {msg['client']}] with tau={msg['tau']} \n-- msg: {reply_msg}")
                        return reply_msg
                

            transaction = msg["transaction"]
            tau = msg["tau"]

            
            client = msg["client"]

            self.all_logs_strings += (f"\n-- Received Request msg from [Client {client}] with tau={tau}. \n-- msg: {msg}")
            # First save the request to queue

            # Update sequence number and check if this request is duplicate

            seq = self.return_seq_num(msg)

            self.client_request.put({"transaction": msg["transaction"], "tau": tau, 'seq': seq})

            # if msg in self.all_client_request:
                # TODO: 단순히 무시하면 안됨. consensus가 reach되지 못한 request에 대해서는 다시 시도해야하기 때문.

            # if election is running, only queue the request.
            if self.election_running:

                return None
            # Am I the leader?

            # if self.id != self.currLeader and self.currLeader is None:
            #     print("[Node {self.id}] I am not the leader, but I am starting election...")
            #     self.elect_leader()
            #     # update transaction here
            #     reply_msg = self.last_reply[client_id]

            # Forward if I know the leader, and it's not me
            if self.id != self.currLeader and self.currLeader is not None:
                self.all_logs_strings += (f"\n-- I am not the leader, but I know the leader. Forwarding the msg to leader Node {self.currLeader}.\n-- msg: {msg}")

                # if self.currLeader is not None and self.currLeader != self.id:
                #     if self.timer:
                #         self.timer.cancel()
                #         self.timer = None
                #     self.timer = threading.Timer(0.5 + self.id*3.5, self.elect_leader) # timeout is node's ID for now
                #     self.timer.start()
                # This make the leader process_msg this REQUEST msg
                # Forward the request to leader in a separate thread
                # threading.Thread(
                #     target=self.send_to_node, 
                #     args=(self.currLeader, msg),
                #     daemon=True
                # ).start()
                
                # Return immediately so this follower doesn't block
                if "forwarded" in msg:
                    if msg["forwarded"]:
                        return None
                else:
                    msg["forwarded"] = True
                    reply = self.send_to_node(self.currLeader, msg)

                    return reply
            # I am the leader
            elif self.id == self.currLeader:

                self.all_logs_strings += (f"\n-- I am the leader. I will start processing the request msg. --\n-- msg: {msg}")

                # Start the consensus, and consensus will execute the transaction

                while self.client_request.qsize() > 0:
                    self.consensus()
                return_msg = None
                # At the end of consensus, reply message should be saved in corresponding client index
                for reply_msg in self.last_replay[client_id]:
                    if reply_msg["tau"] == tau:
                        return_msg = reply_msg
            if return_msg:
                self.all_logs_strings += (f'\n-- Consensus has been reached. Returning reply msg to [Client {client}].\n-- msg: {return_msg}')
            elif return_msg is None:
                self.all_logs_strings += (f'\n-- Consensus has NOT been reached. Returning None to [Client {client}].')
            return return_msg
    
    def return_seq_num(self, msg):

        if self.seq_tau_pair == []:

            self.seq_num += 1
            seq = self.seq_num
        else:

            highest_seq = max([pair[0] for pair in self.seq_tau_pair])

            self.seq_num = highest_seq+1

            seq = self.seq_num
            for pair in self.seq_tau_pair:
                s = pair[0]
                tau = pair[1]
                c = pair[2]
                if msg["tau"] == tau and c == msg['client']:

                    return s

        # if self.acceptLog != []:
        #     highest_seq = max([acceptVal[1] for acceptVal in self.acceptLog])
        #     if highest_seq in self.seq_status:
        #         if self.seq_status[highest_seq] == "C" or self.seq_status[highest_seq] == "E":

        #             self.seq_num = highest_seq + 1
        #         else:
                    
        #             self.seq_num = highest_seq
        # else:
        #     # initial case where acceptLog is empty
        #     self.seq_num += 1

        tau = msg["tau"]
        pair = (seq, tau, msg["client"])

        self.seq_tau_pair.append(pair)

        return seq

    def send_to_node(self, target_id, msg):
        try:
            with xmlrpc.client.ServerProxy(f"http://localhost:{9000+target_id}", allow_none=True) as proxy:
                reply = proxy.process_message(msg) # This part is RPC: calls "process_message" function in the "TARGET" server node, get result message back

                return reply
        except:
            pass

    
    def execute_transaction(self, transaction, seq):
        # t = (c, c', amt)
        self.executed_logs.append((seq, transaction))
        self.seq_status[seq] = "E"
        sender = transaction[0]
        receiver = transaction[1]
        amt = transaction[2]
        self.all_logs_strings += (f"\n-- Executing transaction: {transaction} with Sequence num: {seq}")
        if self.client_balance[sender] >= amt:
            self.client_balance[sender] -= amt
            self.client_balance[receiver] += amt
            return True
        return False

    def merge_accept_logs(self, accept_logs):
        # The input ack_logs = [acceptLog1, acceptLog2, ...]
        # and, acceptLog1 = [(b,s,m), (b2,s2,m2),...] 
        new_acceptLog = []
        seq_dict = {}
        
        # However, in the assignment instruction example, b is the ballot number of current leader
        my_b = self.ballot_num

        for log in accept_logs:
            for triplet in log:
                b = triplet[0]
                s = triplet[1]
                m = triplet[2]
                if s not in seq_dict:
                    seq_dict[s] = (b, s, m)
                elif s in seq_dict and b != seq_dict[s][0]:
                    # if seq is in the dict but ballot is different
                    if s == seq_dict[s][1] and m == seq_dict[s][2]:
                        if b[0] > seq_dict[s][0][0]:
                            seq_dict[s] = (b,s,m)
                        elif b[0] == seq_dict[s][0][0]:
                            if b[1] > seq_dict[s][0][1]:
                                seq_dict[s] = (b,s,m)

        keys = list(seq_dict.keys())

        highest_seq = max(keys) if keys != [] else 0
        for i in range(1, highest_seq+1):
            if i in seq_dict:
                new_acceptLog.append((my_b, seq_dict[i][1], seq_dict[i][2]))
            else:
                t = ("no-op")
                new_acceptLog.append((my_b, i, t))
        return new_acceptLog
    

    def elect_leader(self):
        # If timer of node n2 expires at 1.0s, and n1 expired at 0.5s, n2 should have already received PREPARE from n1
        # n2 called elect_leader but already has PREPARE message, it will simply choose the highest ballot
        # If PREPARE log is empty, it means no one has started election yet, and my timer has expired, I try to become the leader myself

        # if self.prepare_logs != [] and self.currLeader != self.id: # just start election
        #     self.ballot_num = (self.ballot_num[0]+1, self.id)
        #     print(f"[Node {self.id}] Starting election with ballot number {self.ballot_num}...")
        #     vote = 1
        if self.currLeader == self.id:
            return
        # 얘는 그냥 문제 많음 2번
        if self.prepare_logs == [] and self.currLeader != self.id: #if it's empty, proposer try to become leader itself

            new_ballot_num = (self.ballot_num[0] + 1, self.id)

            self.all_logs_strings += (f"\n-- I am starting election with ballot number: {new_ballot_num}")
            vote = 1

            self.election_running = True

            prepare_msg = {"type": "PREPARE", "ballot": new_ballot_num}
            # Broadcast
            ack_logs = []
            for i in range(1,6):
                if i == self.id:
                    continue
                self.all_logs_strings += (f"\n-- Sending PREPARE msg to [Node {i}].\n-- msg: {prepare_msg}")
                reply = self.send_to_node(i, prepare_msg) # If nodes receive PREPARE message, it means election is running, so only queue up the requests and send the queue to the leader if leader is elected.

                if reply and reply["type"] == "ACK":
                    self.all_logs_strings += (f"\n-- Received ACK msg from [Node {i}].\n-- msg: {reply}")
                    vote += 1
                    # add the reply here, so I can include ONLY the ACKs from the quorum
                
                    ack_logs.append(reply["AcceptLog"])
                
            if vote >= 3:
                self.election_running = False
                self.currLeader = self.id
                self.prepare_logs = []
                self.ballot_num = (self.ballot_num[0]+1, self.id)
                self.all_logs_strings += (f"\n-- I am elected as the leader with ballot {self.ballot_num}!")
                # Also, now that I am the leader, I should start heartbeat thread to tell others I am alive
                self.send_heartbeat()


                # Now, organize the received AcceptLog from ACKs
                new_acceptLog = []
                if self.acceptLog != []:
                    ack_logs.append(self.acceptLog)

                new_acceptLog = self.merge_accept_logs(ack_logs)
                self.acceptLog = new_acceptLog

                # Initial case when leader elected and there are pending requests
                if self.client_request.qsize() > 0 and self.acceptLog == []: 

                    self.consensus()
                elif self.acceptLog != []:
                    # In the case where it's not initial election.
                    # There could be acceptLog from the backup nodes

                    
                    new_view_msg = {"type": "NEW-VIEW", "ballot": self.ballot_num, "AcceptLog": self.acceptLog}
                    self.all_logs_strings += (f"\n-- I am sending NEW-VIEW msg as the new leader now.\n-- msg: {new_view_msg}")
                    # Broadcase NEW-VIEW message
                    for i in range(1,6):
                        if i == self.id:
                            continue
                        # reply about NEW-VIEW is not mentioned in the assignment instruction

                        self.send_to_node(i, new_view_msg)

                    # Now, work on the acceptLog one by one
                    self.all_logs_strings += (f"\n-- I am broadcasting ACCEPT msg for each accept msg in the new AcceptLog: {self.acceptLog}")
                    for t in self.acceptLog:
                         # t = (b,s,m)

                        if t[2] == "no-op":
                            continue
                        accept_msg = {"type": "ACCEPT", "ballot": t[0], "seq": t[1], "m": t[2]}

                        self.consensus(accept_msg)

                    self.all_logs_strings += ("\n-- Consensus for ACCEPTs in NEW-VIEW Finished.")
                    self.all_logs_strings += (f"\n-- If I have any pending client requests in the queue, I will now start sending consensus on them.")
                    while self.client_request.qsize() > 0:
                        
                        msg = self.client_request.get()

                        self.consensus(msg)
                        # print(f"[Node {self.id}] Processing {msg} from the client request query")

            
            

        elif self.prepare_logs != [] and self.currLeader != self.id:
            # if there are multiple PREPARE messages, choose the highest ballot and elect him
            for b in self.prepare_logs:
                if b[0] > self.ballot_num[0]:
                    self.ballot_num = b
                    self.currLeader = b[1]
                elif b[0] == self.ballot_num[0]:
                    if b[1] > self.ballot_num[1]:
                        self.ballot_num = b
                        self.currLeader = b[1]
            self.reset_timer()
            
            self.prepare_logs = [] # reset the log
            
    def send_heartbeat(self):
        heartbeat_msg = {"type": "HEARTBEAT", "ballot": self.ballot_num}
        for i in range(1,6):
            if i == self.id:
                continue
            self.send_to_node(i, heartbeat_msg)
        if self.heartBeat:
            self.heartBeat.cancel()
        # 0.3s since the fastest timer is 0.5s (Node 1)
        self.heartBeat = threading.Timer(0.7, self.send_heartbeat)
        self.heartBeat.start()

   

    # 아래 consensus 메소드를 고쳐야함. 그래야, 위에서 NEW-VIEW 보내고, 각 accept로그 다시 계속 보내기 가능.
    # 즉, consensus(input: msg) 이렇게 바꿔줘야, 메시지 별로 consensus를 진행 가능.

    def consensus(self, msg=None): # basic paxos replication (Now, integrate to the NEW-VIEW message logic in assignment)
        # msg is the msg to agree on
        vote = 1

        # print(f"[Node {self.id}] Running consensus on msg: {msg}")
        if msg is None:
            

            # send <ACCEPT, b, s, m>
            q_obj = self.client_request.get()
            curr_msg = q_obj["transaction"]
            tau = q_obj["tau"]

           
            # TODO: tau가 무언가 안에있을경우, seq_num은 그걸로 유지
            # elif tau in self.seq_tau_pair[]:
            #     seq_num = 

            # print(f"[Node {self.id}] input type msg was None, so running consensus on Queued msg: {q_obj}")
            ballot_num = self.ballot_num
            seq_num = q_obj['seq']

            accept_msg = {"type": "ACCEPT", "ballot": ballot_num, "seq": seq_num, "m": curr_msg}



            # Also update my own accept log (leader) since I am assuming to send ACCEPT message to myself too
            self.acceptLog.append((accept_msg["ballot"], accept_msg["seq"], accept_msg["m"]))

        elif msg is not None and "type" in msg:
            
            accept_msg = msg
            seq_num = accept_msg["seq"]
            curr_msg = accept_msg["m"]
            ballot_num = accept_msg["ballot"]
            tau = None

        elif msg is not None and "type" not in msg:
            # q-obj (t, tau) -> this should be (t, tau, s)
            
            seq_num = msg['seq']
            curr_msg = msg["transaction"]
            ballot_num = self.ballot_num
            tau = msg["tau"]
            accept_msg = {"type": "ACCEPT", "ballot": ballot_num, "seq": seq_num, "m": curr_msg}
            self.acceptLog.append((accept_msg["ballot"], accept_msg["seq"], accept_msg["m"]))
        self.all_logs_strings += (f"\n-- Trying to reach consensus with nother nodes now with an ACCEPT msg.\n-- msg: {accept_msg}")
        for i in range(1,6):
            if i == self.id:
                self.seq_status[seq_num] = "A"
                continue
            self.all_logs_strings += (f"\n-- Sending ACCEPT msg to [Node {i}]")
            # print(f"[Node {self.id}] CKPT 4, SENDING ACCEPT TO {i}")
            acc_reply = self.send_to_node(i, accept_msg)
            
            # print(f"[Node {self.id}] CKPT 5, RECEIVED: ", acc_reply)
            if acc_reply and acc_reply["type"] == "ACCEPTED":
                # print(f"[Node {self.id}] Node {i} accepted my ACCEPT message.")
                self.all_logs_strings += (f"\n-- [Node {i}] ACCEPTED my msg. \n-- msg: {acc_reply}")
                vote += 1
        # print(f"[Node {self.id}] Current Vote: {vote}")
        # print(f"[Node {self.id}] CKPT 6, GOT VOTE: ", vote)
        if vote >= 3:
            # Commit Phase
            commit_msg = {"type": "COMMIT", "ballot": ballot_num, "seq": seq_num, "m": curr_msg}
            self.seq_status[seq_num] = "C"
            # if the seq_num is already in execution history log, don't execute again
            # ""The leader also executes
            # the requested transaction if all requests with lower sequence numbers have already been
            # executed. ""
            self.all_logs_strings += (f"\n-- Consensus Reached! I will start sending out COMMIT msg.\n-- msg: {commit_msg}")
            for s, t in self.executed_logs:
                if self.seq_num <= s:
                    # already exectued this sequence transaction
                    return # do nothing for now (perhaps I have to discard it from the queue)
            

            # execute transaction
            status = self.execute_transaction(curr_msg, seq_num)

            
            # Broadcast COMMIT
            for i in range(1,6):
                if i == self.id:
                    continue
                self.send_to_node(i, commit_msg)
            
            # Now, send reply back to client
            client_id = self.client_phonebook[curr_msg[0]]
            if tau:
                reply_msg = {"type": "REPLY", "ballot": ballot_num, "tau": tau, "c": curr_msg[0], "r": "SUCCESS" if status else "FAILED"}
                self.last_reply[client_id].append(reply_msg)
            # done, go back to process_message and get the last_reply[cliend_id]



    # All the additional functions that needs to be implemented

    def leader_failure(self):
        # This function to stop all timers, as if server stopped working.
        if self.currLeader == self.id:
            self.heartBeat.cancel()
            if self.timer:
                self.timer.cancel()
            self.is_alive = False
            self.currLeader = None
    
    def revive_node(self):
        self.is_alive=True
        self.reset_timer()
    
    # For the case where we are excluding some node as described in test case set
    def node_failure(self):
        if self.heartBeat:
            self.heartBeat.cancel()
        if self.timer:
            self.timer.cancel()
        self.is_alive = False


    def printLog(self):
        return {"all_logs": self.all_logs_strings}

    def printDB(self):

        # print(self.client_balance)

        return self.client_balance
    
    def printStatus(self):

        str_key_status = {str(k): v for k, v in self.seq_status.items()}

        return str_key_status

    def printView(self):
        ...


