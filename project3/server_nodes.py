from xmlrpc.server import SimpleXMLRPCServer
from socketserver import ThreadingMixIn
import xmlrpc.server
import xmlrpc.client
import time
import threading
from sqlitedict import SqliteDict
from cluster_info import CLUSTER_1, CLUSTER_2, CLUSTER_3




class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


class ServerNode:
    def __init__(self, id):
        self.id = id
        self.cluster_id = (self.id-1)//3
        self.client_balance = SqliteDict(f"bank_db_{self.id}.sqlite", autocommit=False)
        self.all_logs = ""

        self.ballot_num = (0,self.id)

        self.leader_id = None
        self.timer = None
        self.pending_prepare_msg = []
        self.all_accept_logs_paxos = [] # will be triplet of (acceptNum, acceptSeq, acceptVal) -> same as (b, s, m)
        self.paxos_seq_num = 0
        self.paxos_seq_status = {} # key: seq_num, val: status
        self.c_t_status = {} # key: (client_id, tau), val: status <--- use this to ensure exactly-once semantic
        self.c_t_reply_msg = {} # key: (cid, tau), val: reply msg
        self.is_crashed = False

        self.all_new_view_msg = "" # str of new-view msgs received
        self.s_lock_table = {} # key: client_id, val: lock or None
        self.s_lock_table_lock = threading.Lock()
        self.x_lock_table = {} # key: client_id, val: lock or None
        self.x_lock_table_lock = threading.Lock()

        self.WAL_TABLE = {} # key: cross_seq_num, val: txn
        self.pc_seq_num = 0
        self.cross_seq_num = 0
        self.cross_seq_status = {} # key: seq_num, val: status

        self.cluster_1_primary = 8001
        self.cluster_2_primary = 8004
        self.cluster_3_primary = 8007



    def initialize_server(self):
        server = ThreadedXMLRPCServer(("localhost", 8000+self.id), allow_none=True)
        server.register_function(self.process_msg, 'process_msg')
        server.register_function(self.printLog, 'printLog')
        server.register_function(self.fail_node, 'fail_node')
        server.register_function(self.recover_node, 'recover_node')
        server.register_function(self.printView, 'printView')
        server.register_function(self.printDB, 'printDB')
        server.register_function(self.printBalance, 'printBalance')
        
        server.serve_forever()

    def printLog(self):
        return {"all_logs": self.all_logs}

    
    def printBalance(self, client_id):
        if client_id in self.client_balance:
            return {"balance": self.client_balance[client_id]}
        return None
    
    def printDB(self):
        modified_balance = {}
        for key,val in self.client_balance.items():
            if val != 10:
                modified_balance[key] = val
        return modified_balance
    
    def printView(self):
        return {"new-view_msgs": self.all_new_view_msg}

    def reset_timer(self):
        if self.timer:
            self.timer.cancel()
            self.timer = None
        self.timer = threading.Timer(2 + self.id % 3, self.elect_leader)
        self.timer.daemon = True
        self.timer.start()

    def s_lock(self, client_id):
        # if this returns False, there's already a lock
        with self.s_lock_table_lock:
            if self.s_lock_table.get(client_id, False):
                return False  # already locked
            # aqcuiree locks

            self.s_lock_table[client_id] = True
            return True
    
    def x_lock(self, client_id):
        with self.x_lock_table_lock:
            if self.x_lock_table.get(client_id, False):
                return False  # already locked
            # aqcuiree locks

            self.x_lock_table[client_id] = True
            return True
    
    def check_s_lock(self, client_id):
        # if this returns False, there's already a lock
        with self.s_lock_table_lock:
            if self.s_lock_table.get(client_id, False):
                return False  # already locked
                
            return True
    
    def check_x_lock(self, client_id):
        with self.x_lock_table_lock:
            if self.x_lock_table.get(client_id, False):
                return False  # already locked

            return True

    def unlock_s_lock(self, client_id):
        with self.s_lock_table_lock:
            if client_id in self.s_lock_table:
                del self.s_lock_table[client_id]

    def unlock_x_lock(self, client_id):
        with self.x_lock_table_lock:
            if client_id in self.x_lock_table:
                del self.x_lock_table[client_id]

    def send_msg_to_replica(self, node_id, msg):
        if self.is_crashed:
            return None

        try:
            with xmlrpc.client.ServerProxy(f"http://localhost:{8000+node_id}", allow_none=True) as proxy:
                reply = proxy.process_msg(msg)
                return reply
        except:
            pass

    def fail_node(self):
        # crash this node
        if self.timer:
            self.timer.cancel()
            self.timer = None
        self.is_crashed = True

    def recover_node(self):
        # recover this node
        self.is_crashed = False



    def elect_leader(self):

        if self.pending_prepare_msg == []:
            # start election
            new_ballot_num = (self.ballot_num[0] + 1, self.id)
            prepare_msg = {
                'type': 'PREPARE',
                'ballot_num': new_ballot_num,
            }
            
            all_replies = []
        
            
            for i in range(self.cluster_id*3+1, self.cluster_id*3 + 4):
                if i == self.id:
                    continue
                reply = self.send_msg_to_replica(i, prepare_msg)

                if reply:
                    all_replies.append(reply)
        
            if len(all_replies)+1 >= 2:
                    
                self.leader_id = self.id
                self.ballot_num = new_ballot_num
                # Now, send out NEW-VIEW message
                for ack_msg in all_replies: # ack is going to be {type: 'ACK', ballot_num:..., accept_log:...}
                    # accept_log = list of (b, s, m)
                    for log in ack_msg['accept_log']:
                        if log[1] not in [entry[1] for entry in self.all_accept_logs_paxos]:
                            self.all_accept_logs_paxos.append( (log[0], log[1], log[2]) )
                            self.execute_paxos_transaction(log[1], log[2])

                

                new_view_msg = {
                    'type': 'NEW-VIEW',
                    'ballot_num': self.ballot_num,
                    'accept_log': (self.all_accept_logs_paxos)
                }
                self.all_new_view_msg += f"\n {new_view_msg}"

                for i in range(self.cluster_id*3+1, self.cluster_id*3 + 4):
                    if i == self.id:

                        continue
                    self.send_msg_to_replica(i, new_view_msg)


                

                
        else:
            if self.timer:
                self.timer.cancel()
            self.timer = None
            return
    
    def execute_paxos_transaction(self, seq_num, msg):
        if self.timer:
            self.timer.cancel()
            self.timer = None
        s = msg['txn'][0]
        r = msg['txn'][1]
        amt = msg['txn'][2]
        self.paxos_seq_status[seq_num] = 'EXECUTED'
        self.c_t_status[(msg['c'], msg['tau'])] = 'EXECUTED'

        self.unlock_x_lock(s)
        self.unlock_x_lock(r)
        if self.client_balance[s] >= amt:
            self.client_balance[s] -= amt
            self.client_balance[r] += amt
            self.client_balance.commit()
            return True
        else:
            return False

    def get_primary_port(self, client_id):
        if client_id in CLUSTER_1:
            return self.cluster_1_primary
        elif client_id in CLUSTER_2:
            return self.cluster_2_primary
        elif client_id in CLUSTER_3:
            return self.cluster_3_primary

    def update_paxos_seq_num(self):
        if self.all_accept_logs_paxos == []:
            self.paxos_seq_num = 1
            self.paxos_seq_status[self.paxos_seq_num] = 'PENDING'
        else:
            self.paxos_seq_num += 1
            self.paxos_seq_status[self.paxos_seq_num] = 'PENDING'

    def update_pc_seq_num(self):
        self.pc_seq_num = len(self.WAL_TABLE) + 1
    
    def update_paxos_seq_num_backup(self,seq):
        if seq > self.paxos_seq_num:
            self.paxos_seq_num = seq
            self.paxos_seq_status[self.paxos_seq_num] = 'PENDING'
    
    def update_WAL(self,msg):
        self.WAL_TABLE[(msg['c'], msg['tau'])] = msg

    def execute_pc_transaction(self,txn):
        s = txn[0]
        r = txn[1]
        amt = txn[2]
        if s in self.client_balance:
            self.client_balance[s] -= amt

        elif r in self.client_balance:
            self.client_balance[r] += amt
            
        

    def process_msg(self, msg):
        if self.is_crashed:
            return None


        if msg['type'] == 'REQUEST':
                

            # two things to consider first
            # 1. read-only or write?
            # 2. Is this intra or cross-shard?


            if type(msg['txn']) == int:
                # read-only
                s = msg['txn']
                if s in self.client_balance and self.check_s_lock(s) and self.check_x_lock(s):
                    self.s_lock(s)
                    balance = self.client_balance[s]
                    read_only_reply_msg = {
                        'type': "REPLY",
                        'ballot_num': self.ballot_num,
                        'tau': msg['tau'],
                        'c': msg['c'],
                        'result': balance
                    }
                    self.c_t_status[(msg['c'], msg['tau'])] = "EXECUTED"
                    self.c_t_reply_msg[(msg['c'], msg['tau'])] = read_only_reply_msg
                    self.unlock_s_lock(s)
        
                    return read_only_reply_msg

            else:
                # write request
                s = msg['txn'][0]
                r = msg['txn'][1]
                amt = msg['txn'][2]
                if (msg['c'], msg['tau']) in self.c_t_status:
                    if (msg['c'], msg['tau']) in self.c_t_reply_msg:
                        return self.c_t_reply_msg[(msg['c'], msg['tau'])]

                # check if intra or cross-shard
                if s in self.client_balance and r in self.client_balance:
                    # intra-shard
        

                    # am I leader?
                    if self.leader_id == self.id:
                            
                        
                        # checklock s and r
                        if not self.check_s_lock(s) or not self.check_s_lock(r) or not self.check_x_lock(s) or not self.check_x_lock(r):
                                
                            return

                        sender_lock = self.x_lock(s)
                        receiver_lock = self.x_lock(r)
        
                        self.all_accept_logs_paxos.append( (self.ballot_num, self.paxos_seq_num, msg) )
                        self.update_paxos_seq_num()
                        accept_msg = {
                            'type': 'ACCEPT',
                            'ballot_num': self.ballot_num,
                            's': self.paxos_seq_num,
                            'm': msg
                        }
                        # broadcast this
                        all_replies = []
        
                        self.paxos_seq_status[self.paxos_seq_num] = 'ACCEPTED'
                        self.c_t_status[(msg['c'], msg['tau'])] = 'ACCEPTED'
                        for i in range(self.cluster_id*3+1, self.cluster_id*3 + 4):
                            if i == self.id:
                                continue
                            reply = self.send_msg_to_replica(i, accept_msg)

                            if reply and reply['type'] == 'ACCEPTED':
                                all_replies.append(reply)
        
                        # reach quorum?
                        if len(all_replies) + 1 >= 2:
                            commit_msg = {
                                'type': 'COMMIT',
                                'ballot_num': self.ballot_num,
                                's': self.paxos_seq_num,
                                'm': msg
                            }
                            # broadcast commit
        
                            self.paxos_seq_status[self.paxos_seq_num] = 'COMMITTED'
                            self.c_t_status[(msg['c'], msg['tau'])] = 'COMMITTED'

                            for i in range(self.cluster_id*3+1, self.cluster_id*3 + 4):
                                if i == self.id:
                                    continue
                                self.send_msg_to_replica(i, commit_msg)
                            for k,v in self.paxos_seq_status.items():
                                if v != 'EXECUTED' and k != self.paxos_seq_num:
                                    self.execute_paxos_transaction(k, msg)
                            result = self.execute_paxos_transaction(self.paxos_seq_num, msg)
                            reply_msg = {
                                'type': 'REPLY',
                                'ballot_num': self.ballot_num,
                                'tau': msg['tau'],
                                'c': msg['c'],
                                'result': "success" if result else "failure"
                            }
                            self.c_t_reply_msg[(msg['c'], msg['tau'])] = reply_msg
        
                            return reply_msg

                    else:
                        if self.leader_id is None:
                                
                            self.all_logs += f"[Server {self.id}] No leader elected yet. Starting leader election."
                            self.elect_leader()
                        else:
                                
                            self.reset_timer()
                            self.send_msg_to_replica(self.leader_id, msg)
                            
                elif s in self.client_balance and r not in self.client_balance:
                    if self.timer:
                        self.timer.cancel()
                        self.timer = None
                    # cross-shard
                    # 2PC START
                    # Request msg: <REQ, txn, tau, c>
                    # 1. Confirm there are no locks on s and r
                    # 2. Verify the balance of s is equal to or greater than amt
                    # IF BOTH STATISFY:
                    #       1. Lock record s
                    #       2. Send <PREPARE, t, m> to Leader of the other cluster
                    #       3. Initiate Paxos inside this cluster (Assign new seq num for 2PC)
                    #           - Broadcast <ACCEPT, ballot_num, s, P, m> where P to show this is "Prepare" phase of 2PC
                    #           - Also, update and maintain WAL for possible undoing changes WHEN ABORT happens
                    #       4. Same 1,2,3 for Participant Leader
                    #       4-1. If quorum is reached (ACCEPTED) in Participant Leader, it issues a COMMIT msg to the nodes in the P-Cluster
                    #       4-1. And send <PREPARED, txn, m> to the leader of Coordinator (C-Cluster)
                    #       4-1. If txn is committed and executed, the nodes will update their WAL
                    #       
                    #       4-2. If r is LOCKED! P-Cluster's leader initiate Abort Paxos: <ACCEPT, ballot_num, s, A, m>
                    #       4-2. Once the txn is committed, the P-Cluster leader simply sends an abot msg <ABORT, txn, m> back to leader of C-Cluster
                    #       
                    #       Now, leader of C-Cluster either receive PREPARED or ABORT from P-Cluster
                    #       5-1. If Leader of C-Cluster receive PREPARED, AND request is committed in the cluster,
                    #       5-1. now, initiate COMMIT phase by broadcasting <ACCEPT, ballot_num, s, C, m> in the cluster
                    #       5-1. Once leader receives ACCEPTED enough, send <COMMIT, txn, m> to the leader of P-Cluster
                    #       5-1. Also send REPLY msg back to client
                    #
                    #       5-2. If leader of C-Cluster receive abort from P-Cluster / Or, timed out / Or, consensus not reached in previous phase
                    #       5-2. Broadcast <ACCEPT, ballot_num, s, A, m> to all nodes in the cluster
                    #       5-2. Upon receiving ACCEPTED from majority, the leader of C-cluster send <ABORT, txn, m> to leader of P-Cluster
                    #       5-2. From the 3 IF condition, if leader of C-cluster aborted because P-Cluster Aborted, no need to send ABORT msg to P-Cluster.
                    #       5-2. Send REPLY to client to notify the outcome.
                    
                    #       Then:
                    #       Similarly, the leader of the participant cluster initiates consensus within its cluster to
                    #       replicate the commit or abort entry by broadcasting an accept message to all nodes in its
                    #       own cluster. The leader also sends an acknowledgment message back to the leader of the
                    #       coordinator cluster once consensus on the second phase is achieved.
                    #       When the outcome is commit, each node will simply release the lock on the corresponding
                    #       records. However, if the outcome is an abort or if a timeout occurs, the nodes will utilize
                    #       the WAL to undo the executed operations before releasing the locks.
                    #       The leader of the coordinator cluster waits for an acknowledgment message from the leader
                    #       of the participant cluster, and if the acknowledgment message is not received, it re-sends the
                    #       commit or abort message.
                    
                    condition_1 = self.client_balance[s] >= amt
                    condition_2 = self.check_x_lock(s)

                    if condition_1 and condition_2:
                        self.x_lock(s)
                        self.update_pc_seq_num()

                        all_a_p_replies = []
                        accept_prepare_msg = {
                                    'type': 'ACCEPT_2PC_PAXOS',
                                    's': self.pc_seq_num,
                                    'type_2': 'P',
                                    'm': msg
                                }


                        for i in range(self.cluster_id*3+1, self.cluster_id*3 + 4):
                            if i == self.id:
                                continue
                            a_p_reply = self.send_msg_to_replica(i, accept_prepare_msg)

                            if a_p_reply and a_p_reply['type'] == 'ACCEPTED_2PC':
                                all_a_p_replies.append(a_p_reply)

                        if len(all_a_p_replies) + 1 >= 2:
                            commit_prepare_msg = {
                                    'type': 'COMMIT_2PC_PAXOS',
                                    'txn': msg['txn'],
                                    'type_2': 'P',
                                    'm': msg
                                }
                            self.update_WAL(msg)
                            for i in range(self.cluster_id*3+1, self.cluster_id*3 + 4):
                                if i == self.id:
                                    continue
                                c_p_reply = self.send_msg_to_replica(i, commit_prepare_msg)

                        else:

                            # send reply back to client
                            reply_msg = {
                                'type': 'REPLY',
                                'ballot_num': self.ballot_num,
                                'tau': msg['tau'],
                                'c': msg['c'],
                                'result': "ABORTED"
                            }

                            return reply_msg

                            

                        cs_prep_msg = {
                            'type': 'PREPARE_2PC',
                            'txn': msg['txn'],
                            'm': msg
                        }
                        participant_leader = self.get_primary_port(r)

                        prep_reply = self.send_msg_to_replica(participant_leader-8000, cs_prep_msg)
                        # prep_reply will be either ABORT or PREPARED


                        if prep_reply:
                            
                            if prep_reply['type'] == 'ABORT':

                                # broad cast <ACCEPT, ballot_num, s, A, m> to all nodes in the cluster
                                accept_abort_msg = {
                                    'type': 'ACCEPT_2PC_PAXOS',
                                    's': self.pc_seq_num,
                                    'type_2': 'A',
                                    'm': msg
                                }
                                all_a_a_reply = []
                                for i in range(self.cluster_id*3+1, self.cluster_id*3 + 4):
                                    if i == self.id:
                                        continue
                                    a_a_reply = self.send_msg_to_replica(i, accept_abort_msg)
                                    if a_a_reply:
                                        if a_a_reply['type'] == 'ACCEPTED_2PC':
                                            all_a_a_reply.append(a_a_reply)
                                if len(a_a_reply) + 1 >= 2:
                                    # send reply back to client
                                    reply_msg = {
                                        'type': 'REPLY',
                                        'ballot_num': self.ballot_num,
                                        'tau': msg['tau'],
                                        'c': msg['c'],
                                        'result': "ABORTED"
                                    }

                                    return reply_msg
                                
                            elif prep_reply['type'] == 'PREPARED_2PC':

                                accept_commit_msg = {
                                    'type': 'ACCEPT_2PC_PAXOS',
                                    's': self.pc_seq_num,
                                    'type_2': 'C',
                                    'm': msg
                                }
                                all_a_c_replies = []

                                for i in range(self.cluster_id*3+1, self.cluster_id*3 + 4):
                                    if i == self.id:
                                        continue
                                    a_c_reply = self.send_msg_to_replica(i, accept_commit_msg)

                                    if a_c_reply and a_c_reply['type'] == 'ACCEPTED_2PC':
                                        all_a_c_replies.append(a_c_reply)
                                if len(all_a_c_replies) + 1 >= 2:
                                    commit_commit_msg = {
                                            'type': 'COMMIT_2PC_PAXOS',
                                            'txn': msg['txn'],
                                            'type_2': 'C',
                                            'm': msg
                                        }

                                    for i in range(self.cluster_id*3+1, self.cluster_id*3 + 4):
                                        if i == self.id:
                                            continue
                                        c_c_reply = self.send_msg_to_replica(i, commit_commit_msg)
                                    #       5-1. now, initiate COMMIT phase by broadcasting <ACCEPT, ballot_num, s, C, m> in the cluster
                                    #       5-1. Once leader receives ACCEPTED enough, send <COMMIT, txn, m> to the leader of P-Cluster
                                    #       5-1. Also send REPLY msg back to client
                                    cs_commit_msg = {
                                            'type': 'COMMIT_2PC', # 이건 반대편 클러스터용
                                            'txn': msg['txn'],
                                            'm': msg
                                        }

                                    self.send_msg_to_replica(participant_leader, cs_commit_msg)
                                    reply_msg = {
                                        'type': 'REPLY',
                                        'ballot_num': self.ballot_num,
                                        'tau': msg['tau'],
                                        'c': msg['c'],
                                        'result': "Success"
                                    }
                                    self.execute_pc_transaction(msg['txn'])
                                    self.unlock_x_lock(s)

                                    return reply_msg

                                else:
                                    reply_msg = {
                                        'type': 'REPLY',
                                        'ballot_num': self.ballot_num,
                                        'tau': msg['tau'],
                                        'c': msg['c'],
                                        'result': "ABORTED"
                                    }
                                    return reply_msg



                                
        if msg['type'] == 'PREPARE_2PC':

            s = msg['txn'][0]
            r = msg['txn'][1]
            amt = msg['txn'][2]
            # run consensus
            self.x_lock(r)
            self.update_pc_seq_num()

            all_a_p_replies = []
            accept_prepare_msg = {
                        'type': 'ACCEPT_2PC_PAXOS',
                        's': self.pc_seq_num,
                        'type_2': 'P',
                        'm': msg['m']
                    }


            for i in range(self.cluster_id*3+1, self.cluster_id*3 + 4):
                if i == self.id:
                    continue
                a_p_reply = self.send_msg_to_replica(i, accept_prepare_msg)

                if a_p_reply and a_p_reply['type'] == 'ACCEPTED_2PC':
                    all_a_p_replies.append(a_p_reply)

            if len(all_a_p_replies) + 1 >= 2:
                commit_prepare_msg = {
                        'type': 'COMMIT_2PC_PAXOS',
                        'txn': msg['txn'],
                        'type_2': 'P',
                        'm': msg['m']
                    }

                self.update_WAL(msg['m'])

                for i in range(self.cluster_id*3+1, self.cluster_id*3 + 4):
                    if i == self.id:
                        continue
                    c_p_reply = self.send_msg_to_replica(i, commit_prepare_msg)
                # update WAL
                # return PREPARED_2PC


                prepare_pc_msg = {
                    'type': 'PREPARED_2PC',
                    'm': msg['m']
                }

                return prepare_pc_msg
            else:

                # send reply back to C-cluster
                abort_msg = {
                    'type': 'ABORT',
                    'm': msg['m']
                }
                return abort_msg
            

        if msg['type'] == 'COMMIT_2PC':
            # run consensus
            # update execution
            accept_commit_msg = {
                'type': 'ACCEPT_2PC_PAXOS',
                's': self.pc_seq_num,
                'type_2': 'C',
                'm': msg['m']
            }
            all_a_c_replies = []

            for i in range(self.cluster_id*3+1, self.cluster_id*3 + 4):
                if i == self.id:
                    continue
                a_c_reply = self.send_msg_to_replica(i, accept_commit_msg)

                if a_c_reply and a_c_reply['type'] == 'ACCEPTED_2PC':
                    all_a_c_replies.append(a_c_reply)
            if len(all_a_c_replies) + 1 >= 2:
                commit_commit_msg = {
                        'type': 'COMMIT_2PC_PAXOS',
                        'txn': msg['txn'],
                        'type_2': 'C',
                        'm': msg['m']
                    }

                for i in range(self.cluster_id*3+1, self.cluster_id*3 + 4):
                    if i == self.id:
                        continue
                    c_c_reply = self.send_msg_to_replica(i, commit_commit_msg)

                self.execute_pc_transaction(msg['txn'])
                self.unlock_x_lock(r)
                return reply_msg

            else:
                return None

        if msg['type'] == 'ACCEPT_2PC_PAXOS':
            if msg['type_2'] == 'A':
                # 
                accept_abort_msg = {
                    'type': 'ACCEPTED_2PC',
                    'm': msg['m']
                }
                return accept_abort_msg
            elif msg['type_2'] == 'P':

                # return ACCEPTED
                accept_prep_msg = {
                    'type': 'ACCEPTED_2PC',
                    'm': msg['m']
                }
                return accept_prep_msg
            elif msg['type_2'] == 'C':
                accept_prep_msg = {
                    'type': 'ACCEPTED_2PC',
                    'm': msg['m']
                }
                return accept_prep_msg

        if msg['type'] == 'COMMIT_2PC_PAXOS':
            if msg['type_2'] == 'A':
                return None
            elif msg['type_2'] == 'P':
                # update WAL
                # return none
                self.update_WAL(msg['m'])
                return None
            elif msg['type_2'] == 'C':
                # update execution
                # return non
                
                self.execute_pc_transaction(msg['txn'])
                return None



        
        if msg['type'] == 'PREPARE':
            if self.timer == None:
                highest = False
                all_ballots = []
                received_ballot = (msg['ballot_num'][0], msg['ballot_num'][1])
                for prep_msg in self.pending_prepare_msg:
                    all_ballots.append((prep_msg['ballot_num'][0], prep_msg['ballot_num'][1]))
                all_ballots.append(self.ballot_num)
                if received_ballot >= max(all_ballots):
                    highest = True

                if highest:
                    self.ballot_num = msg['ballot_num']
                    ack_msg = {
                        'type': 'ACK',
                        'ballot_num': msg['ballot_num'],
                        'accept_log': self.all_accept_logs_paxos
                    }
                    self.pending_prepare_msg = []
                    return ack_msg
            else:
                self.timer.cancel()
                self.timer = None
                self.pending_prepare_msg.append(msg)
                highest = False
                all_ballots = []
                received_ballot = (msg['ballot_num'][0], msg['ballot_num'][1])
                for prep_msg in self.pending_prepare_msg:
                    all_ballots.append((prep_msg['ballot_num'][0], prep_msg['ballot_num'][1]))
                all_ballots.append(self.ballot_num)
                if received_ballot >= max(all_ballots):
                    highest = True

                if highest:
                    self.ballot_num = msg['ballot_num']
                    ack_msg = {
                        'type': 'ACK',
                        'ballot_num': msg['ballot_num'],
                        'accept_log': self.all_accept_logs_paxos
                    }
                    self.pending_prepare_msg = []
                    return ack_msg
    
        if msg['type'] == 'ACCEPT':
            if msg['ballot_num'] >= self.ballot_num:
                if self.timer:
                    self.timer.cancel()
                    self.timer = None
                self.ballot_num = msg['ballot_num']
                self.all_accept_logs_paxos.append( (msg['ballot_num'], msg['s'], msg['m']) )
                self.update_paxos_seq_num_backup(msg['s'])
                accepted_msg = {
                    'type': 'ACCEPTED',
                    'ballot_num': msg['ballot_num'],
                    's': msg['s'],
                    'm': msg['m'],
                    'n': self.id
                }
                self.paxos_seq_status[msg['s']] = 'ACCEPTED'
                self.c_t_status[(msg['m']['c'], msg['m']['tau'])] = 'ACCEPTED'

                return accepted_msg
        
        if msg['type'] == 'COMMIT':
            if msg['ballot_num'] >= self.ballot_num:
                if self.timer:
                    self.timer.cancel()
                    self.timer = None
                self.ballot_num = msg['ballot_num']
                self.update_paxos_seq_num_backup(msg['s'])
                self.paxos_seq_status[msg['s']] = 'COMMITTED'
                self.c_t_status[(msg['m']['c'], msg['m']['tau'])] = 'COMMITTED'

                
                for k,v in self.paxos_seq_status.items():
                    if v != 'EXECUTED' and k != msg['s']:
                        self.execute_paxos_transaction(k, msg['m'])
                result = self.execute_paxos_transaction(msg['s'], msg['m'])
                reply_msg = {
                                'type': 'REPLY',
                                'ballot_num': self.ballot_num,
                                'tau': msg['m']['tau'],
                                'c': msg['m']['c'],
                                'result': "success" if result else "failure"
                            }
                self.c_t_reply_msg[(msg['m']['c'], msg['m']['tau'])] = reply_msg
                return
        
        if msg['type'] == 'NEW-VIEW':

            if self.timer:
                self.timer.cancel()
                self.timer = None
            if msg['ballot_num'] >= self.ballot_num:
                self.all_new_view_msg+= f"\n {msg}"
                self.ballot_num = msg['ballot_num']
                self.leader_id = msg['ballot_num'][1]
                for log in msg['accept_log']:
                    if log[1] not in [entry[1] for entry in self.all_accept_logs_paxos]:
                        self.all_accept_logs_paxos.append( (log[0], log[1], log[2]) )
                        self.update_paxos_seq_num_backup(log[1])
                        self.execute_paxos_transaction(log[1], log[2])
                

                return
                