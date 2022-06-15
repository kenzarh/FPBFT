import threading
from threading import Lock
import socket
import json
import time
import datetime
import hashlib
from cmath import inf
from nacl.signing import SigningKey
from nacl.signing import VerifyKey

ports_file = "ports.json"
with open(ports_file):
    ports_format= open(ports_file)
    ports = json.load(ports_format)
    ports_format.close()

nodes_starting_port = ports["nodes_starting_port"]
nodes_max_number = ports["nodes_max_number"]

nodes_ports = [(nodes_starting_port + i) for i in range (0,nodes_max_number)]

preprepare_format_file = "messages_formats/preprepare_format.json"
prepare_format_file = "messages_formats/prepare_format.json"
validate_format_file = "messages_formats/validate_format.json"
confirm_format_file = "messages_formats/confirm_format.json"
checkpoint_format_file = "messages_formats/checkpoint_format.json"
checkpoint_vote_format_file = "messages_formats/checkpoint_vote_format.json"
view_change_format_file = "messages_formats/view_change_format.json"
new_view_format_file = "messages_formats/new_view_format.json"

def run_BFT(nodes,proportion,checkpoint_frequency0,timer_limit_before_view_change0): # All the nodes participate in the consensus

    global p
    p = proportion

    global number_of_messages
    number_of_messages = {} # This dictionary will store for each request the number of exchanged messages from preprepare to confirm: number_of_messages

    #global replied_requests
    #replied_requests = {} # This dictionary tells if a request was replied to (1) or not

    global timer_limit_before_view_change
    timer_limit_before_view_change = timer_limit_before_view_change0


    global accepted_replies
    accepted_replies = {} # Dictionary that stores for every request the confirm accepted by the primary

    global n
    n = 0 # total nodes number - It is initiated to 0 and incremented each time a node is instantiated

    global f
    f = (n - 1) // 3 # Number of permitted faulty nodes - Should be updated each time n is changed

    global the_nodes_ids_list
    the_nodes_ids_list = [i for i in range (n)]

    global j # next id node (each time a new node is instantiated, it is incremented)
    j = 0

    global checkpoint_frequency
    checkpoint_frequency=checkpoint_frequency0

    global sequence_number
    sequence_number = 1 # Initiate the sequence number to 0 and increment it with each new request - we choosed 0 so that we can have a stable checkpoint at the beginning (necessary for a view change)

    global nodes_list
    nodes_list = []

    global total_processed_messages
    total_processed_messages = 0 # The total number of preocessed messages - this is the total number of send messages through the netwirk while processing a request

    # Nodes evaluation metrics:

    global processed_messages 
    processed_messages = [] # Number of processed messages by each node
    
    global messages_processing_rate
    messages_processing_rate = [] # This is the rate of processed messages among all the nodes in the network - calculated as the ratio of messages sent by the node to all sent messages through the network by all the nodes

    global scores
    scores=[]

    ###################

    global consensus_nodes # ids of nodes participating in the consensus
    consensus_nodes=[]

    global slow_nodes 
    slow_nodes=[]

    global malicious_nodes 
    malicious_nodes=[]

    threading.Thread(target=run_nodes,args=(nodes,)).start()

def run_nodes(nodes):

    global j
    global n
    global f
    
    total_initial_nodes = 0
    for node_type in nodes[0]:
        total_initial_nodes = total_initial_nodes + node_type[1]
    proportion_initial_nodes = int(total_initial_nodes*p) # number of nodes to start at the beginning

    initial_nodes = 0 # This is the number of initial nodes, we only take a proportion and add the others to the new nodes set
    
    # Starting nodes:
    last_waiting_time = 0
    for waiting_time in nodes:
        for tuple in nodes[waiting_time]:
            for i in range (tuple[1]):
                time.sleep(waiting_time-last_waiting_time)
                last_waiting_time=waiting_time
                node_type = tuple[0]
                if (node_type=="honest_node"):
                    node=HonestNode(node_id=j)
                elif (node_type=="non_responding_node"):
                    node=NonRespondingNode(node_id=j)
                #elif (node_type=="faulty_primary"):
                #    node=FaultyPrimary(node_id=j)
                elif (node_type=="slow_nodes"):
                    node=SlowNode(node_id=j)
                #elif (node_type=="faulty_node"):
                #    node=FaultyNode(node_id=j)
                #elif (node_type=="faulty_replies_node"):
                #    node=FaultyRepliesNode(node_id=j)
                threading.Thread(target=node.receive,args=()).start()
                nodes_list.append(node)
                the_nodes_ids_list.append(j)
             
                processed_messages.append(0)
                messages_processing_rate.append(0) # Initiated with 0
                scores.append(50) # Scores are initialized with 100
                #if waiting_time == 0 and (initial_nodes<proportion_initial_nodes  or initial_nodes<4):
                if waiting_time == 0:
                    consensus_nodes.append(j)
                    initial_nodes = initial_nodes + 1
                    n = n + 1
                    f = (n - 1) // 3
                else:
                    slow_nodes.append(j)
                #print("%s node %d started" %(node_type,j))
                j=j+1

# Update consensus nodes
def update_consensus_nodes():    # We only keep nodes with the highest scores, with a number of nodes between min_nodes and max_nodes.
    
    global consensus_nodes
    #global new_nodes
    min_nodes=4
    max_nodes=int(len(the_nodes_ids_list)*p)
    remaining_nodes_scores = []
    for i in range (len(scores)):
        remaining_nodes_scores.append(scores[i])
    
    consensus_nodes = []

    for i in range (min_nodes):
        if len(remaining_nodes_scores) > 0:
            max_score =  max(remaining_nodes_scores)
            for j in range (len(scores)):
                if scores[j] == max_score and len(consensus_nodes)<min_nodes:
                    consensus_nodes.append(j)
                    if (max_score in remaining_nodes_scores):
                        remaining_nodes_scores.remove(max_score)
    
    #while (min_nodes<max_nodes and len(remaining_nodes_scores) > 0 and max(remaining_nodes_scores)>=0):

        #print(min_nodes,max_nodes)
    for i in range (min_nodes,max_nodes):
            if len(remaining_nodes_scores) > 0 and max(remaining_nodes_scores)>=0:
                max_score =  max(remaining_nodes_scores)            
                for j in range (len(scores)):
                    if scores[j] == max_score and len(consensus_nodes)<max_nodes and (j not in consensus_nodes):
                        consensus_nodes.append(j)
                        if (max_score in remaining_nodes_scores):
                            remaining_nodes_scores.remove(max_score)

    #print(consensus_nodes)
    
    # Put other nodes in slow nodes set:
    global slow_nodes
    slow_nodes=[]

    global slow_nodes_scores
    slow_nodes_scores = []

    for score in remaining_nodes_scores:
        for j in range (len(scores)):
            if (scores[j] == score) and (j not in slow_nodes) and (j not in consensus_nodes):
                slow_nodes.append(j)
                slow_nodes_scores.append(score)
                remaining_nodes_scores.remove(score)
                break

    consensus_nodes.sort()

    global n
    n = len(consensus_nodes)
    global f
    f = (len(consensus_nodes) - 1) // 3

    print(scores)

global processed_requests # This is the total number of requests processed by the network
processed_requests = 0
global first_confirm_time

def confirmed_request(): # This method tells the nodes that the primary received its confirm so that they can know the accepted confirm
    global processed_requests
    processed_requests = processed_requests + 1

    if processed_requests == 1:
        global first_confirm_time
        first_confirm_time = time.time()

    last_confirm_time = time.time()

    if processed_requests%5 == 0 : # We want to stop counting at 100 for example
        print("Network validated %d requests within %f seconds" % (processed_requests,last_confirm_time-first_confirm_time))

    # Update consensus nodes after every validation
    #if (processed_requests % 5 == 0):
        #threading.Thread(target=update_consensus_nodes,args=()).start()
    update_consensus_nodes()
 
    #print(scores)

    
'''
    global scores
    replied_requests[request] = 1
    accepted_replies[request] = confirm
    for i in range (len(the_nodes_ids_list)):
        node = nodes_list[i]
        if request in node.replies_time and node.replies_time[request][0]==confirm:
            if (response_delay[i]==inf): # Means this is the first request the node will confirm to in time
                response_delay[i]=node.replies_time[request][1]
            else:
                response_delay[i]=(response_delay[i]+node.replies_time[request][1])/2 # We take the mean of the delays
        scores[i] = (scores[i] + (connection_quality[i]*credibility[i])/((response_delay[i]))) / 2
        if (scores[i] < 0 and i not in malicious_nodes):
            malicious_nodes.append(i)
            if i in consensus_nodes:
                consensus_nodes.remove(i)

    #update_consensus_nodes()
'''
    
def get_primary_id():
    node_0=nodes_list[0]
    return node_0.primary_node_id

def get_primary():
    p = get_primary_id()
    for i in range (len(nodes_list)):
        if i ==p:
            return nodes_list[i]

def get_nodes_ids_list():
    return consensus_nodes

def get_f():
    return f

class Node():
    def __init__(self,node_id):
        self.node_id = node_id
        self.node_port = nodes_ports[node_id]
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        host = socket.gethostname() 
        s.bind((host, self.node_port))
        s.listen()
        self.socket = s
        self.view_number=0 # Initiated with 1 and increases with each view change
        self.primary_node_id=0
        self.preprepares={} # Dictionary of tuples of accepted preprepare messages: preprepares=[(view_number,sequence_number):digest]
        self.prepared_messages = [] # set of prepared messages
        self.message_confirm = [] # List of all the confirm messages
        self.prepares={} # Dictionary of accepted prepare messages: prepares = {(view_number,sequence_number,digest):[different_nodes_that_replied]}
        self.responses={} # Dictionary of accepted response messages: responses = {(view_number,sequence_number,digest):[different_nodes_that_replied]}
        self.message_log = [] # Set of accepted messages
        self.checkpoints = {} # Dictionary of checkpoints: {checkpoint:[list_of_nodes_that_voted]}
        self.checkpoints_sequence_number = [] # List of sequence numbers where a checkpoint was proposed
        self.stable_checkpoint = {"message_type":"CHECKPOINT", "sequence_number":1,"checkpoint_digest":"the_checkpoint_digest","node_id":self.node_id} # The last stable checkpoint
        self.stable_checkpoint_validators = [] # list of nodes that voted for the last stable checkpoint
        self.h = 0 # The low water mark = sequence number of the last stable checkpoint
        self.H = self.h + 200 # The high watermark, proposed value in the original article
        self.accepted_requests_time = {} # This is a dictionary of the accepted preprepare messages with the time they were accepted so that one the timer is reached, the node starts a wiew change. The dictionary has the form : {"request":starting_time...}. the request is discarded once it is executed.
        self.replies_time = {} # This is a dictionary of the accepted preprepare messages with the time they were replied to. The dictionary has the form : {"request": ["confirm",confirming_time]...}. the request is discarded once it is executed.
        self.received_view_changes = {} # Dictionary of received view-change messages (+ the view change the node itself sent) if the node is the primary node in the new view, it has the form: {new_view_number:[list_of_view_change_messages]}
        self.asked_view_change = [] # view numbers the node asked for

        self.received_prepare = {}  # The master node stores, for each request the node that answered and the results they sent, it had the form: self.received_prepare = {request:(node_id,result)}
        self.sent_requests_without_answer={} # Requests the primary sent but didn't get an answer yet + time when it was sent + list of nodes that answered to the request: self.sent_requests_without_answer={request:[timestamp,[nodes_that_answered]]}

       



    def process_received_message(self,received_message,waiting_time):
            global total_processed_messages
            message_type = received_message["message_type"]
            
            if (message_type=="PREPREPARE"):
                #print("prepare")
                node_id = received_message["node_id"]
                request = received_message["request"]
                digest = hashlib.sha256(request.encode()).hexdigest()
                requests_digest = received_message["request_digest"]
                
                total_processed_messages += 1
                processed_messages[node_id] += 1
                messages_processing_rate[node_id]=processed_messages[node_id]/total_processed_messages

                if(request not in number_of_messages):
                    number_of_messages[request]=1
                

          

                timestamp = received_message["timestamp"]
                request = received_message["request"]
                digest = hashlib.sha256(request.encode()).hexdigest()
                requests_digest = received_message["request_digest"]
                view = received_message["view_number"]
                tuple = (view,received_message["sequence_number"])

                

                if digest != requests_digest: # Means the node that sent the message changed the request => its score is decremented
                    scores[node_id] -= 5
                    if node_id not in malicious_nodes:
                        malicious_nodes.append(node_id)
                        consensus_nodes.remove(node_id)

                # Making sure the digest's request is good + the view number in the message is similar to the view number of the node + We did not broadcast a message with the same view number and sequence number
                if ((digest==requests_digest) and (view==self.view_number)): 
                    if request not in self.accepted_requests_time:
                        self.accepted_requests_time[request] = time.time() # Start timer
                    if tuple not in self.preprepares:
                        self.message_log.append(received_message)
                        self.preprepares[tuple]=digest 
                        self.broadcast_prepare_message(preprepare_message=received_message,consensus_nodes=consensus_nodes)
                       
            elif (message_type=="PREPARE"):
                total_processed_messages += 1
                node_id = received_message["node_id"]
                result = received_message["result"]
                processed_messages[node_id] += 1
                messages_processing_rate[node_id]=processed_messages[node_id]/total_processed_messages
                request = received_message["request"]
                digest = hashlib.sha256(request.encode()).hexdigest()
                requests_digest = received_message["request_digest"]

                if digest != requests_digest: # Means the node that sent the message changed the request => its score is decremented
                    scores[node_id] -= 5
                    if node_id not in malicious_nodes:
                        malicious_nodes.append(node_id)
                        consensus_nodes.remove(node_id)
                    
                number_of_messages[request] += 1
                
                timestamp = received_message["timestamp"]
                the_sequence_number=received_message["sequence_number"]
                the_request_digest=received_message["request_digest"]
                tuple = (received_message["view_number"],received_message["sequence_number"],received_message["request_digest"])
                node_id = received_message["node_id"]
                if ((received_message["view_number"]==self.view_number)): 
                    self.message_log.append(received_message)
                    if (tuple not in self.prepares):
                        self.prepares[tuple]=[node_id]
                    else:
                        if (node_id not in self.prepares[tuple]):
                            self.prepares[tuple].append(node_id)
                # Making sure the node inserted in its message log: a pre-prepare for m in view v with sequence number n
                p=0
                for message in self.message_log:
                    if ((message["message_type"]=="PREPREPARE") and (message["view_number"]==received_message["view_number"]) and (message["sequence_number"]==received_message["sequence_number"]) and (message["request"]==received_message["request"])):
                        p = 1
                        break
                # Second condition: Making sure the node inserted in its message log: 2f prepares from different backups that match the pre-preapare (same view, same sequence number and same digest)
                #print(len(self.prepares[tuple]))

                # Add the prepare message to the list:
                #self.received_prepare = {request:(node_id,result)}
                #if not self.received_prepare[request]:
                if request not in self.received_prepare:
                    self.received_prepare[request] = []
                self.received_prepare[request].append((node_id,result))

                if (p==1 and len(self.prepares[tuple])==(2*f)): # The 2*f received messages also include the node's own received message
                    self.prepared_messages.append(received_message)
                    time.sleep(waiting_time)
                    self.send_validate_message(prepare_message=received_message,consensus_nodes=consensus_nodes,sequence_number=the_sequence_number)

            elif (message_type=="VALIDATE"):

                if (self.node_id == self.primary_node_id): # Only the primary node receives validate messages and send a confirm to the other nodes

                    total_processed_messages += 1
                    node_id = received_message["node_id"]
                    processed_messages[node_id] += 1
                    messages_processing_rate[node_id]=processed_messages[node_id]/total_processed_messages

                    request = received_message["request"]
                    digest = hashlib.sha256(request.encode()).hexdigest()
                    requests_digest = received_message["request_digest"]
                    if digest != requests_digest: # Means the node that sent the message changed the request => its score is decremented
                        scores[node_id] -= 5
                        if node_id not in malicious_nodes:
                            malicious_nodes.append(node_id)
                            consensus_nodes.remove(node_id)

                    number_of_messages[request] += 1
                    timestamp = received_message["timestamp"]

                    # TO DO: Make sure that h<sequence number<H
                    timestamp = received_message["timestamp"]
                    sequence_number = received_message["sequence_number"]

                    # Make sure the message view number = the node view number
                    if (self.view_number == received_message["view_number"]):
                        self.message_log.append(received_message)
                        tuple = (received_message["view_number"],received_message["sequence_number"],received_message["request_digest"])
                        if (tuple not in self.responses):
                            self.responses[tuple]=1
                        else:
                            self.responses[tuple]=self.responses[tuple]+1

                        scores[node_id] += 2

                        if (self.responses[tuple]==(2*f) and (tuple in self.prepares)):
                            accepted_response = received_message["result"]
                        
                            if 1==1: # S'assurer que le timestamp est > au dernier timestamp validé
                                
                                confirm = self.broadcast_confirm_message (received_message)
                                #number_of_messages[request] += len(consensus_nodes)

                                if request in self.sent_requests_without_answer:
                                    sending_time = self.sent_requests_without_answer[request][0]
                                    receiving_time = time.time()
                                    delay = receiving_time - sending_time
                                    
                                    self.accepted_requests_time[received_message["request"]]=-1
                                    self.sent_requests_without_answer.pop(request)
                                    print("Operation validated within %f seconds. The network exchanged %d messages" % (delay,number_of_messages[request]))
                                    confirmed_request()

                                # Update nodes scores:

                                #print(self.received_prepare)

                                results = self.received_prepare[request]

                                for tuple in results:
                                    node_id  = tuple [0]
                                    result = tuple [1]
                             
                                    if result !=accepted_response :
                                        scores[node_id] -= 5
                                    else:
                                        scores[node_id] += 1

                            

                                if (sequence_number % checkpoint_frequency==0 and sequence_number not in self.checkpoints_sequence_number): # Creating a new checkpoint at each checkpoint creation period
                                    with open(checkpoint_format_file):
                                        checkpoint_format= open(checkpoint_format_file)
                                        checkpoint_message = json.load(checkpoint_format)
                                        checkpoint_format.close()
                                    checkpoint_message["sequence_number"] = sequence_number
                                    checkpoint_message["node_id"] = self.node_id
                                    checkpoint_content = [received_message["request_digest"],confirm] # We define the current state as the last executed request
                                    checkpoint_message["checkpoint_digest"]= hashlib.sha256(str(checkpoint_content).encode()).hexdigest()
                                    self.checkpoints_sequence_number.append(sequence_number)

                                    self.checkpoints[str(checkpoint_message)]=[self.node_id]

                                    # Generate a new random signing key
                                    signing_key = SigningKey.generate()

                                    # Sign the message with the signing key
                                    signed_checkpoint = signing_key.sign(str(checkpoint_message).encode())

                                    # Obtain the verify key for a given signing key
                                    verify_key = signing_key.verify_key

                                    # Serialize the verify key to send it to a third party
                                    public_key = verify_key.encode()

                                    checkpoint_message = signed_checkpoint +(b'split')+  public_key

                                    self.broadcast_message(consensus_nodes,checkpoint_message)

            elif (message_type=="CHECKPOINT"):
                lock = Lock()
                lock.acquire()
                for message in self.message_confirm:
                    if (message["message_type"]=="CONFIRM" and message["sequence_number"]==received_message["sequence_number"]):
                        confirm_list=[message["request_digest"],message["result"]]
                        confirm_digest = hashlib.sha256(str(confirm_list).encode()).hexdigest()
                        if (confirm_digest == received_message["checkpoint_digest"]):
                            with open(checkpoint_vote_format_file):
                                checkpoint_vote_format= open(checkpoint_vote_format_file)
                                checkpoint_vote_message = json.load(checkpoint_vote_format)
                                checkpoint_vote_format.close()
                            checkpoint_vote_message["sequence_number"] = received_message["sequence_number"]
                            checkpoint_vote_message["checkpoint_digest"] = received_message["checkpoint_digest"]
                            checkpoint_vote_message["node_id"] = self.node_id

                            # Generate a new random signing key
                            signing_key = SigningKey.generate()

                            # Sign the message with the signing key
                            signed_checkpoint_vote = signing_key.sign(str(checkpoint_vote_message).encode())

                            # Obtain the verify key for a given signing key
                            verify_key = signing_key.verify_key

                            # Serialize the verify key to send it to a third party
                            public_key = verify_key.encode()

                            checkpoint_vote_message = signed_checkpoint_vote +(b'split')+  public_key

                            self.send(received_message["node_id"],checkpoint_vote_message) 

                lock.release()

            elif (message_type=="VOTE"):
                lock = Lock()
                lock.acquire()
                for checkpoint in self.checkpoints:
                    checkpoint = checkpoint.replace("\'", "\"")
                    checkpoint = json.loads(checkpoint)
                    if (received_message["sequence_number"]==checkpoint["sequence_number"] and received_message["checkpoint_digest"]==checkpoint["checkpoint_digest"]):
                        node_id = received_message["node_id"]
                        if (node_id not in self.checkpoints[str(checkpoint)]):
                            self.checkpoints[str(checkpoint)].append(node_id)
                            if (len(self.checkpoints[str(checkpoint)]) == (2*f)):
                                # This will be the last stable checkpoint
                                self.stable_checkpoint = checkpoint
                                self.stable_checkpoint_validators = self.checkpoints[str(checkpoint)]
                                self.h = checkpoint["sequence_number"]
                                # TO DO: Delete checkpoints and messages log <= n
                                self.checkpoints.pop(str(checkpoint))
                                for message in self.message_log:
                                    if (message["message_type"] != "REQUEST"):
                                        if (message["sequence_number"]<= checkpoint["sequence_number"]):
                                            self.message_log.remove(message)
                                break
                lock.release()

            elif (message_type=="VIEW-CHANGE"):
                new_asked_view = received_message["new_view"]
                node_requester = received_message["node_id"]
                if (new_asked_view % len(consensus_nodes) == self.node_id): # If the actual node is the primary node for the next view
                    if new_asked_view not in self.received_view_changes:
                        self.received_view_changes[new_asked_view]=[received_message]
                    else:
                        requested_nodes = [] # Nodes that requested changing the view
                        for request in self.received_view_changes[new_asked_view]:
                            requested_nodes.append(request["node_id"])
                        if node_requester not in requested_nodes:
                            self.received_view_changes[new_asked_view].append(received_message)
                    if len(self.received_view_changes[new_asked_view])==2*f:
                        #The primary sends a view-change message for this view if it didn't do it before
                        if new_asked_view not in self.asked_view_change:
                            view_change_message = self.broadcast_view_change()
                            self.received_view_changes[new_asked_view].append(view_change_message)
                      
                        # Broadcast a new view message:
                        with open(new_view_format_file):
                            new_view_format= open(new_view_format_file)
                            new_view_message = json.load(new_view_format)
                            new_view_format.close()
                        new_view_message["new_view_number"]=new_asked_view

                        V=self.received_view_changes[new_asked_view]
                        new_view_message["V"]=V

                        # Creating the "O" set of the new view message:
                        # Initializing min_s and max_s:
                        min_s=0
                        max_s=0
                        if (len(V)>0):
                            sequence_numbers_in_V=[view_change_message["last_sequence_number"] for view_change_message in V]
                            min_s=min(sequence_numbers_in_V) # min sequence number of the latest stable checkpoint in V
                        sequence_numbers_in_prepare_messages=[message["sequence_number"] for message in self.message_log if message["message_type"]=="PREPARE"]
                        if len(sequence_numbers_in_prepare_messages)!=0:
                            max_s=max(sequence_numbers_in_prepare_messages)

                        O = []
                        
                        if (max_s>min_s):

                            # Creating a preprepare-view message for new_asked_view for each sequence number between max_s and min_s
                            for s in range (min_s,max_s):
                                with open(preprepare_format_file):
                                    preprepare_format= open(preprepare_format_file)
                                    preprepare_message = json.load(preprepare_format)
                                    preprepare_format.close()
                                preprepare_message["view_number"]=new_asked_view
                                preprepare_message["sequence_number"]=s

                                i=0 # There is no set Pm in P with sequence number s - In our code: there is no prepared message in P where sequence number = s (case 2 in the paper) => It turns i=1 if we find such a set
                                P = received_message["P"]
                                v=0 # Initiate the view number so that we can find the highest one in P
                                for message in P:
                                    if (message["sequence_number"])==s:
                                        i=1
                                        if (message["view_number"]>v):
                                            v = message["view_number"]
                                            d=message["request_digest"]
                                            r=message["request"]
                                            t=message["timestamp"]

                                            preprepare_message["request"]=r
                                            preprepare_message["timestamp"]=t

                                    # Restart timers:
                                    for request in self.accepted_requests_time:
                                                self.accepted_requests_time[request]=time.time()
                                                
                                    if (i==0): # Case 2
                                                preprepare_message["request_digest"]="null"
                                                O.append(preprepare_message)
                                                self.message_log.append(preprepare_message)
                                            
                                    else: # Case 1
                                                preprepare_message["request_digest"]=d
                                                O.append(preprepare_message)
                                                self.message_log.append(preprepare_message)

                        new_view_message["O"]=O

                        if (min_s>=self.stable_checkpoint["sequence_number"]):
                            # The primary node enters the new view
                            self.view_number=new_asked_view

                            # Decrement the credibility of the previous primary node
                            #credibility[self.primary_node_id] -= 1

                            # Change primary node (locally first then broadcast view change)
                            self.primary_node_id=self.node_id

                            self.broadcast_message(consensus_nodes,new_view_message)
                            print("New view!")

                            consensus_nodes.sort()
                           
            elif (message_type=="NEW-VIEW"):
                # TO DO : Verify the set O in the new view message

                # Restart timers:
                for request in self.accepted_requests_time:
                    self.accepted_requests_time[request]=time.time()
      
                O = received_message["O"]
                # Broadcast a prepare message for each preprepare message in O
                if len(O)!=0:
                    for message in O:
                        if (received_message["request_digest"]!="null"):
                            self.message_log.append(message)
                            prepare_message=self.broadcast_prepare_message(message,consensus_nodes)
                            self.message_log.append(prepare_message)                
                self.view_number = received_message["new_view_number"]
                self.primary_node_id=received_message["new_view_number"]%n
                self.asked_view_change.clear()
    
    def receive(self,waiting_time): # The waiting_time parameter is for nodes we want to be slow, they will wait for a few seconds before processing a message =0 by default
        while True:
            s = self.socket
            c,_ = s.accept()
            received_message = c.recv(2048)
            #print("Node %d got message: %s" % (self.node_id , received_message))
            [received_message,public_key] = received_message.split(b'split')

            # Create a VerifyKey object from a hex serialized public key    
            verify_key = VerifyKey(public_key)   
            received_message  = verify_key.verify(received_message).decode()
            received_message = received_message.replace("\'", "\"")
            received_message = json.loads(received_message)
            threading.Thread(target=self.check,args=(received_message,waiting_time,)).start()

    def check (self,received_message,waiting_time):
            # Start view change if one of the timers has reached the limit:
            
            i = 0 # Means no timer reached the limit , i = 1 means one of the timers reached their limit
            if len(self.accepted_requests_time)!=0 and len(self.asked_view_change)==0: # Check if the dictionary is not empty
                for request in self.accepted_requests_time:
                    if self.accepted_requests_time[request] != -1:
                        actual_time = time.time()
                        timer = self.accepted_requests_time[request]
                        if (actual_time - timer) >= timer_limit_before_view_change:
                            i = 1 # One of the timers reached their limit
                            new_view = self.view_number+1
                            break
            if i==1 and new_view not in self.asked_view_change:
                print("a")
                # Broadcast a view change:
                threading.Thread(target=self.broadcast_view_change,args=()).start()
                self.asked_view_change.append(new_view)
                for request in self.accepted_requests_time:
                    if self.accepted_requests_time[request] != -1:
                        self.accepted_requests_time[request]=time.time()
            message_type = received_message["message_type"]
            if i == 1 or len(self.asked_view_change)!= 0: # Only accept checkpoints, view changes and new-view messages
                    if message_type in ["CHECKPOINT","VOTE","VIEW-CHANGE","NEW-VIEW"]:
                            threading.Thread(target=self.process_received_message,args=(received_message,waiting_time,)).start()
            else: # if i==0 and len(self.asked_view_change)!= 0
                    request = received_message["request"]
                    threading.Thread(target=self.process_received_message,args=(received_message,waiting_time,)).start()

    def send(self,destination_node_id,message):
        destination_node_port = nodes_ports[destination_node_id]
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        host = socket.gethostname() 
        try:
            s.connect((host, destination_node_port))
            s.send(message)  
            s.close() 
        except:
            scores[destination_node_id] -= 2
            pass
     
    def broadcast_message(self,consensus_nodes,message): # Send to all connected nodes # Acts as a socket server
        for destination_node_id in consensus_nodes:
            if (destination_node_id != self.node_id):
                self.send(destination_node_id,message)

    def broadcast_preprepare_message(self,consensus_nodes,message): # The primary node prepares and broadcats a PREPREPARE message
        #if (replied_requests[request_message["request"]]==0):
            with open(preprepare_format_file):
                preprepare_format= open(preprepare_format_file)
                preprepare_message = json.load(preprepare_format)
                preprepare_format.close()
            preprepare_message["view_number"]=self.view_number
            global sequence_number
            preprepare_message["sequence_number"]=sequence_number
            now = datetime.datetime.now().timestamp()
            preprepare_message["timestamp"]=now
            tuple = (self.view_number,sequence_number)
            sequence_number = sequence_number + 1 # Increment the sequence number after each request 
            #Calculating the request's digest using SHA256
            request = message
            digest = hashlib.sha256(request.encode()).hexdigest()
            preprepare_message["request_digest"]=digest
            preprepare_message["request"]=request
            self.preprepares[tuple]=digest
            self.message_log.append(preprepare_message)

            # Generate a new random signing key
            signing_key = SigningKey.generate()

            # Sign the message with the signing key
            signed_preprepare = signing_key.sign(str(preprepare_message).encode())

            # Obtain the verify key for a given signing key
            verify_key = signing_key.verify_key

            # Serialize the verify key to send it to a third party
            public_key = verify_key.encode()

            preprepare_message = signed_preprepare +(b'split')+  public_key

            number_of_messages[request] = len(consensus_nodes)-1

            self.broadcast_message(consensus_nodes,preprepare_message)

            if (request not in self.sent_requests_without_answer):
                self.sent_requests_without_answer[request] = [now,[]]

    def broadcast_prepare_message(self,preprepare_message,consensus_nodes): # The node broadcasts a prepare message
        #if (replied_requests[preprepare_message["request"]]==0):
            # S'assurer des conditions avant d'envoyer le PREPARE message
            with open(prepare_format_file):
                prepare_format= open(prepare_format_file)
                prepare_message = json.load(prepare_format)
                prepare_format.close()
            prepare_message["view_number"]=self.view_number
            prepare_message["sequence_number"]=preprepare_message["sequence_number"]
            prepare_message["request_digest"]=preprepare_message["request_digest"]
            prepare_message["request"]=preprepare_message["request"]
            prepare_message["node_id"]=self.node_id
            prepare_message["timestamp"]=preprepare_message["timestamp"]

            # Generate a new random signing key
            signing_key = SigningKey.generate()

            # Sign the message with the signing key
            signed_prepare = signing_key.sign(str(prepare_message).encode())

            # Obtain the verify key for a given signing key
            verify_key = signing_key.verify_key

            # Serialize the verify key to send it to a third party
            public_key = verify_key.encode()

            prepare_message = signed_prepare +(b'split')+  public_key

            self.broadcast_message(consensus_nodes,prepare_message)

            return prepare_message

    def send_validate_message(self,prepare_message,consensus_nodes,sequence_number): # The node broadcasts a validate message
        #if (replied_requests[prepare_message["request"]]==0):
            with open(validate_format_file):
                validate_format= open(validate_format_file)
                validate_message = json.load(validate_format)
                validate_format.close()
            validate_message["view_number"]=self.view_number
            validate_message["sequence_number"]=sequence_number
            validate_message["node_id"]=self.node_id
            validate_message["request_digest"]=prepare_message["request_digest"]
            validate_message["request"]=prepare_message["request"]
            validate_message["timestamp"]=prepare_message["timestamp"]

            # Generate a new random signing key
            signing_key = SigningKey.generate()

            # Sign the message with the signing key
            signed_validate = signing_key.sign(str(validate_message).encode())

            # Obtain the verify key for a given signing key
            verify_key = signing_key.verify_key

            # Serialize the verify key to send it to a third party
            public_key = verify_key.encode()

            validate_message = signed_validate +(b'split')+  public_key

            self.send(self.primary_node_id,validate_message)

    def broadcast_view_change(self): # The node broadcasts a view change
        with open(view_change_format_file):
            view_change_format= open(view_change_format_file)
            view_change_message = json.load(view_change_format)
            view_change_format.close()
        new_view = self.view_number+1
        view_change_message["new_view"]=new_view
        view_change_message["last_sequence_number"]=self.stable_checkpoint["sequence_number"]
        view_change_message["C"]=self.stable_checkpoint_validators
        view_change_message["node_id"]=self.node_id
        if new_view not in self.received_view_changes:
            self.received_view_changes[new_view]=[view_change_message]
        else:
            self.received_view_changes[new_view].append(view_change_message)
        
        # We define P as a set of prepared messages at the actual node with sequence number higher than the sequence number in the last checkpoint
        view_change_message["P"]=[message for message in self.prepared_messages if message["sequence_number"]>self.stable_checkpoint["sequence_number"]]

        # Generate a new random signing key
        signing_key = SigningKey.generate()

        # Sign the message with the signing key
        signed_view_change = signing_key.sign(str(view_change_message).encode())

        # Obtain the verify key for a given signing key
        verify_key = signing_key.verify_key

        # Serialize the verify key to send it to a third party
        public_key = verify_key.encode()

        view_change_message = signed_view_change +(b'split')+  public_key

        self.broadcast_message(consensus_nodes,view_change_message)

        return view_change_message

    def broadcast_confirm_message (self,validate_message):
        with open(confirm_format_file):
            confirm_format= open(confirm_format_file)
            confirm_message = json.load(confirm_format)
            confirm_format.close()
        confirm_message["view_number"]=self.view_number
        confirm_message["node_id"]=self.node_id
        confirm_message["timestamp"]=validate_message["timestamp"]
        confirm = "Request executed"
        confirm_message["result"]=confirm
        confirm_message["sequence_number"]=validate_message["sequence_number"]
        confirm_message["request"]=validate_message["request"]
        confirm_message["request_digest"]=validate_message["request_digest"]

        # Generate a new random signing key
        signing_key = SigningKey.generate()

        # Sign the message with the signing key
        signed_confirm = signing_key.sign(str(confirm_message).encode())

        # Obtain the verify key for a given signing key
        verify_key = signing_key.verify_key

        # Serialize the verify key to send it to a third party
        public_key = verify_key.encode()

        signed_confirm_message = signed_confirm +(b'split')+  public_key

      
        self.broadcast_message(consensus_nodes,signed_confirm_message)

        return confirm
                             
class HonestNode(Node):
    def receive(self,waiting_time=0):
        Node.receive(self,waiting_time)

class SlowNode(Node):
    def receive(self,waiting_time=0.05):
        Node.receive(self,waiting_time)

class NonRespondingNode(Node):
    def receive(self):
        pass

# Partie à revoir
'''
          
class FaultyPrimary(Node): # This node changes the client's request digest while sending a preprepare message
    def receive(self,waiting_time=0):
        Node.receive(self,waiting_time)
    def broadcast_preprepare_message(self,request_message,consensus_nodes): # The primary node prepares and broadcats a PREPREPARE message
        with open(preprepare_format_file):
            preprepare_format= open(preprepare_format_file)
            preprepare_message = json.load(preprepare_format)
            preprepare_format.close()
        preprepare_message["view_number"]=self.view_number
        global sequence_number
        preprepare_message["sequence_number"]=sequence_number
        preprepare_message["timestamp"]=request_message["timestamp"]
        tuple = (self.view_number,sequence_number)
        sequence_number = sequence_number + 1 # Increment the sequence number after each request 
        #Calculating the request's digest using SHA256
        request = request_message["request"]+"abc"
        digest = hashlib.sha256(request.encode()).hexdigest()
        preprepare_message["request_digest"]=digest
        preprepare_message["request"]=request_message["request"]
        preprepare_message["client_id"]=request_message["client_id"]
        preprepare_message["node_id"]=self.node_id
        self.preprepares[tuple]=digest
        self.message_log.append(preprepare_message)

        # Generate a new random signing key
        signing_key = SigningKey.generate()

        # Sign the message with the signing key
        signed_preprepare = signing_key.sign(str(preprepare_message).encode())

        # Obtain the verify key for a given signing key
        verify_key = signing_key.verify_key

        # Serialize the verify key to send it to a third party
        public_key = verify_key.encode()

        preprepare_message = signed_preprepare +(b'split')+  public_key

        self.broadcast_message(consensus_nodes,preprepare_message)

class FaultyNode(Node): # This node changes digest in prepare message
    def receive(self,waiting_time=0):
        Node.receive(self,waiting_time)
    def broadcast_prepare_message(self,preprepare_message,consensus_nodes): # The node broadcasts a prepare message
        if (replied_requests[preprepare_message["request"]]==0):
            # S'assurer des conditions avant d'envoyer le PREPARE message
            with open(prepare_format_file):
                prepare_format= open(prepare_format_file)
                prepare_message = json.load(prepare_format)
                prepare_format.close()
            prepare_message["view_number"]=self.view_number
            prepare_message["sequence_number"]=preprepare_message["sequence_number"]
            prepare_message["request_digest"]=preprepare_message["request_digest"]+"abc"
            prepare_message["request"]=preprepare_message["request"]
            prepare_message["node_id"]=self.node_id
            prepare_message["client_id"]=preprepare_message["client_id"]
            prepare_message["timestamp"]=preprepare_message["timestamp"]

            # Generate a new random signing key
            signing_key = SigningKey.generate()

            # Sign the message with the signing key
            signed_prepare = signing_key.sign(str(prepare_message).encode())

            # Obtain the verify key for a given signing key
            verify_key = signing_key.verify_key

            # Serialize the verify key to send it to a third party
            public_key = verify_key.encode()

            prepare_message = signed_prepare +(b'split')+  public_key

            self.broadcast_message(consensus_nodes,prepare_message)

            return prepare_message

class FaultyRepliesNode(Node): # This node sends a fauly confirm to the client
    def receive(self,waiting_time=0):
        Node.receive(self,waiting_time)
    def send_confirm_message_to_client (self,validate_message):
        client_id = validate_message["client_id"]
        client_port = clients_ports[client_id]
        with open(confirm_format_file):
            confirm_format= open(confirm_format_file)
            confirm_message = json.load(confirm_format)
            confirm_format.close()
        confirm_message["view_number"]=self.view_number
        confirm_message["client_id"]=client_id
        confirm_message["node_id"]=self.node_id
        confirm_message["timestamp"]=validate_message["timestamp"]
        confirm = "Faulty confirm"
        confirm_message["result"]=confirm
        confirm_message["sequence_number"]=validate_message["sequence_number"]
        confirm_message["request"]=validate_message["request"]
        confirm_message["request_digest"]=validate_message["request_digest"]

        # Generate a new random signing key
        signing_key = SigningKey.generate()

        # Sign the message with the signing key
        signed_confirm = signing_key.sign(str(confirm_message).encode())

        # Obtain the verify key for a given signing key
        verify_key = signing_key.verify_key

        # Serialize the verify key to send it to a third party
        public_key = verify_key.encode()

        signed_confirm_message = signed_confirm +(b'split')+  public_key

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host = socket.gethostname() 
        try:
            s.connect((host, client_port))
            s.send(signed_confirm_message)
            s.close()
            self.message_confirm.append(confirm_message)
        except:
            pass
        return confirm

'''