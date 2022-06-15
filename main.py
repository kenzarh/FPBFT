from BFT import *

import threading
import time

# Parameters to be defined by the user

waiting_time_before_resending_request = 20000 # Time the client will wait before resending the request. This time, it broadcasts the request to all nodes
timer_limit_before_view_change = 20000 # There is no value proposed in the paper so let's fix it to 120s
checkpoint_frequency = 100 # 100 is the proposed value in the original article

# Define the proportion of nodes we want in the consensus set
p = 1/2

# Define the nodes we want in our network + their starting time + their type
nodes={} # This is a dictionary of nodes we want in our network. Keys are the nodes types, and values are a list of tuples of starting time and number of nodes 
#nodes[starting time] = [(type of nodes , number of nodes)]
nodes[0]=[("faulty_primary",0),("slow_nodes",4),("honest_node",3),("non_responding_node",2),("faulty_node",0),("faulty_replies_node",0)] # Nodes starting from the beginning

#nodes[0]=[("faulty_primary",0),("slow_nodes",15),("honest_node",15),("non_responding_node",2),("faulty_node",0),("faulty_replies_node",0)] # Nodes starting from the beginning
#nodes[1]=[("faulty_primary",0),("honest_node",1),("non_responding_node",0),("slow_nodes",1),("faulty_node",1),("faulty_replies_node",0)] # Nodes starting after 2 seconds
#nodes[2]=[("faulty_primary",0),("honest_node",0),("non_responding_node",0),("slow_nodes",2),("faulty_node",1),("faulty_replies_node",0)]

# Running BFT protocol
run_BFT(nodes=nodes,proportion=p,checkpoint_frequency0=checkpoint_frequency,timer_limit_before_view_change0=timer_limit_before_view_change)

time.sleep(1) # Waiting for the network to start...

# Choose the number of "blocks" to validate
blocks_number = 5  # The user chooses the number of blocks he wants to execute simultaneously (They are all sent to the PBFT network at the same time)

for i in range (blocks_number):
    Primary = get_primary()# The primary_node
    Nodes = get_nodes_ids_list()
    #print(Nodes)
    message = "This is the operation " + str(i)
    threading.Thread(target=Primary.broadcast_preprepare_message(Nodes,message)).start()
    time.sleep(5) #Exécution plus rapide lorsqu'on attend un moment avant de lancer la requête suivante