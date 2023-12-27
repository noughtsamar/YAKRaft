from http.server import HTTPServer,BaseHTTPRequestHandler
from enum import Enum
import threading
import random
import time
import json
import requests
import uuid 
from datetime import datetime

HOST='192.168.208.50'
PORT=8080

metadata_list = []

class ElectionTimer:
	def __init__(self,timerCallback):
		self.thread=None
		self.lock=threading.Lock()
		self.timerCallback=timerCallback
		self.startTimer()

	def startTimer(self):
		self.stopTimer()
		self.thread=threading.Timer(random.uniform(500, 800) / 1000,self.timerCallback)
		self.thread.start()

	def stopTimer(self):
		with self.lock:
			if self.thread is not None and self.thread.is_alive():
				self.thread.cancel()

class HeartBeatTimer:
	def __init__(self,timerCallback):
		self.thread=None
		self.lock=threading.Lock()
		self.timerCallback=timerCallback
		self.startTimer()

	def startTimer(self):
		self.stopTimer()
		self.thread=threading.Timer(random.uniform(30, 60) / 1000,self.timerCallback)
		self.thread.start()

	def stopTimer(self):
		with self.lock:
			if self.thread is not None and self.thread.is_alive():
				self.thread.cancel()

class NodeState(Enum):
	follower=1
	candidate=2
	leader=3

class LogEntry:
	def __init__(self):
		self.term=0
		self.command=""

class RequestVoteRPC:
	def __init__(self,term,nodeID,lastLogIndex,lastLogTerm):
		self.term=term
		self.nodeID=nodeID
		self.lastLogIndex=lastLogIndex
		self.lastLogTerm=lastLogTerm

class RequestVoteResponse:
	def __init__(self,term,voteGranted):
		self.term=term
		self.voteGranted=voteGranted

class AppendEntriesRPC:
	def __init__(self,term,leaderID,prevLogIndex,prevLogTerm,entries,leaderCommit):
		self.term=term
		self.leaderID=leaderID	
		self.prevLogIndex=prevLogIndex	
		self.prevLogTerm=prevLogTerm
		self.entries=entries	
		self.leaderCommit=leaderCommit

class AppendEntriesResponse:
	def __init__(self,term,index,success):
		self.term=term
		self.index=index
		self.success=success

class NodeConfiguration:
	def __init__(self,nodeID,nodeState,currentTerm,votedFor,log):
		self.nodeID=nodeID
		self.nodeState=nodeState
		self.currentTerm=currentTerm
		self.votedFor=votedFor
		self.log=log		

class KRaftNode:
	def __init__(self,nodeID,clusterNodes):
		self.nodeID=nodeID
		self.clusterNodes=clusterNodes
		self.term=0
		self.electionTimer=ElectionTimer(self.startElection)
		self.heartBeatTimer=None
		self.logIndex=0
		self.commitIndex=0
		self.nodeState=NodeState.follower
		self.votedFor=None
		self.log=[LogEntry()]

	def requestVote(self,rpc):
		#If the node requesting for vote is behind the current term
		if rpc.term < self.term:
			return RequestVoteResponse(self.term,False)

		#If the node requesting for vote is ahead of the current term
		if rpc.term > self.term:
			self.term=rpc.term
			self.votedFor=rpc.nodeID

		#If the current node has given its vote to some other node for the current term
		if self.votedFor and self.votedFor!=rpc.nodeID:
			return RequestVoteResponse(self.term,False)

		lastLogIndex=len(self.log)-1
		lastLogTerm=self.log[lastLogIndex].term

		#If the candidates log is not up to date
		if lastLogTerm > rpc.lastLogTerm or (lastLogTerm==rpc.lastLogTerm and lastLogIndex>rpc.lastLogIndex):
			return RequestVoteResponse(self.term,False)

		#Giving the vote
		self.votedFor=rpc.nodeID
		return RequestVoteResponse(self.term,True) 
		
	def appendEntries(self,rpc):
		#If the node requesting to append log entry is behind the current term
		if rpc.term < self.term:
			return RequestVoteResponse(self.term,self.commitIndex,False)

		#If the node requesting to append log entry is ahead of the current term
		if rpc.term > self.term:
			self.term=rpc.term
			self.nodeState=NodeState.follower

		#If the previous log index and term don't match the node's lag
		prevLogIndex=rpc.prevLogIndex
		prevLogTerm=rpc.prevLogTerm
		if prevLogIndex>=len(self.log) or self.log[prevLogIndex].term!=prevLogTerm:
			return AppendEntriesResponse(self.term,self.commitIndex,False)

		#Add the entry
		self.log=[*self.log[:prevLogIndex+1],*rpc.entries]
		self.commitIndex=min(rpc.leaderCommit,len(self.log)-1)
		return AppendEntriesResponse(self.term,self.commitIndex,True)

	def startElection(self):
		print(self.term)
		print(self.nodeState)
		#Make the node a candidate for the next term
		self.term+=1
		self.nodeState=NodeState.candidate

		#Reset the voted for field
		self.votedFor=None
		#Request votes from other nodes
		self.sendRequestVoteRPC()
		self.electionTimer.startTimer()

	def sendRequestVoteRPC(self):	
		responses=[]
		for node_id,node_host, node_port in self.clusterNodes:
			# Prepare the RequestVoteRPC object
			request_vote_rpc = RequestVoteRPC(self.term, self.nodeID, len(self.log) - 1, self.log[-1].term)

			# Make a POST request to the other node
			try:
				response = requests.post(f'http://{node_host}:{node_port}/request_vote', json=request_vote_rpc.__dict__,timeout=0.5)

				# Handle the response
				if response.status_code == 200:
					response_data = response.json()
					print(response_data)
					responses.append(RequestVoteResponse(response_data['term'],response_data['voteGranted']))		
			except requests.exceptions.Timeout:
				print(f'Node {node_id} is not responding')										
		self.handleRequestVoteResponses(responses)

	def handleRequestVoteResponses(self,responses):
		majority=(len(responses) + 1)//2 + 1

		votesObtained=list(filter(lambda x:x.voteGranted,responses))

		if len(votesObtained)+1>=majority:
			self.nodeState=NodeState.leader
			self.heartBeatTimer=HeartBeatTimer(self.sendAppendEntriesRPC)

	def insert_metadata(self):
		for node_id,node_host, node_port in self.clusterNodes:
			# Send the filtered records as a JSON response
			response_data = {"selected_elements": metadata_list}
			# print(response_data)
			try:
				response = requests.post("http://"+node_host+":"+str(node_port)+"/send_record_all", json=response_data, timeout=0.5)
			except requests.exceptions.Timeout:
				continue
			except requests.exceptions.ConnectionError:
				print("Unable to connect to the node.", node_id)

	def sendAppendEntriesRPC(self):
		responses=[]
		for node_id,node_host, node_port in self.clusterNodes:
			# Prepare the RequestVoteRPC object
			append_entries_rpc = AppendEntriesRPC(self.term, self.nodeID, len(self.log) - 1, self.log[-1].term,self.log[self.commitIndex+1:],self.commitIndex)

			# Make a POST request to the other node
			try:
				response = requests.post(f'http://{node_host}:{node_port}/append_entry', json=append_entries_rpc.__dict__,timeout=0.5)

				# Handle the response
				if response.status_code == 200:
					response_data = response.json()
					print(response_data)
					responses.append(AppendEntriesResponse(response_data['term'],response_data['index'],response_data['success']))	
					# self.insert_metadata()	
			except requests.exceptions.Timeout:
				print(f'Node {node_id} is not responding')
			except requests.exceptions.ConnectionError:
				print(f'Leader already exists')
		if self.heartBeatTimer:
			self.heartBeatTimer.startTimer()										
		self.advanceCommitIndex(responses)

	def handleRequestVoteRPC(self,rpc):
		#If the node requesting for vote is behind the current term
		if rpc.term < self.term:
			return RequestVoteResponse(self.term,False)

		#If the node requesting for vote is ahead of the current term
		if rpc.term > self.term:
			self.term=rpc.term
			self.nodeState=NodeState.follower

		#If a node is a leader or candidate
		if self.nodeState==NodeState.leader or self.nodeState==NodeState.candidate:
			return RequestVoteResponse(self.term,False)

		#Node is already voted
		if self.votedFor and self.votedFor!=rpc.nodeID:
			return RequestVoteResponse(self.term,False)

		return self.requestVote(rpc)

	def handleAppendEntriesRPC(self,rpc):
		self.electionTimer.startTimer()
		print(self.nodeState)
		#If rpc is behind the current term
		if rpc.term < self.term:
			return AppendEntriesResponse(self.term,self.commitIndex,False)

		#If rpc is ahead of the current term
		if rpc.term > self.term:
			self.term=rpc.term
			self.nodeState=NodeState.follower
			if self.heartBeatTimer:
				self.heartBeatTimer.stopTimer()
				self.heartBeatTimer=None

		#If a node is a leader
		if self.nodeState==NodeState.leader:
			return AppendEntriesResponse(self.term,self.commitIndex,False)

		return self.appendEntries(rpc)	

	def advanceCommitIndex(self,responses):
		responses.sort(key=lambda x:(x.term,x.index))

		majority=len(responses)//2 + 1
		commitIndex=0

		#Setting the commitIndex of the leader to the highest commitIndex of majority nodes
		for i in range(len(responses)):
			if len(list(filter(lambda x:x.success,responses[:i+1]))) >= majority:
				commitIndex=responses[i].index
		
		self.commitIndex=commitIndex



node=KRaftNode('1',[('2','192.168.208.189', 8080)])

# class MetadataStorage:
#     def __init__(self):
#         self.register_broker_records = {'records': [], 'timestamp': None}
#         self.topic_records = {'records': [], 'timestamp': None}
#         self.partition_records = {'records': [], 'timestamp': None}

#     def add_register_broker_record(self, record):
#         record['fields']['internalUUID'] = f"generated_uuid_{len(self.register_broker_records['records'])}"
#         record['fields']['epoch'] += 1
#         self.register_broker_records['records'].append(record)
#         self.register_broker_records['timestamp'] = time.time()

#     def get_all_register_broker_records(self):
#         return self.register_broker_records['records']

#     def add_topic_record(self, record):
#         record['fields']['topicUUID'] = f"generated_topic_UUID_{len(self.topic_records['records'])}"
#         self.topic_records['records'].append(record)
#         self.topic_records['timestamp'] = time.time()

#     def get_all_topic_records(self):
#         return self.topic_records['records']

#     def add_partition_record(self, record):
#         record['fields']['partitionId'] = len(self.partition_records['records']) + 1
#         self.partition_records['records'].append(record)
#         self.partition_records['timestamp'] = time.time()

#     def get_all_partition_records(self):
#         return self.partition_records['records']

# metadata = MetadataStorage()

class Server(BaseHTTPRequestHandler):
	def __init__(self, request, client_address, server):
		super().__init__(request, client_address, server)
		
	def do_DELETE(self):
		if self.path.startswith('/delete_record'):
			if node.nodeState == NodeState.leader:
				# Extract broker ID from the URL
				broker_id = int(self.path.split('/')[-1])

				# Process the DELETE request (replace this with your logic)
				for meta in metadata_list:
					if meta['name'] == "RegisterBrokerRecord" and meta['brokerId'] == broker_id:
						break
				metadata_list.remove(meta)

				# Send the response back to the client
				self.send_response(200)
				self.send_header('Content-type', 'application/json')
				self.end_headers()

				response_data = {'status': "deleted"}
				self.wfile.write(bytes(json.dumps(response_data), 'utf-8'))
			else:
				self.send_response(200)
				self.send_header('Content-type', 'application/json')
				self.end_headers()
				response_data = {'status': "not the leader"}
				self.wfile.write(bytes(json.dumps(response_data), 'utf-8'))
		else:
		# Handle other paths
			self.send_response(404)
			self.send_header('Content-type', 'text/plain')
			self.end_headers()
			self.wfile.write(bytes('Not Found', 'utf-8'))


	def do_GET(self):
		if self.path == '/read_record' and node.nodeState == NodeState.leader:
		    # Filter metadata_list to get records with name 'RegisterBrokerRecord'
			register_broker_records = [record for record in metadata_list if record.get('name') == 'RegisterBrokerRecord']
			# Send the filtered records as a JSON response
			response_data = {"metadata":register_broker_records}
			print(response_data)
			# requests.post("http://"+self.client_address[0]+":"+str(self.client_address[1]),json=response_data)
			self.send_response(200)
			self.send_header('Content-type', 'application/json')
			self.end_headers()
			response_data = json.dumps(register_broker_records).encode('utf-8')
			self.wfile.write(response_data)
		else:
			self.send_response(200)
			self.send_header('Content-type', 'application/json')
			self.end_headers()
			response_data = json.dumps([]).encode('utf-8')
			self.wfile.write(response_data)

	def do_POST(self):
		content_length = int(self.headers['Content-Length'])
		post_data = self.rfile.read(content_length)
		rpc_data = json.loads(post_data.decode('utf-8'))
		rpc_data_list = rpc_data.get('selected_elements', [])
		if self.path == '/request_vote':
			# Handle the RequestVoteRPC message
			response = node.handleRequestVoteRPC(RequestVoteRPC(**rpc_data))

			# Send the response back to the requesting node
			self.send_response(200)
			self.send_header('Content-type', 'application/json')
			self.end_headers()
			self.wfile.write(bytes(json.dumps(response.__dict__), 'utf-8'))

		if self.path == '/append_entry':
			# Handle the RequestVoteRPC message
			response = node.handleAppendEntriesRPC(AppendEntriesRPC(**rpc_data))

			# Send the response back to the requesting node
			self.send_response(200)
			self.send_header('Content-type', 'application/json')
			self.end_headers()
			self.wfile.write(bytes(json.dumps(response.__dict__), 'utf-8'))

		if self.path == '/send_record':
			if node.nodeState == NodeState.leader:
				print("Received record data:")
				# print(json.dumps(rpc_data, indent=2))
				# Send a response back to the client (you can customize this response if needed)
				self.send_response(200)
				self.send_header('Content-type', 'application/json')
				self.end_headers()
				self.wfile.write(bytes('{"status": "Record received successfully"}', 'utf-8'))
				for rpc in rpc_data_list:
					if rpc['name'] == "RegisterBrokerRecord":
						broker_id = rpc['fields']['brokerId']
						broker_host = rpc['fields']['brokerHost']
						broker_port = rpc['fields']['brokerPort']
						security_protocol = rpc['fields']['securityProtocol']
						rack_id = rpc['fields']['rackId']

						# Create a new broker record
						new_broker_record = {
						"name": "RegisterBrokerRecord",
						"internalUUID": str(uuid.uuid4()),  # generate a UUID for internal use
						"brokerId": broker_id,
						"brokerHost": broker_host,
						"brokerPort": broker_port,
						"securityProtocol": security_protocol,
						"brokerStatus": "INIT",  # set an initial status
						"rackId": rack_id,
						"epoch": 0,
						"timestamp":str(datetime.now())
						}

						# Update your metadata store with the new broker record
						metadata_list.append(new_broker_record)
					if rpc['name'] == "PartitionRecord":
						pid=rpc['fields']['partitionId']
						broker_record={
							"name": "PartitionRecord",
							"partitionId": pid,
							"topicUUID": str(uuid.uuid4()),
							"replicas": rpc['fields']['replicas'],
							"ISR":rpc['fields']['ISR'],
							"removingReplicas": rpc['fields']['removingReplicas'],
							"leader": rpc['fields']['leader'],
							"partitionEpoch":rpc['fields']['partitionEpoch'],
							"timestamp":str(datetime.now())
						}
						metadata_list.append(broker_record)
					
					if rpc['name'] == "ProducerIdsRecord":
						broker_record={
							"name": "ProducerIdsRecord",
							"brokerId": rpc['fields']['brokerId'],
							"brokerEpoch": rpc['fields']['brokerEpoch'],
							"producerId": rpc['fields']['producerId'],
							"timestamp":str(datetime.now())
						}
						metadata_list.append(broker_record)
					
					if rpc['name'] == "TopicRecord":
						broker_record={
							"name": "TopicRecord",
							"topicUUID": str(uuid.uuid4()),
							"nameTopic": rpc['fields']['name'],
							"timestamp":str(datetime.now())
						}
						metadata_list.append(broker_record)
					
					if rpc['name'] == "RegistrationChangeBrokerRecord":
						for meta in metadata_list:
							if meta['name'] == "RegisterBrokerRecord" and meta['brokerId'] == rpc['fields']['brokerId']:
								print("Older values:",meta)
								broker_id = rpc['fields']['brokerId']
								broker_host = rpc['fields']['brokerHost']
								broker_port = rpc['fields']['brokerPort']
								security_protocol = rpc['fields']['securityProtocol']
								rack_id = meta['rackId']

								# Create a new broker record
								new_broker_record = {
								"name": "RegisterBrokerRecord",
								"internalUUID": meta['internalUUID'],  # generate a UUID for internal use
								"brokerId": broker_id,
								"brokerHost": broker_host,
								"brokerPort": broker_port,
								"securityProtocol": security_protocol,
								"brokerStatus": "INIT",  # set an initial status
								"rackId": rack_id,
								"epoch": meta['epoch']+1,
								"timestamp":str(datetime.now())
								}
								print("New Values:",new_broker_record)
								break

								# Update your metadata store with the new broker record
						metadata_list.append(new_broker_record) #updated broker
						metadata_list.remove(meta) #unregister
				print(metadata_list)
			else:
				self.send_response(200)
				self.send_header('Content-type', 'application/json')
				self.end_headers()
				self.wfile.write(bytes('{"status": "Not leader"}', 'utf-8'))


		if self.path == '/send_record_all':
			print("Received record data:")
			# print(json.dumps(rpc_data, indent=2))
			# Send a response back to the client (you can customize this response if needed)
			self.send_response(200)
			self.send_header('Content-type', 'application/json')
			self.end_headers()
			self.wfile.write(bytes('{"status": "Record received successfully"}', 'utf-8'))
			for rpc in rpc_data_list:
				if rpc['name'] == "RegisterBrokerRecord":
					broker_id = rpc['fields']['brokerId']
					broker_host = rpc['fields']['brokerHost']
					broker_port = rpc['fields']['brokerPort']
					security_protocol = rpc['fields']['securityProtocol']
					rack_id = rpc['fields']['rackId']

					# Create a new broker record
					new_broker_record = {
					"name": "RegisterBrokerRecord",
					"internalUUID": str(uuid.uuid4()),  # generate a UUID for internal use
					"brokerId": broker_id,
					"brokerHost": broker_host,
					"brokerPort": broker_port,
					"securityProtocol": security_protocol,
					"brokerStatus": "INIT",  # set an initial status
					"rackId": rack_id,
					"epoch": 0,
					"timestamp":str(datetime.now())
					}

					# Update your metadata store with the new broker record
					metadata_list.append(new_broker_record)
				if rpc['name'] == "PartitionRecord":
					pid=rpc['fields']['partitionId']
					broker_record={
						"name": "PartitionRecord",
						"partitionId": pid,
						"topicUUID": str(uuid.uuid4()),
						"replicas": rpc['fields']['replicas'],
						"ISR":rpc['fields']['ISR'],
						"removingReplicas": rpc['fields']['removingReplicas'],
						"leader": rpc['fields']['leader'],
						"partitionEpoch":rpc['fields']['partitionEpoch'],
						"timestamp":str(datetime.now())
					}
					metadata_list.append(broker_record)
				
				if rpc['name'] == "ProducerIdsRecord":
					broker_record={
						"name": "ProducerIdsRecord",
						"brokerId": rpc['fields']['brokerId'],
						"brokerEpoch": rpc['fields']['brokerEpoch'],
						"producerId": rpc['fields']['producerId'],
						"timestamp":str(datetime.now())
					}
					metadata_list.append(broker_record)
				
				if rpc['name'] == "TopicRecord":
					broker_record={
						"name": "TopicRecord",
						"topicUUID": str(uuid.uuid4()),
						"nameTopic": rpc['fields']['name'],
						"timestamp":str(datetime.now())
					}
					metadata_list.append(broker_record)
				
				if rpc['name'] == "RegistrationChangeBrokerRecord":
					for meta in metadata_list:
						if meta['name'] == "RegisterBrokerRecord" and meta['brokerId'] == rpc['fields']['brokerId']:
							print("Older values:",meta)
							broker_id = rpc['fields']['brokerId']
							broker_host = rpc['fields']['brokerHost']
							broker_port = rpc['fields']['brokerPort']
							security_protocol = rpc['fields']['securityProtocol']
							rack_id = meta['rackId']

							# Create a new broker record
							new_broker_record = {
							"name": "RegisterBrokerRecord",
							"internalUUID": meta['internalUUID'],  # generate a UUID for internal use
							"brokerId": broker_id,
							"brokerHost": broker_host,
							"brokerPort": broker_port,
							"securityProtocol": security_protocol,
							"brokerStatus": "INIT",  # set an initial status
							"rackId": rack_id,
							"epoch": meta['epoch']+1,
							"timestamp":str(datetime.now())
							}
							print("New Values:",new_broker_record)
							break

							# Update your metadata store with the new broker record
					metadata_list.append(new_broker_record) #updated broker
					metadata_list.remove(meta) #unregister
			print(metadata_list)
			
			
server=HTTPServer((HOST,PORT),Server)
print(f'Server running at {HOST}:{PORT}')
server.serve_forever()
server.serve_close()
