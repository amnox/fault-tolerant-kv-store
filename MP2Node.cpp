/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
Transaction::Transaction(int id, int msgType,string key, string value, bool valid, int timeout,int replies): id(id), msgType(msgType), key(key),  value(value), valid(valid),timeout(timeout), replies(replies){}

/**
 * Copy constructor
 */
Transaction::Transaction(const Transaction &anotherMLE) {
	this->id = anotherMLE.id;
	this->msgType = anotherMLE.msgType;
	this->key = anotherMLE.key;
	this->value = anotherMLE.value;
	this->valid = anotherMLE.valid;
	this->timeout=anotherMLE.timeout;
	this->replies=anotherMLE.replies;
}

Transaction::Transaction(){}



MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();
	
	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
	ring = curMemList;

	/*for (vector<Node>::iterator i = curMemList.begin(); i != curMemList.end(); i++) {

        cout<<i->getHashCode()<<", ";
    }
	cout<<"This was "<<to_string(memberNode->addr.addr[0])<<endl;*/
	vector<Node> diss;
	diss = curMemList;
	for (int i = 0; i<curMemList.size() ; i++){
		//cout<<"Then: "<<diss.front().getHashCode()<<" Size: "<<diss.size()<<"\n";
		vector<Node>::iterator it = diss.begin();
		Node current = diss.front();
		diss.erase(it);
		diss.emplace_back(current);
		//cout<<"Now: "<<diss.front().getHashCode()<<" Size: "<<diss.size()<<"\n";
		if(current.nodeAddress.addr[0]== memberNode->addr.addr[0]){
			//cout<<"I'm inside "<<to_string(current.nodeAddress.addr[0])<<" ,AKA: "<<current.getHashCode()<<"\n";
			if(hasMyReplicas.empty() || haveReplicasOf.empty()) {
				hasMyReplicas.emplace_back(diss[0]);
				hasMyReplicas.emplace_back(diss[1]);
				haveReplicasOf.emplace_back(diss[diss.size()-2]);
				haveReplicasOf.emplace_back(diss[diss.size()-3]);
			} else if(hasMyReplicas[0].getHashCode() != diss[0].getHashCode() || hasMyReplicas[1].getHashCode()!=diss[1].getHashCode()){
				// One of my replicas have failed
			} else if (haveReplicasOf[0].getHashCode()!=diss[diss.size()-2].getHashCode()){
				// My predecessor failed, time to be the primary of its keys!
			}

		}
	}
	/*cout<<"My Replicas live at: ";
	for (vector<Node>::iterator i = hasMyReplicas.begin(); i != hasMyReplicas.end(); i++) {

        cout<<i->getHashCode()<<", ";
    }
	cout<<"\n";
	cout<<"I have: ";
	for (vector<Node>::iterator i = haveReplicasOf.begin(); i != haveReplicasOf.end(); i++) {

        cout<<i->getHashCode()<<", ";
    }
	cout<<"\n";*/
	diss.clear();

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	check_timeout();
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
	/*	cout<<"Incoming => {'"<<key<<"' : '"<<value<<"'}"<<g_transID<<endl;
	cout<<"To be stored @: "<<keyReplicas[0].getHashCode()<<", "<<keyReplicas[1].getHashCode()<<", "<<keyReplicas[2].getHashCode()<<endl;
	*/
	vector<Node> keyReplicas = findNodes(key);
	Transaction new_create_trans(g_transID,MessageType::CREATE,key,value, true,0,0);
	Message msg(new_create_trans.id, this->memberNode->addr, MessageType::CREATE, key, value);
	
	if (keyReplicas.size() == 3) {
		this->emulNet->ENsend(&this->memberNode->addr, keyReplicas[0].getAddress(), msg.toString());
		this->emulNet->ENsend(&this->memberNode->addr, keyReplicas[1].getAddress(), msg.toString());
		this->emulNet->ENsend(&this->memberNode->addr, keyReplicas[2].getAddress(), msg.toString());
		transactions.emplace_back(new_create_trans);
		g_transID++;
	}
	
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
	vector<Node> keyReplicas = findNodes(key);
	
	Transaction new_read_trans(g_transID,MessageType::READ,key,"READ", true, this->par->globaltime,0);
	new_read_trans.timeout=this->par->globaltime;
	Message msg(g_transID, this->memberNode->addr, MessageType::READ, key);

	if (keyReplicas.size() == 3) {
		this->emulNet->ENsend(&this->memberNode->addr, keyReplicas[0].getAddress(), msg.toString());
		this->emulNet->ENsend(&this->memberNode->addr, keyReplicas[1].getAddress(), msg.toString());
		this->emulNet->ENsend(&this->memberNode->addr, keyReplicas[2].getAddress(), msg.toString());
		g_transID++;
		transactions.emplace_back(new_read_trans);
	}
	
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */

	vector<Node> keyReplicas = findNodes(key);

	Message msg(g_transID, this->memberNode->addr, MessageType::UPDATE, key, value);

	if (keyReplicas.size() == 3) {
		this->emulNet->ENsend(&this->memberNode->addr, keyReplicas[0].getAddress(), msg.toString());
		this->emulNet->ENsend(&this->memberNode->addr, keyReplicas[1].getAddress(), msg.toString());
		this->emulNet->ENsend(&this->memberNode->addr, keyReplicas[2].getAddress(), msg.toString());

	}
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
	vector<Node> keyReplicas = findNodes(key);
	Transaction new_delete_trans(g_transID,MessageType::DELETE,key,"DELETE", true, 0,0);
	Message msg(g_transID, this->memberNode->addr, MessageType::DELETE, key);

	if (keyReplicas.size() == 3) {
		this->emulNet->ENsend(&this->memberNode->addr, keyReplicas[0].getAddress(), msg.toString());
		this->emulNet->ENsend(&this->memberNode->addr, keyReplicas[1].getAddress(), msg.toString());
		this->emulNet->ENsend(&this->memberNode->addr, keyReplicas[2].getAddress(), msg.toString());
		transactions.emplace_back(new_delete_trans);
		g_transID++;
	}
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table

	this->ht->create(key, Entry(value, par->getcurrtime(), replica).convertToString());
	vector<Node> replicas = findNodes(key);

	if (replicas.size() == 3){
		return true;
	}

	return false;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	string entryStr = this->ht->read(key);
	if (!entryStr.empty()){
		return Entry(entryStr).value;
	}

	return entryStr;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table

	return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */

		Message msg(message);
		//cout<<"Node: "<<to_string(memberNode->addr.addr[0])<<" Key: "<<msg.key<<endl;
		switch (msg.type) {
			case MessageType::CREATE:
				handle_create_msg(msg);
				break;
			case MessageType::DELETE:
				handle_delete_msg(msg);
				break;
			case MessageType::READ:
				handle_read_msg(msg);
				break;
			case MessageType::UPDATE:
				handle_update_msg(msg);
				break;
			case MessageType::REPLY:
				handle_reply_msg(msg);
				break;
			case MessageType::READREPLY:
				handle_readreply_msg(msg);
				break;
			default:
				// TODO ERROR
				break;
		}

	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
}

void MP2Node::handle_create_msg(Message msg){
	bool created  = createKeyValue(msg.key, msg.value, msg.replica);
	if (created){
		log->logCreateSuccess(&this->memberNode->addr, false, msg.transID, msg.key, msg.value);
		Message reply(msg.transID, this->memberNode->addr, MessageType::REPLY, created);
		this->emulNet->ENsend(&this->memberNode->addr, &msg.fromAddr, reply.toString());
	}
	else{
		log->logCreateFail(&this->memberNode->addr, false, msg.transID, msg.key, msg.value);
	}
	
}
void MP2Node::handle_read_msg(Message msg){
	string returned_value = readKey(msg.key);
	if (!returned_value.empty()){
		log->logReadSuccess(&this->memberNode->addr, false, msg.transID, msg.key,returned_value);
		Message reply(msg.transID, this->memberNode->addr, returned_value);
		this->emulNet->ENsend(&this->memberNode->addr, &msg.fromAddr, reply.toString());
	} else {
		log->logReadFail(&this->memberNode->addr, false, msg.transID, msg.key);
	}
}
void MP2Node::handle_update_msg(Message msg){

}
void MP2Node::handle_delete_msg(Message msg){
	bool deleted = deletekey(msg.key);
	if (deleted){
		log->logDeleteSuccess(&this->memberNode->addr, false, msg.transID, msg.key);
	} else{
		log->logDeleteFail(&this->memberNode->addr, false, msg.transID, msg.key);
	}

	Message reply(msg.transID, this->memberNode->addr, MessageType::REPLY, deleted);
	this->emulNet->ENsend(&this->memberNode->addr, &msg.fromAddr, reply.toString());
}
void MP2Node::handle_reply_msg(Message msg){
	Transaction current_transaction;
	
	for(vector<Transaction>::iterator i = transactions.begin(); i != transactions.end(); i++){
		if(msg.transID==i->id){
			//cout<<i->key;
			current_transaction.id = i->id;
			current_transaction.msgType = i->msgType;
			current_transaction.key = i->key;
			current_transaction.value = i->value;
			current_transaction.valid = i->valid;
			i->valid=false;
			break;
		}
	}
	
	switch(current_transaction.msgType){
		case MessageType::CREATE:
			if(current_transaction.valid){
				log->logCreateSuccess(&this->memberNode->addr, true, msg.transID, current_transaction.key, current_transaction.value);
			}
			
			break;
		case MessageType::DELETE:
			if(current_transaction.valid && msg.success){
				log->logDeleteSuccess(&this->memberNode->addr, true, msg.transID, current_transaction.key);
			} else if(!msg.success && current_transaction.valid){
				
				log->logDeleteFail(&this->memberNode->addr, true, msg.transID, current_transaction.key);
			}
			
			break;
		
	}
}

void MP2Node::handle_readreply_msg(Message msg){
	Transaction current_transaction;
	string returned_value = msg.value;

	for(vector<Transaction>::iterator i = transactions.begin(); i != transactions.end(); i++){
		if(msg.transID==i->id){
			//cout<<i->key;
			current_transaction.id = i->id;
			current_transaction.msgType = i->msgType;
			current_transaction.key = i->key;
			current_transaction.value = i->value;
			current_transaction.valid = i->valid;
			current_transaction.replies=++i->replies;
			i->value=returned_value;
			if(current_transaction.replies>=2){
				i->valid=false;
			}
			break;
		}
	}
	//cout<<"Transaction: "<<current_transaction.id<<" Reply no: "<<current_transaction.replies<<" Truth:"<<(current_transaction.valid && !returned_value.empty() && current_transaction.replies>=2)<<endl;
	if(current_transaction.valid && !returned_value.empty() && current_transaction.replies>=2){
		log->logReadSuccess(&this->memberNode->addr, true, msg.transID, current_transaction.key, returned_value);
	} else if(returned_value.empty() && current_transaction.valid){
		
		log->logReadFail(&this->memberNode->addr, true, msg.transID, msg.key);
	}


	//log->logReadSuccess(&this->memberNode->addr, true, msg.transID, current_transaction.key, returned_value);
	//log->logReadFail(&this->memberNode->addr, true, msg.transID, msg.key);
}

void MP2Node::check_timeout(){
	for(vector<Transaction>::iterator i = this->transactions.begin(); i != this->transactions.end(); i++){
		switch(i->msgType){
			case MessageType::READ:
				if(par->globaltime-i->timeout>TIMEOUT && i->valid){
					//cout<<"time to retire:"<<i->id<<" Diff: "<<this->par->globaltime-i->timeout<<endl;
					i->valid=false;
					log->logReadFail(&this->memberNode->addr, true, i->id, i->key);
				}
				
				break;
		}
	}
}