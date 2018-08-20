# Distributed Key-Value Store

This project is an implementation of of distributed key value store, having multiple nodes, with no central dependency.

## Project Details

This project simulates a real world distributed, fault-tolerant key value store. Every node in the system stores some keys, collectively all the nodes in the system complete the database. Like any other datastore it's possible to read or modify data on the store through client side CURD api.

## Architecture
The system is divided in 3 major components:
1. Application Layer
2. Membership Protocol/Hash Table
3. Emulated Network Layer

**The Application layer** sits above the other two coponents and is responsible for bootstraping the system and maintaining global values like time, node details etc.

Nodes are initialized by the app layer using **Membership Protocol** layer. After each node has been initialized. They introduce themselves to the group by sending join requests.

The system runs a membership protocol, where each node maintains a list of all its neighbouring members, which it pings by sending heartbeats periodically, to detect node faliures and refresh the membership table.

Every node uses its membership list to make a Virtual Ring. In that ring, each node is positined according to the hashed value of its address, its because of this **distributed hashing algorithm**, each node has the ring and the whole system appears to be connected in a ring.

The keys of incoming key-value pairs are hashed and are stored in their designated node. Each key has three replzas- PRIMARY, SECONDAY, TERTIARY. the secondary and tertiary replicas are stored in the consecutive neighbours of the primary, in clockwise direction.

### Data manipulation and retrieval

The sytem has api to modify and read data. It's server side API is used to make changes in the data store to stabalize the data or replicate keys in case of faliures. The client side API is used by the user to store or read data.

The system maintains 3 replicas of each key and the success of each read, delete, update or create command depends on the quorum of all replicas _i.e: atleast 2 successful reads._

To run the program, first compile all the files using _make_. Then execute the command below with one of the test cases as the parameter.
```
./Application testcases/{choose_one}
```

