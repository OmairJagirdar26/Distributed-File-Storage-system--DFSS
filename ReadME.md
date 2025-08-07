Distributed File Storage System (DFSS)



A simple distributed file storage system built in Python that splits files into chunks, distributes them across multiple nodes, and reassembles them on retrieval. This project demonstrates key concepts like consistent hashing, replication, and fault tolerance.
Features

Splits files into 1MB chunks for efficient storage.
Distributes chunks across multiple nodes using consistent hashing.
Replicates each chunk on 2 nodes for redundancy.
Provides REST API endpoints for storing, retrieving, and deleting files.
Runs locally with 3 nodes for demo purposes.

How It Works:

Split: Files are broken into chunks and assigned unique IDs.
Distribute: Chunks are stored on multiple nodes based on a hash ring.
Retrieve: Chunks are fetched and reassembled into the original file.
Delete: All chunks are removed from nodes.

Setup and Running

Prerequisites:

Python 3.7+
Install dependencies: pip install requests


Clone or Copy:

Clone this repo or copy the DFSS folder to your local machine.


Run:
python client.py


Test:

Created a demo.txt file in the DFSS folder with some text.
Run the script again to see it process your file.



Files:

consistent.py: Handles node distribution with consistent hashing.
storage_node.py: Manages individual node storage and HTTP server.
coordinator.py: Coordinates file splitting, distribution, and retrieval.
client.py: Runs the demo by interacting with the system.

Contributing:
Feel free to fork and improve this project! Add features like disk storage or more nodes.
