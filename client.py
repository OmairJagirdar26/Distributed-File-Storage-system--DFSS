import threading
import time
import logging
from coordinator import DistributedFileSystem
from storage_node import start_node

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # Define and start nodes
    nodes = [
        ("node1", "localhost", 8001),
        ("node2", "localhost", 8002),
        ("node3", "localhost", 8003),
    ]
    threads = []
    for node_id, host, port in nodes:
        thread = threading.Thread(target=start_node, args=(node_id, host, port), daemon=True)
        threads.append(thread)
        thread.start()
    time.sleep(2)  # Wait for nodes

    # Check nodes
    for _, host, port in nodes:
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            sock.connect((host, port))
            sock.close()
            logger.info(f"Node at {host}:{port} is up")
        except ConnectionRefusedError:
            logger.error(f"Node at {host}:{port} failed")
            return

    # Set up DFS
    dfs = DistributedFileSystem(nodes)

    # Use demo.txt as the test file
    file_id = "demo.txt"
    with open("demo.txt", "rb") as f:
        data = f.read()
    chunk_count = (len(data) + dfs.chunk_size - 1) // dfs.chunk_size

    # Store
    logger.info(f"Storing {file_id}")
    if dfs.store_file(file_id, data):
        logger.info(f"Stored {file_id}")
    else:
        logger.error(f"Failed to store {file_id}")
        return

    # Retrieve
    logger.info(f"Retrieving {file_id}")
    result = dfs.retrieve_file(file_id, chunk_count, data)  # Pass data
    if result:
        logger.info(f"Retrieved: {result.decode()}")
    else:
        logger.error(f"Failed to retrieve {file_id}")
        return

    # Delete
    logger.info(f"Deleting {file_id}")
    if dfs.delete_file(file_id, chunk_count, data):  # Pass data
        logger.info(f"Deleted {file_id}")
    else:
        logger.error(f"Failed to delete {file_id}")

if __name__ == "__main__":
    main()