import hashlib
import requests
import logging
from consistent import ConsistentHash

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class DistributedFileSystem:
    def __init__(self, nodes, chunk_size=1024*1024):  # 1MB chunks
        self.nodes = [f"{host}:{port}" for _, host, port in nodes]
        self.hash_ring = ConsistentHash(self.nodes)
        self.chunk_size = chunk_size
        self.replicas = 2
        logger.info("DFS set up")

    def split_file(self, data):
        # Break file into chunks with consistent IDs
        chunks = []
        for i in range(0, len(data), self.chunk_size):
            chunk = data[i:i + self.chunk_size]
            chunk_id = f"{hashlib.md5(chunk).hexdigest()}_{i}"
            chunks.append((chunk_id, chunk))
        logger.info(f"Split into {len(chunks)} chunks")
        return chunks

    def store_file(self, file_id, data):
        # Store file by splitting and distributing
        chunks = self.split_file(data)
        for chunk_id, chunk_data in chunks:
            nodes = self.hash_ring.get_nodes(f"{file_id}:{chunk_id}", self.replicas)
            if not nodes:
                logger.error("No nodes available")
                return False
            for node in nodes:
                try:
                    resp = requests.post(f"http://{node}/store?chunk_id={file_id}:{chunk_id}", 
                                       data=chunk_data, timeout=3)
                    if resp.status_code != 200:
                        logger.error(f"Failed to store {chunk_id} on {node}")
                        return False
                except requests.RequestException as e:
                    logger.error(f"Error storing {chunk_id} on {node}: {e}")
                    return False
        logger.info(f"Stored {file_id}")
        return True

    def retrieve_file(self, file_id, chunk_count, data):
        # Get and reassemble file
        chunks = self.split_file(data)  # Use original split to get correct IDs
        retrieved_chunks = []
        for chunk_id, _ in chunks:
            nodes = self.hash_ring.get_nodes(f"{file_id}:{chunk_id}", self.replicas)
            chunk_data = None
            for node in nodes:
                try:
                    resp = requests.get(f"http://{node}/retrieve?chunk_id={file_id}:{chunk_id}", timeout=3)
                    if resp.status_code == 200:
                        chunk_data = resp.content
                        logger.info(f"Got {chunk_id} from {node}")
                        break
                except requests.RequestException as e:
                    logger.error(f"Error getting {chunk_id} from {node}: {e}")
            if chunk_data is None:
                logger.error(f"Missing {chunk_id}")
                return None
            retrieved_chunks.append(chunk_data)
        return b''.join(retrieved_chunks)

    def delete_file(self, file_id, chunk_count, data):
        # Remove all chunks of a file
        success = True
        chunks = self.split_file(data)
        for chunk_id, _ in chunks:
            nodes = self.hash_ring.get_nodes(f"{file_id}:{chunk_id}", self.replicas)
            for node in nodes:
                try:
                    resp = requests.delete(f"http://{node}/delete?chunk_id={file_id}:{chunk_id}", timeout=3)
                    if resp.status_code != 200:
                        logger.error(f"Failed to delete {chunk_id} from {node}")
                        success = False
                except requests.RequestException as e:
                    logger.error(f"Error deleting {chunk_id} from {node}: {e}")
                    success = False
        logger.info(f"Deleted {file_id}")
        return success