from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs
import json
import threading
import logging
import socket

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    pass

class StorageNode:
    def __init__(self, node_id, host, port):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.storage = {}
        self.lock = threading.Lock()
        logger.info(f"Node {node_id} set up at {host}:{port}")

    def store_chunk(self, chunk_id, data):
        # Store a chunk safely
        with self.lock:
            self.storage[chunk_id] = data
            logger.info(f"Node {self.node_id} stored {chunk_id}")
            return True

    def get_chunk(self, chunk_id):
        # Get a chunk if it exists
        with self.lock:
            data = self.storage.get(chunk_id)
            if data:
                logger.info(f"Node {self.node_id} got {chunk_id}")
            else:
                logger.warning(f"Node {self.node_id} no {chunk_id}")
            return data

    def delete_chunk(self, chunk_id):
        # Delete a chunk if it exists
        with self.lock:
            if chunk_id in self.storage:
                del self.storage[chunk_id]
                logger.info(f"Node {self.node_id} deleted {chunk_id}")
                return True
            logger.warning(f"Node {self.node_id} no {chunk_id} to delete")
            return False

class NodeHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        # Handle storing a chunk
        if self.path.startswith("/store"):
            chunk_id = parse_qs(urlparse(self.path).query).get('chunk_id', [None])[0]
            content_length = int(self.headers.get('Content-Length', 0))
            data = self.rfile.read(content_length)
            if chunk_id and data:
                self.server.node.store_chunk(chunk_id, data)
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"status": "ok"}).encode())
            else:
                self.send_response(400)
                self.end_headers()

    def do_GET(self):
        # Handle retrieving a chunk
        if self.path.startswith("/retrieve"):
            chunk_id = parse_qs(urlparse(self.path).query).get('chunk_id', [None])[0]
            if chunk_id:
                data = self.server.node.get_chunk(chunk_id)
                if data:
                    self.send_response(200)
                    self.send_header('Content-type', 'application/octet-stream')
                    self.end_headers()
                    self.wfile.write(data)
                else:
                    self.send_response(404)
                    self.end_headers()
            else:
                self.send_response(400)
                self.end_headers()

    def do_DELETE(self):
        # Handle deleting a chunk
        if self.path.startswith("/delete"):
            chunk_id = parse_qs(urlparse(self.path).query).get('chunk_id', [None])[0]
            if chunk_id:
                success = self.server.node.delete_chunk(chunk_id)
                self.send_response(200 if success else 404)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"status": "ok" if success else "not found"}).encode())
            else:
                self.send_response(400)
                self.end_headers()

def start_node(node_id, host, port):
    # Start a node server, checking if port is free
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind((host, port))
        sock.close()
        server = ThreadingHTTPServer((host, port), NodeHandler)
        server.node = StorageNode(node_id, host, port)
        logger.info(f"Node {node_id} running at {host}:{port}")
        server.serve_forever()
    except OSError:
        logger.error(f"Port {port} is in use or blocked. Try a different port.")
        raise