import hashlib

class ConsistentHash:
    def __init__(self, nodes, replicas=2):
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []
        for node in nodes:
            self.add_node(node)

    def add_node(self, node):
        # Add a node to the hash ring with replicas
        for i in range(self.replicas):
            key = self._hash(f"{node}:{i}")
            self.ring[key] = node
            self.sorted_keys.append(key)
        self.sorted_keys.sort()

    def get_nodes(self, key, count):
        # Get 'count' nodes for a key (for replication)
        if not self.ring:
            return []
        nodes = []
        hash_key = self._hash(key)
        for k in self.sorted_keys:
            node = self.ring[k]
            if node not in nodes:
                nodes.append(node)
                if len(nodes) == count:
                    break
        while len(nodes) < count and self.sorted_keys:
            node = self.ring[self.sorted_keys[0]]
            if node not in nodes:
                nodes.append(node)
        return nodes[:count]

    def _hash(self, key):
        # Simple MD5 hash for key placement
        return int(hashlib.md5(key.encode()).hexdigest(), 16)