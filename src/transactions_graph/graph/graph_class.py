class DirectedGraph:
    
    # Creator
    def __init__(self):
        self.nodes = set()
        self.adjecencies = {}

    def __del__(self):
        for v in list(self.nodes):
            self.delete_node(v)
        
        del self.adjecencies
        del self.nodes

    # Add node to graph
    def add_node(self, v):
        if v not in self.nodes:
            self.nodes.add(v)
        # Ensure adjacency set exists
        if v not in self.adjecencies:
            self.adjecencies[v] = set()

    # Delete node from graph    
    def delete_node(self,v):
        if v in self.nodes:
            self.nodes.remove(v)
        if v in self.adjecencies:
            del self.adjecencies[v]

        # Delete connections from other nodes
        for w in list(self.nodes):
            self.delete_edge(w, v)

    # Lazily gets the graph's nodes
    def get_nodes(self):
        for v in self.nodes:
            yield v

    # Delete edge from graph
    def delete_edge(self, v, w):
        # Remove w from v's adjacency if present
        if v in self.adjecencies and w in self.adjecencies[v]:
            self.adjecencies[v].remove(w)

    # Adds edge between nodes
    def add_edge(self, v, w):
        # Ensure adjacency entry exists for v
        if v not in self.adjecencies:
            self.adjecencies[v] = set()
        self.adjecencies[v].add(w)

    # Returns True if edge v -> w exists and False otherwise
    def are_connected(self, v, w):
        if v not in self.adjecencies:
            return False
        return w in self.adjecencies.get(v, set())

    # Lazily get a node's neighbours
    def get_neighbors(self, v):
        if v in self.adjecencies:
            for w in self.adjecencies[v]:
                yield w
