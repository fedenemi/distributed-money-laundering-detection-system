class DirectedGraph:
    
    # Creator
    def __init__(self):
        self.nodes = set()
        self.adjecencies = {}

    def __del__(self):
        for v in self.nodes:
            self.delete_node(v)
        
        del self.adjecencies
        del self.nodes

    # Add node to graph
    def add_node(self, v):
        self.nodes.add(v)
        self.adjecencies[v] = set()

    # Delete node from graph    
    def delete_node(self,v):
        self.nodes.remove(v)
        del self.adjecencies[v]

        # Delete connections from other nodes
        for w in self.nodes:
            self.delete_edge(w, v)

    # Lazily gets the graph's nodes
    def get_nodes(self):
        for v in self.nodes:
            yield v

    # Delete edge from graph
    def delete_edge(self, v, w):
        if self.are_connected(v, w):
            self.adjecencies[w].remove(v)

    # Adds edge between nodes
    def add_edge(self, v, w):
        self.adjecencies[v].add(w)

    # Returns True if edge v -> w exists and False otherwise
    def are_connected(self, v, w):
        return w in self.adjecencies[v]

    # Lazily get a node's neighbours
    def get_neighbors(self, v):
        for w in self.adjecencies[v]:
            yield w