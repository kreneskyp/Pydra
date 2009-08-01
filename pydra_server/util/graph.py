# Copyright 2009 Oregon State University
#
# This file is part of Pydra.
#
# Pydra is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Pydra is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Pydra.  If not, see <http://www.gnu.org/licenses/>.

class DirectedGraph:

    def __init__(self):
        self.adjacency_lists = {} # vertex: adj_list


    def add_vertex(self, vertex):
        if vertex not in self.adjacency_lists:
            self.adjacency_lists[vertex] = []


    def add_vertices(self, vertices):
        for vertex in vertices:
            self.add_vertex(vertex)


    def add_edge(self, from_vertex, to_vertex):
        try:
            if to_vertex not in self.adjacency_lists[from_vertex]:
                self.adjacency_lists[from_vertex].append(to_vertex)
        except KeyError:
            pass


def dfs(graph, root=None):
    """
    Returns a tuple of:
    1) the generated spanning tree after dfs
    2) if this graph has cycles
    """
    def dfs_visit(vertex):
        color[vertex] = 1 # mark it as visited
        for next_vertex in graph.adjacency_lists[vertex]:
            if color[next_vertex] == 0:
                spanning_tree[next_vertex] = vertex
                dfs_visit(next_vertex)
            elif color[next_vertex] == 1:
                # a cycle
                dfs.has_cycle = True
            else:
                # reaching another spanning tree
                pass
        color[vertex] = 2 # finished exploring this vertex

    color = {}
    spanning_tree = {} # node: parent
    dfs.has_cycle = False

    for vertex in graph.adjacency_lists.iterkeys():
        color[vertex] = 0

    if root is not None:
        spanning_tree[root] = None
        dfs(root)
        return spanning_tree, dfs.has_cycle

    for vertex in graph.adjacency_lists.iterkeys():
        if color[vertex] == 0:
            spanning_tree[vertex] = None
            dfs_visit(vertex)

    return spanning_tree, dfs.has_cycle

