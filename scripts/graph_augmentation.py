import os
import networkx as nx
import random
from random import randrange
from tqdm import tqdm
from itertools import product

def is_iso(query, seen_queries):
    def node_match(node1, node2):
        return node1['label'] == node2['label']
    def edge_match(edge1, edge2):
        return edge1['label'] == edge2['label']
    for seen in seen_queries:
        if nx.is_isomorphic(query, seen, node_match=node_match, edge_match=edge_match):
            return True
    return False

def save_graph(graph, graph_save_path):
    if graph is None:
        return
    with open(graph_save_path, 'w') as file:
        file.write("t {} {} \n".format(graph.number_of_nodes(), graph.number_of_edges()))
        for v in graph.nodes():
            file.write("v {} {} {} \n".format(v, graph.nodes[v]['label'], graph.degree[v]))
        for (u, v) in graph.edges():
            file.write("e {} {} {} \n".format(u, v, graph[u][v]['label']))

def load_true_counts(path):
    names = []
    with open(path, 'r') as f:
        for line in f.readlines():
            tokens = line.split(',')
            names.append(tokens[0])
    return names

def load_graph(query_path):
    nodes = []
    edges = []
    with open(query_path, 'r') as f:
        for line in f.readlines():
            if line.strip().startswith('v'):
                tokens = line.strip().split()
                # v nodeID labelID degree
                id = int(tokens[1])
                label = int(tokens[2])
                nodes.append((id, {"label": label}))
            if line.strip().startswith('e'):
                tokens = line.strip().split()
                # e srcID dstID possible{label}
                src, dst = int(tokens[1]), int(tokens[2])
                if len(tokens) == 4:
                    edges.append((src, dst, {"label" : int(tokens[3])}))
                else:
                    edges.append((src, dst, {"label" : 0}))
    graph = nx.Graph()
    graph.add_nodes_from(nodes)
    graph.add_edges_from(edges)
    return graph

def verify_outputs(queries_dir, query_aug_dir, level):
    for query_aug_file_name in os.listdir(query_aug_dir):
        tokens = query_aug_file_name.split('_')
        original_query_file_name = tokens[0] + '_' + tokens[1] + '_' + tokens[2] + '_' + tokens[3] + '.graph'
        original_path = queries_dir + original_query_file_name
        aug_path = query_aug_dir + query_aug_file_name
        original_graph = load_graph(original_path)
        aug_graph = load_graph(aug_path)
        
        # verify all node lables are the same
        if len(original_graph.nodes()) != len(aug_graph.nodes()):
            raise Exception("{} and {} have different node numbers".format(original_path, aug_path))
        for node in original_graph.nodes():
            if original_graph.nodes()[node]['label'] != aug_graph.nodes()[node]['label']:
                raise Exception("{} and {} have different node labels".format(original_path, aug_path))
            
        # if dense
        # verify all edges in original graph are exist in dense graph
        if tokens[4] == 'dense':
            for edge in original_graph.edges():
                if not aug_graph.has_edge(edge[0], edge[1]):
                    raise Exception("{}'s edge doesn't exist in {}".format(original_path, aug_path))
                if original_graph[edge[0]][edge[1]]['label'] != aug_graph[edge[0]][edge[1]]['label']:
                    raise Exception("{}'s edge doesn't exist in {}".format(original_path, aug_path))
                if len(aug_graph.edges()) - len(original_graph.edges()) != level:
                    raise Exception("{} doesn't have {} more edges than {}".format(aug_path, original_path, level))
        else:
            # if sparse
            # verify all edges in sparse graph are exist in original graph
            for edge in aug_graph.edges():
                if not original_graph.has_edge(edge[0], edge[1]):
                    raise Exception("{}'s edge doesn't exist in {}".format(aug_path, original_path))
                if aug_graph[edge[0]][edge[1]]['label'] != original_graph[edge[0]][edge[1]]['label']:
                    raise Exception("{}'s edge doesn't exist in {}".format(aug_path, original_path))
                if len(original_graph.edges()) - len(aug_graph.edges()) != level:
                    raise Exception("{} doesn't have {} more edges than {}".format(original_path, aug_path, level))

# undirested graphs
def generate_possible_remove_edges(graph, level):
    # return ((src, dst), ...)
    edges = list(graph.edges())
    random.shuffle(edges)
    seen = set()
    for cur in product(edges, repeat=level):
        if len(set(cur)) != len(cur) or len(set(cur)) != level:
            continue
        cur = tuple(sorted(cur))
        if cur not in seen:
            seen.add(cur)
            yield cur

# undirested graphs
def generate_possible_add_edges(graph, level, available_edge_labels):
    # return ((src, dst, {'label': }), ...)
    exist_edges = set()
    for edge in graph.edges():
        exist_edges.add((edge[0], edge[1]))
        exist_edges.add((edge[1], edge[0]))

    nodes = list(graph.nodes())
    seen = set()
    product_list = []
    for _ in range(level * 2):
        cur = nodes.copy()
        random.shuffle(cur)
        product_list.append(cur)
    for cur in product(*product_list):
        valid = True
        edges = []
        for i in range(0, len(cur), 2):
            if cur[i] == cur[i + 1]:
                valid = False
                break
            else:
                edges.append(tuple(sorted((cur[i], cur[i + 1]))))
        if not valid:
            continue
        edges = tuple(sorted(edges))
        # unnecessary, just to make sure no mistake
        if len(set(edges)) != len(edges) or len(set(edges)) != level:
            continue
        for edge in edges:
            if edge in exist_edges:
                valid = False
                break
        if not valid:
            continue
        if edges not in seen:
            seen.add(edges)
            result = []
            for edge in edges:
                result.append((edge[0], edge[1], {"label" : available_edge_labels[randrange(0, len(available_edge_labels))]}))
            yield tuple(result)

if __name__ == "__main__":
    data_graph = "eu2005"
    augment_level = 2
    max_dense_graphs = 1
    max_sparse_graphs = 1

    suffix = ".graph"
    dataset_dir = "/home/ubuntu/workspace/RapidMatch/dataset/real_dataset/{}".format(data_graph)
    data_graph_path = "{}/data_graph/{}".format(dataset_dir, data_graph + suffix)
    original_queries_dir =  "{}/query_graph/".format(dataset_dir)
    graph_aug_save_dir = "{}/query_graph_aug_{}/".format(dataset_dir, str(augment_level))
    exist_query_dirs = [original_queries_dir]
    true_count_paths = ['{}/query_graph.csv'.format(dataset_dir)]
    for level in reversed(range(1, augment_level)):
        exist_query_dirs.append("{}/query_graph_aug_{}/".format(dataset_dir, str(level)))
        true_count_paths.append("{}/query_graph_aug_{}.csv".format(dataset_dir, str(level)))

    # # verify outputs
    # verify_outputs(original_queries_dir, graph_aug_save_dir, augment_level)
    # exit()

    # Get true counts
    queries_with_true_counts = set()
    for path in true_count_paths:
        true_counts = load_true_counts(path)
        queries_with_true_counts.update(true_counts)
        print('There are', len(true_counts), 'queries with a card in', path.split('/')[-1])    

    # load original queries
    original_queries = {} # original_queries -> {query_name: query_graph, ...}
    for file in os.listdir(original_queries_dir):
        name = file.split('.')[0]
        if name not in queries_with_true_counts:
            continue
        graph = load_graph(original_queries_dir + file)
        original_queries[name] = graph
    print('There are', len(original_queries), 'original queries being loaded')

    # load exist queries
    exist_queries = []
    for exist_query_dir in exist_query_dirs:
        for file in os.listdir(exist_query_dir):
            name = file.split('.')[0]
            if name not in queries_with_true_counts:
                continue
            graph = load_graph(exist_query_dir + file)
            exist_queries.append(graph)
    print('There are', len(exist_queries), 'exist queries being loaded')

    # get edge labels in the data graph
    available_edge_labels = set()
    with open(data_graph_path, 'r') as f:
        for line in f.readlines():
            if line.strip().startswith('e'):
                # e srcID dstID possible{label}
                tokens = line.strip().split()
                if len(tokens) == 4:
                    available_edge_labels.add(int(tokens[3]))
                else:
                    available_edge_labels.add(0)
    available_edge_labels = list(available_edge_labels)
    print("There are", len(available_edge_labels), "distinct edge label(s) in the graph")

    # perform graph aug
    bar = tqdm(total=len(original_queries))
    for name, original_query in original_queries.items():                
        # select dense and sparse augmented graphs according to the required number
        sparse_queries = []
        dense_queries = []
        for possible_remove_edge in generate_possible_remove_edges(original_query, augment_level):
            possible_sparse_query = original_query.copy()
            for i in range(augment_level):
                possible_sparse_query.remove_edge(possible_remove_edge[i][0], possible_remove_edge[i][1])
            if nx.is_connected(possible_sparse_query) and not is_iso(possible_sparse_query, exist_queries):
                sparse_queries.append(possible_sparse_query)
                exist_queries.append(possible_sparse_query)
                if len(sparse_queries) >= max_sparse_graphs:
                    break
        for possible_add_edge in generate_possible_add_edges(original_query, augment_level, available_edge_labels):
            possible_dense_query = original_query.copy()
            for i in range(augment_level):
                possible_dense_query.add_edge(possible_add_edge[i][0], possible_add_edge[i][1], label=possible_add_edge[i][2]['label'])
            if not is_iso(possible_dense_query, exist_queries):
                dense_queries.append(possible_dense_query)
                exist_queries.append(possible_dense_query)
                if len(dense_queries) >= max_dense_graphs:
                    break
        os.makedirs(graph_aug_save_dir, exist_ok=True)
        for index, sparse_query in enumerate(sparse_queries):
            sparse_query_path = graph_aug_save_dir + name + '_sparse_' + str(augment_level) + '_' + str(index) + suffix
            save_graph(sparse_query, sparse_query_path)
        for index, dense_query in enumerate(dense_queries):
            dense_query_path = graph_aug_save_dir + name + '_dense_' + str(augment_level) + '_' + str(index) + suffix
            save_graph(dense_query, dense_query_path)
        bar.update(1)
    bar.close()

    # verify outputs
    verify_outputs(original_queries_dir, graph_aug_save_dir, augment_level)