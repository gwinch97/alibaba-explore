import multiprocessing as mp
import lzma
import pickle
from tqdm import tqdm
from collections import defaultdict
import numpy as np
from itertools import combinations
import time

def jaccard_similarity(set1, set2):
    """
    Compute the Jaccard similarity between two sets.
    """
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union != 0 else 0

def compute_similarity1(args):
    """
    Wrapper function to compute the Jaccard similarity between two sets
    with arguments unpacked.
    """
    set1, set2 = args
    return jaccard_similarity(set1, set2)


def parallel_jaccard_similarity2(sets, num_processes=None):
    """
    Compute Jaccard similarities for a list of sets in parallel.

    Parameters:
    - sets: List of sets to compute similarities for.
    - num_processes: Number of processes to use (default: None, which uses all available cores).

    Returns:
    A matrix of Jaccard similarity scores.
    """
    num_sets = len(sets)
    # Generate all possible pairs of sets
    pairs = [(sets[i], sets[j]) for i in range(num_sets) for j in range(i + 1, num_sets)]

    # Use multiprocessing to compute the similarities
    with mp.Pool(processes=num_processes) as pool:
        results = pool.map(compute_similarity1, pairs)

    average_similarity = sum(results) / len(results) if results else 0.0

    return average_similarity

def compute_similarity(pair):
    set1, set2 = pair
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    if union == 0:
        return 0.0
    return intersection / union

def parallel_jaccard_similarity1(sets, num_processes=None):
    """
    Compute the average Jaccard similarity for a list of sets in parallel.

    Parameters:
    - sets: List of sets to compute similarities for.
    - num_processes: Number of processes to use (default: None, which uses all available cores).

    Returns:
    The average Jaccard similarity score.
    """
    num_sets = len(sets)
    # Generate all possible pairs of sets
    pairs = [(sets[i], sets[j]) for i in range(num_sets) for j in range(i + 1, num_sets)]

    # Use multiprocessing to compute the similarities
    with mp.Pool(processes=num_processes) as pool:
        results = pool.imap_unordered(compute_similarity, pairs)

        # Calculate the average similarity on-the-fly
        total_similarity = 0.0
        num_pairs = 0

        for similarity in results:
            total_similarity += similarity
            num_pairs += 1

    # Avoid division by zero in case there are no pairs
    average_similarity = total_similarity / num_pairs if num_pairs > 0 else 0.0

    return average_similarity

# def mean_jaccard_similarity(service_data):
#     unique_patterns = service_data
#
#     if len(unique_patterns) < 2:
#         return np.nan
#
#     similarities = (jaccard_similarity(patterns1, patterns2) for patterns1, patterns2 in combinations(unique_patterns, 2))
#
#     return list(similarities)

def mean_jaccard_similarity(service_data):
    unique_patterns = service_data

    if len(unique_patterns) < 2:
        return np.nan

    similarities = (jaccard_similarity(patterns1, patterns2) for patterns1, patterns2 in combinations(unique_patterns, 2))
    total, count = 0, 0
    for similarity in similarities:
        total += similarity
        count += 1
    return total / count if count else 0

def init_dict():
    return defaultdict(int)

def calculate_depth(graph):
    """
    Calculate the depth of the call graph.
    graph: list of tuples representing the call graph edges
    returns: integer representing the maximum depth
    """
    # Build adjacency list excluding self-loops and invalid nodes
    adj_list = defaultdict(list)
    for start, end in graph:
        if start != end and start not in {"UNAVAILABLE", "UNKNOWN"} and end not in {"UNAVAILABLE", "UNKNOWN"}:
            adj_list[start].append(end)
    # Function to find depth from a starting node using DFS
    def dfs(node, visited):
        if node in visited:
            return 0
        visited.add(node)
        max_depth = 0
        for neighbor in adj_list[node]:
            max_depth = max(max_depth, dfs(neighbor, visited) + 1)
        visited.remove(node)
        return max_depth
    # Find the maximum depth among all nodes
    max_depth = 0
    nodes = list(adj_list.keys())  # Make a list of keys to avoid size change error
    for node in nodes:
        max_depth = max(max_depth, dfs(node, set()))
    return max_depth


def average_service_depth(patterns):
    depth_list = []
    for key in patterns.keys():
        depth_list.append(calculate_depth(key))

if __name__ == "__main__":
    with lzma.open("data/aggregate_patterns.xz","rb") as f:
        aggregate_calls = pickle.load(f)

    jaccard_dict = {}
    aggregate_calls = dict(aggregate_calls)

    for service in tqdm(aggregate_calls.keys()):
        patterns = dict(aggregate_calls[service])
        sets = [set(x) for x in list(patterns.keys())]
        if len(sets) > 1000:
            # start_time = time.time()
            jaccard_dict[service] = parallel_jaccard_similarity2(sets, num_processes=20)
            # end_time = time.time()
            # print(f"Length of data: {len(sets)}")
            # print(f"Parallel Method 2 took {end_time - start_time}")

            # start_time = time.time()
            # jaccard_dict1[service] = mean_jaccard_similarity(sets)
            # end_time = time.time()
            # print(f"Serial Method 1 took {end_time - start_time} \n")

        else:
            jaccard_dict[service] = mean_jaccard_similarity(sets)

    with lzma.open("data/jaccard_similarity.xz","wb") as f:
        pickle.dump(jaccard_dict, f)