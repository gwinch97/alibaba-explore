import numpy as np
import math
from itertools import combinations
import pandas as pd
from tqdm import tqdm
import timeit
from concurrent.futures import ProcessPoolExecutor

def calculate_diversity(service_data):
    if not service_data:
        return np.nan
    total_patterns = sum(service_data.values())
    unique_patterns = len(service_data)
    diversity = unique_patterns / total_patterns if total_patterns else np.nan
    return diversity


def calculate_entropy(service_data):
    if not service_data:
        return 0
    total_patterns = sum(service_data.values())
    probabilities = [count / total_patterns for count in service_data.values()]
    entropy = -sum(p * math.log2(p) for p in probabilities if p > 0)
    return entropy


def jaccard_similarity(set1, set2):
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if union else 0


# def mean_jaccard_similarity(service_data):
#     # Generate a list of unique patterns, each repeated according to its count
#     # patterns = [pattern for pattern, count in service_data.items() for _ in range(count)]
#     unique_patterns = set(list(service_data.keys()))
#     # unique_patterns = set(map(tuple, patterns))  # Unique patterns for comparison
#
#     if len(unique_patterns) < 2:
#         return np.nan
#
#     similarities = (jaccard_similarity(set(a), set(b)) for a, b in combinations(unique_patterns, 2))
#     total, count = 0, 0
#     for similarity in similarities:
#         total += similarity
#         count += 1
#     return total / count if count else 0

def batch_jaccard_sim(batch):
    # Process a batch of pairwise combinations
    return [jaccard_similarity(set(a), set(b)) for a, b in batch]

def mean_jaccard_similarity(service_data, parallel=False, batch_size=100):
    unique_patterns = list(service_data.keys())

    if len(unique_patterns) < 2:
        return np.nan

    pattern_combinations = list(combinations(unique_patterns, 2))
    num_combinations = len(pattern_combinations)

    if len(unique_patterns) > 10000 and parallel:
        # Divide the work into batches
        batches = [pattern_combinations[i:i + batch_size] for i in range(0, num_combinations, batch_size)]
        with ProcessPoolExecutor() as executor:
            # Use a map to process batches in parallel
            results = executor.map(batch_jaccard_sim, batches)
            similarities = [sim for sublist in results for sim in sublist]
    else:
        # Calculate similarities sequentially for smaller datasets
        similarities = [jaccard_similarity(set(a), set(b)) for a, b in pattern_combinations]

    # Calculate the mean of the similarities
    total = sum(similarities)
    return total / num_combinations if num_combinations else 0


def apply_metrics_to_services(services):
    results = []

    for service_name, service_data in tqdm(services.items(),total=len(services)):

        # start_time_diversity = timeit.default_timer()
        diversity = calculate_diversity(service_data)
        # end_time_diversity = timeit.default_timer()
        # print(f"Diversity took: {end_time_diversity-start_time_diversity}s")

        # start_time_entropy = timeit.default_timer()
        entropy = calculate_entropy(service_data)
        # end_time_entropy = timeit.default_timer()
        # print(f"Entropy took: {end_time_entropy - start_time_entropy}s")

        # start_time_jaccard = timeit.default_timer()
        if len(service_data) < 5000:
            jaccard_sim = mean_jaccard_similarity(service_data)
        else:
            jaccard_sim = np.nan
        # end_time_jaccard = timeit.default_timer()
        # print(f"Jaccard took: {end_time_jaccard - start_time_jaccard}s")

        results.append({
            "service": service_name,
            "patterns": service_data,
            "diversity": diversity,
            "entropy": entropy,
            "jaccard_similarity": jaccard_sim
        })

        # results.append({
        #     "service": service_name,
        #     "patterns": service_data,
        #     "diversity": diversity,
        #     "entropy": entropy,
        # })

    return pd.DataFrame(results)