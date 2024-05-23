import os
import pandas as pd
import itertools
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import combinations, islice
from tqdm import tqdm
from math import ceil


def calculate_similarity_for_service(patterns):
    return mean_jaccard_similarity([set(pattern) for pattern in patterns])


def mean_jaccard_similarity(patterns):
    # Generator expression - calculates one similarity at a time
    similarities = (jaccard_similarity(set(a), set(b)) for a, b in combinations(patterns, 2))
    total, count = 0, 0
    for similarity in similarities:
        total += similarity
        count += 1
    return total / count if count else 0


def jaccard_similarity(set1, set2):
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if union else 0


def calculate_batch_similarity(batch):
    total, count = 0, 0
    for a, b in batch:
        similarity = jaccard_similarity(set(a), set(b))
        total += similarity
        count += 1
    return total, count


def mean_jaccard_similarity_parallel(patterns, batch_size=1000, num_workers=None):
    # If num_workers is not set, default to a sensible number less than the maximum
    if not num_workers:
        num_workers = max(1, os.cpu_count() - 1)

    pair_generator = combinations(patterns, 2)
    total_similarity, total_pairs = 0, 0

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        batch_futures = []
        print("Creating batch for similarity")
        while True:
            batch = list(islice(pair_generator, batch_size))  # Take batch_size pairs
            if not batch:
                break
            batch_futures.append(executor.submit(calculate_batch_similarity, batch))

        print("Calculating batch similarity")
        for future in tqdm(as_completed(batch_futures), total=len(batch_futures), desc="Calculating Similarities"):
            try:
                result = future.result()
                total_similarity += result[0]
                total_pairs += result[1]
            except Exception as e:
                print(f"An error occurred: {e}")

    return total_similarity / total_pairs if total_pairs else 0

# def jaccard_similarity(set1, set2):
#     intersection = set1.intersection(set2)
#     union = set1.union(set2)
#     return len(intersection) / len(union) if union else 0
#
# def worker_function(patterns, start, end):
#     # Local generation of combinations to avoid passing iterators
#     total, count = 0, 0
#     for i, pair in enumerate(itertools.combinations(patterns, 2)):
#         if i >= start and i < end:
#             similarity = jaccard_similarity(set(pair[0]), set(pair[1]))
#             total += similarity
#             count += 1
#         if i >= end:
#             break
#     return total, count
#
# def mean_jaccard_similarity_parallel(patterns, batch_size=1000, num_workers=10):
#     num_combinations = len(patterns) * (len(patterns) - 1) // 2
#     total_similarity, total_pairs = 0, 0
#
#     with ProcessPoolExecutor(max_workers=num_workers) as executor:
#         futures = []
#         for start in range(0, num_combinations, batch_size):
#             end = min(start + batch_size, num_combinations)
#             future = executor.submit(worker_function, patterns, start, end)
#             futures.append(future)
#
#         for future in tqdm(as_completed(futures), total=len(futures), desc="Calculating Similarities"):
#             part_sum, part_count = future.result()
#             total_similarity += part_sum
#             total_pairs += part_count
#
#     return total_similarity / total_pairs if total_pairs else 0