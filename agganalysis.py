import numpy as np
import math
from itertools import combinations
import pandas as pd
from tqdm import tqdm

def calculate_diversity(patterns):
    if not patterns:
        return np.nan
    total_patterns = len(patterns)
    unique_patterns = len(set(patterns))
    diversity = unique_patterns / total_patterns if total_patterns else np.nan
    return diversity


def calculate_entropy(patterns):
    if not patterns:
        return 0
    total_patterns = len(patterns)
    probabilities = [len(pattern) / total_patterns for pattern in patterns]
    entropy = -sum(p * math.log2(p) for p in probabilities if p > 0)
    return entropy


def jaccard_similarity(patterns1, patterns2):
    set1 = set(patterns1)
    set2 = set(patterns2)
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if union else 0


def mean_jaccard_similarity(service_data):
    unique_patterns = list(service_data.values())

    if len(unique_patterns) < 2:
        return np.nan

    similarities = (jaccard_similarity(patterns1, patterns2) for patterns1, patterns2 in combinations(unique_patterns, 2))
    total, count = 0, 0
    for similarity in similarities:
        total += similarity
        count += 1
    return total / count if count else 0

def apply_metrics_to_services(services):
    results = []

    for service_name, service_data in tqdm(services.items(), total=len(services)):
        diversity = calculate_diversity(service_data)
        entropy = calculate_entropy(service_data)
        jaccard_sim = mean_jaccard_similarity(service_data)

        results.append({
            "service": service_name,
            "diversity": diversity,
            "entropy": entropy,
            "jaccard_similarity": jaccard_sim
        })

    return pd.DataFrame(results)
