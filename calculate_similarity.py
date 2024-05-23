from itertools import combinations
import numpy as np
from tqdm import tqdm
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed

# Function to calculate Jaccard similarity
# def jaccard_similarity(set1, set2):
#     intersection = set1.intersection(set2)
#     union = set1.union(set2)
#     return len(intersection) / len(union) if union else 0

def jaccard_similarity(pair):
    set1, set2 = pair  # Unpack the tuple into two sets
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if union else 0



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

# This function will now use multiprocessing to calculate pairwise similarities
def mean_jaccard_similarity_parallel(patterns):
    # Check if the problem size justifies parallelization
    if len(patterns) > 100:
        # Use multiprocessing to calculate similarities
        with ProcessPoolExecutor() as executor:
            # Create all combinations of patterns and submit them to the executor
            future_similarity = executor.map(jaccard_similarity, combinations(map(set, patterns), 2))
            similarities = list(future_similarity)  # Convert to list to force execution
    else:
        # For a small number of combinations, use sequential computation
        similarities = [jaccard_similarity((set(a), set(b))) for a, b in combinations(patterns, 2)]

    # Compute the mean Jaccard similarity
    return sum(similarities) / len(similarities) if similarities else 0

def process_similarities(service_call_patterns):
    # Dictionary to store the results
    service_similarities = {}

    # Set up the process pool for parallel execution
    with ProcessPoolExecutor() as executor:
        futures = {}
        # Submit tasks for parallel execution
        for index, row in tqdm(service_call_patterns.iterrows()):
            service = row['service']
            patterns = row['pattern_list']
            if len(patterns) > 10:  # Only parallelize if more than 10 patterns
                futures[executor.submit(calculate_similarity_for_service, patterns)] = service
            else:
                service_similarities[service] = calculate_similarity_for_service(patterns)

        # Retrieve results as they are completed
        for future in as_completed(futures):
            service = futures[future]
            try:
                similarity = future.result()
                service_similarities[service] = similarity
            except Exception as e:
                print(f"Service {service} generated an exception: {e}")

    # Convert results to DataFrame for better visualization and analysis
    df_results = pd.DataFrame.from_dict(service_similarities, orient='index', columns=['mean_jaccard_similarity'])

    # Display the results
    return(df_results)