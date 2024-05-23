import os
import math
import numpy as np
from tqdm import trange
import tarfile
import pandas as pd
from io import StringIO
import lzma
from itertools import combinations


def preprocess_line(line):
    fields = line.strip().split(',')
    if len(fields) > 11:  # Assuming there should be 11 fields
        fields[3] = '.'.join(fields[3:3 + (len(fields) - 11) + 1])
        del fields[4:4 + (len(fields) - 11)]
    return ','.join(fields)


def read_and_preprocess(tar_path):
    with tarfile.open(tar_path, 'r:gz') as tar:
        # Assuming there is only one file and it is a CSV
        for member in tar.getmembers():
            if member.isfile() and member.name.endswith('.csv'):
                # Extract file content
                f = tar.extractfile(member)
                if f:
                    # Read and preprocess the data
                    content = StringIO()
                    for line in f:
                        processed_line = preprocess_line(line.decode('utf-8'))
                        content.write(processed_line + '\n')

                    # Reset the pointer of StringIO object to the beginning
                    content.seek(0)

                    # Load the data into a DataFrame
                    df = pd.read_csv(content)
                    return df

# Function to calculate dissimilarity
def calculate_diversity(patterns):
    if not patterns:  # Check if the list is empty
        return np.nan
    unique_patterns = set(patterns)
    diversity = len(unique_patterns) / len(patterns)
    return diversity


def calculate_entropy(patterns):
    # Count the frequency of each unique pattern
    pattern_counts = {}
    for pattern in patterns:
        if pattern in pattern_counts:
            pattern_counts[pattern] += 1
        else:
            pattern_counts[pattern] = 1

    # Calculate probabilities
    total_patterns = len(patterns)
    probabilities = [count / total_patterns for count in pattern_counts.values()]

    # Calculate entropy
    entropy = -sum(p * math.log2(p) for p in probabilities if p > 0)
    return entropy

def jaccard_similarity(set1, set2):
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if union else 0

def mean_jaccard_similarity(patterns):
    patterns = set(patterns)
    if len(patterns) < 2:
        return np.nan  # Return NaN for cases with fewer than 2 unique patterns

    patterns = set(map(tuple, patterns))  # Convert list of patterns to a set of tuples
    similarities = (jaccard_similarity(set(a), set(b)) for a, b in combinations(patterns, 2))
    total, count = 0, 0
    for similarity in similarities:
        total += similarity
        count += 1
    return total / count if count else 0  # Safeguard against division by zero

if __name__ == "__main__":
    for i in trange(0,480):
        wd = "//wsl.localhost/ubuntu/home/gw240/projects/alibaba/clusterdata/cluster-trace-microservices-v2022/data/CallGraph/"
        # Path to the directory containing your files
        files = os.listdir(wd)
        file_path = os.path.join(wd, f"CallGraph_{i}.tar.gz")

        # Load the DataFrame
        # We have to preprocess because sometimes field 4 has a comma (e.g. 0.0.1,0.0.3)
        MSCallGraph = read_and_preprocess(file_path)

        # Create a unique identifier for each call pattern
        MSCallGraph['call_pattern'] = list(zip(MSCallGraph['um'], MSCallGraph['dm']))

        # Group by service and traceid, then create a list of call_pattern tuples for each group
        grouped = MSCallGraph.groupby(['service', 'traceid'])['call_pattern'].apply(list).reset_index()

        # Now group by service only, to create a list of lists of call patterns for each service
        service_call_patterns = grouped.groupby('service')['call_pattern'].apply(list).reset_index(name='pattern_list')

        # Now sort each call pattern such that tuples will be in the same order for each pattern
        service_call_patterns['normalized_patterns'] = service_call_patterns['pattern_list'].apply(
            lambda x: tuple(sorted(tuple(sub) for sub in x)))

        service_call_patterns['entropy'] = service_call_patterns['normalized_patterns'].apply(calculate_entropy)

        service_call_patterns['diversity'] = service_call_patterns['normalized_patterns'].apply(calculate_diversity)

        service_call_patterns['jaccard'] = service_call_patterns['normalized_patterns'].apply(mean_jaccard_similarity)

        with lzma.open(f"data/entropy_diversity_results/{i}.xz","wb") as f:
            service_call_patterns.to_pickle(f)
