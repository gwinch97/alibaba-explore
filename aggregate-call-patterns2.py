from collections import defaultdict
import pandas as pd
import os
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import lzma
import pickle

def init_dict():
    return defaultdict(int)

def process_dataframe(file_path):
    df = pd.read_pickle(file_path)
    aggregate_patterns = defaultdict(init_dict)

    for index, row in df.iterrows():
        service_id = row['service']
        patterns = row['sorted_patterns']

        if isinstance(patterns, tuple):
            patterns = list(patterns)

        for pattern in patterns:
            aggregate_patterns[service_id][pattern] += 1

    return aggregate_patterns

def main():
    file_directory = 'data/MSCallGraphsProcessed'
    file_paths = [os.path.join(file_directory, f'{i}.pkl') for i in range(480)]

    final_aggregate_patterns = defaultdict(init_dict)

    with ProcessPoolExecutor(max_workers=20) as executor:
        results = executor.map(process_dataframe, file_paths)

        for partial_aggregate in tqdm(results,total=len(file_paths)):
            for service_id, patterns in partial_aggregate.items():
                for pattern, count in patterns.items():
                    final_aggregate_patterns[service_id][pattern] += count

    return final_aggregate_patterns

if __name__ == '__main__':
    aggregate_patterns = main()
    with lzma.open("data/aggregate_patterns.xz","wb") as f:
        pickle.dump(dict(aggregate_patterns),f)
