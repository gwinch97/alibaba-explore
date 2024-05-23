import pandas as pd
import os
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import lzma
import pickle

def process_dataframe(file_path):
    df = pd.read_pickle(file_path)
    aggregate_patterns = {}

    # Append patterns to the aggregate dictionary
    for index, row in df.iterrows():
        service_id = row['service']
        patterns = row['sorted_patterns']

        # Convert tuple to list if patterns are a tuple
        if isinstance(patterns, tuple):
            patterns = list(patterns)

        if service_id in aggregate_patterns:
            aggregate_patterns[service_id].extend(patterns)
        else:
            aggregate_patterns[service_id] = patterns.copy()  # Use copy to create a mutable list

    return aggregate_patterns


def main():
    file_directory = 'data/MSCallGraphsProcessed'
    file_paths = [os.path.join(file_directory, f'{i}.pkl') for i in range(20)]

    # Initialize an empty dictionary to store aggregated patterns per service
    final_aggregate_patterns = {}

    # Using multiprocessing for parallel execution
    with ProcessPoolExecutor(max_workers=4) as executor:
        # Map each file path to the processing function
        results = executor.map(process_dataframe, file_paths)

        # Combine results from all futures
        for partial_aggregate in tqdm(results, total=len(file_paths)):
            for service_id, patterns in partial_aggregate.items():
                if service_id in final_aggregate_patterns:
                    final_aggregate_patterns[service_id].extend(patterns)
                else:
                    final_aggregate_patterns[service_id] = patterns.copy()  # Ensure a new list is created

    # with lzma.open("data/aggregate_patterns.xz","wb") as f:
    #     pickle.dump(final_aggregate_patterns, f)

    return final_aggregate_patterns


if __name__ == '__main__':
    aggregate_patterns = main()
    # print(aggregate_patterns)  # Optionally print the results

