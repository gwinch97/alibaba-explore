from io import StringIO
import tarfile
import pandas as pd
import os
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
import pickle

def preprocess_line(line):
    fields = line.strip().split(',')
    if len(fields) > 11:  # Assuming there should be 11 fields
        fields[3] = '.'.join(fields[3:3 + (len(fields) - 11) + 1])
        del fields[4:4 + (len(fields) - 11)]
    return ','.join(fields)


def read_and_preprocess(tar_path):
    with tarfile.open(tar_path, 'r:gz') as tar:
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


def preprocess_and_store(file_index):
    wd = "//wsl.localhost/ubuntu/home/gw240/projects/alibaba/clusterdata/cluster-trace-microservices-v2022/data/CallGraph/"
    file_path = os.path.join(wd, f"CallGraph_{file_index}.tar.gz")

    MSCallGraph = read_and_preprocess(file_path) # preprocess to handle mismatched number of columns
    if MSCallGraph is None:
        return None  # Handle case where file read fails or is empty

    # Convert 'um' and 'dm' into a tuple in it's own column 'call_pattern'
    MSCallGraph['call_pattern'] = list(zip(MSCallGraph['um'], MSCallGraph['dm']))
    # Group (um,dm) tuples based on trace_id as a list of tuples
    grouped = MSCallGraph.groupby(['service', 'traceid'])['call_pattern'].apply(list).reset_index()
    # Group all tuple lists into a single list per service
    service_call_patterns = grouped.groupby('service')['call_pattern'].apply(list).reset_index(name='pattern_list')
    # For each service sort each list of tuples and convert into immutable tuple - this allows for comparison
    service_call_patterns['sorted_patterns'] = service_call_patterns['pattern_list'].apply(lambda x: tuple([tuple(sorted(tuple(y))) for y in x]))
    # Create a unique patterns column by converting the tuple into a set, out last step ensures all patterns are unique
    service_call_patterns['unique_patterns'] = service_call_patterns['sorted_patterns'].apply(set)

    # Save the results
    output_path = f"data/MSCallGraphsProcessed/{file_index}.pkl"
    with open(output_path, "wb") as f:
        pickle.dump(service_call_patterns,f)
        # service_call_patterns.to_pickle(f)

    return file_index

def main():
    files_completed = os.listdir('data/MSCallGraphsProcessed')
    indices_completed = []
    for file in files_completed:
        indices_completed.append(int(file[:-4]))

    indices_to_complete = [x for x in range(480) if x not in indices_completed]
    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(preprocess_and_store, i) for i in indices_to_complete]
        for future in futures:
            print(f"File processed: {future.result()}")

if __name__ == "__main__":
    main()