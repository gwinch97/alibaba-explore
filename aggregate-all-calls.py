import pandas as pd
import os
from tqdm import tqdm
import lzma
import pickle
from io import StringIO
import tarfile
from collections import defaultdict

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


if __name__ == '__main__':
    wd = "//wsl.localhost/ubuntu/home/gw240/projects/alibaba/clusterdata/cluster-trace-microservices-v2022/data/CallGraph/"

    # Path to the directory containing your files
    directory_path = wd

    # Initialize an empty dictionary to store your data
    aggregate_calls = defaultdict(int)

    # Iterate over each file in the directory
    for filename in tqdm(os.listdir(directory_path)):
        if filename.endswith('.gz'):  # Make sure to process only CSV files
            file_path = os.path.join(directory_path, filename)
            # Load the DataFrame
            df = read_and_preprocess(file_path)

            # Group by 'um' and 'dm', then count occurrences
            counts = df.groupby(['um', 'dm']).size()

            # Update the dictionary with counts
            for key, count in counts.items():
                aggregate_calls[key] += count




    with lzma.open("./data/aggregate_calls.xz","wb") as f:
        pickle.dump(dict(aggregate_calls),f)