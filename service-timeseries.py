from io import StringIO
import tarfile
import pandas as pd
import os
from tqdm import trange
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


if __name__ == '__main__':
    wd = "//wsl.localhost/ubuntu/home/gw240/projects/alibaba/clusterdata/cluster-trace-microservices-v2022/data/CallGraph/"
    big_df = pd.DataFrame()
    for i in trange(0,480):
        file_path = os.path.join(wd, f"CallGraph_{i}.tar.gz")
        df = read_and_preprocess(file_path)  # preprocess to handle mismatched number of columns
        if df is None:
            continue

        filtered_df = df.loc[df.groupby('traceid')['timestamp'].idxmin()]
        filtered_df['timestamp_sec'] = (filtered_df['timestamp'] // 1000)

        # Create a pivot table with seconds as index and services as columns, count occurrences
        pivot_df = pd.pivot_table(filtered_df, values='rpc_id', index='timestamp_sec', columns='service',
                                  aggfunc='count', fill_value=0)

        # Concatenate to the big dataframe, assuming continuous time index is not necessary
        big_df = pd.concat([big_df, pivot_df], axis=0, ignore_index=True)

    # To handle zero requests periods explicitly and ensure all possible services are included
    if not big_df.empty:
        all_services = big_df.columns
        big_df = big_df.reindex(columns=all_services, fill_value=0)

    with open("data/service_timeseries.pkl", "wb") as f:
        pickle.dump(big_df,f)