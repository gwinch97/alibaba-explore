import pandas as pd
import os
from tqdm import tqdm
import lzma
import pickle

wd = "//wsl.localhost/ubuntu/home/gw240/projects/alibaba/clusterdata/cluster-trace-microservices-v2022/data/CallGraph/"
#
# for i in trange(0,480):
#     df = pd.read_csv(f'{wd}/CallGraph_{i}.tar.gz', compression='gzip', header=0, sep=',', quotechar='"', error_bad_lines=False)
#     services = df['service'].unique()

# Path to the directory containing your files
directory_path = wd

# Initialize an empty dictionary to store your data
services_dict = {}

# Iterate over each file in the directory
for filename in tqdm(os.listdir(directory_path)):
    if filename.endswith('.gz'):  # Make sure to process only CSV files
        file_path = os.path.join(directory_path, filename)
        # Load the DataFrame
        df = pd.read_csv(file_path, compression='gzip', header=0, sep=',', quotechar='"', error_bad_lines=False, warn_bad_lines=False)

        # Iterate through each row in the DataFrame
        for index, row in df.iterrows():
            service = row['service']
            um = row['um']
            dm = row['dm']

            # Check if the service is already in the dictionary
            if service not in services_dict:
                services_dict[service] = []

            # Append the 'um' and 'dm' values to the service's list if they're not None or NaN
            if pd.notna(um) and um not in services_dict[service]:
                services_dict[service].append(um)
            if pd.notna(dm) and dm not in services_dict[service]:
                services_dict[service].append(dm)

with lzma.open("./data/services_dict.xz","wb") as f:
    pickle.dump(services_dict,f)