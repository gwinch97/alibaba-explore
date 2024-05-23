import os
from tqdm import tqdm
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import combinations


# Function to calculate Jaccard similarity
def jaccard_similarity(set1, set2):
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


def calculate_batch_similarity(batch):
    total, count = 0, 0
    for a, b in batch:
        similarity = jaccard_similarity(set(a), set(b))
        total += similarity
        count += 1
    return total, count


def process_batches(patterns, batch_size):
    # Creating batches of pairs
    all_pairs = list(combinations(patterns, 2))
    batches = [all_pairs[i:i + batch_size] for i in range(0, len(all_pairs), batch_size)]

    results = []
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(calculate_batch_similarity, batch) for batch in batches]
        for future in as_completed(futures):
            results.append(future.result())

    # Aggregate results from all batches
    total_similarity, total_pairs = 0, 0
    for total, count in results:
        total_similarity += total
        total_pairs += count

    return total_similarity / total_pairs if total_pairs else 0

if __name__ == "__main__":
    wd = "//wsl.localhost/ubuntu/home/gw240/projects/alibaba/clusterdata/cluster-trace-microservices-v2022/data/CallGraph/"
    # Path to the directory containing your files
    directory_path = wd
    files = os.listdir(directory_path)
    filename = files[0]
    file_path = os.path.join(directory_path, filename)
    # Load the DataFrame
    df = pd.read_csv(file_path, compression='gzip', header=0, sep=',', quotechar='"',on_bad_lines="warn");
    MSCallGraph = df

    # Create a unique identifier for each call pattern
    MSCallGraph['call_pattern'] = list(zip(MSCallGraph['um'], MSCallGraph['dm']))

    # Group by service and traceid, then create a list of call_pattern tuples for each group
    grouped = MSCallGraph.groupby(['service', 'traceid'])['call_pattern'].apply(list).reset_index()

    # Now group by service only, to create a list of lists of call patterns for each service
    service_call_patterns = grouped.groupby('service')['call_pattern'].apply(list).reset_index(name='pattern_list')

    # Dictionary to store the results
    service_similarities = {}

    # # Set up the process pool for parallel execution
    # with ProcessPoolExecutor() as executor:
    #     futures = {}
    #     # Submit tasks for parallel execution
    #     for index, row in tqdm(service_call_patterns.iterrows(), total=len(service_call_patterns)):
    #         service = row['service']
    #         patterns = row['pattern_list']
    #         if 500 < len(patterns) < 10000:  # Only parallelize if more than 5000 patterns
    #             futures[executor.submit(calculate_similarity_for_service, patterns)] = service
    #         elif len(patterns) > 10000:
    #             batch_size = 1000
    #             service_similarities[service] = process_batches(patterns, batch_size)
    #         elif len(patterns) != 1:
    #             service_similarities[service] = (len(patterns), calculate_similarity_for_service(patterns))
    #         else:
    #             service_similarities[service] = (1, 1)
    #
    #     # Retrieve results as they are completed
    #     for future in tqdm(as_completed(futures), total=len(futures)):
    #         service = futures[future]
    #         try:
    #             similarity = future.result()
    #             service_similarities[service] = similarity
    #         except Exception as e:
    #             print(f"Service {service} generated an exception: {e}")
    #
    # print("Aggregating into dataframe")
    # # Convert results to DataFrame for better visualization and analysis
    # df_results = pd.DataFrame.from_dict(service_similarities, orient='index', columns=['mean_jaccard_similarity'])
    #
    # # Display the results
    # print(df_results)



    # Function to calculate Jaccard similarity
    def jaccard_similarity(set1, set2):
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


    # Dictionary to store the results
    service_similarities = {}

    # Set up the process pool for parallel execution
    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = {}
        # Submit tasks for parallel execution
        for index, row in tqdm(service_call_patterns.iterrows(), total=len(service_call_patterns)):
            service = row['service']
            patterns = row['pattern_list']
            if len(patterns) > 20000:  # Only parallelize if more than 5000 patterns
                print(len(patterns))
                futures[executor.submit(calculate_similarity_for_service, patterns)] = service
            elif len(patterns) != 1:
                service_similarities[service] = (len(patterns), calculate_similarity_for_service(patterns))
            else:
                service_similarities[service] = (1, 1)

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
    print(df_results)