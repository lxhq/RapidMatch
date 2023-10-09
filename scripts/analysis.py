import os

def get_true_counts(outputs_dir):
    true_counts = []
    for file in os.listdir(outputs_dir):
        time, count = 0, 0
        with open(outputs_dir + file, 'r') as f:
            for line in f.readlines():
                if line.strip().startswith('Query time (seconds):'):
                    time = float(line.split(':')[1].strip())
                if line.strip().startswith('#Embeddings:'):
                    count = int(line.split(':')[1].strip())
        true_counts.append((file, count, time))
    true_counts = sorted(true_counts, key=lambda x: x[0])
    return true_counts

def save_true_counts(true_counts, true_counts_dir, file_name, time_limit):
    with open(true_counts_dir + file_name, 'w') as f:
        for file, count, time in true_counts:
            # filter out incomplete queries
            if time > time_limit:
                continue
            # filter out non exist queries
            if count == 0:
                continue
            f.write(file + ',' + str(count) + ',' + str(time) + '\n')

if __name__ == '__main__':
    # Read and save queries with true counts
    data_graph = 'eu2005'
    time_limit = 1800
    is_aug = True
    level = 2

    aug = ''
    if is_aug:
        aug = '_aug_{}'.format(level)
    outputs_dir = '/home/ubuntu/workspace/RapidMatch/outputs/{}/'.format(data_graph + aug)
    true_counts_dir = '/home/ubuntu/workspace/RapidMatch/dataset/real_dataset/{}/'.format(data_graph)
    true_counts = get_true_counts(outputs_dir)
    save_true_counts(true_counts, true_counts_dir, "query_graph{}.csv".format(aug), time_limit)

    #select out queries with true counts
    # dataset_dir = '/home/ubuntu/workspace/RapidMatch/dataset/real_dataset/{}'.format(data_graph)
    # query_graph_with_true_count_dir = dataset_dir + '/query_graph{}_with_true_count/'.format(aug)
    # queries_dir = dataset_dir + "/query_graph{}/".format(aug)
    # os.makedirs(query_graph_with_true_count_dir, exist_ok=True)
    # for file, count, time in true_counts:
    #     if time > time_limit:
    #         continue
    #     query_file_path = queries_dir + file + '.graph'
    #     shutil.copy(query_file_path, query_graph_with_true_count_dir)
