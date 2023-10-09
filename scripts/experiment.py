from subprocess import Popen, PIPE
import multiprocessing as mp
import copy
import os

def generate_args(binary, *params):
    arguments = [binary]
    arguments.extend(list(params))
    return arguments


def execute_binary(args):
    process = Popen(' '.join(args), shell=True, stdout=PIPE, stderr=PIPE)
    (std_output, std_error) = process.communicate()
    process.wait()
    rc = process.returncode
    return rc, std_output, std_error


def error_callback(error):
    print(f"Error info: {error}", flush=True)

def multiCoreProcessing(fn, params):
    num_cores = int(mp.cpu_count())
    print("there are " + str(num_cores) + " cores", flush=True)
    pool = mp.Pool(num_cores - 1)
    pool.map_async(fn, params, error_callback=error_callback)
    pool.close()
    pool.join()

def execute_query(parameters):
    print('Start: ' + parameters['data_graph'] + ', ' + parameters['query_graph'], flush=True)
    args = generate_args(parameters['binary_path'], 
                         '-d', parameters['data_graph_dir'] + parameters['data_graph'] + parameters['graph_suffix'], 
                         '-q', parameters['query_graph_dir'] + parameters['query_graph'] + parameters['graph_suffix'], 
                        '-order', parameters['order'], 
                        '-time_limit', parameters['time_limit'], 
                        '-preprocess', parameters['preprocess'], 
                        '-num', parameters['num'])
    (rc, std_output, std_error) = execute_binary(args)
    with open(parameters['output_dir'] + parameters['output_file_name'], 'w') as file:
        if std_error:
            file.write(std_error.decode())
        file.write(std_output.decode())
        file.flush()
    print('Done: ' + parameters['data_graph'] + ', ' + parameters['query_graph'], flush=True)

if __name__ == '__main__':
    data_graph = "eu2005"
    aug = "_aug_2"
    parameters = {
        'data_graph_dir': '/home/ubuntu/workspace/RapidMatch/dataset/real_dataset/{}/data_graph/'.format(data_graph),
        'query_graph_dir' : '/home/ubuntu/workspace/RapidMatch/dataset/real_dataset/{}/query_graph{}/'.format(data_graph, aug),
        'output_dir': '/home/ubuntu/workspace/RapidMatch/outputs/{}/'.format(data_graph + aug),
        'binary_path': '/home/ubuntu/workspace/RapidMatch/build/matching/RapidMatch.out',
        'order': 'nd',
        'time_limit': '1860',
        'preprocess': 'true',
        'num': 'MAX',
        'graph_suffix': '.graph',
        'data_graph': data_graph,
        'query_graph': '',
        'output_file_name': ''
    }
    parameters_set = []
    for query_file in os.listdir(parameters['query_graph_dir']):
        query_graph = query_file.split('.')[0]
        cur_parameters = copy.deepcopy(parameters)
        cur_parameters['query_graph'] = query_graph
        cur_parameters['output_file_name'] = query_graph
        parameters_set.append(cur_parameters)
    multiCoreProcessing(execute_query, parameters_set)