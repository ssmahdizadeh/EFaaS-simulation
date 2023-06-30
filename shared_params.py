import pandas as pd
import numpy as np
import random
import math

random_seed = 100
random.seed(random_seed)
# %
USER_NUM_JITTER = 0.05

# Data size rang in Mb
MIN_DATA_SIZE = 0.1
MAX_DATA_SIZE = 10

MIN_FUN_EXEC_TIME = 2
MAX_FUN_EXEC_TIME = 20

# Edge servers config
REGION_SIZE = 1000
NUM_EDGE_SERVERS = 9

MIN_EDGE_MEMORY = 100
MAX_EDGE_MEMORY = 120

CLOUD_COEF = 2
EXE_ERROR_RATE = 0.1
# container config
WARM_START_TIME = 0.01
COLD_START_TIME = 2.5

# simulation config
SIM_TIME = 1000
SCHEDULER_INTERVAL = 10

SIM_REPEAT = 1
# num_users_list = [50,100,200,300,400]
# num_users_list = [100,200,300,400,500,600]
# ,100,150,200,250,300,350
# NUM_USERS_LIST = [200]
USERS_INITIAL_NUMBER = 45
USER_ENTRY_RATES = [0.2, 0.2, 0.2]
EDGE_SIZE = ['D', 'M', 'L']
WORK_FLOW_SIZE = ['D', 'D', 'D']
scenario = 0
CONFIG_CODE = 21

cont = None
logs = None
jobs = None
functions = None


def reset_logs():
    global logs
    logs = {"allocations": [], "containers_info": [],
            "workflow_info": [], "logs": []}


def parse_instance_file():
    global functions
    global jobs
    file_path = 'instances6.csv'
    columns = ['instance_name', 'task_name', 'job_name', 'task_type', 'status', 'start_time', 'end_time',
               'machine_id', 'seq_no', 'total_seq_no', 'cpu_avg', 'cpu_max', 'mem_avg', 'mem_max']
    df = pd.read_csv(file_path, header=None, names=columns)
    jobs = df.groupby('job_name')
    group_sizes = jobs.size()
    print(group_sizes)
    function_list = df.groupby(['job_name', 'task_name'])
    functions = []
    for func, instances in function_list:
        functions.append({"id": len(functions), "name": ''.join(func), "cpu_avg": round(instances["cpu_avg"].mean()/100, 2), "mem_avg": round(instances["mem_avg"].mean(
        ), 2), "ex_time": round(random.uniform(MIN_FUN_EXEC_TIME, MAX_FUN_EXEC_TIME), 2), "data_size": round(random.uniform(MIN_DATA_SIZE, MAX_DATA_SIZE), 2)})
    print("num_functions: ", len(functions))


def get_function(function_name):
    global functions
    function_o = [f for f in functions if f["name"] == function_name]
    if len(function_o):
        return function_o[0]
    else:
        return False


def get_random_egde_size():
    # 60 100 250 500
    size = EDGE_SIZE[scenario]
    mu1, mu2, mu3 = 80, 175, 375
    sigma1, sigma2, sigma3 = 10, 30, 50
    weights = [0.33, 0.34, 0.33]
    if size == 'S':
        weights = [0.7, 0.2, 0.1]
    else:
        if size == 'M':
            weights = [0.2, 0.7, 0.1]
        else:
            if size == 'L':
                weights = [0.1, 0.2, 0.7]
    x = np.random.choice([1, 2, 3], p=weights)
    if x == 1:
        number = np.random.normal(mu1, sigma1)
    elif x == 2:
        number = np.random.normal(mu2, sigma2)
    else:
        number = np.random.normal(mu3, sigma3)
    return number


def get_random_job():
    global jobs
    global scenario
    prob_dist = [0.33, 0.34, 0.33]
    # prob_dist = [0.5, 0.5 ]
    size_ranges = [(0, 20), (20, 40), (40, np.inf)]
    # size_ranges = [ (0, 20),(20, 40)]
    size = WORK_FLOW_SIZE[scenario]
    if size == 'S':
        prob_dist = [0.7, 0.2, 0.1]
    else:
        if size == 'M':
            prob_dist = [0.2, 0.7, 0.1]
        else:
            if size == 'L':
                prob_dist = [0.1, 0.2, 0.7]
    # Calculate the number of records in each group
    group_sizes = jobs.size()
    index = [i for i, j in jobs]
    size_range_idx = np.random.choice(len(size_ranges), p=prob_dist)
    size_range = size_ranges[size_range_idx]
    group_idxs = np.where((size_range[0] <= group_sizes) & (
        group_sizes < size_range[1]))[0]
    group_idx = np.random.choice(group_idxs)
    selected_group = jobs.get_group(index[group_idx])
    return selected_group


ES_locations = None


def locate_edge_servers():
    global ES_locations
    ES_locations = []
    antenna_distance = REGION_SIZE / np.sqrt(NUM_EDGE_SERVERS)
    for i in range(int(np.sqrt(NUM_EDGE_SERVERS))):
        for j in range(int(np.sqrt(NUM_EDGE_SERVERS))):
            x = int((i + 0.5) * antenna_distance)
            y = int((j + 0.5) * antenna_distance)
            ES_locations.append((x, y))


# Function to compute path loss in dB
def path_loss(d):
    d0 = 1  # Reference distance in meters
    PL_d0 = -30  # Path loss at reference distance in dB
    n = 2  # Path loss exponent
    X_f = 4.0  # Shadowing effect in dB
    return PL_d0 + 10 * n * math.log10(d/d0) + X_f

# Function to compute uplink speed


def uplink_speed(d):
    R = 1  # Spectral efficiency in bits per second per Hertz
    P_t = 0.1  # Transmit power in Watts
    G_t = 1  # UE antenna gain
    G_r = 10  # BS antenna gain
    h = 50  # Height of the BS and UE above the ground in meters
    N_0 = 1e-9  # Noise power spectral density in Watts per Hertz
    B = 1e6  # Channel bandwidth in Hertz
    L = path_loss(d)
    return R * math.log2(1 + (P_t * G_t * G_r * h**2) / (N_0 * B * 10**(L/10)))


def get_accessible_servers(x, y):
    global ES_locations
    accessible_servers = []
    for j, antenna_location in enumerate(ES_locations):
        distance = np.sqrt((x - antenna_location[0]) ** 2 +
                           (y - antenna_location[1]) ** 2)
        speed = uplink_speed(distance*100)
        if speed >= 1:
            # uplink speed in Mbps
            accessible_servers.append(
                {"server_id": j, "t_rate": round(speed, 3)})
    accessible_servers.sort(key=lambda x: x['t_rate'], reverse=True)
    return accessible_servers


def transfer_rate(d):
    H = d ** -4
    noisePower = dbm2watt(-100)
    # Transfer power user in watt
    P = 1.2
    # the transmission bandwidth in MHtz
    bandwidth = 5
    rate = bandwidth * math.log2(1 + (P * H / noisePower ** 2))
    return rate


def dbm2watt(input):
    return 10 ** (input/10) / 1000
