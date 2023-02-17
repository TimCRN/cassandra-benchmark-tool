from cassandra.cluster import ResultSet, Cluster, ResponseFuture
from cassandra.query import SimpleStatement
from cassandra.query import *
from alive_progress import alive_bar
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
import math
import json
import os
import func_timeout
from objsize import get_deep_size

tfc_file_name = 'table_fetch_combis'
cluster = Cluster(['crounse.nl'], port=9042) #10.86.185.1
session = cluster.connect("env1")

test_tables = [
    "custom",
    "images",
    "videos"
]

time_limit = 60 # 1 minuut
punishment = 1.2
benchmark_count = 10
bulk_loop = 10

table_fetch_combi = {}
table_results = []
fetch_changed = False
current_data = []

results: ResultSet = None
    
def process(table: str, fetch: int):
    global current_data
    global results

    statement = SimpleStatement(f'SELECT * FROM {table}',fetch_size=fetch)
    results = session.execute(statement) 
    
    while results.has_more_pages:
        yield fetch
        current_data.extend(results.current_rows)
        results.fetch_next_page()
    
def process_bar(table,fetch):
    global fetch_changed

    try:        
        with alive_bar() as bar:
            update_bar(table,fetch,bar)
            bar.pause()
            return (table,fetch,bar.rate[0:-2],bar.current)     

    except cassandra.OperationTimedOut as e:
        print(type(e))
        print(e)
        fetch_changed = True
        new_fetch = calculate_fetch(fetch)
        if new_fetch != fetch:
            print(f'!PROCESS FAILED! Trying again with a batch of {new_fetch} in 5s . . .')
            time.sleep(5)
            return process_bar(table,new_fetch)
        else:
            print('!PROCESS FAILED CRITICALLY! Stopping current process...')
            return

    except cassandra.Unavailable:
        print("!CASSANDRA UNAVAILABLE! Restarting in benchmark 5s . . .")
        time.sleep(5)
        return process_bar(table,fetch)

    except Exception as e:
        print(type(e))
        print(e)
        raise

def visualize_bar(table: str, fetch: int = 5000):    
    global table_results
    global current_data

    table_stats = process_bar(table,fetch)

    print("Calculating total size...")            
    size = get_deep_size(current_data)
    current_data.clear()
    time_stamp = time.time()
    table_results.append(
        f"{table_stats[0]},"
        f"{table_stats[1]},"
        f"{table_stats[2]},"
        f"{time_limit},"
        f"{table_stats[3]},"
        f"{size},"
        f"{float(table_stats[2])*(size/table_stats[3])},"
        f"{time_stamp}"
    )  
    table_fetch_combi[table] = fetch
    print("Size calculated, and table stats succesfully stored!")

def update_bar(table,fetch,bar):
    def process_timeout_wrap():
        for i in process(table, fetch):               
            bar(i)
    try:
        func_timeout.func_timeout(time_limit, process_timeout_wrap)
    except func_timeout.FunctionTimedOut:
        return  
    except Exception as e:
        raise e   

def calculate_fetch(fetch: int):
    return math.ceil(fetch / punishment)

def update_table_fetch_combinations():
    with open(f'{tfc_file_name}.json','w') as f:
        json.dump(table_fetch_combi,f)

def setup_table_fetch_combinations():
    global table_fetch_combi
    if os.path.isfile(f'./{tfc_file_name}.json'):
        with open(f'{tfc_file_name}.json','r') as f:
            table_fetch_combi = json.load(f)
    else:
        with open(f'{tfc_file_name}.json','w') as f:
            f.write('{"custom": 5000, "images": 2413, "videos": 17}')

def get_std_fetch(table_name: str):
    fetch = 5000
    if table_name in table_fetch_combi:
        fetch = table_fetch_combi[table_name]  
    return fetch

def setup_results_file(tag: str):
    if not os.path.exists('results'): os.mkdir('results')
    if not os.path.isfile(f'./results/{tag}.csv'):
        with open(f'./results/{tag}.csv','w') as f:
            f.write('table-name,fetch-size,fetch-rate,total-time,fetched-record-count,total-size,size-rate,timestamp')

def save_results(tag: str):
    setup_results_file(tag)
    with open(f'./results/{tag}.csv','a') as f:
        for r in table_results:
            f.write("\n" + r)
        
def plot_results(tag: str):    
    df = pd.read_csv(f'./results/{tag}.csv')

    # Averages bar chart
    xf = df.groupby('table-name').mean().reset_index()

    fig, ax = plt.subplots(constrained_layout=True)

    count = 0
    for b in xf['fetch-rate']:
        rects = ax.bar(count, b, 0.25)
        ax.bar_label(rects)
        count = count + 1

    xf['fetch-rate'].plot(kind='bar',ylabel='±Records /s', ax=ax)
    xf['size-rate'].plot(secondary_y=True,colormap='RdBu', ylabel='±Bytes /s', ax=ax).legend(loc='center left', bbox_to_anchor=(0.25,-0.15))

    ax.set_xticklabels(xf['table-name'])
    ax.set_title('Read performance')

    ax.legend(loc='center left', bbox_to_anchor=(0,-0.15))

    plt.savefig(f'./results/{tag}.png', bbox_inches='tight')

def print_divider(count: int):
    term_size = os.get_terminal_size()
    print('=' * term_size.columns)
    print(f'ITERATION {count} OUT OF {benchmark_count}'.center(term_size.columns))
    print('=' * term_size.columns)

def benchmark():
    setup_table_fetch_combinations()

    for j in range(0,bulk_loop):
        tag = str(datetime.now())
        # for windows
        tag = tag.replace(":",";")
        for i in range(0,benchmark_count):
            print_divider(i+1)
            for t in test_tables:        
                print(f'BENCHMARK STARTED FOR "{t}"')        
                visualize_bar(t,get_std_fetch(t))
        if fetch_changed: update_table_fetch_combinations()
        save_results(tag)
        plot_results(tag)        

if __name__ == "__main__":
    benchmark()