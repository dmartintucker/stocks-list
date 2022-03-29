# Dependencies
import numpy as np 
import pandas as pd 
import warnings, multiprocessing, itertools, string

# Custom methods
from utils import YahooFinance

# Ignore warnings
warnings.filterwarnings('ignore')

# Globals
N_WORKERS = 50

def compile_candidates() -> None:

    # Define space
    namespace = [i for i in string.ascii_uppercase] + ['.']
    candidates = []

    for j in range(4,5):

        res = [''.join(i) for i in itertools.product(namespace, repeat=j)]
        candidates += res
    
    return candidates

def parallel_search(arg) -> pd.DataFrame:
    res = pd.DataFrame()
    y = YahooFinance()
    worker_id, candidates = arg

    for c in candidates:
        res = res.append(y.get_info(c), ignore_index=True)

    return res

def search_candidates(candidates) -> pd.DataFrame:

    res = pd.DataFrame()
    logged = pd.read_csv('temp/securities.csv')
    candidates = list(set(candidates) - set(logged['symbol'].values))

    if len(candidates) > 1000:
        print(f"Reducing candidates pool from {len(candidates)} to 1000.")
        candidates = candidates[:1000]
    
    # Split candidates array into partitions based on number of N_WORKERS
    print(f"{N_WORKERS} workers initiated on {len(candidates)} candidates...")
    splits = np.array_split(candidates, N_WORKERS)
    workers_manifest = [(i, splits[i]) for i in range(N_WORKERS)]

    pool = multiprocessing.Pool(processes=N_WORKERS)
    temp_results = pool.map(parallel_search, workers_manifest)
    pool.close()
    pool.join()
    results = pd.concat(temp_results, axis=0, sort=True)
    res = pd.concat([logged, results], axis=0, sort=True).drop_duplicates()

    return res

if __name__ == '__main__':

    candidates = compile_candidates()
    res = search_candidates(candidates)
    res.to_csv('temp/securities.csv', index=False)
    res.dropna(subset=['quoteType']).to_csv('stocks_list.csv.gz', index=False, compression='gzip')
    print(f"{len(res.dropna(subset=['quoteType']))} valid securities discovered so far!")