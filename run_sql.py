import duckdb
import sys
import time
import gc
import os
import signal


class QueryTimeoutError(TimeoutError):
    pass


def _raise_query_timeout(signum, frame):
    raise QueryTimeoutError("query execution timed out")

def run_sql_file(path: str, con=None, timeout=None):
    import re
    statements = []
    current = []
    with open(path) as f:
        for line in f:
            line = re.sub(r'--.*', '', line)  # strip comments
            if line.rstrip().endswith(';'):
                current.append(line.rstrip().rstrip(';'))
                stmt = '\n'.join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []
            else:
                current.append(line.rstrip())
    # handle any trailing statement without semicolon
    if current:
        stmt = '\n'.join(current).strip()
        if stmt:
            statements.append(stmt)

    # Strip trailing commas before SELECT (CTE generator artifact)
    statements = [re.sub(r',\s*\n(SELECT\b)', r'\n\1', s) for s in statements]
    # Drop empty statements (were pure comments)
    statements = [s for s in statements if s.strip()]

    if con is None:
        con = duckdb.connect()

    tm = 0
    previous_handler = signal.getsignal(signal.SIGALRM)
    if timeout is not None:
        signal.signal(signal.SIGALRM, _raise_query_timeout)
        signal.setitimer(signal.ITIMER_REAL, timeout)
    try:
        for stmt in statements:
            print(f"--- Executing ---\n{stmt[:200]}\n")
            try:
                start = time.time()
                result = con.execute(stmt)
                tm += time.time() - start
                print(f"duration=: {time.time() - start:.3f}s")
                # Print results for SELECT statements
                if result and result.description:
                    cols = [d[0] for d in result.description]
                    rows = result.fetchall()
                    print('\t'.join(cols))
                    print('-' * (len('\t'.join(cols)) + 8))
                    for row in rows[0:5]:
                        print('\t'.join(str(v) for v in row))
                    print(f"({len(rows)} rows)")
            except Exception as e:
                print(f"Error executing:\n{stmt[:120]}...\n{e}")
                raise
    finally:
        if timeout is not None:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous_handler)
    print(f"Execution time: {tm:.3f}s")

DATASETS = {
    "skitters":  "./datasets/as-skitter.csv",
    "topcats":   "./datasets/wiki-topcats.csv",
    "gplus":     "./datasets/gplus.csv",
    "uspatent":  "./datasets/cit-Patents.csv",
}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("sql", nargs="?", default="1.sql", help="SQL file to run")
    parser.add_argument("--dataset", "-d", default="skitters",
                        choices=list(DATASETS), help="Dataset to load (default: skitters)")
    parser.add_argument("--timeout", type=float, default=None, metavar="SECONDS",
                        help="Maximum query execution time in seconds (default: no limit)")
    args = parser.parse_args() 

    if args.timeout is not None and args.timeout <= 0:
        parser.error("--timeout must be greater than zero")
    
    dataset_path = DATASETS[args.dataset]
    print(f"Loading dataset: {args.dataset} ({dataset_path})")

    con = duckdb.connect(config={"temp_directory": "", "max_memory": "220GB"})
    con.execute(f"CREATE TABLE graph AS SELECT col0, col1 FROM read_csv('{dataset_path}', names=['col0','col1'], header=false);")
    con.execute("SET THREADS=32;")
    con.execute("CREATE TEMP TABLE R AS SELECT * FROM graph;")
    con.execute("DROP TABLE graph;")

    try:
        run_sql_file(args.sql, con, timeout=args.timeout)
    except QueryTimeoutError:
        print("STATUS: TIMEOUT")
        sys.exit(124)
    except Exception as e:
        message = str(e).lower()
        if "out of memory" in message or "memory limit" in message:
            print("STATUS: MEMORY_OUT")
            sys.exit(137)
        print("STATUS: ERROR")
        raise
    else:
        print("STATUS: NORMAL")
    
