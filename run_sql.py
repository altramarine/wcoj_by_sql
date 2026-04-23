import duckdb
import sys
import time

def run_sql_file(path: str, con=None):
    import re
    statements = []
    current = []
    with open(path) as f:
        for line in f:
            line = re.sub(r',\s*\n', lambda m: m.group(0), line)  # preserve line
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

    if con is None:
        con = duckdb.connect()

    tm = 0
    for stmt in statements:
        print(f"--- Executing ---\n{stmt[:200]}\n")
        try:
            start = time.time()
            result = con.execute(stmt)
            tm += time.time() - start;
            print(f"duration=: {time.time() - start:.3f}s")
            # Print results for SELECT statements
            if result.description:
                cols = [d[0] for d in result.description]
                rows = result.fetchall()
                print('\t'.join(cols))
                print('-' * (len('\t'.join(cols)) + 8))
                for row in rows[0:1]:
                    print('\t'.join(str(v) for v in row))
                print(f"({len(rows)} rows)")
        except Exception as e:
            print(f"Error executing:\n{stmt[:120]}...\n{e}")
            raise
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
    args = parser.parse_args()

    dataset_path = DATASETS[args.dataset]
    print(f"Loading dataset: {args.dataset} ({dataset_path})")

    con = duckdb.connect(config={"temp_directory": "", "max_memory": "220GB"})
    con.execute(f"CREATE TABLE graph AS SELECT * FROM read_csv_auto('{dataset_path}');")
    con.execute("SET THREADS=32;")
    con.execute("CREATE TEMP TABLE R AS SELECT * FROM graph;")
    con.execute("DROP TABLE graph;")

    run_sql_file(args.sql, con)
    
