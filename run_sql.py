import duckdb
import sys

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
    for stmt in statements:
        print(f"--- Executing ---\n{stmt[:200]}\n")
        try:
            result = con.execute(stmt)
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
            print(f"Error executing:\n{stmt[:120]}...\n{e}", file=sys.stderr)
            raise

if __name__ == "__main__":
    import pathlib
    con = duckdb.connect(config={"temp_directory": "", "max_memory": "220GB"})
    
    con.execute("CREATE TABLE graph AS SELECT * FROM read_csv_auto('./datasets/as-skitter.csv');")
    # con.execute("CREATE TABLE graph AS SELECT * FROM read_csv_auto('./datasets/wiki-topcats.csv');")
    con.execute("SET THREADS=32;")
    con.execute("CREATE TEMP TABLE R AS SELECT * FROM graph;")

    # profile_dir = pathlib.Path("./tmp/profiles")
    # profile_dir.mkdir(parents=True, exist_ok=True)
    # sql_name = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "1.sql").stem
    # profile_path = profile_dir / f"{sql_name}.json"
    # con.execute(f"PRAGMA enable_profiling='json'")
    # con.execute(f"PRAGMA profile_output='{profile_path}'")

    print("TEMP IS CREATED")

    import time
    path = sys.argv[1] if len(sys.argv) > 1 else "1.sql"
    start = time.time()
    run_sql_file(path, con)
    print(f"Execution time: {time.time() - start:.3f}s")
