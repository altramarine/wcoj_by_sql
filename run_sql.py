import duckdb
import sys

def run_sql_file(path: str):
    with open(path) as f:
        content = f.read()

    # Split into individual statements on semicolons
    # Strip trailing commas left by CTE generators before the final SELECT
    import re
    content = re.sub(r',\s*\n(SELECT\b)', r'\n\1', content)
    statements = [s.strip() for s in content.split(';') if s.strip()]

    con = duckdb.connect()
    for stmt in statements:
        try:
            result = con.execute(stmt)
            # Print results for SELECT statements
            if result.description:
                cols = [d[0] for d in result.description]
                rows = result.fetchall()
                print('\t'.join(cols))
                print('-' * (len('\t'.join(cols)) + 8))
                for row in rows:
                    print('\t'.join(str(v) for v in row))
                print(f"({len(rows)} rows)")
        except Exception as e:
            print(f"Error executing:\n{stmt[:120]}...\n{e}", file=sys.stderr)
            raise

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "1.sql"
    run_sql_file(path)
