import subprocess
import sys
import time
import re
import os
import threading
import queue

UMBRA_IMAGE = "umbradb/umbra:latest"
UMBRA_IMAGE_WCOJ = "umbradb/umbra:25.07.1"

DATASETS = {
    "skitters": "./datasets/as-skitter.csv",
    "topcats":  "./datasets/wiki-topcats.csv",
    "gplus":    "./datasets/gplus.csv",
    "uspatent": "./datasets/cit-Patents.csv",
}


def rewrite_semi_joins(sql: str) -> str:
    pattern = re.compile(r'\bsemi join\s+(\w+)\s+on\s+', re.IGNORECASE)
    while True:
        m = pattern.search(sql)
        if not m:
            break
        tbl = m.group(1)
        cond_start = m.end()
        rest = sql[cond_start:]
        end = re.search(r'\b(semi join|join|where|group by|order by|limit|union|except|intersect|\))\b', rest, re.IGNORECASE)
        cond = rest[:end.start()].strip() if end else rest.strip()
        after = rest[end.start():] if end else ''
        before = sql[:m.start()].rstrip()
        sql = f"{before} where exists (select 1 from {tbl} where {cond}) {after}"
    return sql


def split_statements(path: str) -> list[str]:
    statements = []
    current = []
    with open(path) as f:
        for line in f:
            if line.rstrip().endswith(';'):
                current.append(line.rstrip().rstrip(';'))
                stmt = '\n'.join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []
            else:
                current.append(line.rstrip())
    if current:
        stmt = '\n'.join(current).strip()
        if stmt:
            statements.append(stmt)
    statements = [re.sub(r',\s*\n(SELECT\b)', r'\n\1', s) for s in statements]
    statements = [s.lower() for s in statements]
    statements = [re.sub(r'\bcreate temp table\b', 'create table', s) for s in statements]
    statements = [rewrite_semi_joins(s) for s in statements]
    return statements


def build_statements(csv_path: str, sql_path: str) -> list[str]:
    abs_csv = "/data/" + os.path.basename(csv_path)
    statements = split_statements(sql_path)
    return (
        [f"create table r (col0 bigint, col1 bigint)",
         f"copy r from '{abs_csv}' (format csv, header false)",
         f"select count(*) from r"]
        + statements
    )


def run(csv_path: str, sql_path: str, wcoj: bool = False, binary: bool = False):
    abs_dataset_dir = os.path.abspath(os.path.dirname(csv_path))
    statements = build_statements(csv_path, sql_path)

    cmd = [
        "docker", "run", "--rm", "-i",
        "--memory=220g",
        "--ulimit", "nofile=1048576:1048576",
        "--tmpfs", "/var/db:size=220g",
        "-v", f"{abs_dataset_dir}:/data:ro",
        "--entrypoint", "umbra-sql",
        UMBRA_IMAGE_WCOJ if wcoj else UMBRA_IMAGE,
    ]

    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)

    TIMEOUT = 900  # 15 minutes

    line_q = queue.Queue()

    def _reader():
        for line in proc.stdout:
            line_q.put(line.rstrip())
        line_q.put(None)

    threading.Thread(target=_reader, daemon=True).start()

    def read_until_info():
        """Return (lines, timed_out)."""
        lines = []
        deadline = time.time() + TIMEOUT
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                return lines, True
            try:
                line = line_q.get(timeout=remaining)
            except queue.Empty:
                return lines, True
            if line is None:
                return lines, False
            lines.append(line)
            if line.startswith("INFO:") or line.startswith("ERROR:") or line.startswith("FATAL:") or line.startswith("SUCCESS:"):
                return lines, False

    info_re = re.compile(
        r'INFO:.*execution: \(([0-9.]+) min.*compilation: \(([0-9.]+) min'
    )

    n_setup = 3  # create table r, copy r, warm-up count(*)
    start = None
    total_exec = 0.0
    total_comp = 0.0

    if wcoj:
        # send SET before main loop — it produces no output, do not wait
        set_stmt = "set debug.multiway = 'e'"
        print(f"\n--- {set_stmt}")
        proc.stdin.write(set_stmt + ";\n")
        proc.stdin.flush()
    if binary:
        set_stmt = "set debug.multiway = 'd'"
        print(f"\n--- {set_stmt}")
        proc.stdin.write(set_stmt + ";\n")
        proc.stdin.flush()

    for i, stmt in enumerate(statements):
        if i == n_setup:
            start = time.time()
        print(f"\n--- {stmt}")
        proc.stdin.write(stmt + ";\n")
        proc.stdin.flush()
        lines, timed_out = read_until_info()
        for line in lines:
            print(line)
            if i >= n_setup:
                m = info_re.search(line)
                if m:
                    total_exec += float(m.group(1))
                    total_comp += float(m.group(2))
        if timed_out:
            print(f"\nExecution time: Timeout")
            proc.kill()
            return

    proc.stdin.close()
    proc.wait()
    print(f"\nExecution time: {total_exec + total_comp:.3f}s")
    if proc.returncode != 0:
        sys.exit(proc.returncode)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("sql", nargs="?", default="1.sql")
    parser.add_argument("--dataset", "-d", default="skitters",
                        choices=list(DATASETS))
    parser.add_argument("--wcoj", action="store_true",
                        help="Use Umbra WCOJ build (25.07.1)")
    parser.add_argument("--binary", action="store_true",
                        help="Force binary join (set debug.multiway = 'd')")
    args = parser.parse_args()

    csv_path = DATASETS[args.dataset]
    print(f"Loading dataset: {args.dataset} ({csv_path})")
    run(csv_path, args.sql, wcoj=args.wcoj, binary=args.binary)
