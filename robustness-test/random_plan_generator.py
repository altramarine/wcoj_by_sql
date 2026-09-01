#!/usr/bin/env python3

import argparse
import os
import random
import re
import selectors
import subprocess
import sys
from pathlib import Path


PROMPT = 'enter split (e.g. "R1 R2 R3"): '
GROUP_PATTERN = re.compile(r"-- \[\d+\] group: (.+)")
RELATION_PATTERN = re.compile(r"([A-Za-z_]\w*(?:__\w+)*)\(([^)]*)\)")


def parse_group(stderr_text: str) -> list[tuple[str, set[str]]]:
    group_lines = GROUP_PATTERN.findall(stderr_text)
    if not group_lines:
        raise RuntimeError("make_split_plan.py did not print the current group")
    relations = []
    for name, variables in RELATION_PATTERN.findall(group_lines[-1]):
        relations.append((name, {variable.strip() for variable in variables.split(',')}))
    return relations


def choose_split(relations: list[tuple[str, set[str]]], rng: random.Random) -> str:
    choices = []
    for start_name, start_variables in relations:
        intersecting = [
            name for name, variables in relations
            if name != start_name and start_variables & variables
        ]
        if len(intersecting) >= 2:
            choices.append((start_name, intersecting))
    if not choices:
        group = ', '.join(name for name, _ in relations)
        raise RuntimeError(
            "current group has no relation with two intersecting relations: " + group
        )
    start_name, intersecting = rng.choice(choices)
    candidates = rng.sample(intersecting, 2)
    return ' '.join([start_name, *candidates])


def read_query(args: argparse.Namespace) -> str:
    if args.query is not None:
        return args.query.strip()
    if args.query_file is not None:
        return args.query_file.read_text().splitlines()[0].strip()
    query = sys.stdin.readline().strip()
    if not query:
        raise ValueError("query must be supplied as an argument, file, or stdin")
    return query


def generate(
    query: str,
    make_split_plan: Path,
    rng: random.Random,
    mode: str,
    seed: int | None,
) -> str:
    command = [sys.executable, str(make_split_plan)]
    if mode == "binary":
        command.extend(["--binary", "--seed", str(seed)]) if seed is not None else command.append("--binary")
    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdin is not None
    assert process.stdout is not None
    assert process.stderr is not None
    process.stdin.write((query + "\n").encode())
    process.stdin.flush()

    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ, "stdout")
    selector.register(process.stderr, selectors.EVENT_READ, "stderr")
    stdout_data = bytearray()
    stderr_data = bytearray()
    prompt_bytes = PROMPT.encode()

    try:
        while selector.get_map():
            for key, _ in selector.select():
                chunk = os.read(key.fileobj.fileno(), 4096)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                if key.data == "stdout":
                    stdout_data.extend(chunk)
                    continue

                stderr_data.extend(chunk)
                while prompt_bytes in stderr_data:
                    prompt_position = stderr_data.index(prompt_bytes)
                    protocol_text = stderr_data[:prompt_position].decode(errors="replace")
                    relations = parse_group(protocol_text)
                    split = choose_split(relations, rng)
                    print(f"-- random split: {split}", file=sys.stderr)
                    process.stdin.write((split + "\n").encode())
                    process.stdin.flush()
                    del stderr_data[:prompt_position + len(prompt_bytes)]
    finally:
        selector.close()
        process.stdin.close()

    return_code = process.wait()
    if return_code != 0:
        diagnostic = stderr_data.decode(errors="replace").strip()
        raise RuntimeError(
            f"make_split_plan.py exited with status {return_code}: {diagnostic}"
        )
    return stdout_data.decode()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a random split plan through make_split_plan.py IO."
    )
    parser.add_argument("query", nargs="?", help="Datalog query, e.g. 'q(a,b) :- R(a,b), ...'")
    parser.add_argument("--query-file", type=Path, help="Read the query from the first line of a file")
    parser.add_argument("--seed", type=int, default=None, help="Random seed")
    parser.add_argument(
        "--mode",
        choices=["wcoj", "binary"],
        default="wcoj",
        help="Plan type to generate (default: wcoj)",
    )
    parser.add_argument(
        "--make-split-plan",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "make_split_plan.py",
        help="Path to make_split_plan.py",
    )
    args = parser.parse_args()
    if args.query is not None and args.query_file is not None:
        parser.error("query and --query-file cannot be used together")
    query = read_query(args)
    print(
        generate(
            query,
            args.make_split_plan,
            random.Random(args.seed),
            args.mode,
            args.seed,
        ),
        end="",
    )


if __name__ == "__main__":
    main()