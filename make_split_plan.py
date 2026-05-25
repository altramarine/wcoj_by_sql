import sys
import heapq
import duckdb
import re
from dataclasses import dataclass, field
import math

# a Relation is having an extensible item from x to y
@dataclass
class Relation:
  name: str                    # relation name, e.g. "R"
  var_list: list[str]              # ordered vars by position, e.g. R(x,y,x) -> ["x","y","x"]
  variables: dict[str, list[int]]  # var -> positions it appears at, e.g. R(x,y,x) -> {"x":[0,2],"y":[1]}

def Parse(s: str) -> list[Relation]:
  relations = []
  for token in re.findall(r'\w+\s*\([^)]*\)', s):
    m = re.match(r'(\w+)\s*\(([^)]*)\)', token)
    name = m.group(1)
    var_list = [v.strip() for v in m.group(2).split(',')]
    variables: dict[str, list[int]] = {}
    for i, v in enumerate(var_list):
      variables.setdefault(v, []).append(i)
    relations.append(Relation(name=name, var_list=var_list, variables=variables))

  # rename: if a base name appears >1 time, suffix each with _1, _2, ...
  name_count: dict[str, int] = {}
  for rel in relations:
    name_count[rel.name] = name_count.get(rel.name, 0) + 1
  name_seen: dict[str, int] = {}
  for rel in relations:
    base = rel.name
    if name_count[base] > 1:
      name_seen[base] = name_seen.get(base, 0) + 1
      new_name = f'{base}{name_seen[base]}'
      print(f'-- rename: {base} -> {new_name}', file=sys.stderr)
      rel.name = new_name

  relations.sort(key=lambda r: r.name)

  summary = ', '.join(f'{r.name}({", ".join(r.var_list)})' for r in relations)
  print(f'-- relations: {summary}', file=sys.stderr)

  return relations

def table_name(rels: list[Relation]) -> str:
  return '__'.join(sorted(r.name for r in rels))


def merge(rels: list[Relation]) -> Relation:
  """Merge multiple relations into one: name = table_name, var_list = union in order."""
  name = table_name(rels)
  var_list: list[str] = []
  seen: set[str] = set()
  for rel in rels:
    for v in rel.var_list:
      if v not in seen:
        var_list.append(v)
        seen.add(v)
  variables: dict[str, list[int]] = {}
  for i, v in enumerate(var_list):
    variables.setdefault(v, []).append(i)
  return Relation(name=name, var_list=var_list, variables=variables)


def GenViews(relations: list[Relation], base_table: str) -> str:
  lines = []
  for rel in relations:
    cols = ', '.join(f'col{j} AS {v}' for j, v in enumerate(rel.var_list))
    lines.append(f'CREATE VIEW {rel.name} AS SELECT {cols} FROM {base_table};')
  lines.append('')
  return '\n'.join(lines)


def group_key(group: list[Relation]) -> tuple:
  names = sorted(r.name for r in group)
  return (-len(group), ''.join(names))


def heap_push(heap: list, seen: set, group: list[Relation]) -> None:
  key = group_key(group)
  if key not in seen:
    seen.add(key)
    heapq.heappush(heap, (key, group))


def ask_for_plan(idx, group: list[Relation], heap: list, seen: set) -> None:
  if len(group) <= 1:
    return

  rel_map = {r.name: r for r in group}
  summary = ', '.join(f'{r.name}({", ".join(r.var_list)})' for r in group)
  print(f'-- [{idx}] group: {summary}', file=sys.stderr)

  print(f'-- [{idx}] enter split (e.g. "R1 R2 R3"): ', end='', file=sys.stderr)
  raw = input().strip()
  parts = raw.split()
  start_rel  = rel_map[parts[0]]
  split_rels = [rel_map[p] for p in parts[1:]]

  print(f'-- [{idx}] start: {start_rel.name}  split: {" vs ".join(r.name for r in split_rels)}', file=sys.stderr)

  for s in split_rels:
    merged = merge([start_rel, s])
    remaining = [r for r in group if r is not start_rel and r is not s]

    changed = True
    while changed:
      changed = False
      still_out = []
      for r in remaining:
        if set(r.var_list).issubset(set(merged.var_list)):
          merged = merge([merged, r])
          changed = True
        else:
          still_out.append(r)
      remaining = still_out

    branch = sorted([merged] + remaining, key=lambda r: r.name)
    branch_summary = ', '.join(f'{r.name}({", ".join(r.var_list)})' for r in branch)
    print(f'-- [{idx}] {start_rel.name} join w/ {s.name}: {branch_summary}', file=sys.stderr)
    
    if len(branch) > 2:
      heap_push(heap, seen, branch)


def main():
  first_line = input()
  relations = Parse(first_line)

  print(GenViews(relations, base_table='R'), flush=True)
  seen: set = set()
  heap: list = []
  heap_push(heap, seen, relations)
  idx = 0
  while heap:
    _, group = heapq.heappop(heap)
    ask_for_plan(idx, group, heap, seen)
    idx += 1

if __name__ == "__main__":
  main()