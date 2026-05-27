import sys
import re
from dataclasses import dataclass, field

@dataclass(order=False)
class Relation:
  name: str
  var_list: list[str]

  def __lt__(self, other: 'Relation') -> bool: return self.name < other.name
  def __le__(self, other: 'Relation') -> bool: return self.name <= other.name
  def __gt__(self, other: 'Relation') -> bool: return self.name > other.name
  def __ge__(self, other: 'Relation') -> bool: return self.name >= other.name
  def __eq__(self, other: object) -> bool:
    return isinstance(other, Relation) and self.name == other.name


def Parse(s: str) -> list[Relation]:
  relations = []
  for token in re.findall(r'\w+\s*\([^)]*\)', s):
    m = re.match(r'(\w+)\s*\(([^)]*)\)', token)
    name = m.group(1)
    var_list = [v.strip() for v in m.group(2).split(',')]
    relations.append(Relation(name=name, var_list=var_list))

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

  relations.sort()
  summary = ', '.join(f'{r.name}({", ".join(r.var_list)})' for r in relations)
  print(f'-- relations: {summary}', file=sys.stderr)
  return relations


def table_name(rels: list[Relation]) -> str:
  parts = []
  for r in rels:
    parts.extend(r.name.split('__'))
  return '__'.join(sorted(set(parts)))


def merge(rels: list[Relation]) -> Relation:
  rels = sorted(rels)
  name = table_name(rels)
  seen: set[str] = set()
  var_list: list[str] = []
  for rel in rels:
    for v in rel.var_list:
      if v not in seen:
        var_list.append(v)
        seen.add(v)
  return Relation(name=name, var_list=var_list)


def GenViews(relations: list[Relation], base_table: str) -> str:
  lines = []
  for rel in relations:
    cols = ', '.join(f'col{j} AS {v}' for j, v in enumerate(rel.var_list))
    lines.append(f'CREATE VIEW {rel.name} AS SELECT {cols} FROM {base_table};')
  lines.append('')
  return '\n'.join(lines)


@dataclass
class TreeNode:
  id: int
  group: list[Relation]
  origin: dict[str, int] = field(default_factory=dict)  # relation.name -> node.id that produced it
  split_start: Relation | None = None
  split_candidates: list[Relation] = field(default_factory=list)
  children: list['TreeNode'] = field(default_factory=list)
  child_merged: list[Relation] = field(default_factory=list)
  acyclic: bool = False


_next_id = 0
collect: list[str] = []

def ask_for_plan(group: list[Relation], origin: dict[str, int] | None = None) -> TreeNode:
  global _next_id
  if origin is None:
    origin = {}
  node = TreeNode(id=_next_id, group=group, origin=dict(origin))
  _next_id += 1

  if len(group) <= 1 or gyo(group):
    node.acyclic = True
    return node

  rel_map = {r.name: r for r in group}
  summary = ', '.join(f'{r.name}({", ".join(r.var_list)})' for r in group)
  print(f'-- [{node.id}] group: {summary}', file=sys.stderr)
  print(f'-- [{node.id}] enter split (e.g. "R1 R2 R3"): ', end='', file=sys.stderr)

  raw = input().strip()
  parts = raw.split()
  start_rel  = rel_map[parts[0]]
  split_rels = [rel_map[p] for p in parts[1:]]

  node.split_start      = start_rel
  node.split_candidates = split_rels

  print(f'-- [{node.id}] start: {start_rel.name}  split: {" vs ".join(r.name for r in split_rels)}', file=sys.stderr)

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

    branch = sorted([merged] + remaining)
    branch_summary = ', '.join(f'{r.name}({", ".join(r.var_list)})' for r in branch)
    print(f'-- [{node.id}] {start_rel.name} join w/ {s.name}: {branch_summary}', file=sys.stderr)

    child_origin = dict(origin)
    child_origin[merged.name] = _next_id  # merged will be owned by the child node

    child = ask_for_plan(branch, child_origin)
    node.children.append(child)
    node.child_merged.append(merged)

  return node


def print_tree(node: TreeNode, indent: int = 0) -> None:
  prefix = '-- ' + '| ' * indent
  if node.split_start is None:
    return
  splits = ', '.join(r.name for r in node.split_candidates)
  print(f'{prefix}{node.split_start.name} [{splits}]')
  for child in node.children:
    group_str = ', '.join(f'{r.name}({", ".join(r.var_list)})' for r in child.group)
    tag = ' [acyclic]' if child.acyclic else ''
    print(f'{prefix}[{child.id}] {group_str}{tag}')
    print_tree(child, indent + 1)


def gyo(group: list[Relation]) -> bool:
  """Return True if the group is acyclic (GYO reduction succeeds).
  R is an ear if its shared vars (vars appearing in other relations) are all
  covered by some single other relation S."""
  rels = list(group)
  changed = True
  while changed and len(rels) > 1:
    changed = False
    ears = []
    for r in rels:
      others = [o for o in rels if o is not r]
      all_other_vars = set(v for o in others for v in o.var_list)
      shared = set(r.var_list) & all_other_vars
      if not shared or any(shared.issubset(set(o.var_list)) for o in others):
        ears.append(r)
    if ears:
      rels = [r for r in rels if r is not ears[0]]
      changed = True
  return len(rels) <= 1

# def semi_join(a: Relation, b: Relation)->str:
#   s = f"SEMI JOIN {b.name} ON"
#   equi = []
#   for var in b.var_lists:
#     if var in a.var_list:
#       equi.append(f"{a.name}.{var} = {b.name}.{var}")
#   return (s + " AND ".join(equi)) if len(equi) != 0 else ""

def rel_table(node: TreeNode, rel: Relation) -> str:
  if '__' not in rel.name:
    return rel.name
  origin_id = node.origin[rel.name]
  return f"node{origin_id}_{rel.name}"

def sql_gen(node: TreeNode) -> str:
  node_name = f"node{node.id}_"
  R: Relation = node.split_start
  if R is None:
    return ""
  cmds = []
  # if '__' not in R.name:
  #   cols = ', '.join(f"{R.name}.{var} as {var}" for var in R.var_list)
  #   s = f"SELECT {cols} FROM {rel_table(node, R)}"
  #   for U in node.group:
  #     if U != R:
  #       s = s + ' ' + semi_join(R, U)

  #   cmds.append(f'CREATE TEMP TABLE {node_name}{R.name} AS' + s + ';')

  b = f"CREATE TEMP TABLE {node_name}best AS WITH"
  last_best = ""
  for idx, U in enumerate(node.split_candidates):
    s = f"SELECT {','.join(var for var in U.var_list if var in R.var_list)}, COUNT(*) as cnt FROM {rel_table(node, U)} GROUP BY {','.join(var for var in U.var_list if var in R.var_list)}"
    s = f"cnt_{U.name} as (" + s + "),"
    b = b + '\n  ' + s
    if last_best == "":
      equi = ' AND '.join(f"cnt_{U.name}.{var} = {rel_table(node, R)}.{var}" for var in U.var_list if var in R.var_list)
      s = (
        f"SELECT {", ".join(f"{rel_table(node, R)}.{var} as {var}"for var in R.var_list)}, {idx} as tag, cnt_{U.name}.cnt as cnt"
        + f" FROM {rel_table(node, R)}, cnt_{U.name}"
        + f" WHERE {equi}" if equi != "" else ""
      )
    else:
      equi = ' AND '.join(f"cnt_{U.name}.{var} = {last_best}.{var}" for var in U.var_list if var in R.var_list)
      s = (
        f"SELECT {", ".join(f"{last_best}.{var} as {var}"for var in R.var_list)}, "
        + f"CASE WHEN {last_best}.cnt < cnt_{U.name}.cnt THEN {last_best}.tag ELSE {idx} END as tag,"
        + f"CASE WHEN {last_best}.cnt < cnt_{U.name}.cnt THEN {last_best}.cnt ELSE cnt_{U.name}.cnt END as cnt"
        + f" FROM {last_best}, cnt_{U.name}"
        + f" WHERE {equi}" if equi != "" else ""
      )
    s = f"best_{U.name} as (" + s + "),"
    last_best = f"best_{U.name}"
    b = b + '\n  ' + s
  b = b + "\n" + f"SELECT {', '.join(R.var_list)}, tag FROM {last_best};"
  cmds.append(b)
  for idx, child in enumerate(node.children):
    child_name = f"node{child.id}_"
    U = node.split_candidates[idx]
    select = []
    varmap = {}
    join_cond = " AND ".join(f"{node_name}best.{var} = {rel_table(node, U)}.{var}" for var in R.var_list if var in U.var_list)
    for var in node.child_merged[idx].var_list:
      if var in R.var_list:
        select.append(f"{node_name}best.{var} as {var}")
        varmap[var] = f"{node_name}best.{var}"
      else:
        select.append(f"{rel_table(node, U)}.{var} as {var}")
        varmap[var] = f"{rel_table(node, U)}.{var}"
    s = (
      f"SELECT {", ".join(select)} FROM"
      +f" {node_name}best JOIN {rel_table(node, U)} {f"ON {join_cond}" if join_cond != '' else ''}"
    )
    for V in node.split_candidates:
      if V != R and V != U:
        if V.name in node.child_merged[idx].name:
          s = s + f" SEMI JOIN {rel_table(node, V)} ON {" AND ".join(f"{rel_table(node, V)}.{var} = {varmap[var]}" for var in V.var_list)}"
    s = s + f" WHERE {node_name}best.tag = {idx}"
    if child.acyclic:
      merged_alias = rel_table(child, node.child_merged[idx])
      others = [r for r in child.group if r != node.child_merged[idx]]
      from_part = f"({s}) {merged_alias}"
      outer_select = {var: f"{merged_alias}.{var}" for var in node.child_merged[idx].var_list}
      for V in others:
        join_vars = [var for var in V.var_list if var in outer_select]
        cond = " AND ".join(f"{rel_table(child, V)}.{var} = {outer_select[var]}" for var in join_vars)
        from_part = from_part + f" JOIN {rel_table(child, V)}" + (f" ON {cond}" if cond else "")
        for var in V.var_list:
          if var not in outer_select:
            outer_select[var] = f"{rel_table(child, V)}.{var}"
      s = f"SELECT {', '.join(f'{expr} as {var}' for var, expr in sorted(outer_select.items()))} FROM {from_part}"
      collect.append(s)
    else:
      s = f"CREATE TEMP TABLE {rel_table(child, node.child_merged[idx])} AS " + s + ';'
      cmds.append(s)

  return '\n'.join(cmds)

def main():
  first_line = input()
  relations = Parse(first_line)
  print(GenViews(relations, base_table='R'), flush=True)

  root = ask_for_plan(relations)

  print('-- tree:')
  group_str = ', '.join(f'{r.name}({", ".join(r.var_list)})' for r in root.group)
  print(f'-- {group_str}')
  print_tree(root)

  from collections import deque
  queue: deque[TreeNode] = deque([root])
  while queue:
    node = queue.popleft()
    if not node.acyclic:
      sql = sql_gen(node)
      if sql:
        print(sql)
    for child in node.children:
      queue.append(child)

  if collect:
    print('SELECT COUNT(*) FROM (\n' + '\nUNION ALL\n'.join(collect) + '\n);')



if __name__ == "__main__":
  main()
