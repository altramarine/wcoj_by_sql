import sys
import duckdb
import re
from dataclasses import dataclass, field
import math 

@dataclass
class Atom:
  table: str                    # relation name, e.g. "R"
  var_list: list[str]              # ordered vars by position, e.g. R(x,y,x) -> ["x","y","x"]
  variables: dict[str, list[int]]  # var -> positions it appears at, e.g. R(x,y,x) -> {"x":[0,2],"y":[1]}

def Name_of_column(table: str, idx: int):
  return f"{table}.col{idx}"

@dataclass
class CQ:
  head_vars: set[str]         # projected/output variables
  atoms: list[Atom]            # body atoms
  atom_vars: list[str]        # sorted, deduplicated list of all variables in body
  def print(self) -> str:
    atom_strs = [f"{at.table}({', '.join(at.var_list)})" for at in self.atoms]
    return f"q({', '.join(self.head_vars)}) :- {', '.join(atom_strs)}"
  
@dataclass
class CQ_to_SQL_Holder:
  cq: CQ
  query_name: str
  queries: list[str] = field(default_factory=list)
  def previous_CQ(self, prev):
    # self.queries.append(f"{prev.query_name} AS WITH")
    for q in prev.queries:
      self.queries.append(f"{q}") 
    # self.queries.append(f"SELECT * FROM {prev.query_name}")
  
  def pack(self, prev):
    self.queries.append(f"CREATE TEMP TABLE __result__ AS WITH")
    for q in prev.queries:
      self.queries.append(f"{q}")
    self.queries.append(f"SELECT * FROM {prev.query_name};")
    # self._drop_unused_tables()

  def _drop_unused_tables(self):
    # find all created table names and their index
    created = []
    for i, q in enumerate(self.queries):
      m = re.match(r'CREATE TEMP TABLE\s+(\S+)\s+AS', q, re.IGNORECASE)
      if m:
        created.append((m.group(1), i))

    last_created_name = created[-1][0] if created else None

    # for each table, find the last query index that references it
    drops = {}  # index -> list of table names to drop after
    for name, create_idx in created:
      if name == last_created_name:
        continue
      last_use = -1
      for i, q in enumerate(self.queries):
        if i == create_idx:
          continue
        if re.search(re.escape(name) + r'[, ]', q):
          last_use = i
      if last_use == -1:
        print(f"[dep WARNING] table '{name}' is never used after creation", file=sys.stderr)
      else:
        drops.setdefault(last_use, []).append(name)

    # insert DROP TABLE statements after the last use, in reverse order to not shift indices
    for idx in sorted(drops.keys(), reverse=True):
      for name in drops[idx]:
        self.queries.insert(idx + 1, f"DROP TABLE {name};")
  

  def append_query(self, strs):
    # strs = "CREATE TEMP TABLE " + strs
    # for i in strs.split('\n'):
    self.queries.append(strs)
    self.queries[-1] = self.queries[-1] + ','


def parse_atom(s: str) -> Atom:
  # e.g. "R(x,y)" -> Atom(table="R", variables=["x","y"])
  m = re.match(r'(\w+)\(([^)]*)\)', s.strip())
  if not m:
    raise ValueError(f"Invalid atom: {s!r}")
  table = m.group(1)
  var_list = [v.strip() for v in m.group(2).split(',')]
  variables: dict[str, list[int]] = {}
  for pos, var in enumerate(var_list):
    if var != '_':
      variables.setdefault(var, []).append(pos)
  return Atom(table=table, var_list=var_list, variables=variables)

def parse_CQ(s: str) -> CQ:
  # e.g. "Q(x,z) :- R(x,y), S(y,z)"
  head_str, body_str = s.split(':-')
  head_atom = parse_atom(head_str)
  # split body on ',' but only at top level (outside parentheses)
  atoms = []
  depth, start = 0, 0
  for i, c in enumerate(body_str):
    if c == '(': depth += 1
    elif c == ')': depth -= 1
    elif c == ',' and depth == 0:
      atoms.append(parse_atom(body_str[start:i]))
      start = i + 1
  atoms.append(parse_atom(body_str[start:]))
  head_vars: set[str] = set(head_atom.var_list)
  atom_vars: list[str] = sorted({var for atom in atoms for var in atom.var_list if var != '_'})
  return CQ(head_vars=head_vars, atoms=atoms, atom_vars=atom_vars)

def remove_var(cq: CQ, var: str) -> CQ:
  new_atoms = []
  for at in cq.atoms:
    new_var_list = ['_' if v == var else v for v in at.var_list]
    new_variables: dict[str, list[int]] = {}
    for pos, v in enumerate(new_var_list):
      new_variables.setdefault(v, []).append(pos)
    new_atoms.append(Atom(table=at.table, var_list=new_var_list, variables=new_variables))
  new_atom_vars = sorted({v for v in cq.atom_vars if v != var})
  return CQ(head_vars=cq.head_vars, atoms=new_atoms, atom_vars=new_atom_vars)


def Count(atom: Atom, cur_var: str, active_vars: str) -> str:
  var_mappings = {}
  equi = []
  var_map = []
  group_by = []
  for (i, var) in enumerate(atom.var_list):
    # print(i, var)
    if(var in active_vars or var == cur_var):
      if var in var_mappings:
        equi.append(f"{Name_of_column(atom.table, i)} = {Name_of_column(atom.table, var_mappings[var])}")
      else:
        var_mappings[var] = i
        if(var == cur_var):
          var_map.append((var, f"COUNT(DISTINCT {Name_of_column(atom.table, i)}) as cnt_{var}"))
        else:
          var_map.append((var, f"{Name_of_column(atom.table, i)} as {var}"))
          group_by.append(f"{Name_of_column(atom.table, i)}")
  var_map_sorted = [col for (_, col) in sorted(var_map, key=lambda t: t[0])]
  
  if cur_var in var_mappings:
    s = f"""SELECT {', '.join(var_map_sorted)} FROM {atom.table} {'WHERE (' if len(equi) else ''} {' AND '.join(equi)} {')' if len(equi) else ''} {'GROUP BY ' if len(group_by) else ''} {', '.join(group_by)} """
  else:
    print(f"Count() ERROR !!!!! [{cur_var} not in var_map for atom = {atom.table}] CHECK IMPLEMENTATION")
  return s


# best_table does not have cur_var
def Prop(atom: Atom, cur_var: str, best_table: str, extra_where_args: list[str], cq_vars: list[str]) -> str:
  var_mappings = {}
  equi = []
  var_map = []
  for var in cq_vars: 
    if var != cur_var:
      var_map.append(f"{best_table}.{var} as {var}")

  for (i, var) in enumerate(atom.var_list):
    # print(i, var)
    if(var in cq_vars):
      if(var == cur_var):
        if var in var_mappings:
          equi.append(f"{Name_of_column(atom.table, i)} = {Name_of_column(atom.table, var_mappings[var])}")
        else:
          var_mappings[var] = i
          var_map.append(f"{Name_of_column(atom.table, i)} as {var}")
      else:
        # var_map.append((var, f"{best_table}.{var} as {var}"))
        equi.append(f"{Name_of_column(atom.table, i)} = {best_table}.{var}")
  equi.extend(extra_where_args)

  

  var_map.sort(key=lambda x: x.split(' as ')[-1])
  if cur_var in var_mappings:
    s = f"""SELECT {', '.join(var_map)} FROM {atom.table}, {best_table} {'WHERE (' if len(equi) else ''} {' AND '.join(equi)} {')' if len(equi) else ''} """
  else:
    print(f"Prop() ERROR !!!!! [{cur_var} not in var_map for atom = {atom.table}] CHECK IMPLEMENTATION")
  return s

def Get_Query_j(atoms: list[Atom], prop_table: str, newcq_vars: list[str]) -> str:
  # equi = []
  var_map = []
  for var in newcq_vars: var_map.append(f"{prop_table}.{var} as {var}")
  
  s = f"""SELECT {', '.join(var_map)} FROM {prop_table} """
  
  for atom in atoms:
    equi = []
    for (i, var) in enumerate(atom.var_list):
      if(var in newcq_vars):
        equi.append(f"{Name_of_column(atom.table, i)} = {prop_table}.{var}")
    if len(equi):
      s = s + f""" SEMI JOIN {atom.table} ON {' AND '.join(equi)}"""
  # s = f"""SELECT {', '.join(var_map)} FROM {", ".join(atom.table for atom in atoms)}, {prop_table} {'WHERE (' if len(equi) else ''} {' AND '.join(equi)} {')' if len(equi) else ''} GROUP BY {", ".join(newcq_vars)}"""
  s = s + f""" GROUP BY {", ".join(newcq_vars)}"""

  return s

def Get_Query_1(atoms: list[Atom], cur_var: str) -> str:
  equi = []
  var_map = []

  var_mappings = {}
  s = """"""
  for atom in atoms:
    for (i, var) in enumerate(atom.var_list):
      if(var == cur_var):
        if var in var_mappings:
          equi.append(f"{Name_of_column(atom.table, i)} = {var_mappings[var]}")
          s = s + f" SEMI JOIN {atom.table} ON {Name_of_column(atom.table, i)} = {var_mappings[var]}"
        else:
          s = f"SELECT {Name_of_column(atom.table, i)} as {var} FROM {atom.table}" + s
          var_mappings[var] = Name_of_column(atom.table, i)
          var_map.append(f"{Name_of_column(atom.table, i)} as {var}")
  # s = f"""SELECT {', '.join(var_map)} FROM {", ".join(atom.table for atom in atoms)} {'WHERE (' if len(equi) else ''} {' AND '.join(equi)} {')' if len(equi) else ''} GROUP BY {cur_var}"""
  s = s + f""" GROUP BY {cur_var}"""

  return s

def CQ_to_SQL(cq_: CQ): 
  variable_list = sorted(cq_.atom_vars)

  print(variable_list)

  initial = variable_list[0];

  def print_binary(x: int) -> str:
    s = ""
    # print(x, len(cq_.atom_vars))
    for i in range(0, len(cq_.atom_vars)):
      s = s + ("1" if ((x >> i) & 1) else "0")
    # print(s)
    return s

  holder = CQ_to_SQL_Holder(cq = cq_, query_name = f"query_{print_binary((1 << len(cq_.atom_vars)) - 1)}_", queries = []);

  s = Get_Query_1(atoms = [atom for atom in cq_.atoms if initial in atom.var_list], cur_var = initial)
  s = f"query_{print_binary(1)}_ AS ({s})"
  print(s, file=sys.stderr)
  holder.append_query(s)

  var_id = {}
  for i, var in enumerate(cq_.atom_vars, 0):
    var_id[var] = i
  table_id = {}
  for i, atom in enumerate(cq_.atoms, 0):
    table_id[atom.table] = i

  # print(variable_list)

  bitmap = 1

  n = len(variable_list)
  pi_tag = "posi_tag"
  var_tag = "var_tag"
  pc_tag = "cnt_tag"

  # TODO: make a good bitmap order

  bitmap_order = []
  for bitmap in range(1, (1 << n)):
    if bitmap & 1 == 1:
      bitmap_order.append(bitmap)
  bitmap_order.sort(key = lambda x : x.bit_count() )
  print(bitmap_order)
  for bitmap in bitmap_order:
    cq = cq_
    
    query_name = f"query_{print_binary(bitmap)}_"

    active_vars = [variable_list[i] for i in range(0, n) if (bitmap >> i & 1)]
    non_active_vars = [var for var in variable_list if var not in active_vars]

    last_count_name = ""
    last_best_name = ""

    # Get query for this coun
    if bitmap != 1:
      prop_name = f"_prop_{print_binary(bitmap)}"

      # s = f"{next_name} AS (\n"
      
      s = (f"{prop_name} AS (" + '\n')

      atom_list = []
      for atom in cq.atoms:
        flag = 0
        for var_this_round in atom.var_list:
          if var_this_round in active_vars and var_this_round != initial:
            if flag == 0:
              atom_list.append(atom)
              flag = 1
            best_name = f"best_{print_binary(bitmap ^ (1 << var_id[var_this_round]))}_"
            s = s + (
              Prop(atom, var_this_round, best_name, [f"{best_name}.{pi_tag} = {table_id[atom.table]} AND {best_name}.{var_tag} = {var_id[var_this_round]}"], active_vars)
            ) 
            s = s + "\n UNION ALL \n"
      s = s.removesuffix("\n UNION ALL \n")
      s = s + ')'
      print(s, file=sys.stderr)
      holder.append_query(s)

      s = (
        f"{query_name} AS ("
        + Get_Query_j(atom_list, prop_name, active_vars)
        + ")"
      )
      print(s, file=sys.stderr)
      holder.append_query(s)
    
    if bitmap == (1 << (n)) - 1:
      break
          
    for atom in cq.atoms:
      for var_this_round in atom.var_list:
        if var_this_round in non_active_vars:
          
          count_name = f"count_{print_binary(bitmap)}_{atom.table}_{var_this_round}"
          best_name = f"best_{print_binary(bitmap)}_{atom.table}_{var_this_round}"

          s = f"{count_name} AS (" + Count(atom, var_this_round, active_vars) + ")"
          print(s, file=sys.stderr)
          holder.append_query(s)

          if last_count_name == "":
            join_conds = " AND ".join(f"{query_name}.{var} = {count_name}.{var}" for var in atom.var_list if var in active_vars)
            select_args = (
              ", ".join(f"{query_name}.{var} as {var}" for var in active_vars)
              + f", {table_id[atom.table]} as {pi_tag}"
              + f", {var_id[var_this_round]} as {var_tag}"
              + f", {count_name}.{f"cnt_{var_this_round}"} as {pc_tag}"
            )
            s = (f"""{best_name} as (SELECT {select_args} FROM {query_name}, {count_name} {'WHERE' if join_conds != '' else ''} {join_conds} ) """)

            print(s, file=sys.stderr)
            holder.append_query(s)

          else:
            join_conds = " AND ".join(f"{last_best_name}.{var} = {count_name}.{var}" for var in atom.var_list if var in active_vars)
            select_args = (
              ", ".join(f"{last_best_name}.{var} as {var}" for var in active_vars)
              + f", CASE WHEN {last_best_name}.{pc_tag} < {count_name}.{f"cnt_{var_this_round}"} THEN {last_best_name}.{pi_tag} ELSE {table_id[atom.table]} END as {pi_tag}"
              + f", CASE WHEN {last_best_name}.{pc_tag} < {count_name}.{f"cnt_{var_this_round}"} THEN {last_best_name}.{var_tag} ELSE {var_id[var_this_round]} END as {var_tag}"
              + f", CASE WHEN {last_best_name}.{pc_tag} < {count_name}.{f"cnt_{var_this_round}"} THEN {last_best_name}.{pc_tag} ELSE {count_name}.{f"cnt_{var_this_round}"} END as {pc_tag}"
            )
            s = (f"""{best_name} AS ( SELECT {select_args} FROM {last_best_name}, {count_name} {'WHERE' if join_conds != '' else ''} {join_conds} ) """)

            print(s, file=sys.stderr)
            holder.append_query(s)
          last_count_name = count_name
          last_best_name = best_name

    # best_table_name = last_best_name
    join_conds = ''
    select_args = (
      ", ".join(f"{last_best_name}.{var} as {var}" for var in active_vars)
      + f", {last_best_name}.{pi_tag} as {pi_tag}"
      + f", {last_best_name}.{var_tag} as {var_tag}"
    )
    best_tag = f"best_{print_binary(bitmap)}_"

    s = (f"""{best_tag} AS (SELECT {select_args} FROM {last_best_name})""")
    print(s, file=sys.stderr)
    
    holder.append_query(s)

  return holder

def main():
  wcoj_dir = sys.argv[1] if len(sys.argv) > 1 else None
  default_dir = sys.argv[2] if len(sys.argv) > 1 else None
  con = duckdb.connect()
  # extensions
  #
  #
  s = input()
  print(s)
  CQ = parse_CQ(s)
  print( CQ.print() )
  
  names = {}
  
  select = []
  equ = []
  head = []
  for a in CQ.atoms:
    orig = a.table
    count = names.get(orig, 0)
    # if count > 0:
    names[orig] = count + 1
    a.table = f"{orig}_{count + 1}"
    select.append(f"{orig} as {orig}_{count + 1}")

  for i in CQ.atom_vars:
    vars_ = []
    found_one = False
    for a in CQ.atoms:
      for idx, j in enumerate(a.var_list):
        if j == i:
          if not found_one:
            head.append(f"{Name_of_column(a.table, idx)} as {i}")
            found_one = True
          vars_.append(Name_of_column(a.table, idx))
    for i in range(1, len(vars_)):
      equ.append(f"{vars_[i-1]} = {vars_[i]}")


  for i in range(50): print("#", end='')
  print("default sql:", end = '')
  for i in range(50): print("#", end='')
  print()
  
  out = open(default_dir, "w") if default_dir else sys.stdout
  try:
    print(f"CREATE TEMP TABLE __query__result__ AS SELECT {", ".join(head)} FROM {", ".join(select)} WHERE {" and ".join(equ)}", file = out)
  finally:
    if default_dir:
      out.close()

    
  c = CQ_to_SQL(CQ)

  x = CQ_to_SQL_Holder(CQ, "q")
  x.pack(c)


  for i in range(50): print("#", end='')
  print("wcoj sql:", end = '')
  for i in range(50): print("#", end='')
  print()


  out = open(wcoj_dir, "w") if wcoj_dir else sys.stdout
  try:
    for name, cnt in names.items():
      for j in range(1, cnt + 1):
        print(f"CREATE VIEW {name}_{j} AS SELECT * FROM {name};", file=out)

    for l in x.queries:
      print(l, file=out)
  finally:
    if wcoj_dir:
      out.close()
  return
      

if __name__ == "__main__":
  main()
