import sys
import duckdb
import re
from dataclasses import dataclass, field

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


def Count(atom: Atom, cur_var: str) -> str:
  var_mappings = {}
  equi = []
  var_map = []
  group_by = []
  for (i, var) in enumerate(atom.var_list):
    # print(i, var)
    if(var == '_'):
      continue
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
def Prop(atom: Atom, cur_var: str, best_table: str, extra_where_args: list[str], cq_vars: list[str], atoms: list[Atom]) -> str:
  var_mappings = {}
  equi = []
  var_map = []

  var_mappings2 = {}
  for var in cq_vars: 
    var_map.append(f"{best_table}.{var} as {var}")
    var_mappings2[var] = f"{best_table}.{var}"
  for (i, var) in enumerate(atom.var_list):
    # print(i, var)
    if(var == '_'):
      continue
    
    if(var == cur_var):
      if var in var_mappings:
        equi.append(f"{Name_of_column(atom.table, i)} = {Name_of_column(atom.table, var_mappings[var])}")
      else:
        var_mappings[var] = i
        var_map.append(f"{Name_of_column(atom.table, i)} as {var}")
        var_mappings2[var] = Name_of_column(atom.table, i)
    else:
      # var_map.append((var, f"{best_table}.{var} as {var}"))
      equi.append(f"{Name_of_column(atom.table, i)} = {best_table}.{var}")
  equi.extend(extra_where_args)

  if cur_var in var_mappings:
    s = f"""SELECT {', '.join(var_map)} FROM {best_table} JOIN {atom.table} {'ON (' if len(equi) else ''} {' AND '.join(equi)} {')' if len(equi) else ''} """
  else:
    print(f"Prop() ERROR !!!!! [{cur_var} not in var_map for atom = {atom.table}] CHECK IMPLEMENTATION")
    return 
  atom_ = atom
  for atom in atoms:
    if atom != atom_:
      equi = []
      for (i, var) in enumerate(atom.var_list):
        if(var != "_"):
          equi.append(f"{Name_of_column(atom.table, i)} = {var_mappings2[var]}")
      if len(equi):
        s = s + f""" SEMI JOIN {atom.table} ON {' AND '.join(equi)}"""
  return s

def Get_Query_j(atoms: list[Atom], cur_var: str, prop_table: str, newcq_vars: list[str]) -> str:
  # equi = []
  var_map = []
  for var in newcq_vars: var_map.append(f"{prop_table}.{var} as {var}")
  
  s = f"""SELECT {', '.join(var_map)} FROM {prop_table} """
  
  # for atom in atoms:
  #   equi = []
  #   for (i, var) in enumerate(atom.var_list):
  #     if(var != "_"):
  #       equi.append(f"{Name_of_column(atom.table, i)} = {prop_table}.{var}")
  #   if len(equi):
  #     s = s + f""" SEMI JOIN {atom.table} ON {' AND '.join(equi)}"""
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
          s = s + f"SELECT {Name_of_column(atom.table, i)} as {var} FROM {atom.table}"
          var_mappings[var] = Name_of_column(atom.table, i)
          var_map.append(f"{Name_of_column(atom.table, i)} as {var}")
  # s = f"""SELECT {', '.join(var_map)} FROM {", ".join(atom.table for atom in atoms)} {'WHERE (' if len(equi) else ''} {' AND '.join(equi)} {')' if len(equi) else ''} GROUP BY {cur_var}"""
  s = s + f""" GROUP BY {cur_var}"""

  return s

def CQ_to_SQL(cq: CQ) -> CQ_to_SQL_Holder:

  # print(cq.print())
  if len(cq.atom_vars) == 1:
    # print(cq.print())
    # join them
    var_this_round = max(cq.atom_vars)
    
    
    print(f"var this round = {var_this_round}", file=sys.stderr)

    query_name = "query_0"
    new_holder = CQ_to_SQL_Holder(cq, query_name)

    s = (
      f"{query_name} AS ("
      + Get_Query_1([atom for atom in cq.atoms if var_this_round in atom.var_list], var_this_round)
      + ")"
    )

    print(s, file=sys.stderr)
    new_holder.append_query(s)

    return new_holder
  else:
    var_this_round = max(cq.atom_vars)
    cq_remaining = remove_var(cq, var_this_round)
    holder_ = CQ_to_SQL(cq_remaining)
    
    next_name = "query_" + str(int(holder_.query_name.split("_")[1]) + 1)
    new_holder = CQ_to_SQL_Holder(cq, next_name)

    new_holder.previous_CQ(holder_)
    # past_res = holder_.result_atom_;

    last_atom = ""

    atom_list = []
    
    pi_tag = "posi_tag"
    pc_tag = "cnt_tag"

    print(f"var this round = {var_this_round}", file=sys.stderr)

    best_tag = f"{new_holder.query_name}_best_"

    for atom in cq.atoms:
      if var_this_round in atom.var_list:
        print(atom.var_list, file=sys.stderr)
        """count(atom_j to i)"""

        count_name = f"{new_holder.query_name}_count_{atom.table}"

        
        """
        generate count
        """

        s = f"{count_name} as (" + Count(atom, var_this_round) + ")"
        print(s, file=sys.stderr)

        new_holder.append_query(s)
        
        """
        generate best
        """

        atom_list.append(atom)

        if (last_atom == ""):
          
          join_conds = " AND ".join(f"{holder_.query_name}.{var} = {count_name}.{var}" for var in atom.var_list if var != '_' and var != var_this_round)
          select_args = (
            ", ".join(f"{holder_.query_name}.{var} as {var}" for var in holder_.cq.atom_vars)
            + f", 1 as {pi_tag}"
            + f", {count_name}.{f"cnt_{var_this_round}"} as {pc_tag}"
          )


          s = (f"""{best_tag}{atom.table} as (SELECT {select_args} FROM {holder_.query_name}, {count_name} {'WHERE' if join_conds != '' else ''} {join_conds} ) """)

          print(s, file=sys.stderr)
          new_holder.append_query(s)

          last_atom  = atom.table
          # print(join_conds)
        else:
          join_conds = " AND ".join(f"{best_tag}{last_atom}.{var} = {count_name}.{var}" for var in atom.var_list if var != '_' and var != var_this_round)
          select_args = (
            ", ".join(f"{best_tag}{last_atom}.{var} as {var}" for var in holder_.cq.atom_vars)
            + f", CASE WHEN {best_tag}{last_atom}.{pc_tag} < {count_name}.{f"cnt_{var_this_round}"} THEN {best_tag}{last_atom}.{pi_tag} ELSE {len(atom_list)} END as {pi_tag}"
            + f", CASE WHEN {best_tag}{last_atom}.{pc_tag} < {count_name}.{f"cnt_{var_this_round}"} THEN {best_tag}{last_atom}.{pc_tag} ELSE {count_name}.{f"cnt_{var_this_round}"} END as {pc_tag}"
          )
          s = (f"""{best_tag}{atom.table} as ( SELECT {select_args} FROM {best_tag}{last_atom}, {count_name} {'WHERE' if join_conds != '' else ''} {join_conds} ) """)

          print(s, file=sys.stderr)
          new_holder.append_query(s)

          last_atom = atom.table

    
    best_table_name = f"{best_tag}{last_atom}"
    
    

    join_conds = ''
    select_args = (
      ", ".join(f"{best_table_name}.{var} as {var}" for var in holder_.cq.atom_vars)
      + f", {best_table_name}.{pi_tag} as {pi_tag}"
    )

    s = (f"""{best_tag} as (SELECT {select_args} FROM {best_tag}{last_atom} {'WHERE' if join_conds != '' else ''} {join_conds} )""")

    print(s, file=sys.stderr)
    new_holder.append_query(s)

    """
    GENERATE prop
    """

    prop_tag = f"{new_holder.query_name}_prop_"

    # s = f"{next_name} AS (\n"
    
    s = (f"{next_name} AS (" + '\n')

    i = 1
    for atom in cq.atoms:
      if var_this_round in atom.var_list:
        s = s + (
          Prop(atom, var_this_round, best_tag, [f"{best_tag}.{pi_tag} = {i}"], holder_.cq.atom_vars, atom_list)
        ) 
        if(i < len(atom_list)):
          s = s + "\n UNION ALL \n"
        
        i = i + 1
    s = s + ')'
    print(s, file=sys.stderr)
    new_holder.append_query(s)


    # print(Best(atom, var_this_round))x
    """
    get query_j
    """
    
    
    # s = (
    #   f"{next_name} AS ("
    #   + Get_Query_j(atom_list, var_this_round, prop_tag, cq.atom_vars)
    #   + ")"
    # )
    # print(s, file=sys.stderr)
    # new_holder.append_query(s)

    return new_holder

    
  # input a conjunctive query and output an SQL which implements WCOJ.


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
    print(f"CREATE TEMP TABLE __query__result__ AS SELECT {", ".join(head)} FROM {", ".join(select)} WHERE {" and ".join(equ)};", file = out)
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
