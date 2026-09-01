import random


def binary_gen(relations, rng: random.Random) -> str:
  """Generate a randomized binary join tree with explicit join order."""
  ordered = rng.sample(relations, len(relations))
  aliases = {rel.name: f't{i}' for i, rel in enumerate(relations, 1)}

  def build(group):
    if len(group) == 1:
      rel = group[0]
      return f'{rel.name} {aliases[rel.name]}', group
    cut = rng.randint(1, len(group) - 1)
    lhs, left = build(group[:cut])
    rhs, right = build(group[cut:])
    conditions = []
    for lrel in left:
      for rrel in right:
        for var in sorted(set(lrel.var_list) & set(rrel.var_list)):
          conditions.append(
            f'{aliases[lrel.name]}.{var} = {aliases[rrel.name]}.{var}')
    if conditions:
      return f'({lhs} JOIN {rhs} ON {" AND ".join(conditions)})', left + right
    return f'({lhs} CROSS JOIN {rhs})', left + right

  from_part, _ = build(ordered)
  return 'SELECT COUNT(*) FROM ' + from_part + ';'