Problems with directly writing: 

1. We should use semi join whenever it is a projection, otherwise it won't be detected.
2. use WITH into multiple events would led to a plan breaking responses? (verify that) 
   1. two times slower for q(x, y, z) :- R(x, y), R(y, z), R(x, z)
3. The Union All part is slow!