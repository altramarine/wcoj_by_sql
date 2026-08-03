numactl --cpunodebind=0 --membind=0 uv run python run_sql.py queries/q4_plans/plan1.sql -d topcats
numactl --cpunodebind=0 --membind=0 uv run python run_sql.py queries/q4_plans/plan2.sql -d topcats

numactl --cpunodebind=0 --membind=0 uv run python run_sql.py queries/wcoj/1.sql -d topcats

uv run python run_sql.py queries/wcoj/1.sql -d uspatent
uv run python run_sql.py queries/normal/1.sql -d uspatent

numactl --cpunodebind=0 --membind=0 uv run python run_sql.py queries/norm_print/1.sql -d topcats


uv run python lp_statistics.py \
  --dataset topcats \
  --query queries/1.sql \
  --lp queries/lp_boundaries/1.sql.txt \
  --max-p 5

uv run python lp_statistics.py \
  --dataset topcats \
  --query queries/3cycle.sql \
  --lp queries/lp_boundaries/3cycle.sql.txt \
  --max-p 5

uv run python lp_statistics.py \
  --dataset topcats \
  --query queries/tmp.sql \
  --lp queries/lp_boundaries/tmp.sql.txt \
  --max-p 5


  
uv run python lp_statistics.py \
  --dataset topcats \
  --query queries/2.sql \
  --lp queries/lp_boundaries/2.sql.txt \
  --max-p 5