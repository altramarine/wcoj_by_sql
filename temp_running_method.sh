numactl --cpunodebind=0 --membind=0 uv run python run_sql.py queries/q4_plans/plan1.sql -d topcats
numactl --cpunodebind=0 --membind=0 uv run python run_sql.py queries/q4_plans/plan2.sql -d topcats

numactl --cpunodebind=0 --membind=0 uv run python run_sql.py queries/wcoj/1.sql -d topcats

numactl --cpunodebind=0 --membind=0 uv run python run_sql.py queries/norm_print/1.sql -d topcats