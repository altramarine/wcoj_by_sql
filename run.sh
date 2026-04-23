
trap 'echo "Interrupted, killing all child processes..."; kill 0; exit 1' INT TERM

uv sync

mkdir -p log
mkdir -p tmp

for dataset in gplus; do

mkdir -p log/${dataset}
mkdir -p results/${dataset}

for qfile in 1-var.sql; do

result_file="./results/${dataset}/${qfile}.result.txt"

uv run python optm.py tmp/wcoj.sql tmp/default.sql < queries/${qfile}

uv run python run_sql.py tmp/wcoj.sql -d ${dataset} > log/${dataset}/wcoj_${qfile}.txt
grep "Execution time:" log/${dataset}/wcoj_${qfile}.txt | sed 's/Execution time:/wcoj Execution time:/' >> ${result_file}

uv run python run_sql.py tmp/default.sql -d ${dataset} > log/${dataset}/default_${qfile}.txt
grep "Execution time:" log/${dataset}/default_${qfile}.txt | sed 's/Execution time:/default Execution time:/' >> ${result_file}

done
done
