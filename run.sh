
trap 'echo "Interrupted, killing all child processes..."; kill 0; exit 1' INT TERM

uv sync

mkdir -p tmp log-duckdb results-duckdb log-umbra results-umbra

# require explicit backend: umbra or duckdb
if [ -z "$1" ]; then
  echo "Usage: $0 <umbra|duckdb>"
  exit 1
fi
BACKEND="$1"

if [ "$BACKEND" = "umbra" ] || [ "$BACKEND" = "--umbra" ]; then
  LOG_DIR="log-umbra"
  RESULT_DIR="results-umbra"
else
  LOG_DIR="log-duckdb"
  RESULT_DIR="results-duckdb"
fi

# for dataset in topcats skitters uspatent; do
for dataset in topcats; do

mkdir -p ${LOG_DIR}/${dataset}
mkdir -p ${RESULT_DIR}/${dataset}

# for qfile in 2.sql 3.sql 4cycle.sql; do
for qfile in 5cycle.sql; do

result_file="./${RESULT_DIR}/${dataset}/${qfile}.result.txt"

uv run python optm.py tmp/wcoj.sql tmp/default.sql < queries/${qfile}

if [ "$BACKEND" = "umbra" ]; then

uv run python run_umbra.py tmp/wcoj.sql -d ${dataset} > ${LOG_DIR}/${dataset}/wcoj_${qfile}.txt
grep "Execution time:" ${LOG_DIR}/${dataset}/wcoj_${qfile}.txt | sed 's/Execution time:/wcoj Execution time:/' >> ${result_file}

uv run python run_umbra.py tmp/default.sql -d ${dataset} --wcoj > ${LOG_DIR}/${dataset}/default_${qfile}.txt
grep "Execution time:" ${LOG_DIR}/${dataset}/default_${qfile}.txt | sed 's/Execution time:/default Execution time:/' >> ${result_file}

else

# uv run python run_sql.py tmp/wcoj.sql -d ${dataset}

# uv run python run_sql.py tmp/default.sql -d ${dataset}


# uv run python run_sql.py tmp/wcoj.sql -d ${dataset} > ${LOG_DIR}/${dataset}/wcoj_${qfile}.txt
uv run python run_sql.py queries/wcoj/${qfile} -d ${dataset} > ${LOG_DIR}/${dataset}/wcoj_${qfile}.txt
grep "Execution time:" ${LOG_DIR}/${dataset}/wcoj_${qfile}.txt | sed 's/Execution time:/wcoj Execution time:/' >> ${result_file}

uv run python run_sql.py tmp/default.sql -d ${dataset} > ${LOG_DIR}/${dataset}/default_${qfile}.txt
grep "Execution time:" ${LOG_DIR}/${dataset}/default_${qfile}.txt | sed 's/Execution time:/default Execution time:/' >> ${result_file}

fi

done
done
