
uv sync

mkdir -p log
mkdir -p tmp

for qfile in 1-var.sql; do

result_file="./results/${qfile}.result.txt"

# uv run python variation.py tmp/wcoj_var.sql tmp/default.sql < queries/${qfile}
# uv run python main.py tmp/wcoj.sql tmp/default.sql < queries/${qfile}

uv run python run_sql.py tmp/wcoj.sql > log/wcoj.txt
grep "Execution time:" log/wcoj.txt | sed 's/Execution time:/wcoj Execution time:/' >> ${result_file}

uv run python run_sql.py tmp/default.sql > log/default.txt
grep "Execution time:" log/default.txt | sed 's/Execution time:/default Execution time:/' >> ${result_file}

# uv run python run_sql.py tmp/wcoj_var.sql > log/wcoj_var.txt
# grep "Execution time:" log/wcoj_var.txt | sed 's/Execution time:/variation Execution time:/' >> ${result_file}

done