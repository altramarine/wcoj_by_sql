#!/bin/bash
echo "col0,col1" > /home/jerom/wcoj_by_sql/as-skitter.csv && zcat /home/jerom/wcoj_by_sql/as-skitter.txt.gz | grep -v '^#' | tr '\t' ',' >> /home/jerom/wcoj_by_sql/as-skitter.csv