#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Wrong number of arguments to the import shell script."
    exit 1
fi

csv_filename=$1
db_name=$2

mysql -ubenhong  <<QUERY_INPUT
USE vizio;
SET foreign_key_checks=0;
SET unique_checks=0;
SET sql_log_bin=0;
SET autocommit=0;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
LOAD DATA LOCAL INFILE '$csv_filename' INTO TABLE $db_name
FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n';
COMMIT;
SET sql_log_bin=1;
SET unique_checks=1;
SET foreign_key_checks=1;
SET autocommit=1;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
QUERY_INPUT
