#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Wrong number of arguments to the import shell script."
    exit 1
fi

csv_filename=$1

mysql -ubenhong  <<QUERY_INPUT
USE vizio;
CREATE TABLE temp_to_update_activity LIKE vizio_activity_dim;
ALTER TABLE temp_to_update_activity DROP COLUMN household_id;
SET foreign_key_checks=0;
SET unique_checks=0;
SET sql_log_bin=0;
SET autocommit=0;
LOAD DATA LOCAL INFILE '$csv_filename' INTO TABLE temp_to_update_activity
FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n';
COMMIT;
UPDATE vizio_activity_dim AS Original
INNER JOIN temp_to_update_activity AS New USING(id)
SET Original.last_active_date = New.last_active_date;
COMMIT;
DROP TABLE temp_to_update_activity;
COMMIT;
SET sql_log_bin=1;
SET unique_checks=1;
SET foreign_key_checks=1;
SET autocommit=1;
QUERY_INPUT
