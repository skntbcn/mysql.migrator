#!/bin/bash
echo "Creating things..."
mysql -uuser -ppassword -e "SET GLOBAL local_infile = 'ON'" 2>&1 >/dev/null
mysql -uroot -ptoor -e "CREATE DATABASE employees;" 2>&1 >/dev/null
mysql -uroot -ptoor -e "CREATE DATABASE menagerie;" 2>&1 >/dev/null
####
cd /app/test_db/
echo "Creating test-db..."
mysql -uuser -ppassword < /app/test_db/objects.sql 2>&1 >/dev/null
mysql -uuser -ppassword < /app/test_db/employees.sql 2>&1 >/dev/null
###
echo "Creating world-db..."
cd /app/world-db/
mysql -uuser -ppassword < /app/world-db/world.sql 2>&1 >/dev/null
###
echo "Creating menagerie-db..."
cd /app/menagerie-db/
mysql -uuser -ppassword menagerie < /app/menagerie-db/cr_pet_tbl.sql 2>&1 >/dev/null
mysql -uuser -ppassword --local-infile=1 menagerie -e "LOAD DATA LOCAL INFILE '/app/menagerie-db/pet.txt' INTO TABLE pet;" 2>&1 >/dev/null
mysql -uuser -ppassword menagerie < /app/menagerie-db/ins_puff_rec.sql 2>&1 >/dev/null
mysql -uuser -ppassword menagerie < /app/menagerie-db/cr_event_tbl.sql 2>&1 >/dev/null
mysql -uuser -ppassword --local-infile=1 menagerie -e "LOAD DATA LOCAL INFILE '/app/menagerie-db/event.txt' INTO TABLE event;" 2>&1 >/dev/null
####
echo "Creating sakila-db..."
cd /app/sakila-db/
mysql -uuser -ppassword < /app/sakila-db/sakila-schema.sql 2>&1 >/dev/null
mysql -uuser -ppassword < /app/sakila-db/sakila-data.sql 2>&1 >/dev/null
###
mkdir /root/.oci/ -p
touch /root/.oci/config
####
cd /app/airportdb/
echo "Creating airport-db..."
mysqlsh user@localhost -ppassword -e 'util.loadDump("/app/airport-db", {threads: 16, deferTableIndexes: "all", ignoreVersion: true})' 2>&1 >/dev/null



