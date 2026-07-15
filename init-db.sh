#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE order_db;
    CREATE DATABASE payment_db;
    CREATE DATABASE inventory_db;
    CREATE DATABASE shipping_db;
EOSQL
