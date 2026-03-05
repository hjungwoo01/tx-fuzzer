-- Create MySQL user and database for litmus tests.
-- From root:  mysql -u root -p < scripts/setup_mysql_litmus.sql
-- From root: sudo mysql < scripts/setup_mysql_litmus.sql
-- Run: python scripts/run_litmus.py --target mysql --test lost_update --dsn 'mysql://test:test@localhost:3306/test'

CREATE DATABASE IF NOT EXISTS test;
CREATE USER IF NOT EXISTS 'test'@'localhost' IDENTIFIED BY 'test';
GRANT ALL PRIVILEGES ON test.* TO 'test'@'localhost';
FLUSH PRIVILEGES;
