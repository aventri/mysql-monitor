FROM mysql:5.7

COPY docker/db/replication.cnf /etc/mysql/conf.d/
COPY docker/db/test.sql /docker-entrypoint-initdb.d/

