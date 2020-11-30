#!/bin/bash

docker exec -it some-sql sh -c 'exec mysqldump -uroot -p"$MYSQL_ROOT_PASSWORD" --databases nifi'
