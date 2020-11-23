# nifi-sql
A simple endpoint for nifi data which stores the data into one or more local SQL servers.


# Example usage

```
docker pull mysql
echo starting docker container
cont=$( docker run --rm -d -p 3306:3306 --name some-mysql -e MYSQL_ROOT_PASSWORD=my-secret-pw -d mysql:latest --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci )
echo running main.go
DATABASE_USERNAME=root DATABASE_PASSWORD=my-secret-pw DATABASE=nifi go run main.go
# Leave the terminal at this so queries can be ran
docker stop $cont
```

create test files for upload
```
echo '{"kind":"users","name": "mary","age":23}' > a
echo '{"kind":"users","name": "paul","age":15}' > b
```

create target table
```
echo 'USE nifi; CREATE TABLE users ( name VARCHAR(30), age INT(6), timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP );' | docker exec -i some-mysql sh -c 'exec mysql -uroot -p"$MYSQL_ROOT_PASSWORD"'
```

do a nifi RestPOST to the endpoint
```
curl -d "@a" -d "@b" -X POST http://localhost:8080
```

create target table
```
echo 'USE nifi; SELECT * from nifi;' | docker exec -i some-mysql sh -c 'exec mysql -uroot -p"$MYSQL_ROOT_PASSWORD"'
```

run mysql commands interactively
```
docker exec -it some-mysql sh -c 'exec mysql -uroot -p"$MYSQL_ROOT_PASSWORD"'
```
