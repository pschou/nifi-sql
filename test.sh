docker pull mysql
echo starting docker container
#cont=$( docker run --rm -d -p 3306:3306 --name some-mysql -e MYSQL_ROOT_PASSWORD=my-secret-pw -d mysql:latest --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci )
echo running main.go
DATABASE_USERNAME=root DATABASE_PASSWORD=my-secret-pw DATABASE=nifi go run main.go

#docker stop $cont


# create table
#echo 'CREATE TABLE users ( name VARCHAR(30), age INT(6) );' | docker exec -i some-mysql sh -c 'exec mysql -uroot -p"$MYSQL_ROOT_PASSWORD"'

# run mysql interactively
#docker exec -it some-mysql sh -c 'exec mysql -uroot -p"$MYSQL_ROOT_PASSWORD"'
#CREATE TABLE MyGuests (
#id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
#firstname VARCHAR(30) NOT NULL,
#lastname VARCHAR(30) NOT NULL,
#email VARCHAR(50),
#reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
#)

#  CREATE TABLE users ( name VARCHAR(30), age INT(6) )
