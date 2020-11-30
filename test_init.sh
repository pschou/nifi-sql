#docker pull mysql
echo starting mysql
msq_cont=$( docker run --rm -d -p 3306:3306 --name some-sql -e MYSQL_ROOT_PASSWORD=my-secret-pw -d mysql:latest --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci )


#docker pull docker.elastic.co/elasticsearch/elasticsearch:7.10.0
echo starting elasticsearch
es_cont=$( docker run --rm -d -p 9200:9200 --name some-es -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.10.0 )

